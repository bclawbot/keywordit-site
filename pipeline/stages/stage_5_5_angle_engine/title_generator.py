"""
title_generator.py — H1 title generation for each angle type.

Two modes:
  1. LLM-powered (default when source_trend is available) — generates
     trend-grounded titles via qwen3:14b on Ollama.
  2. Template fallback — deterministic string templates used when
     source_trend is missing or LLM call fails.

Native-language templates for: es, fr, de, pt, it, nl, pl, ja, ko, zh, ar, vi, th, el, sv, tr.
Script-based language detection for keywords where language_code is "en"/"?"
but the keyword contains non-Latin characters (Japanese, Thai, Greek, etc.).
Title length capped at 150 characters (truncate with ellipsis).

Sources: spec Section 3.1 confirmed title formulas from 22 live RSOC articles.
"""
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

# Ensure workspace root is importable (for llm_client)
_WORKSPACE = Path(__file__).resolve().parents[3]
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))

from llm_client import call as _llm_call  # noqa: E402

CURRENT_YEAR = datetime.now().year

ANGLE_TYPE_NAMES = {
    "eligibility_explainer":      "Eligibility Explainer",
    "how_it_works_explainer":     "How-It-Works Explainer",
    "trend_attention_piece":      "Trend / Attention Piece",
    "lifestyle_fit_analysis":     "Lifestyle Fit Analysis",
    "pre_review_guide":           "Pre-Review Guide",
    "policy_year_stamped_update": "Policy / Year Update",
    "accusatory_expose":          "Consumer Awareness Expose",
    "hidden_costs":               "Hidden Costs Analysis",
    "diagnostic_signs":           "Diagnostic Signs Article",
    "comparison":                 "Neutral Comparison",
}

_RAF_TITLE_BANNED = [
    "guaranteed", "you will receive", "apply now", "free quote",
    "best price", "call now", "limited time", "act now",
    "sign up", "you qualify", "you are eligible",
]

# Whole-word banned tokens. Matched with \b boundaries so "bestow" etc. pass.
# Substring entries in _RAF_TITLE_BANNED continue to match as substrings.
_RAF_TITLE_BANNED_WORDS = [
    "best",
]
_RAF_TITLE_BANNED_WORD_RX = re.compile(
    r"\b(?:" + "|".join(re.escape(w) for w in _RAF_TITLE_BANNED_WORDS) + r")\b",
    re.IGNORECASE,
)

_LANG_NAMES = {
    "es": "Spanish", "fr": "French", "de": "German", "pt": "Portuguese",
    "it": "Italian", "nl": "Dutch", "pl": "Polish", "ja": "Japanese",
    "ko": "Korean", "zh": "Chinese", "ar": "Arabic", "vi": "Vietnamese",
    "th": "Thai", "el": "Greek", "sv": "Swedish", "tr": "Turkish",
}


def _cap(title: str, max_len: int = 150) -> str:
    """Cap title to max_len characters, truncating with ellipsis if needed."""
    if len(title) <= max_len:
        return title
    return title[: max_len - 3].rstrip() + "..."


def _title_case_kw(keyword: str) -> str:
    """Title-case a keyword, preserving common acronyms."""
    acronyms = {"ssi", "ssdi", "va", "eeoc", "gi", "tv", "dna", "dfs", "cpc"}
    words = keyword.split()
    result = []
    for w in words:
        if w.lower() in acronyms:
            result.append(w.upper())
        elif len(w) > 3:
            result.append(w.capitalize())
        else:
            result.append(w)
    return " ".join(result)


def _detect_lang_from_script(keyword: str) -> str | None:
    """
    Detect language from Unicode script analysis of keyword characters.
    Returns a language code if a non-Latin script is dominant, else None.
    Only used as a fallback when language_code is "en", "?", or empty
    but the keyword clearly belongs to another language.
    """
    counts = {
        "ja_kana": 0,  # Hiragana/Katakana → definitely Japanese
        "cjk": 0,      # CJK ideographs → Japanese by default (most in pipeline)
        "ko": 0,       # Hangul
        "ar": 0,       # Arabic
        "th": 0,       # Thai
        "el": 0,       # Greek
        "he": 0,       # Hebrew
        "cy": 0,       # Cyrillic
    }
    total_nonspace = 0
    for ch in keyword:
        if ch.isspace():
            continue
        total_nonspace += 1
        try:
            name = unicodedata.name(ch, "")
        except Exception:
            continue
        if "HIRAGANA" in name or "KATAKANA" in name:
            counts["ja_kana"] += 1
        elif "CJK" in name or "IDEOGRAPH" in name or "KANGXI" in name:
            counts["cjk"] += 1
        elif "HANGUL" in name:
            counts["ko"] += 1
        elif "ARABIC" in name:
            counts["ar"] += 1
        elif "THAI" in name:
            counts["th"] += 1
        elif "GREEK" in name:
            counts["el"] += 1
        elif "HEBREW" in name:
            counts["he"] += 1
        elif "CYRILLIC" in name:
            counts["cy"] += 1

    if total_nonspace == 0:
        return None

    threshold = total_nonspace * 0.25  # 25% of chars in a script → that language

    if counts["ja_kana"] > 0:
        return "ja"  # Any kana = definitely Japanese
    if counts["cjk"] >= threshold:
        return "ja"  # Default CJK to Japanese (most common CJK in pipeline)
    if counts["ko"] >= threshold:
        return "ko"
    if counts["ar"] >= threshold:
        return "ar"
    if counts["th"] >= threshold:
        return "th"
    if counts["el"] >= threshold:
        return "el"
    if counts["he"] >= threshold:
        return "he"
    if counts["cy"] >= threshold:
        return "uk"  # Cyrillic → Ukrainian (primary Cyrillic country in pipeline)
    return None


def generate_title(
    keyword: str,
    angle_type: str,
    language_code: str = "en",
    country: str = "US",
    year: int = None,
    audience: str = "",
    option_a: str = "",
    option_b: str = "",
) -> str:
    """
    Generate an SEO-optimised H1 title for the given angle type.
    Template-based — consistent and fast.

    Native-language templates are used for all supported languages.
    Script-based detection auto-corrects language_code when "en"/"?" is
    supplied but the keyword contains non-Latin characters.
    """
    year = year or CURRENT_YEAR
    lang = (language_code or "en").lower().strip()

    # Auto-detect from Unicode script when language_code is missing/wrong
    if lang in ("en", "?", "") and keyword:
        detected = _detect_lang_from_script(keyword)
        if detected:
            lang = detected

    kw   = _title_case_kw(keyword)
    kw_r = keyword  # raw (for mid-sentence usage)

    dispatch = {
        "es": _generate_title_es,
        "fr": _generate_title_fr,
        "de": _generate_title_de,
        "pt": _generate_title_pt,
        "it": _generate_title_it,
        "nl": _generate_title_nl,
        "pl": _generate_title_pl,
        "ja": _generate_title_ja,
        "ko": _generate_title_ko,
        "zh": _generate_title_zh,
        "ar": _generate_title_ar,
        "vi": _generate_title_vi,
        "th": _generate_title_th,
        "el": _generate_title_el,
        "sv": _generate_title_sv,
        "tr": _generate_title_tr,
    }

    if lang in dispatch:
        return dispatch[lang](keyword, angle_type, country, year, kw, kw_r,
                              audience, option_a)

    if lang == "en":
        titles = {
            "eligibility_explainer": (
                f"A Closer Look at {kw} and How Eligibility Works in {year}"
            ),
            "how_it_works_explainer": (
                f"Understanding How {kw} Works in {year}: A Complete Overview"
            ),
            "trend_attention_piece": (
                f"Why {kw} Continues to Gain Attention in {year}"
            ),
            "lifestyle_fit_analysis": (
                f"How {kw} Fits {audience or 'Your'} Lifestyle and Daily Priorities"
            ),
            "pre_review_guide": (
                f"What to Know Before Reviewing {kw} Options in {year}"
            ),
            "policy_year_stamped_update": (
                f"Reviewing {kw} Developments in {year}: What You Should Know"
            ),
            "accusatory_expose": (
                f"What the {kw} Industry Doesn't Always Tell You"
            ),
            "hidden_costs": (
                f"The Hidden Costs of {kw} and What to Watch For"
            ),
            "diagnostic_signs": (
                f"How to Know If Your Situation Qualifies for {kw} Assistance"
            ),
            "comparison": (
                f"How {option_a or kw + ' Options'} Compare in {year}: A Neutral Overview"
            ),
        }
        raw = titles.get(angle_type, f"{kw}: What You Need to Know in {year}")
        return _cap(raw)

    # Minimal neutral fallback for low-volume languages without templates
    # (no, da, fi, cs, ro, hu, id, uk, he) — avoids mixing languages
    raw = f"{kw_r} – {year}"
    return _cap(raw)


# ── Spanish ───────────────────────────────────────────────────────────────────

def _generate_title_es(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    """Spanish-adapted title templates per spec Section 6.2."""
    titles = {
        "eligibility_explainer": (
            f"Una Visión General Sobre {kw} y Cómo Funciona la Elegibilidad"
        ),
        "how_it_works_explainer": (
            f"Cómo Funciona {kw} y Qué Aspectos Considerar en {year}"
        ),
        "trend_attention_piece": (
            f"Por Qué {kw} Sigue Siendo Relevante en {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Cómo {kw} Se Adapta a las Necesidades de Su Situación"
        ),
        "pre_review_guide": (
            f"Lo Que Conviene Saber Antes de Explorar Opciones de {kw}"
        ),
        "policy_year_stamped_update": (
            f"Aspectos Clave Sobre {kw} en {year}: Información Actualizada"
        ),
        "accusatory_expose": (
            f"Lo Que No Siempre Se Informa Sobre {kw}"
        ),
        "hidden_costs": (
            f"Los Costos Ocultos de {kw} y Cómo Protegerse"
        ),
        "diagnostic_signs": (
            f"Señales de Que Su Situación Podría Relacionarse con {kw}"
        ),
        "comparison": (
            f"Cómo Comparar las Opciones de {kw} en {year}"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Información Esencial para {year}")
    return _cap(raw)


# ── French ────────────────────────────────────────────────────────────────────

def _generate_title_fr(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Ce Qu'il Faut Savoir sur {kw} et les Critères d'Éligibilité en {year}"
        ),
        "how_it_works_explainer": (
            f"Comment Fonctionne {kw} en {year} : Tour d'Horizon Complet"
        ),
        "trend_attention_piece": (
            f"Pourquoi {kw} Continue d'Attirer l'Attention en {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Comment {kw} S'Adapte à Votre Style de Vie et Vos Priorités"
        ),
        "pre_review_guide": (
            f"Ce Qu'il Faut Savoir Avant d'Explorer les Options {kw} en {year}"
        ),
        "policy_year_stamped_update": (
            f"Développements de {kw} en {year} : Ce Qu'il Faut Savoir"
        ),
        "accusatory_expose": (
            f"Ce Que l'Industrie {kw} Ne Dit Pas Toujours"
        ),
        "hidden_costs": (
            f"Les Coûts Cachés de {kw} et Comment S'en Protéger"
        ),
        "diagnostic_signs": (
            f"Comment Savoir Si Votre Situation Correspond à {kw}"
        ),
        "comparison": (
            f"Comparer les Options {kw} en {year} : Une Vue d'Ensemble Neutre"
        ),
    }
    raw = titles.get(angle_type, f"{kw} : L'Essentiel à Savoir en {year}")
    return _cap(raw)


# ── German ────────────────────────────────────────────────────────────────────

def _generate_title_de(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"{kw}: Zulassungskriterien und Was Sie {year} Wissen Sollten"
        ),
        "how_it_works_explainer": (
            f"Wie {kw} Funktioniert in {year}: Eine Vollständige Übersicht"
        ),
        "trend_attention_piece": (
            f"Warum {kw} {year} Weiterhin Aufmerksamkeit Erregt"
        ),
        "lifestyle_fit_analysis": (
            f"Wie {kw} zu Ihrem Alltag und Ihren Prioritäten Passt"
        ),
        "pre_review_guide": (
            f"Was vor der Prüfung von {kw}-Optionen zu Beachten Ist"
        ),
        "policy_year_stamped_update": (
            f"Entwicklungen bei {kw} in {year}: Was Sie Wissen Sollten"
        ),
        "accusatory_expose": (
            f"Was die {kw}-Branche Nicht Immer Sagt"
        ),
        "hidden_costs": (
            f"Die Versteckten Kosten von {kw} und Worauf Sie Achten Sollten"
        ),
        "diagnostic_signs": (
            f"Wie Sie Feststellen, Ob Ihre Situation {kw} Betrifft"
        ),
        "comparison": (
            f"{kw}-Optionen im Vergleich {year}: Eine Neutrale Übersicht"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Das Wichtigste für {year}")
    return _cap(raw)


# ── Portuguese ────────────────────────────────────────────────────────────────

def _generate_title_pt(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Entendendo {kw} e Como Funciona a Elegibilidade em {year}"
        ),
        "how_it_works_explainer": (
            f"Como Funciona {kw} em {year}: Uma Visão Geral Completa"
        ),
        "trend_attention_piece": (
            f"Por Que {kw} Continua a Atrair Atenção em {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Como {kw} Se Adapta ao Seu Estilo de Vida e Prioridades"
        ),
        "pre_review_guide": (
            f"O Que Saber Antes de Analisar as Opções de {kw} em {year}"
        ),
        "policy_year_stamped_update": (
            f"Desenvolvimentos em {kw} em {year}: O Que Você Deve Saber"
        ),
        "accusatory_expose": (
            f"O Que o Setor de {kw} Nem Sempre Revela"
        ),
        "hidden_costs": (
            f"Os Custos Ocultos de {kw} e Como Se Proteger"
        ),
        "diagnostic_signs": (
            f"Como Saber Se Sua Situação Se Enquadra em {kw}"
        ),
        "comparison": (
            f"Comparando as Opções de {kw} em {year}: Uma Visão Neutra"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: O Essencial para {year}")
    return _cap(raw)


# ── Italian ───────────────────────────────────────────────────────────────────

def _generate_title_it(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Tutto su {kw} e Come Funziona l'Ammissibilità nel {year}"
        ),
        "how_it_works_explainer": (
            f"Come Funziona {kw} nel {year}: Una Panoramica Completa"
        ),
        "trend_attention_piece": (
            f"Perché {kw} Continua ad Attirare Attenzione nel {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Come {kw} Si Adatta al Tuo Stile di Vita e alle Tue Priorità"
        ),
        "pre_review_guide": (
            f"Cosa Sapere Prima di Esaminare le Opzioni {kw} nel {year}"
        ),
        "policy_year_stamped_update": (
            f"Aggiornamenti su {kw} nel {year}: Quello Che Devi Sapere"
        ),
        "accusatory_expose": (
            f"Cosa Non Dice Sempre il Settore {kw}"
        ),
        "hidden_costs": (
            f"I Costi Nascosti di {kw} e Come Proteggersi"
        ),
        "diagnostic_signs": (
            f"Come Capire Se la Tua Situazione Riguarda {kw}"
        ),
        "comparison": (
            f"Confronto tra le Opzioni {kw} nel {year}: Una Panoramica Neutrale"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: L'Essenziale per il {year}")
    return _cap(raw)


# ── Dutch ─────────────────────────────────────────────────────────────────────

def _generate_title_nl(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Wat U Moet Weten over {kw} en de Geschiktheidsvereisten in {year}"
        ),
        "how_it_works_explainer": (
            f"Hoe {kw} Werkt in {year}: Een Volledig Overzicht"
        ),
        "trend_attention_piece": (
            f"Waarom {kw} Blijft Aandacht Trekken in {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Hoe {kw} Past bij Uw Levensstijl en Dagelijkse Prioriteiten"
        ),
        "pre_review_guide": (
            f"Wat U Moet Weten Voordat U {kw}-Opties Bekijkt in {year}"
        ),
        "policy_year_stamped_update": (
            f"Ontwikkelingen bij {kw} in {year}: Wat U Moet Weten"
        ),
        "accusatory_expose": (
            f"Wat de {kw}-Industrie Niet Altijd Vertelt"
        ),
        "hidden_costs": (
            f"De Verborgen Kosten van {kw} en Waar U op Moet Letten"
        ),
        "diagnostic_signs": (
            f"Hoe U Weet of Uw Situatie Betrekking Heeft op {kw}"
        ),
        "comparison": (
            f"{kw}-Opties Vergelijken in {year}: Een Neutraal Overzicht"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Wat U Moet Weten in {year}")
    return _cap(raw)


# ── Polish ────────────────────────────────────────────────────────────────────

def _generate_title_pl(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Co Warto Wiedzieć o {kw} i Zasadach Kwalifikowalności w {year}"
        ),
        "how_it_works_explainer": (
            f"Jak Działa {kw} w {year}: Pełny Przegląd"
        ),
        "trend_attention_piece": (
            f"Dlaczego {kw} Nadal Przyciąga Uwagę w {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Jak {kw} Pasuje do Twojego Stylu Życia i Codziennych Priorytetów"
        ),
        "pre_review_guide": (
            f"Co Warto Wiedzieć Przed Zapoznaniem się z Opcjami {kw} w {year}"
        ),
        "policy_year_stamped_update": (
            f"Zmiany w {kw} w {year}: Co Powinieneś Wiedzieć"
        ),
        "accusatory_expose": (
            f"Czego Branża {kw} Nie Zawsze Ujawnia"
        ),
        "hidden_costs": (
            f"Ukryte Koszty {kw} i Na Co Uważać"
        ),
        "diagnostic_signs": (
            f"Jak Sprawdzić, Czy Twoja Sytuacja Dotyczy {kw}"
        ),
        "comparison": (
            f"Porównanie Opcji {kw} w {year}: Neutralny Przegląd"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Co Musisz Wiedzieć w {year}")
    return _cap(raw)


# ── Japanese ──────────────────────────────────────────────────────────────────

def _generate_title_ja(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    # Use raw keyword — Japanese does not use title-case
    kw = kw_r
    titles = {
        "eligibility_explainer": f"{kw}の対象要件について：{year}年ガイド",
        "how_it_works_explainer": f"{kw}の仕組みと{year}年の概要",
        "trend_attention_piece":  f"{year}年に{kw}が注目される理由",
        "lifestyle_fit_analysis": f"{kw}があなたの生活スタイルに合うかどうか",
        "pre_review_guide":       f"{kw}を検討する前に知っておくべきこと",
        "policy_year_stamped_update": f"{year}年の{kw}の動向と最新情報",
        "accusatory_expose":      f"{kw}が常に伝えるとは限らないこと",
        "hidden_costs":           f"{kw}の見えないコストと注意点",
        "diagnostic_signs":       f"あなたの状況が{kw}に関連するか確認する方法",
        "comparison":             f"{year}年の{kw}オプション比較ガイド",
    }
    raw = titles.get(angle_type, f"{kw}：{year}年版ガイド")
    return _cap(raw)


# ── Korean ────────────────────────────────────────────────────────────────────

def _generate_title_ko(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    kw = kw_r
    titles = {
        "eligibility_explainer": f"{kw} 자격 요건 심층 분석 – {year}년 가이드",
        "how_it_works_explainer": f"{kw}의 작동 방식과 {year}년 완전 개요",
        "trend_attention_piece":  f"{year}년에도 {kw}이 주목받는 이유",
        "lifestyle_fit_analysis": f"{kw}이 당신의 생활 방식에 맞는지 확인하기",
        "pre_review_guide":       f"{kw} 옵션을 검토하기 전에 알아야 할 것들",
        "policy_year_stamped_update": f"{year}년 {kw} 최신 동향과 핵심 정보",
        "accusatory_expose":      f"{kw} 업계가 항상 말하지 않는 것들",
        "hidden_costs":           f"{kw}의 숨겨진 비용과 주의사항",
        "diagnostic_signs":       f"내 상황이 {kw}에 해당하는지 확인하는 방법",
        "comparison":             f"{year}년 {kw} 옵션 중립 비교 가이드",
    }
    raw = titles.get(angle_type, f"{kw} – {year}년 가이드")
    return _cap(raw)


# ── Chinese ───────────────────────────────────────────────────────────────────

def _generate_title_zh(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    kw = kw_r
    titles = {
        "eligibility_explainer":  f"深入了解{kw}及{year}年资格要求",
        "how_it_works_explainer": f"{kw}如何运作：{year}年完整概述",
        "trend_attention_piece":  f"{year}年{kw}为何持续受到关注",
        "lifestyle_fit_analysis": f"{kw}如何融入您的生活方式与日常优先事项",
        "pre_review_guide":       f"探索{kw}选项前您需要了解的事项",
        "policy_year_stamped_update": f"{year}年{kw}最新动态：您应了解的要点",
        "accusatory_expose":      f"{kw}行业不总是告诉您的事实",
        "hidden_costs":           f"{kw}的隐性成本及防范措施",
        "diagnostic_signs":       f"如何判断您的情况是否与{kw}相关",
        "comparison":             f"{year}年{kw}选项中立比较指南",
    }
    raw = titles.get(angle_type, f"{kw}：{year}年完整指南")
    return _cap(raw)


# ── Arabic ────────────────────────────────────────────────────────────────────

def _generate_title_ar(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    kw = kw_r  # No title-case for Arabic
    titles = {
        "eligibility_explainer":  f"نظرة متعمقة على {kw} وشروط الأهلية في {year}",
        "how_it_works_explainer": f"كيف يعمل {kw} في {year}: نظرة عامة شاملة",
        "trend_attention_piece":  f"لماذا يستمر {kw} في استقطاب الاهتمام في {year}",
        "lifestyle_fit_analysis": f"كيف يتناسب {kw} مع أسلوب حياتك وأولوياتك",
        "pre_review_guide":       f"ما تحتاج معرفته قبل مراجعة خيارات {kw} في {year}",
        "policy_year_stamped_update": f"مستجدات {kw} في {year}: ما يجب أن تعرفه",
        "accusatory_expose":      f"ما لا تخبرك به صناعة {kw} دائماً",
        "hidden_costs":           f"التكاليف الخفية لـ{kw} وكيفية الحماية منها",
        "diagnostic_signs":       f"كيف تعرف إذا كان وضعك مؤهلاً للحصول على {kw}",
        "comparison":             f"مقارنة خيارات {kw} في {year}: نظرة عامة محايدة",
    }
    raw = titles.get(angle_type, f"{kw}: الدليل الأساسي لعام {year}")
    return _cap(raw)


# ── Vietnamese ────────────────────────────────────────────────────────────────

def _generate_title_vi(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Tìm Hiểu về {kw} và Điều Kiện Đủ Tiêu Chuẩn năm {year}"
        ),
        "how_it_works_explainer": (
            f"Cách {kw} Hoạt Động trong {year}: Tổng Quan Đầy Đủ"
        ),
        "trend_attention_piece": (
            f"Tại Sao {kw} Tiếp Tục Thu Hút Sự Chú Ý trong {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Cách {kw} Phù Hợp với Lối Sống và Ưu Tiên Hàng Ngày"
        ),
        "pre_review_guide": (
            f"Những Điều Cần Biết Trước Khi Xem Xét Tùy Chọn {kw} năm {year}"
        ),
        "policy_year_stamped_update": (
            f"Diễn Biến của {kw} năm {year}: Những Gì Bạn Nên Biết"
        ),
        "accusatory_expose": (
            f"Những Điều Ngành {kw} Không Phải Lúc Nào Cũng Nói"
        ),
        "hidden_costs": (
            f"Chi Phí Ẩn của {kw} và Những Điều Cần Lưu Ý"
        ),
        "diagnostic_signs": (
            f"Làm Thế Nào Biết Tình Huống của Bạn Liên Quan đến {kw}"
        ),
        "comparison": (
            f"So Sánh Các Tùy Chọn {kw} năm {year}: Tổng Quan Khách Quan"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Hướng Dẫn Cơ Bản năm {year}")
    return _cap(raw)


# ── Thai ──────────────────────────────────────────────────────────────────────

def _generate_title_th(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    kw = kw_r  # No title-case for Thai
    titles = {
        "eligibility_explainer":  f"ทำความเข้าใจ{kw}และเงื่อนไขการมีสิทธิ์ในปี {year}",
        "how_it_works_explainer": f"วิธีการทำงานของ{kw}ในปี {year}: ภาพรวมที่สมบูรณ์",
        "trend_attention_piece":  f"ทำไม{kw}จึงยังคงได้รับความสนใจในปี {year}",
        "lifestyle_fit_analysis": f"{kw}เหมาะกับไลฟ์สไตล์และความต้องการของคุณอย่างไร",
        "pre_review_guide":       f"สิ่งที่ควรรู้ก่อนพิจารณาตัวเลือก{kw}ในปี {year}",
        "policy_year_stamped_update": f"ความเคลื่อนไหวของ{kw}ในปี {year}: สิ่งที่คุณควรรู้",
        "accusatory_expose":      f"สิ่งที่อุตสาหกรรม{kw}ไม่ได้บอกเสมอไป",
        "hidden_costs":           f"ค่าใช้จ่ายที่ซ่อนอยู่ของ{kw}และสิ่งที่ต้องระวัง",
        "diagnostic_signs":       f"วิธีรู้ว่าสถานการณ์ของคุณเกี่ยวข้องกับ{kw}",
        "comparison":             f"เปรียบเทียบตัวเลือก{kw}ในปี {year}: ภาพรวมที่เป็นกลาง",
    }
    raw = titles.get(angle_type, f"{kw}: คู่มือฉบับสมบูรณ์ปี {year}")
    return _cap(raw)


# ── Greek ─────────────────────────────────────────────────────────────────────

def _generate_title_el(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Τι Πρέπει να Γνωρίζετε για το {kw} και τα Κριτήρια Επιλεξιμότητας το {year}"
        ),
        "how_it_works_explainer": (
            f"Πώς Λειτουργεί το {kw} το {year}: Μια Πλήρης Επισκόπηση"
        ),
        "trend_attention_piece": (
            f"Γιατί το {kw} Συνεχίζει να Προσελκύει Ενδιαφέρον το {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Πώς το {kw} Ταιριάζει με τον Τρόπο Ζωής σας"
        ),
        "pre_review_guide": (
            f"Τι να Γνωρίζετε Πριν Εξετάσετε τις Επιλογές {kw} το {year}"
        ),
        "policy_year_stamped_update": (
            f"Εξελίξεις του {kw} το {year}: Τι Πρέπει να Γνωρίζετε"
        ),
        "accusatory_expose": (
            f"Τι Δεν Αναφέρει Πάντα ο Κλάδος {kw}"
        ),
        "hidden_costs": (
            f"Τα Κρυφά Κόστη του {kw} και Πώς να Προστατευτείτε"
        ),
        "diagnostic_signs": (
            f"Πώς να Καταλάβετε αν η Κατάστασή σας Σχετίζεται με {kw}"
        ),
        "comparison": (
            f"Σύγκριση Επιλογών {kw} το {year}: Μια Αντικειμενική Επισκόπηση"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Ο Βασικός Οδηγός για το {year}")
    return _cap(raw)


# ── Swedish ───────────────────────────────────────────────────────────────────

def _generate_title_sv(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"Vad Du Bör Veta om {kw} och Behörighetskrav {year}"
        ),
        "how_it_works_explainer": (
            f"Hur {kw} Fungerar {year}: En Fullständig Översikt"
        ),
        "trend_attention_piece": (
            f"Varför {kw} Fortsätter att Uppmärksammas {year}"
        ),
        "lifestyle_fit_analysis": (
            f"Hur {kw} Passar in i Din Livsstil och Dagliga Prioriteringar"
        ),
        "pre_review_guide": (
            f"Vad Du Bör Veta Innan Du Utforskar {kw}-alternativ {year}"
        ),
        "policy_year_stamped_update": (
            f"Utvecklingen av {kw} {year}: Vad Du Bör Veta"
        ),
        "accusatory_expose": (
            f"Vad {kw}-Branschen Inte Alltid Berättar"
        ),
        "hidden_costs": (
            f"De Dolda Kostnaderna med {kw} och Vad Du Bör Bevaka"
        ),
        "diagnostic_signs": (
            f"Hur Du Vet Om Din Situation Rör {kw}"
        ),
        "comparison": (
            f"Jämföra {kw}-alternativ {year}: En Neutral Översikt"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: Det Du Behöver Veta {year}")
    return _cap(raw)


# ── Turkish ───────────────────────────────────────────────────────────────────

def _generate_title_tr(keyword, angle_type, country, year, kw, kw_r,
                       audience="", option_a="") -> str:
    titles = {
        "eligibility_explainer": (
            f"{kw} ve {year} Yılı Uygunluk Koşullarına Yakından Bakış"
        ),
        "how_it_works_explainer": (
            f"{kw} Nasıl Çalışır {year}: Kapsamlı Bir Genel Bakış"
        ),
        "trend_attention_piece": (
            f"{kw}'nın {year}'da Neden İlgi Çekmeye Devam Ettiği"
        ),
        "lifestyle_fit_analysis": (
            f"{kw}'nın Yaşam Tarzınıza ve Önceliklerinize Nasıl Uyduğu"
        ),
        "pre_review_guide": (
            f"{year}'da {kw} Seçeneklerini İncelemeden Önce Bilinmesi Gerekenler"
        ),
        "policy_year_stamped_update": (
            f"{year}'da {kw} Gelişmeleri: Bilmeniz Gerekenler"
        ),
        "accusatory_expose": (
            f"{kw} Sektörünün Her Zaman Söylemediği Şeyler"
        ),
        "hidden_costs": (
            f"{kw}'nın Gizli Maliyetleri ve Dikkat Edilmesi Gerekenler"
        ),
        "diagnostic_signs": (
            f"Durumunuzun {kw} ile İlgili Olup Olmadığını Nasıl Anlarsınız"
        ),
        "comparison": (
            f"{year}'da {kw} Seçeneklerinin Tarafsız Karşılaştırması"
        ),
    }
    raw = titles.get(angle_type, f"{kw}: {year} Yılı Temel Rehberi")
    return _cap(raw)


# ── H2 Header Suggestions ─────────────────────────────────────────────────────

def generate_h2_headers(angle_type: str, keyword: str, year: int = None,
                        audience: str = "", language_code: str = "en") -> list:
    """
    Generate suggested H2 headers for the article structure.
    Returns a list of 4-5 H2 strings.
    Used in article briefs — the LLM is free to adapt these.
    English patterns for all non-ES languages (LLM translates them).
    """
    year = year or CURRENT_YEAR
    lang = (language_code or "en").lower().strip()

    # Auto-detect script for non-Latin keywords
    if lang in ("en", "?", "") and keyword:
        detected = _detect_lang_from_script(keyword)
        if detected:
            lang = detected

    kw   = _title_case_kw(keyword)

    # For languages that have their own LLM instructions, leave H2s in English
    # so the LLM can contextualise them in the target language naturally.
    # (Only Spanish has explicit H2 translations since it's the highest volume.)
    patterns = {
        "eligibility_explainer": [
            f"What {kw} Eligibility Typically Requires",
            f"How Qualifying Factors Are Evaluated in {year}",
            f"Who Is Generally Considered Eligible for {kw}",
            f"Common Questions About {kw} Qualification",
            f"Next Steps for Those Exploring {kw} Options",
        ],
        "how_it_works_explainer": [
            f"How the {kw} Process Typically Unfolds",
            f"What Drives Variation in {kw} Outcomes",
            f"Key Components of the {kw} System",
            f"What People Often Get Wrong About {kw}",
            f"What to Expect When Navigating {kw}",
        ],
        "trend_attention_piece": [
            f"Why Interest in {kw} Has Grown in {year}",
            f"What People Are Saying About {kw}",
            f"Factors That Drive Ongoing Attention to {kw}",
            f"How {kw} Has Evolved Over Time",
            f"What Observers Note About {kw} Today",
        ],
        "lifestyle_fit_analysis": [
            f"What {audience or 'People'} Typically Look For in {kw}",
            f"How {kw} Aligns With Daily Priorities",
            f"What Benefits Matter Most in {kw} Situations",
            f"When {kw} Makes Practical Sense",
            f"How to Evaluate Whether {kw} Fits Your Needs",
        ],
        "pre_review_guide": [
            f"What to Review Before Committing to {kw}",
            f"Common Questions Worth Asking About {kw}",
            f"What People Often Overlook When Evaluating {kw}",
            f"Red Flags to Watch for With {kw} Options",
            f"How to Approach the {kw} Decision Process",
        ],
        "policy_year_stamped_update": [
            f"What Changed With {kw} in {year}",
            f"How Current {kw} Conditions Affect Your Situation",
            f"Key Updates to {kw} Worth Knowing",
            f"What Experts Are Observing About {year} {kw} Developments",
            f"How to Stay Informed About {kw} Changes",
        ],
        "accusatory_expose": [
            f"Information Not Always Volunteered in {kw} Discussions",
            f"How {kw} Decisions Typically Affect Outcomes",
            f"What the Details Say About {kw} Situations",
            f"Why Independent Research Matters With {kw}",
            f"Questions Worth Asking Before Proceeding With {kw}",
        ],
        "hidden_costs": [
            f"Costs That Aren't Always Mentioned Upfront in {kw} Situations",
            f"How {kw} Expenses Are Typically Calculated",
            f"Factors That Affect the Total Cost of {kw}",
            f"How to Protect Yourself from Unexpected {kw} Expenses",
            f"Planning Ahead for {kw} Financial Considerations",
        ],
        "diagnostic_signs": [
            f"Situations That Commonly Involve {kw}",
            f"What Typically Distinguishes a {kw} Situation",
            f"How Documentation Relates to {kw} Cases",
            f"When People Typically Seek Help with {kw}",
            f"How to Begin Evaluating a Potential {kw} Situation",
        ],
        "comparison": [
            f"Key Differences to Consider With {kw} Options",
            f"What Each {kw} Option Typically Offers",
            f"Cost and Eligibility Considerations for Each Option",
            f"How People Typically Decide Between {kw} Choices",
            f"What to Consider When Evaluating {kw} Options",
        ],
    }
    return patterns.get(angle_type, [
        f"Understanding {kw}",
        f"Key Considerations for {kw} in {year}",
        f"What You Need to Know About {kw}",
        f"How to Approach {kw}",
    ])


# ══════════════════════════════════════════════════════════════════════════════
# LLM-powered title generation (trend-grounded)
# ══════════════════════════════════════════════════════════════════════════════

def _call_llm_title(prompt: str, num_predict: int = 80) -> str:
    """Call LLM for short title generation via centralized llm_client.
    Returns stripped text with <think> tags removed."""
    content = _llm_call(
        [{"role": "system", "content": "/no_think"}, {"role": "user", "content": prompt}],
        num_predict=num_predict,
        temperature=0.7,
        timeout="normal",
        stage="title_generator",
    )
    # qwen3 may wrap output in <think>...</think> — strip it
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
    return content.strip()


def _validate_llm_title(raw: str) -> tuple:
    """
    Validate and clean an LLM-generated title.
    Returns (title, is_valid) tuple.
    """
    title = raw.strip().strip('"\'').strip()
    title = re.sub(r"^[\d]+[\.\)]\s*", "", title)   # "1. Title" → "Title"
    title = re.sub(r"^#+\s*", "", title)              # "# Title" → "Title"
    title = title.split("\n")[0].strip()               # first line only
    title = title.strip('"\'').strip()                 # strip again after line split

    if not title or len(title) < 10:
        return None, False

    title_lower = title.lower()
    for banned in _RAF_TITLE_BANNED:
        if banned in title_lower:
            return None, False

    if _RAF_TITLE_BANNED_WORD_RX.search(title):
        return None, False

    return _cap(title, 150), True


_SIGNAL_TONE_HINTS = {
    "news_event":          "This keyword is rising because of a breaking news event. Ground the title in what is newly happening.",
    "reddit_discussion":   "This keyword surfaced from a Reddit discussion. Frame the title around a real user question or concern.",
    "google_trends":       "This keyword is a Google Trends spike. Signal why search interest is rising right now.",
    "commercial_transform":"This keyword is a commercial follow-on to a trending event. Signal buyer intent without pushy language.",
    "commercial_intent":   "This keyword reflects purchase intent. Keep the title calm and decision-oriented.",
    "keyword_expansion":   "",
}

# One English example per signal type. Kept tight (~40 tokens each).
_FEW_SHOT_EN = {
    "news_event": (
        "Keyword: hurricane milton insurance claim\n"
        "Trend: Hurricane Milton makes landfall in Florida, widespread flooding reported\n"
        "Title: Flood Insurance Claims After Hurricane Milton: What Florida Homeowners Should Know in 2026"
    ),
    "reddit_discussion": (
        "Keyword: student loan forgiveness paye\n"
        "Trend: r/StudentLoans thread: confusion over PAYE switch after SAVE injunction\n"
        "Title: What the SAVE Injunction Means for Borrowers Considering PAYE in 2026"
    ),
    "commercial_transform": (
        "Keyword: wildfire smoke air purifier\n"
        "Trend: West Coast wildfire smoke blankets cities, air quality warnings issued\n"
        "Title: Air Purifiers for Wildfire Smoke: What to Check Before You Buy in 2026"
    ),
}


def _title_few_shot(lang: str, sig_type: str) -> str:
    """Return a single few-shot example for the given language and signal type,
    or empty string for unsupported combinations."""
    if lang != "en":
        return ""
    return _FEW_SHOT_EN.get(sig_type, "")


def generate_titles_batch(
    keyword: str,
    angle_types: list,
    language_code: str = "en",
    country: str = "US",
    year: int = None,
    source_trend: str = "",
    vertical: str = "general",
    audience: str = "",
    option_a: str = "",
    option_b: str = "",
    *,
    discovery_context: dict | None = None,
    _retry_depth: int = 0,
) -> dict:
    """
    Generate titles for multiple angle_types in one LLM call.
    Returns {angle_type: title} dict.

    When source_trend is available, uses LLM to produce trend-grounded titles.
    Falls back to template-based generate_title() per angle on any failure.
    """
    year = year or CURRENT_YEAR

    # No source_trend → skip LLM entirely, use templates
    if not source_trend or not source_trend.strip():
        print(f"[title_gen] SKIP_LLM reason=no_source_trend keyword={keyword!r} "
              f"types={','.join(angle_types[:3])}", file=sys.stderr)
        return {
            at: generate_title(keyword, at, language_code, country, year,
                               audience, option_a, option_b)
            for at in angle_types
        }

    if not angle_types:
        return {}

    lang = (language_code or "en").lower().strip()
    # Auto-detect from Unicode script
    if lang in ("en", "?", "") and keyword:
        detected = _detect_lang_from_script(keyword)
        if detected:
            lang = detected

    lang_instruction = (
        f"Be written entirely in {_LANG_NAMES[lang]}"
        if lang in _LANG_NAMES
        else "Be written in English"
    )

    angle_lines = "\n".join(
        f"{i + 1}. {ANGLE_TYPE_NAMES.get(at, at.replace('_', ' ').title())}"
        for i, at in enumerate(angle_types)
    )

    sig_type     = (discovery_context or {}).get("signal_type", "") or ""
    trend_source = (discovery_context or {}).get("trend_source", "") or ""
    tone_hint    = _SIGNAL_TONE_HINTS.get(sig_type, "")
    few_shot     = _title_few_shot(lang, sig_type)

    context_lines = [
        f"- Keyword: {keyword}",
        f"- Vertical: {vertical}",
        f"- Year: {year}",
        f"- News/trend context: \"{source_trend.strip()}\"",
    ]
    if sig_type:
        context_lines.append(f"- Signal type: {sig_type}")
    if trend_source:
        context_lines.append(f"- Trend source: {trend_source}")
    if tone_hint:
        context_lines.append(f"- Tone guidance: {tone_hint}")

    few_shot_block = f"\nExample:\n{few_shot}\n" if few_shot else ""

    prompt = (
        f"Generate {len(angle_types)} article titles for:\n"
        + "\n".join(context_lines)
        + "\n"
        + few_shot_block
        + f"\nArticle types (one title per type):\n{angle_lines}\n\n"
        f"Rules:\n"
        f"- Each title MUST connect the keyword to the news/trend context naturally\n"
        f"- Each title under 120 characters\n"
        f"- {lang_instruction}\n"
        f"- No \"guaranteed\", \"apply now\", \"free\", or urgency language\n"
        f"- Do not use the word \"best\" as a standalone word anywhere in the title\n"
        f"- Do not copy the news headline verbatim\n\n"
        f"Output EXACTLY {len(angle_types)} titles, one per line, numbered "
        f"1-{len(angle_types)}. No explanations."
    )

    try:
        # num_predict scales with number of titles (generous to avoid truncation)
        num_predict = max(160 * len(angle_types), 400)
        raw = _call_llm_title(prompt, num_predict=num_predict)
        lines = [ln.strip() for ln in raw.strip().split("\n") if ln.strip()]

        result = {}
        for i, at in enumerate(angle_types):
            if i < len(lines):
                cleaned = re.sub(r"^[\d]+[\.\)]\s*", "", lines[i]).strip()
                title, valid = _validate_llm_title(cleaned)
                if valid and title:
                    result[at] = title
                    continue
            # Fallback for this specific angle
            result[at] = generate_title(keyword, at, language_code, country,
                                        year, audience, option_a, option_b)
        return result

    except Exception as e:
        print(f"[title_gen] batch LLM failed (n={len(angle_types)}, depth={_retry_depth}): {e}",
              file=sys.stderr)

        # Split-retry once — a single timeout shouldn't kill all N angles
        if _retry_depth == 0 and len(angle_types) > 2:
            mid = len(angle_types) // 2
            try:
                a = generate_titles_batch(
                    keyword, angle_types[:mid], language_code, country, year,
                    source_trend, vertical, audience, option_a, option_b,
                    discovery_context=discovery_context, _retry_depth=1,
                )
                b = generate_titles_batch(
                    keyword, angle_types[mid:], language_code, country, year,
                    source_trend, vertical, audience, option_a, option_b,
                    discovery_context=discovery_context, _retry_depth=1,
                )
                return {**a, **b}
            except Exception:
                pass  # fall through to templates below

        return {
            at: generate_title(keyword, at, language_code, country, year,
                               audience, option_a, option_b)
            for at in angle_types
        }
