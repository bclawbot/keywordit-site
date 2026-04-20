"""
consequence_generator.py — Stage 2a.7

For each trend with a money-flow classification (set by Stage 1c), generate
commercial-intent consequence keywords that buyers actually search for:
  - service      (e.g., "hurricane damage lawyer near me")
  - information  (e.g., "how to file flood insurance claim")
  - product      (e.g., "emergency generator for home")

Integration: Appends generated entries to transformed_keywords.json using the
same schema as commercial_keyword_transformer. Because Sprint 1 unblocked
Bucket B from the ONCE_PER_DAY_DFS gate, these entries flow into
keyword_extractor's Bucket B → DataForSEO validation → scoring pipeline
with zero additional integration code.
"""

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

from llm_client import call as _llm_call, LLMError  # noqa: E402

TRENDS_PATH = BASE / "explosive_trends.json"
OUTPUT_PATH = BASE / "transformed_keywords.json"
PROVEN_TEMPLATES_PATH = BASE / "data" / "proven_templates.json"
ERROR_LOG = BASE / "error_log.jsonl"

# ── Budget guards (per-run caps) ─────────────────────────────────────────────

MAX_TRENDS_PER_RUN = 80     # classified trends to process per run
MAX_LLM_ATTEMPTS = 1        # no retry — same model+prompt rarely flips JSON shape
CHECKPOINT_EVERY = 20       # flush to transformed_keywords.json every N trends
INTENTS = ("service", "information", "product")

# ── Country → language mapping (shared with validation.py logic) ─────────────

COUNTRY_TO_LANGUAGE = {
    "US": "en", "GB": "en", "AU": "en", "CA": "en", "NZ": "en",
    "IE": "en", "ZA": "en", "SG": "en", "PH": "en", "MY": "en",
    "NG": "en", "KE": "en", "IN": "en",
    "DE": "de", "AT": "de", "CH": "de",
    "FR": "fr", "BE": "nl", "NL": "nl",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "IT": "it", "PT": "pt", "BR": "pt",
    "JP": "ja", "KR": "ko", "HK": "zh", "TW": "zh",
    "PL": "pl", "CZ": "cs", "RO": "ro", "HU": "hu", "GR": "el",
    "SE": "sv", "DK": "da", "FI": "fi",
    "TH": "th", "ID": "id", "VN": "vi",
    "TR": "tr", "UA": "uk", "IL": "he",
    "EG": "ar", "SA": "ar",
}

LANGUAGE_NAME = {
    "en": "English", "es": "Spanish", "pt": "Portuguese", "de": "German",
    "fr": "French", "it": "Italian", "ja": "Japanese", "ko": "Korean",
    "zh": "Chinese", "nl": "Dutch", "pl": "Polish", "cs": "Czech",
    "ro": "Romanian", "hu": "Hungarian", "el": "Greek", "sv": "Swedish",
    "da": "Danish", "fi": "Finnish", "th": "Thai", "id": "Indonesian",
    "vi": "Vietnamese", "tr": "Turkish", "uk": "Ukrainian", "he": "Hebrew",
    "ar": "Arabic",
}


def _country_to_language(country: str) -> str:
    return COUNTRY_TO_LANGUAGE.get((country or "US").upper(), "en")


# ── Prompt templates ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = (
    "You are an RSOC keyword strategist. For each trending topic with a "
    "money-flow archetype, generate commercial-intent keyword variants "
    "that real buyers search for on Google. "
    "Output strict JSON only — no prose, no code fences."
)

USER_TEMPLATE = """Trending topic: "{keyword}"
Money-flow archetype: {archetype}
Target country: {country}
Target language: {language_name} ({language})

Generate exactly 3 commercial consequence keywords — one per intent:
  - service       (a professional service someone will hire, e.g. "lawyer near me")
  - information   (research someone will do, e.g. "how to file insurance claim")
  - product       (a product someone will buy, e.g. "emergency generator for home")

HARD RULES:
- Write keywords in {language_name} the way a native speaker would type them into Google.
- Do NOT translate literally from English — use natural local phrasing.
- Each keyword must be 2-8 words, lowercase preferred (except proper nouns).
- Keywords must be commercial — advertisers will pay to appear for them.
- No hashtags, emojis, quotes, or punctuation at the ends.

{few_shot_block}

Respond in EXACTLY this JSON format (no other text):
{{
  "service":     "<service keyword>",
  "information": "<information keyword>",
  "product":     "<product keyword>"
}}
"""

# Archetype-specific few-shot hints. Keep each block small.
FEW_SHOTS = {
    "natural_disaster": {
        "en": [
            'Topic: "Hurricane hits Florida" →',
            '  {"service": "hurricane damage lawyer near me", '
            '"information": "how to file flood insurance claim", '
            '"product": "portable emergency generator for home"}',
        ],
        "es": [
            'Topic: "Terremoto en Perú" →',
            '  {"service": "abogado por daños de terremoto Lima", '
            '"information": "como reclamar seguro por sismo", '
            '"product": "generador de emergencia precio"}',
        ],
    },
    "health_medical": {
        "en": [
            'Topic: "FDA approves new weight loss drug" →',
            '  {"service": "weight loss clinic near me", '
            '"information": "glp-1 side effects compared", '
            '"product": "ozempic savings card eligibility"}',
        ],
        "pt": [
            'Topic: "Nova droga de emagrecimento" →',
            '  {"service": "clínica de emagrecimento São Paulo", '
            '"information": "efeitos colaterais GLP-1", '
            '"product": "ozempic preço genérico"}',
        ],
    },
    "regulatory_policy": {
        "en": [
            'Topic: "New immigration visa program announced" →',
            '  {"service": "immigration lawyer consultation", '
            '"information": "how to apply for new visa program 2026", '
            '"product": "immigration document translation service"}',
        ],
    },
    "technology": {
        "en": [
            'Topic: "Major ransomware attack on hospitals" →',
            '  {"service": "enterprise cybersecurity audit", '
            '"information": "hipaa breach notification steps", '
            '"product": "endpoint protection software comparison"}',
        ],
    },
    "corporate_market": {
        "en": [
            'Topic: "Tech giant announces 10000 layoffs" →',
            '  {"service": "executive resume writing service", '
            '"information": "severance package negotiation guide", '
            '"product": "online data science bootcamp cost"}',
        ],
    },
    "economic": {
        "en": [
            'Topic: "Fed raises interest rates again" →',
            '  {"service": "mortgage refinance quote", '
            '"information": "variable vs fixed mortgage explained", '
            '"product": "high yield savings account 2026"}',
        ],
    },
}


def _few_shot_block(archetype: str, language: str) -> str:
    """Return archetype+language few-shot snippet, falling back to English."""
    archetype_shots = FEW_SHOTS.get(archetype, {})
    lines = archetype_shots.get(language) or archetype_shots.get("en")
    if not lines:
        return ""
    return "Example:\n" + "\n".join(lines) + "\n"


# ── Utility ──────────────────────────────────────────────────────────────────

def _log_error(stage: str, error: str):
    try:
        with ERROR_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "timestamp": datetime.now().isoformat(),
                "stage": stage,
                "error": str(error)[:500],
            }) + "\n")
    except Exception:
        pass


def _atomic_save(target: Path, data):
    """Atomic JSON write — tmpfile + os.replace."""
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                   encoding="utf-8")
    os.replace(tmp, target)


def _parse_json_object(text: str) -> dict:
    """Extract first JSON object from LLM response."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("no JSON object in response")
    return json.loads(text[start:end + 1])


def _normalize_keyword(kw: str) -> str:
    """Strip punctuation ends, collapse whitespace."""
    kw = (kw or "").strip()
    kw = kw.strip(" \t\"'.,;:!?()[]{}·—–-")
    kw = re.sub(r"\s+", " ", kw)
    return kw


def _load_template_patterns(limit: int = 200) -> set:
    """
    Load top-N proven template patterns as a set of lowercase canonicalized tokens.
    Used for light validation: keywords that share 2+ tokens with any proven
    template are considered grounded. Purely additive — never blocks output.
    """
    if not PROVEN_TEMPLATES_PATH.exists():
        return set()
    try:
        data = json.loads(PROVEN_TEMPLATES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if not isinstance(data, dict):
        return set()
    # Sort templates by revenue, take top-N
    ranked = sorted(
        data.items(),
        key=lambda kv: (kv[1] or {}).get("revenue", 0) if isinstance(kv[1], dict) else 0,
        reverse=True,
    )[:limit]
    tokens: set = set()
    for tpl, _meta in ranked:
        # Strip placeholders like {insurance_line}
        clean = re.sub(r"\{[^}]+\}", " ", tpl).lower()
        for tok in re.findall(r"[a-z]{4,}", clean):
            tokens.add(tok)
    return tokens


# ── Generation ───────────────────────────────────────────────────────────────

def _generate_consequences(keyword: str, archetype: str, country: str) -> list[dict]:
    """Call LLM once, parse 3 intents, return normalized consequence dicts."""
    language = _country_to_language(country)
    language_name = LANGUAGE_NAME.get(language, language)
    user_msg = USER_TEMPLATE.format(
        keyword=keyword,
        archetype=archetype,
        country=country,
        language=language,
        language_name=language_name,
        few_shot_block=_few_shot_block(archetype, language),
    )

    last_err = None
    parsed = None
    for attempt in range(MAX_LLM_ATTEMPTS):
        try:
            raw = _llm_call(
                [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=400,
                temperature=0.4,
                timeout="bg",
                stage="consequence_generator/llm",
                local_only=True,
            )
            parsed = _parse_json_object(raw)
            break
        except (LLMError, ValueError, json.JSONDecodeError) as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            break

    if not parsed:
        _log_error("consequence_generator/llm",
                   f"{type(last_err).__name__ if last_err else 'empty'}: {last_err} "
                   f"(keyword={keyword}, archetype={archetype}, country={country})")
        return []

    results = []
    for intent in INTENTS:
        kw = _normalize_keyword(parsed.get(intent, ""))
        # Reject empty, too short, or too long
        if not kw:
            continue
        word_count = len(kw.split())
        if word_count < 2 or word_count > 10:
            continue
        results.append({
            "keyword": kw,
            "country": country,
            "intent": intent,
            "source_keyword": keyword,
            "archetype": archetype,
            "language": language,
        })
    return results


def generate_consequences(keyword: str, archetype: str, language: str = None,
                          country: str = "US") -> list[dict]:
    """Public entry point for external callers / tests.

    The `language` parameter is accepted for test compatibility but is always
    derived from `country` to avoid a disagreement between the two.
    """
    _ = language  # unused — derived from country
    return _generate_consequences(keyword, archetype, country)


# ── Main run loop ────────────────────────────────────────────────────────────

def _format_as_transformed(c: dict) -> dict:
    """Map a consequence to the transformed_keywords.json schema so
    keyword_extractor (Bucket B) picks it up without any code changes there."""
    return {
        "keyword": c["keyword"],
        "country": c["country"],
        "expansion_seed": c["source_keyword"],
        "source_trend": c["source_keyword"],
        "google_cpc_low": 0.0,
        "google_cpc_high": 0.0,
        "google_estimated_cpc": 0.0,
        "google_volume": None,
        "google_competition": "UNSPECIFIED",
        "google_competition_index": 0,
        "monthly_search_history": [],
        "is_branded": False,
        "metrics_source": "consequence_generator",
        "needs_dataforseo_validation": True,  # enter Bucket B
        "source": "consequence_generator",
        "fetched_at": datetime.now().isoformat(),
        "original_keyword": c["source_keyword"],
        "transformation_relationship": f"consequence:{c['intent']}",
        "transformation_confidence": "medium",
        "transformed_at": datetime.now().isoformat(),
        "intent": c["intent"],
        "archetype": c["archetype"],
        "language": c["language"],
    }


def run(input_path=None):
    trends_path = Path(input_path) if input_path else TRENDS_PATH

    if not trends_path.exists():
        print(f"[consequence] Input not found: {trends_path}")
        return

    try:
        trends = json.loads(trends_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[consequence] Could not parse {trends_path.name}: {e}")
        return
    if not isinstance(trends, list):
        print(f"[consequence] Expected list, got {type(trends).__name__}")
        return

    classified = [t for t in trends
                  if isinstance(t.get("money_flow"), dict)
                  and t["money_flow"].get("archetype")]
    if not classified:
        print("[consequence] No trends with money_flow.archetype — "
              "ensure Stage 1c (money_flow_classifier) ran first")
        return

    # Prefer higher-confidence classifications when capping
    classified.sort(key=lambda t: float(t["money_flow"].get("confidence", 0)),
                    reverse=True)
    if len(classified) > MAX_TRENDS_PER_RUN:
        print(f"[consequence] Capping at {MAX_TRENDS_PER_RUN}/{len(classified)} "
              f"classified trends (budget guard)")
        classified = classified[:MAX_TRENDS_PER_RUN]

    template_tokens = _load_template_patterns()  # used only for stats, never blocks

    total_generated = 0
    total_appended = 0
    per_intent_counts = {k: 0 for k in INTENTS}
    grounded_count = 0
    failed_trends = 0

    # Load existing transformed_keywords.json once; append + atomic-save per batch.
    existing: list = []
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[consequence] Could not parse existing {OUTPUT_PATH.name}: {e}")
            existing = []
    existing_keys = {
        (str(e.get("keyword", "")).lower(), str(e.get("country", "")).upper())
        for e in existing if isinstance(e, dict)
    }

    batch: list[dict] = []

    def _flush_batch():
        nonlocal batch, total_appended
        if not batch:
            return
        new_entries = []
        for c in batch:
            key = (c["keyword"].lower(), c["country"].upper())
            if key in existing_keys:
                continue
            existing_keys.add(key)
            new_entries.append(_format_as_transformed(c))
        if new_entries:
            existing.extend(new_entries)
            _atomic_save(OUTPUT_PATH, existing)
            total_appended += len(new_entries)
        batch = []

    for i, trend in enumerate(classified, 1):
        keyword = trend.get("term") or trend.get("keyword", "")
        if not keyword:
            continue
        archetype = trend["money_flow"]["archetype"]
        country = (trend.get("geo") or trend.get("country", "US") or "US").upper()

        try:
            generated = _generate_consequences(keyword, archetype, country)
        except Exception as e:
            _log_error("consequence_generator/trend",
                       f"unexpected: {type(e).__name__}: {e} (keyword={keyword})")
            failed_trends += 1
            continue

        if not generated:
            failed_trends += 1
            continue

        for c in generated:
            per_intent_counts[c["intent"]] += 1
            if template_tokens:
                kw_toks = set(re.findall(r"[a-z]{4,}", c["keyword"].lower()))
                if len(kw_toks & template_tokens) >= 2:
                    grounded_count += 1
            batch.append(c)
            total_generated += 1

        if i % CHECKPOINT_EVERY == 0:
            _flush_batch()
            print(f"[consequence] checkpoint {i}/{len(classified)} flushed "
                  f"({total_appended} appended so far)")

    _flush_batch()

    if total_generated == 0:
        print(f"[consequence] No consequences generated "
              f"({len(classified)} trends, {failed_trends} failed)")
        return

    print(f"[consequence] Processed {len(classified)} classified trends "
          f"({failed_trends} failed); generated {total_generated} consequences, "
          f"appended {total_appended} new entries to {OUTPUT_PATH.name}")
    print(f"[consequence] Per-intent: "
          f"service={per_intent_counts['service']}, "
          f"information={per_intent_counts['information']}, "
          f"product={per_intent_counts['product']}")
    if template_tokens:
        print(f"[consequence] {grounded_count}/{total_generated} keywords "
              f"share tokens with proven templates")


if __name__ == "__main__":
    run()
