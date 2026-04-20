"""
quality_gates.py — RSOC article quality validation.

validate_rsoc_article() — 8 checks; applies to all languages.
validate_spanish_article() — 4 additional ES-specific checks.

Returns ValidationResult with pass/fail, score (0.0–1.0), failures, and warnings.
Source: spec Section 4.3 and Section 6.3.
"""
import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ValidationResult:
    passed: bool
    score: float                     # 0.0–1.0
    failures: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


# ─── RAF-violating phrases (immediate fail) ───────────────────────────────────
RAF_BANNED_PHRASES = [
    r"you will receive",
    r"guaranteed",
    r"instant approval",
    r"click here to see",
    r"sponsored results below",
    r"see your options below",
    r"you deserve compensation",
    r"you may be owed money",
    r"get money back",
    r"free quote",
    r"apply now",
    r"sign up today",
    r"limited time offer",
    r"call now",
    r"click to compare prices",
    r"best price guaranteed",
    r"\bbest\b",
    r"\byou qualify\b",
    r"\byou are eligible\b",
]

# ─── Yellow-flag phrases (warnings, not failures) ─────────────────────────────
RAF_YELLOW_FLAG_PHRASES = [
    r"\$[\d,]+",
    r"\d+%",
    r"according to",
    r"studies show",
    r"experts say",
    r"\blawsuit\b",
    r"\bsue\b",
    r"\bsettlement\b",
    r"\bcompensation\b",
]


def validate_rsoc_article(
    article_text: str,
    angle_type: str,
    keyword: str,
    language: str,
    vertical: Optional[str] = None,
) -> ValidationResult:
    """
    Validates a generated article for RSOC/RAF compliance and quality standards.
    8 checks; returns ValidationResult.

    Scoring:
        checks_passed / 8 = score (0.0–1.0)
        passed = no failures AND score >= 0.75
    """
    failures = []
    warnings = []
    checks_passed = 0.0
    total_checks  = 8.0

    # 1. Word count
    word_count = len(article_text.split())
    if word_count < 600:
        failures.append(f"FAIL word_count: {word_count} words (min 600)")
    elif word_count > 1400:
        warnings.append(f"WARN word_count: {word_count} words (recommended max 1200)")
        checks_passed += 0.5
    else:
        checks_passed += 1

    # 2. First-paragraph semantic signal check
    paragraphs = [p.strip() for p in article_text.strip().split("\n\n") if p.strip()]
    # skip H1 line if first paragraph is the title
    first_para = paragraphs[1] if len(paragraphs) > 1 and paragraphs[0].startswith("#") else paragraphs[0]
    first_para_lower = first_para.lower()
    keyword_lower    = keyword.lower()

    has_keyword = keyword_lower in first_para_lower or any(
        w in first_para_lower for w in keyword_lower.split()[:2]
    )
    has_process_term = any(t in first_para_lower for t in [
        "eligib", "qualif", "program", "coverage", "attorney", "claim",
        "benefit", "process", "option", "understand", "consider", "require",
        "explore", "evaluate", "determin",
    ])
    if has_keyword and has_process_term:
        checks_passed += 1
    else:
        failures.append(
            f"FAIL first_paragraph_signals: keyword_present={has_keyword}, "
            f"process_term_present={has_process_term}"
        )

    # 3. H2 header count (≥ 3 required)
    h2_count = len(re.findall(r"^## ", article_text, re.MULTILINE))
    if h2_count >= 3:
        checks_passed += 1
    else:
        failures.append(f"FAIL h2_count: found {h2_count} (min 3)")

    # 4. RAF banned phrases
    banned_found = [p for p in RAF_BANNED_PHRASES
                    if re.search(p, article_text, re.IGNORECASE)]
    if not banned_found:
        checks_passed += 1
    else:
        failures.append(f"FAIL raf_banned_phrases: {banned_found}")

    # 5. Yellow-flag phrases (warnings only)
    for pattern in RAF_YELLOW_FLAG_PHRASES:
        if re.search(pattern, article_text, re.IGNORECASE):
            warnings.append(f"WARN yellow_flag: '{pattern}' — review for accuracy")

    # 6. Keyword density (0.3% – 3.5%)
    kw_count = article_text.lower().count(keyword_lower)
    density  = kw_count / max(word_count, 1)
    if 0.003 <= density <= 0.035:
        checks_passed += 1
    elif density < 0.003:
        warnings.append(f"WARN keyword_density: {density:.3%} (low — may reduce ad relevance)")
        checks_passed += 0.7
    else:
        warnings.append(f"WARN keyword_density: {density:.3%} (high — keyword stuffing risk)")
        checks_passed += 0.5

    # 7. Language consistency for Spanish
    if language.lower() == "es":
        has_diacritics    = bool(re.search(r"[áéíóúñü]", article_text))
        has_english_words = bool(re.search(r"\b(the|and|for|with|that|this|are|have)\b",
                                           article_text))
        if has_diacritics and not has_english_words:
            checks_passed += 1
        elif not has_diacritics:
            failures.append("FAIL spanish_diacritics: zero diacritical marks — encoding error")
        else:
            warnings.append("WARN language_mix: English words in Spanish article")
            checks_passed += 0.5
    else:
        checks_passed += 1  # not applicable for EN

    # 8. No direct sales language
    sales_patterns = [r"\bbuy now\b", r"\border now\b", r"\bpurchase\b",
                      r"\badd to cart\b", r"\bcheckout\b"]
    sales_found = [p for p in sales_patterns
                   if re.search(p, article_text, re.IGNORECASE)]
    if not sales_found:
        checks_passed += 1
    else:
        failures.append(f"FAIL direct_sales_language: {sales_found}")

    score  = round(checks_passed / total_checks, 4)
    passed = len(failures) == 0 and score >= 0.75

    return ValidationResult(passed=passed, score=score,
                             failures=failures, warnings=warnings)


def validate_spanish_article(article_text: str, vertical: str) -> ValidationResult:
    """
    Extended validation for Spanish-language RSOC articles.
    Runs base validation first, then appends 4 ES-specific checks.
    """
    # Run base validation (language="es", no keyword needed for register checks)
    base = validate_rsoc_article(
        article_text, angle_type="", keyword="", language="es", vertical=vertical
    )

    extra_failures = []
    extra_warnings = []

    # Rule 1: Diacritical marks present
    if not re.search(r"[áéíóúñü]", article_text):
        extra_failures.append("FAIL es_diacritics: zero diacritical marks — encoding failure")

    # Rule 2: Register consistency (usted vs tú)
    has_usted = bool(re.search(r"\busted\b", article_text, re.IGNORECASE))
    has_tu    = bool(re.search(r"\b(tú|tu\b)", article_text, re.IGNORECASE))
    if has_usted and has_tu:
        extra_warnings.append("WARN es_register: mixed usted/tú — pick one per vertical")

    # Rule 3: Legal/medical verticals should use formal usted
    formal_verticals = ["abogados_es", "employment_law", "personal_injury",
                        "medical_malpractice", "addiction_treatment"]
    if vertical in formal_verticals and has_tu and not has_usted:
        extra_warnings.append("WARN es_formality: legal vertical using tú — recommend usted")

    # Rule 4: No URL-slug artifacts (common encoding artifact)
    if re.search(r"\b[a-z]-[a-z]{2,}\b", article_text[:500]):
        extra_warnings.append(
            "WARN es_slug_artifact: possible URL-slug artifact in opening (e.g., 'c-mo' for 'cómo')"
        )

    base.failures.extend(extra_failures)
    base.warnings.extend(extra_warnings)
    base.passed = len(base.failures) == 0
    return base
