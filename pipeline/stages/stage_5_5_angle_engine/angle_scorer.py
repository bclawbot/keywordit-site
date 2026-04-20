"""
angle_scorer.py — RSOC angle scoring matrices and scoring functions.

All values are from the spec (DWIGHT_RSOC_CONTENT_ENGINE_SPEC.md Section 3.2),
grounded in evidence from 22 confirmed live RSOC articles and cross-source analysis.

No external API calls. Pure computation.
"""
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .schemas import RSocKeyword, DiscoveryContext

# ─── All 10 canonical angle types (spec Section 3.1) ─────────────────────────
ALL_ANGLE_TYPES = [
    "eligibility_explainer",
    "how_it_works_explainer",
    "trend_attention_piece",
    "lifestyle_fit_analysis",
    "pre_review_guide",
    "policy_year_stamped_update",
    "accusatory_expose",
    "hidden_costs",
    "diagnostic_signs",
    "comparison",
]

# ─── Angle → primary ad category (enforces diversity rule) ───────────────────
ANGLE_PRIMARY_AD_CATEGORY: dict = {
    "eligibility_explainer":      "enrollment_comparison",
    "how_it_works_explainer":     "informational_comparison",
    "trend_attention_piece":      "brand_awareness",
    "lifestyle_fit_analysis":     "demographic_targeting",
    "pre_review_guide":           "pre_decision",
    "policy_year_stamped_update": "high_intent_enrollment",
    "accusatory_expose":          "legal_insurance_settlement",
    "hidden_costs":               "financial_legal",
    "diagnostic_signs":           "legal_intake",
    "comparison":                 "multi_provider_comparison",
}


class _FitMatrix(dict):
    """Vertical→angle fit matrix with transparent angle→vertical transpose lookup.

    Primary keys are verticals; inner keys are angle types.
    Accessing a non-existent key (e.g. an angle type) transparently returns the
    transposed view: {vertical: score} for that angle across all verticals.
    """
    def __missing__(self, key):
        transposed = {}
        for vert, angles in self.items():
            if key in angles:
                transposed[vert] = angles[key]
        if transposed:
            return transposed
        raise KeyError(key)


# ─── Angle-vertical fit matrix (spec Section 3.2) ────────────────────────────
# Keyed by vertical → {angle_type: score}.  Values 0.0–1.0.
# Unknown verticals default to 0.50 (neutral, not blocked).
# Transparent transpose via _FitMatrix supports angle→vertical lookups.
ANGLE_VERTICAL_FIT: dict = _FitMatrix({
    # ─── Original 15 scorer verticals (transposed from original angle→vertical) ──
    "ssi_disability": {
        "eligibility_explainer":      1.00,
        "how_it_works_explainer":     0.65,
        "trend_attention_piece":      0.25,
        "lifestyle_fit_analysis":     0.70,
        "pre_review_guide":           0.70,
        "policy_year_stamped_update": 0.80,
        "accusatory_expose":          0.30,
        "hidden_costs":               0.30,
        "diagnostic_signs":           0.80,
        "comparison":                 0.35,
    },
    "medicare_part_d": {
        "eligibility_explainer":      0.95,
        "how_it_works_explainer":     1.00,
        "trend_attention_piece":      0.60,
        "lifestyle_fit_analysis":     0.80,
        "pre_review_guide":           0.85,
        "policy_year_stamped_update": 1.00,
        "accusatory_expose":          0.55,
        "hidden_costs":               0.70,
        "diagnostic_signs":           0.70,
        "comparison":                 1.00,
    },
    "va_home_loans": {
        "eligibility_explainer":      0.90,
        "how_it_works_explainer":     0.85,
        "trend_attention_piece":      0.75,
        "lifestyle_fit_analysis":     0.85,
        "pre_review_guide":           0.80,
        "policy_year_stamped_update": 1.00,
        "accusatory_expose":          0.30,
        "hidden_costs":               0.65,
        "diagnostic_signs":           0.25,
        "comparison":                 0.90,
    },
    "veterans_benefits": {
        "eligibility_explainer":      0.90,
        "how_it_works_explainer":     0.60,
        "trend_attention_piece":      0.90,
        "lifestyle_fit_analysis":     0.95,
        "pre_review_guide":           0.75,
        "policy_year_stamped_update": 0.80,
        "accusatory_expose":          0.20,
        "hidden_costs":               0.25,
        "diagnostic_signs":           0.30,
        "comparison":                 0.70,
    },
    "employment_law": {
        "eligibility_explainer":      0.70,
        "how_it_works_explainer":     0.80,
        "trend_attention_piece":      0.45,
        "lifestyle_fit_analysis":     0.50,
        "pre_review_guide":           0.90,
        "policy_year_stamped_update": 0.85,
        "accusatory_expose":          0.75,
        "hidden_costs":               0.65,
        "diagnostic_signs":           0.95,
        "comparison":                 0.60,
    },
    "personal_injury": {
        "eligibility_explainer":      0.65,
        "how_it_works_explainer":     0.75,
        "trend_attention_piece":      0.30,
        "lifestyle_fit_analysis":     0.25,
        "pre_review_guide":           0.95,
        "policy_year_stamped_update": 0.65,
        "accusatory_expose":          1.00,
        "hidden_costs":               0.90,
        "diagnostic_signs":           1.00,
        "comparison":                 0.40,
    },
    "online_education": {
        "eligibility_explainer":      0.60,
        "how_it_works_explainer":     0.95,
        "trend_attention_piece":      0.70,
        "lifestyle_fit_analysis":     0.85,
        "pre_review_guide":           0.65,
        "policy_year_stamped_update": 0.60,
        "accusatory_expose":          0.25,
        "hidden_costs":               0.65,
        "diagnostic_signs":           0.20,
        "comparison":                 0.85,
    },
    "auto_insurance": {
        "eligibility_explainer":      0.55,
        "how_it_works_explainer":     0.65,
        "trend_attention_piece":      0.55,
        "lifestyle_fit_analysis":     0.55,
        "pre_review_guide":           0.75,
        "policy_year_stamped_update": 0.70,
        "accusatory_expose":          0.90,
        "hidden_costs":               0.75,
        "diagnostic_signs":           0.55,
        "comparison":                 0.95,
    },
    "water_damage": {
        "eligibility_explainer":      0.40,
        "how_it_works_explainer":     0.70,
        "trend_attention_piece":      0.35,
        "lifestyle_fit_analysis":     0.35,
        "pre_review_guide":           0.60,
        "policy_year_stamped_update": 0.50,
        "accusatory_expose":          0.95,
        "hidden_costs":               1.00,
        "diagnostic_signs":           0.65,
        "comparison":                 0.50,
    },
    "phone_deals": {
        "eligibility_explainer":      0.20,
        "how_it_works_explainer":     0.75,
        "trend_attention_piece":      0.65,
        "lifestyle_fit_analysis":     0.60,
        "pre_review_guide":           0.45,
        "policy_year_stamped_update": 0.45,
        "accusatory_expose":          0.40,
        "hidden_costs":               0.50,
        "diagnostic_signs":           0.15,
        "comparison":                 0.85,
    },
    "addiction_treatment": {
        "eligibility_explainer":      0.70,
        "how_it_works_explainer":     0.70,
        "trend_attention_piece":      0.20,
        "lifestyle_fit_analysis":     0.55,
        "pre_review_guide":           0.90,
        "policy_year_stamped_update": 0.65,
        "accusatory_expose":          0.60,
        "hidden_costs":               0.85,
        "diagnostic_signs":           0.85,
        "comparison":                 0.25,
    },
    "abogados_es": {
        "eligibility_explainer":      0.75,
        "how_it_works_explainer":     0.70,
        "trend_attention_piece":      0.40,
        "lifestyle_fit_analysis":     0.45,
        "pre_review_guide":           0.85,
        "policy_year_stamped_update": 0.70,
        "accusatory_expose":          0.70,
        "hidden_costs":               0.70,
        "diagnostic_signs":           0.85,
        "comparison":                 0.50,
    },
    "landlord_negligence": {
        "eligibility_explainer":      0.60,
        "how_it_works_explainer":     0.75,
        "trend_attention_piece":      0.30,
        "lifestyle_fit_analysis":     0.30,
        "pre_review_guide":           0.80,
        "policy_year_stamped_update": 0.75,
        "accusatory_expose":          0.85,
        "hidden_costs":               0.80,
        "diagnostic_signs":           0.90,
        "comparison":                 0.35,
    },
    "pool_home_improvement": {
        "eligibility_explainer":      0.20,
        "how_it_works_explainer":     0.55,
        "trend_attention_piece":      0.60,
        "lifestyle_fit_analysis":     0.80,
        "pre_review_guide":           0.30,
        "policy_year_stamped_update": 0.30,
        "accusatory_expose":          0.35,
        "hidden_costs":               0.65,
        "diagnostic_signs":           0.10,
        "comparison":                 0.55,
    },
    "ostomy_medical": {
        "eligibility_explainer":      0.65,
        "how_it_works_explainer":     0.80,
        "trend_attention_piece":      0.40,
        "lifestyle_fit_analysis":     0.60,
        "pre_review_guide":           0.65,
        "policy_year_stamped_update": 0.55,
        "accusatory_expose":          0.50,
        "hidden_costs":               0.55,
        "diagnostic_signs":           0.75,
        "comparison":                 0.55,
    },
    # ─── Additional vertical from original data ──────────────────────────────
    "medical_malpractice": {
        "eligibility_explainer":      0.50,
        "how_it_works_explainer":     0.50,
        "trend_attention_piece":      0.50,
        "lifestyle_fit_analysis":     0.50,
        "pre_review_guide":           0.50,
        "policy_year_stamped_update": 0.50,
        "accusatory_expose":          0.95,
        "hidden_costs":               0.50,
        "diagnostic_signs":           0.50,
        "comparison":                 0.50,
    },
    # ─── NEW verticals for expansion system keywords (Phase 0) ───────────────
    "life_insurance": {
        "eligibility_explainer":      0.95,
        "how_it_works_explainer":     0.90,
        "comparison":                 0.95,
        "pre_review_guide":           0.85,
        "hidden_costs":               0.80,
        "policy_year_stamped_update": 0.75,
        "lifestyle_fit_analysis":     0.70,
        "trend_attention_piece":      0.40,
        "accusatory_expose":          0.30,
        "diagnostic_signs":           0.25,
    },
    "loans_credit": {
        "eligibility_explainer":      0.90,
        "comparison":                 0.90,
        "hidden_costs":               0.85,
        "how_it_works_explainer":     0.85,
        "pre_review_guide":           0.80,
        "policy_year_stamped_update": 0.70,
        "lifestyle_fit_analysis":     0.55,
        "trend_attention_piece":      0.45,
        "accusatory_expose":          0.40,
        "diagnostic_signs":           0.35,
    },
    "membership_retail": {
        "comparison":                 1.00,
        "hidden_costs":               0.85,
        "pre_review_guide":           0.80,
        "eligibility_explainer":      0.70,
        "how_it_works_explainer":     0.65,
        "lifestyle_fit_analysis":     0.60,
        "trend_attention_piece":      0.55,
        "policy_year_stamped_update": 0.40,
        "accusatory_expose":          0.30,
        "diagnostic_signs":           0.20,
    },
    "home_services": {
        "hidden_costs":               0.95,
        "pre_review_guide":           0.90,
        "comparison":                 0.85,
        "how_it_works_explainer":     0.80,
        "diagnostic_signs":           0.75,
        "accusatory_expose":          0.70,
        "eligibility_explainer":      0.50,
        "policy_year_stamped_update": 0.45,
        "lifestyle_fit_analysis":     0.40,
        "trend_attention_piece":      0.35,
    },
    "latam_delivery": {
        "comparison":                 0.85,
        "how_it_works_explainer":     0.80,
        "eligibility_explainer":      0.70,
        "lifestyle_fit_analysis":     0.65,
        "trend_attention_piece":      0.60,
        "pre_review_guide":           0.55,
        "hidden_costs":               0.50,
        "policy_year_stamped_update": 0.40,
        "accusatory_expose":          0.25,
        "diagnostic_signs":           0.20,
    },
})

# ─── Language multiplier (spec Section 3.2) ──────────────────────────────────
LANGUAGE_MULTIPLIER: dict = {
    "en": 1.00,
    "es": 0.75,
    "fr": 0.80,
    "de": 0.85,
    "pt": 0.65,
    "it": 0.70,
    "nl": 0.75,
    "pl": 0.55,
}
LANGUAGE_MULTIPLIER_DEFAULT = 0.60

# ─── Intent weight (spec Section 3.2) ────────────────────────────────────────
INTENT_WEIGHT: dict = {
    "commercial":    1.00,
    "transactional": 1.00,  # treat same as commercial
    "informational": 0.70,
    "navigational":  0.30,
}

# ─── Vertical CPC signal (spec Section 3.2) ──────────────────────────────────
# Normalized 0.0–1.0 from CPC range midpoints.
# Always prefer live DataForSEO CPC when available (overrides this table).
VERTICAL_CPC_SIGNAL: dict = {
    "personal_injury":       1.00,
    "medical_malpractice":   1.00,
    "water_damage":          0.92,
    "online_education":      0.85,
    "addiction_treatment":   0.75,
    "va_home_loans":         0.55,
    "auto_insurance":        0.70,
    "employment_law":        0.65,
    "medicare_part_d":       0.45,
    "ssi_disability":        0.30,
    "veterans_benefits":     0.30,
    "landlord_negligence":   0.55,
    "phone_deals":           0.25,
    "abogados_es":           0.52,
    "pool_home_improvement": 0.28,
    "ostomy_medical":        0.28,
    # NEW expansion verticals (Phase 0)
    "life_insurance":        0.60,
    "loans_credit":          0.65,
    "membership_retail":     0.35,
    "home_services":         0.70,
    "latam_delivery":        0.30,
}
VERTICAL_CPC_SIGNAL_DEFAULT = 0.40

# ─── Discovery signal boost (spec Section 3.3) ───────────────────────────────
DISCOVERY_SIGNAL_BOOST: dict = {
    "google_trends": {
        "trend_attention_piece":      0.12,
        "policy_year_stamped_update": 0.08,
        "how_it_works_explainer":     0.05,
    },
    "reddit_discussion": {
        "accusatory_expose":  0.12,
        "diagnostic_signs":   0.10,
        "pre_review_guide":   0.08,
        "hidden_costs":       0.06,
    },
    "news_event": {
        "policy_year_stamped_update": 0.12,
        "trend_attention_piece":      0.10,
        "how_it_works_explainer":     0.06,
    },
    "keyword_expansion": {
        "eligibility_explainer":  0.08,
        "comparison":             0.08,
        "how_it_works_explainer": 0.06,
    },
    "commercial_transform": {
        "pre_review_guide": 0.10,
        "hidden_costs":     0.08,
        "comparison":       0.08,
    },
    "manual": {},
}

# ─── Keyword signals → fine-grained vertical (spec Section 5.1b) ─────────────
VERTICAL_KEYWORD_SIGNALS: dict = {
    "personal_injury":    ["attorney", "lawyer", "lawsuit", "injury", "malpractice",
                           "settlement", "negligence", "accident", "compensation"],
    "medical_malpractice":["malpractice", "medical error", "surgical error", "misdiagnosis"],
    "water_damage":       ["water damage", "flood", "restoration", "mold", "remediation",
                           "moisture", "leak", "burst pipe",
                           "plumber", "plumbing", "hvac",
                           "electrician", "roofing", "roof repair", "gutter"],
    "online_education":   ["degree", "certification", "online course", "tuition",
                           "enrollment", "college", "university program", "online degree"],
    "addiction_treatment":["rehab", "addiction", "substance", "treatment center",
                           "detox", "recovery", "sobriety"],
    "va_home_loans":      ["va loan", "veteran mortgage", "military home",
                           "veterans affairs", "préstamo va", "va mortgage"],
    "auto_insurance":     ["car insurance", "auto insurance", "vehicle coverage",
                           "seguro de auto", "car coverage",
                           "life insurance", "term life", "whole life",
                           "insurance quote", "insurance cost", "insurance rate",
                           "insurance policy", "coverage plan"],
    "employment_law":     ["wrongful termination", "discrimination", "workplace",
                           "fired", "eeoc", "labor law", "wrongful dismissal"],
    "medicare_part_d":    ["medicare", "part d", "prescription coverage",
                           "medicare advantage", "medicare plan"],
    "ssi_disability":     ["ssi", "disability benefits", "ssdi", "supplemental security",
                           "disability claim"],
    "landlord_negligence":["landlord", "tenant", "lease", "eviction",
                           "habitability", "apartment negligence"],
    "abogados_es":        ["abogado", "abogados", "demanda", "negligencia",
                           "despido injustificado", "accidente"],
    "veterans_benefits":  ["veterans", "veteran benefits", "gi bill", "va benefits",
                           "military benefits", "servicemember",
                           "military discount", "veteran discount",
                           "va card", "military benefit"],
    "phone_deals":        ["phone deal", "wireless plan", "cell phone", "mobile plan",
                           "phone upgrade", "carrier deal"],
    "pool_home_improvement":["pool installation", "swimming pool", "home renovation",
                              "outdoor improvement", "deck installation"],
    "ostomy_medical":     ["ostomy", "colostomy", "ileostomy", "stoma", "ostomy supply"],
}

# ─── Coarse-to-fine vertical mapping (from vetting.py vertical values) ────────
COARSE_VERTICAL_MAP: dict = {
    "legal":      ["personal_injury", "employment_law", "landlord_negligence",
                   "abogados_es", "medical_malpractice"],
    "finance":    ["va_home_loans", "auto_insurance", "online_education"],
    "health":     ["addiction_treatment", "medicare_part_d", "ostomy_medical",
                   "medical_malpractice"],
    "insurance":  ["auto_insurance", "medicare_part_d", "va_home_loans"],
    "education":  ["online_education"],
    "automotive": ["auto_insurance"],
    "veterans":   ["veterans_benefits", "va_home_loans"],
    "housing":    ["ssi_disability", "landlord_negligence", "va_home_loans"],
    "telecom":    ["phone_deals"],
    "home":       ["pool_home_improvement", "water_damage"],
    "auto_insurance": ["auto_insurance"],
    # Expansion verticals → closest existing scorer verticals (Phase 0)
    "life_insurance":     ["auto_insurance", "medicare_part_d"],
    "veterans_military":  ["veterans_benefits", "va_home_loans"],
    "membership_retail":  ["phone_deals", "online_education"],
    "legal_services":     ["personal_injury", "employment_law", "landlord_negligence"],
    "home_services":      ["water_damage", "pool_home_improvement"],
    "latam_delivery":     ["abogados_es"],
    "loans_credit":       ["va_home_loans", "auto_insurance"],
    "medical_pharma":     ["addiction_treatment", "ostomy_medical", "medicare_part_d"],
    "housing_ssi":        ["ssi_disability", "landlord_negligence"],
    "general":            ["online_education"],
}


def classify_vertical_fine(coarse_vertical: str, keyword: str) -> str:
    """
    Maps a coarse vetting.py vertical + keyword text to a fine-grained vertical key
    used in ANGLE_VERTICAL_FIT. Returns a key present in ANGLE_VERTICAL_FIT or "unknown".

    Algorithm:
      1. Check keyword against VERTICAL_KEYWORD_SIGNALS (most accurate).
      2. Fall back to COARSE_VERTICAL_MAP[coarse_vertical] filtered by keyword signals.
      3. Fall back to "unknown" (scores with neutral defaults — not blocked).
    """
    kw_lower = keyword.lower()

    # Step 1: keyword signal matching (highest confidence).
    # Find the LONGEST matching signal across ALL verticals — longer signals are
    # more specific. E.g. "wrongful termination" (21 chars) beats "attorney" (8 chars).
    best_vertical = None
    best_sig_len  = 0
    for fine_vertical, signals in VERTICAL_KEYWORD_SIGNALS.items():
        for sig in signals:
            sig_l = sig.lower()
            if sig_l in kw_lower and len(sig_l) > best_sig_len:
                best_sig_len  = len(sig_l)
                best_vertical = fine_vertical
    if best_vertical:
        return best_vertical

    # Step 2: coarse → fine map with first candidate
    candidates = COARSE_VERTICAL_MAP.get(coarse_vertical.lower(), [])
    if candidates:
        return candidates[0]

    return "unknown"


def map_discovery_context(opp: dict):
    """
    Derives a DiscoveryContext from existing validated_opportunities fields.
    Pure function. Gracefully handles None/missing fields.
    Returns a DiscoveryContext-compatible dict (avoids circular import).
    """
    source       = opp.get("trend_source") or opp.get("source") or ""
    source_trend = opp.get("source_trend") or opp.get("trend_text") or ""
    cat          = opp.get("commercial_category") or ""

    if "google_trends" in source:
        signal_type = "google_trends"
        signal_text = f"Google Trends spike: {source_trend}" if source_trend else "Google Trends"
    elif "reddit" in source:
        signal_type = "reddit_discussion"
        signal_text = f"Reddit hot: {source_trend}" if source_trend else "Reddit discussion"
    elif "news" in source or "bing" in source:
        signal_type = "news_event"
        signal_text = f"News: {source_trend}" if source_trend else "News event"
    elif cat and cat not in ("general", ""):
        signal_type = "commercial_transform"
        signal_text = f"Transformed: {cat}"
    else:
        signal_type = "keyword_expansion"
        signal_text = source_trend if source_trend else ""

    return {"signal_type": signal_type, "signal_text": signal_text}


def angle_rsoc_score(
    angle_type: str,
    vertical: str,
    language: str,
    cpc_usd: Optional[float],
    intent_classification: str,
    competitor_saturation: float,
    discovery_boost: float,
) -> float:
    """
    Estimates how well a given angle will perform for RSOC context.

    Returns float 0.0–1.0:
        0.0–0.3  = low viability
        0.3–0.6  = moderate
        0.6–0.8  = high
        0.8–1.0  = premium

    Weights:
        angle_vertical_fit:  30%
        cpc_signal:          30%
        intent_weight:       20%
        language_multiplier: 10%
        saturation_penalty:  10%
      + discovery_boost:     0–15% additive
    """
    # 1. Angle-vertical fit (vertical→angle lookup)
    fit_score = ANGLE_VERTICAL_FIT.get(vertical, {}).get(angle_type, 0.50)

    # 2. CPC signal — prefer live value, else use vertical baseline
    if cpc_usd is not None and cpc_usd > 0:
        cpc_norm = min(cpc_usd / 200.0, 1.0)
    else:
        cpc_norm = VERTICAL_CPC_SIGNAL.get(vertical, VERTICAL_CPC_SIGNAL_DEFAULT)

    # 3. Intent weight
    intent_score = INTENT_WEIGHT.get((intent_classification or "informational").lower(), 0.50)

    # 4. Language multiplier
    lang_mult = LANGUAGE_MULTIPLIER.get((language or "en").lower(),
                                         LANGUAGE_MULTIPLIER_DEFAULT)

    # 5. Saturation penalty (max 50% penalty)
    saturation_score = 1.0 - (min(max(competitor_saturation, 0.0), 1.0) * 0.5)

    score = (
        (fit_score        * 0.30) +
        (cpc_norm         * 0.30) +
        (intent_score     * 0.20) +
        (lang_mult        * 0.10) +
        (saturation_score * 0.10) +
        discovery_boost
    )
    return round(min(max(score, 0.0), 1.0), 4)


def get_discovery_boosts(signal_type: str) -> dict:
    """Returns {angle_type: boost_float} for a given signal_type."""
    return DISCOVERY_SIGNAL_BOOST.get(signal_type, {})
