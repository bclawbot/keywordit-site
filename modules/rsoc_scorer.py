"""
modules/rsoc_scorer.py — RSOC arbitrage scoring engine.

Extracted from validation.py for modularity and testability.
Pure computation functions with no I/O or side effects.
"""

import json
import re
from pathlib import Path

# ── RPC Estimator (five-level empirical lookup) ───────────────────────────────
_RPC_ESTIMATOR: dict = {}
_RPC_PATTERNS:  dict = {}
try:
    from modules.rpc_estimator import (
        load_estimator, load_patterns, enrich_keyword_rpc,
    )
    _RPC_ESTIMATOR = load_estimator()
    _RPC_PATTERNS  = load_patterns()
    _RPC_AVAILABLE = True
except Exception:
    _RPC_AVAILABLE = False
    _RPC_ESTIMATOR = {}
    _RPC_PATTERNS = {}

from country_config import get_cpc_floor

# ── Vertical CPC reference (used by tag_opportunity_v2) ─────────────────────
_BASE = Path(__file__).resolve().parent.parent
_VERTICAL_CPC_REF_PATH = _BASE / "data" / "vertical_cpc_reference.json"
_VERTICAL_CPC_REF: dict = {}
try:
    _VERTICAL_CPC_REF = json.loads(_VERTICAL_CPC_REF_PATH.read_text())
except Exception:
    # Try alternate path (file may live in workspace root)
    try:
        _VERTICAL_CPC_REF = json.loads((_BASE / "vertical_cpc_reference.json").read_text())
    except Exception:
        pass

_EMERGING_THRESHOLD = 1.50  # minimum vertical CPC ceiling to qualify as EMERGING

# SERP feature friction weights for SERP Saturation Risk (SSR) score.
# Higher SSR = more zero-click elements = lower RSOC feed CTR.
_SERP_FRICTION = {
    "answer_box":       0.8,
    "featured_snippet": 0.8,
    "ai_overview":      0.7,
    "knowledge_graph":  0.6,
    "local_pack":       0.5,
    "video":            0.3,
    "image":            0.2,
    "shopping":        -0.3,   # commercial intent → reduces risk
    "paid_ads":        -0.2,   # ads present → confirms monetization
}

# Composite score weights per scoring profile.
# EVERGREEN = established verticals with stable high-CPC keywords (Track A in CPC router).
# EMERGING  = trend-driven keywords in price-discovery phase (tagged EMERGING/EMERGING_HIGH).
_RSOC_WEIGHTS = {
    # Revenue-first formula: rpc_ceiling + auction_health + momentum + intent
    # No volume or KD — soft modifiers handle those as penalties after composite.
    "EVERGREEN": {
        "cpc":            0.40,   # RPC ceiling proxy (htpb)
        "auction_health": 0.25,   # Competition depth + bid spread
        "trend":          0.20,   # Momentum — catching the wave
        "intent":         0.15,   # Buyer behind the query
    },
    "EMERGING": {
        "cpc":            0.30,   # Lower weight — CPC still thin at price-discovery
        "auction_health": 0.20,   # Auction may be shallow but growing
        "trend":          0.35,   # Momentum is the primary signal for emerging
        "intent":         0.15,
    },
}


# ── Scoring component functions ───────────────────────────────────────────────

def _compute_cpc_score(high_top_of_page_bid: float) -> float:
    """Map high_top_of_page_bid to a 0-100 score. Uses ceiling CPC as RPC proxy."""
    b = high_top_of_page_bid or 0.0
    if b >= 50:  return 100.0   # insurance, legal — premium
    if b >= 20:  return  85.0   # finance, health
    if b >= 10:  return  70.0   # software, education
    if b >=  5:  return  55.0   # moderate
    if b >=  2:  return  35.0   # low-mid
    if b >= 0.5: return  15.0   # low
    return 0.0


def _compute_intent_score(main_intent: str, secondary_intents: list) -> float:
    """
    Score 0-100 using intent probability floats from secondary_keyword_intents
    where available, falling back to the main_intent label.
    """
    p_com, p_txn, p_nav = 0.0, 0.0, 0.0

    if main_intent == "commercial":      p_com = 1.0
    elif main_intent == "transactional": p_txn = 1.0
    elif main_intent == "navigational":  p_nav = 1.0

    for item in (secondary_intents or []):
        t    = item.get("intent", "")
        prob = float(item.get("probability") or 0)
        if t == "commercial":      p_com = max(p_com, prob)
        elif t == "transactional": p_txn = max(p_txn, prob)
        elif t == "navigational":  p_nav = max(p_nav, prob)

    raw = (p_com * 70) + (p_txn * 100) - (p_nav * 80)
    return max(0.0, min(100.0, raw))


# ── Intent inference (when Labs doesn't return intent data) ───────────────────
_TRANSACTIONAL_PATTERNS = frozenset([
    "buy", "purchase", "order", "apply", "sign up", "get started",
    "free trial", "download", "install",
])
_COMMERCIAL_PATTERNS = frozenset([
    "best", "top", "compare", "vs", "review", "rating", "reviews",
    "cost", "price", "pricing", "cheap", "affordable", "discount",
    "rate", "rates", "quote", "quotes", "estimate",
    "service", "services", "company", "companies", "provider", "providers",
    "near me", "local", "hire", "find", "solution", "solutions",
    "plan", "plans", "option", "options", "alternative",
])
_COMMERCIAL_VERTICALS_INTENT = frozenset([
    "insurance", "auto_insurance", "finance", "loans_credit", "legal",
    "legal_services", "health", "medical_pharma", "real_estate",
    "home_services", "automotive", "software", "saas",
])

def _infer_intent(keyword: str, cpc_high_usd: float, vertical: str) -> str:
    """
    Infer commercial intent when DataForSEO Labs returns no intent data (~96% of keywords).
    Called only when enrichment['main_intent'] is empty/None.
    Returns 'transactional', 'commercial', or '' (unknown — no penalty, no boost).
    """
    kw = keyword.lower()
    tokens = set(kw.split())

    # Transactional: highest buyer signal — check first
    for p in _TRANSACTIONAL_PATTERNS:
        if p in kw:
            return "transactional"

    # Explicit commercial query patterns (word-level match)
    if tokens & _COMMERCIAL_PATTERNS:
        return "commercial"

    # CPC-based: advertisers bidding $5+ top-of-page confirms commercial intent
    if cpc_high_usd >= 5.0:
        return "commercial"

    # Vertical-based: high-value verticals default to commercial
    if vertical in _COMMERCIAL_VERTICALS_INTENT:
        return "commercial"

    return ""  # genuinely unknown — leave blank, no penalty, no boost


def _compute_kd_score(kd: int) -> float:
    """Non-linear: medium-high KD is the RSOC sweet spot.
    KD reflects advertiser investment in a vertical, not ranking difficulty for Dwight.
    Returns 0-100 scale."""
    kd = int(kd or 0)
    if kd >= 75:  return 70.0   # premium, but arb-saturated
    if kd >= 50:  return 100.0  # Track A sweet spot
    if kd >= 30:  return 85.0   # Track B strong signal
    if kd >= 15:  return 55.0   # emerging, needs CPC confirmation
    return 5.0                   # almost certainly junk (< 15)


def _compute_competition_score(competition: float) -> float:
    """Direct linear mapping: 1.0 paid competition index = 100 score."""
    return max(0.0, min(100.0, float(competition or 0) * 100.0))


def _compute_auction_health(competition: float, cpc_high_usd: float, cpc_usd: float) -> float:
    """
    Auction depth (competition) + bid ceiling heating (bid_spread).
    High competition = deep stable auction = stable RPC.
    htpb / avg_cpc > 1.5 = auction heating (advertisers bidding up the ceiling).
    Returns 0-100.
    """
    comp = float(competition or 0)
    avg = max(float(cpc_usd or 0.01), 0.01)
    bid_spread = float(cpc_high_usd or 0) / avg
    # Spread score: 0 at ≤1.0×, linear to 1.0 at ≥2.0×
    spread_score = max(0.0, min(1.0, bid_spread - 1.0))
    # Competition is 75%, bid spread heating is 25%
    return max(0.0, min(100.0, comp * 75.0 + spread_score * 25.0))


def _compute_volume_score(search_volume: int) -> float:
    """
    Log10 scale to prevent mega-volume broad keywords from dominating.
    500 → ~54,  1k → 60,  10k → 80,  100k → 100.
    """
    import math
    v = int(search_volume or 0)
    if v <= 0:
        return 0.0
    return max(0.0, min(100.0, (math.log10(max(v, 1)) - 2.0) / 3.0 * 100.0))


def _compute_trend_score(trend_monthly: float, trend_quarterly: float,
                         trend_yearly: float) -> float:
    """
    Weighted recency: monthly 50%, quarterly 30%, yearly 20%.
    Normalises from range [-50, +200] → 0-100.
    Returns 50.0 (neutral) when all trend inputs are zero.
    """
    if not any([trend_monthly, trend_quarterly, trend_yearly]):
        return 50.0

    def _norm(val, lo=-50, hi=200):
        return max(0.0, min(1.0, (float(val or 0) - lo) / (hi - lo)))

    weighted = (
        _norm(trend_monthly)   * 0.50 +
        _norm(trend_quarterly) * 0.30 +
        _norm(trend_yearly)    * 0.20
    )
    return round(weighted * 100.0, 2)


def _compute_ssr(serp_item_types: list) -> float:
    """
    SERP Saturation Risk: sum of friction weights for SERP features present.
    Higher = more click-hostile SERP. > 1.5 should be rejected.
    """
    return sum(_SERP_FRICTION.get(item, 0.0) for item in (serp_item_types or []))


def classify_emerging(signals: dict, vertical_avg_cpc: float) -> str:
    """
    Classify keyword as EMERGING_HIGH, EMERGING, or None.
    Uses 5 confidence signals per master plan Section 5.
    KVSI >= 0.5 promotes EMERGING → EMERGING_HIGH.

    Args:
        signals: dict with keys: trend_monthly, trend_quarterly, trend_yearly,
                 keyword_difficulty (or kd), high_top_of_page_bid, cpc, monthly_searches
        vertical_avg_cpc: average CPC for this keyword's vertical

    Returns:
        'EMERGING_HIGH', 'EMERGING', or None
    """
    confidence_signals = []

    # Signal 1: Strong trend velocity
    if signals.get("trend_monthly", 0) >= 30:
        confidence_signals.append("trend_velocity")

    # Signal 2: KD is in price-discovery zone (not yet saturated organically)
    kd = signals.get("keyword_difficulty") or signals.get("kd", 0)
    if 15 <= kd <= 44:
        confidence_signals.append("kd_discovery_zone")

    # Signal 3: CPC above vertical average (advertisers already paying)
    cpc = signals.get("high_top_of_page_bid") or signals.get("cpc", 0)
    if vertical_avg_cpc > 0 and cpc >= vertical_avg_cpc * 0.80:
        confidence_signals.append("cpc_above_vertical")

    # Signal 4: Bid ceiling much higher than avg CPC = auction heating up
    avg_cpc = signals.get("cpc", 0) or 0
    high_bid = signals.get("high_top_of_page_bid", 0) or 0
    if avg_cpc > 0 and high_bid >= avg_cpc * 1.5:
        confidence_signals.append("auction_heating")

    # Signal 5: Quarterly growth accelerating beyond yearly (late-stage emerging)
    if signals.get("trend_quarterly", 0) >= 20 and signals.get("trend_yearly", 0) >= 10:
        confidence_signals.append("multi_period_growth")

    count = len(confidence_signals)
    if count >= 3:
        return "EMERGING_HIGH"
    if count >= 1:
        tag = "EMERGING"
        # KVSI promotion: confirms trend isn't a one-week spike
        kvsi_val = _compute_kvsi(signals)
        if kvsi_val >= 0.5:
            tag = "EMERGING_HIGH"
        return tag
    return None


def _apply_hard_gates(keyword: str, country: str, cpc_usd: float,
                      cpc_high_usd: float, competition: float,
                      search_volume: int, enrichment: dict) -> tuple:
    """
    Run pre-scoring rejection checks. Only 3 hard gates remain — all others
    are soft modifiers applied after composite scoring.
    Returns (passes: bool, rejection_reason: str).
    """
    main_intent         = enrichment.get("main_intent", "")
    serp_item_types     = enrichment.get("serp_item_types", [])
    is_another_language = enrichment.get("is_another_language", False)

    # Gate 1 — Wrong language for location (zero audience value)
    if is_another_language:
        return False, "wrong_language"

    # Gate 2 — Navigational intent only (brand searches = zero arbitrage)
    # Informational intent is allowed through — 0.6× soft modifier applied after scoring
    if main_intent == "navigational":
        return False, "navigational_intent"

    # Gate 3 — SERP Saturation Risk (raised to 2.0 — only extreme saturation rejected)
    ssr = _compute_ssr(serp_item_types)
    if ssr >= 2.0:
        return False, f"serp_saturation_risk (ssr={ssr:.2f})"

    return True, ""


def _apply_soft_modifiers(rsoc_score: float, kd: int, competition: float,
                          htpb: float, volume: int, country: str,
                          intent: str) -> float:
    """
    Apply RSOC soft modifiers as full-score multipliers after composite scoring.
    Replaces former hard gates 3, 5, 6, 7 with penalties that keep keywords visible.
    """
    score = rsoc_score

    # Low KD: early-stage keyword, advertisers may not have arrived yet
    if (kd or 0) > 0 and (kd or 0) < 15:
        score *= 0.70

    # Very thin auction: RPC will collapse quickly after small spend
    if (competition or 0) > 0 and (competition or 0) < 0.20:
        score *= 0.85

    # Below country CPC floor: lower monetisation potential but not zero
    floor = get_cpc_floor(country, "htpb")
    if (htpb or 0) < floor:
        score *= 0.75

    # Very low volume: minor penalty — RSOC buys clicks, doesn't wait for them
    if (volume or 0) < 200:
        score *= 0.90

    # Informational intent: underpriced by competitors but lower buyer signal
    if intent == "informational":
        score *= 0.60

    return round(score, 2)


def compute_rsoc_score(cpc_high_usd: float, competition: float,
                       search_volume: int, enrichment: dict,
                       scoring_profile: str = "EVERGREEN",
                       cpc_usd: float = 0.0) -> float:
    """
    Composite RSOC opportunity score 0-100.

    Formula: rpc_ceiling(htpb) × 0.40 + auction_health(comp, bid_spread) × 0.25
             + momentum(trends) × 0.20 + intent_quality × 0.15

    No volume or KD components — those are soft modifiers applied after this score.
    Informational intent 0.6× multiplier is applied externally in _apply_soft_modifiers().

    Args:
        cpc_high_usd:    high_top_of_page_bid (RPC ceiling proxy)
        competition:     paid competition float 0-1
        search_volume:   monthly search volume (unused in formula, kept for signature compat)
        enrichment:      dict from _fetch_dataforseo_labs_batch() (may be empty {})
        scoring_profile: "EVERGREEN" or "EMERGING"
        cpc_usd:         average CPC for bid_spread calculation (htpb / avg_cpc)

    Returns:
        float 0-100
    """
    if scoring_profile not in _RSOC_WEIGHTS:
        scoring_profile = "EVERGREEN"

    w = _RSOC_WEIGHTS[scoring_profile]

    main_intent       = enrichment.get("main_intent", "")
    secondary_intents = enrichment.get("secondary_intents", [])

    component_scores = {
        "cpc":            _compute_cpc_score(cpc_high_usd),
        "auction_health": _compute_auction_health(competition, cpc_high_usd, cpc_usd),
        "trend":          _compute_trend_score(
                              enrichment.get("trend_monthly", 0),
                              enrichment.get("trend_quarterly", 0),
                              enrichment.get("trend_yearly", 0),
                          ),
        "intent":         _compute_intent_score(main_intent, secondary_intents),
    }

    composite  = sum(component_scores.get(k, 0) * w[k] for k in w)
    serp_items = enrichment.get("serp_item_types", [])
    ssr        = _compute_ssr(serp_items)

    # Apply SSR as a post-score multiplier (hard gate rejects at ssr >= 2.0)
    if ssr >= 2.0:
        composite *= 0.0
    elif ssr >= 1.0:
        composite *= (2.0 - ssr)   # linear 1.0→2.0 maps multiplier 1.0→0.0

    return round(composite, 2)


def _compute_kvsi(enrichment: dict) -> float:
    """
    Keyword Volatility and Sustainability Index.
    Formula: (ΔV_YoY + ΔV_QoQ) / (σ(V_12m) + 1)

    High KVSI = growing steadily without erratic spikes = sustainable RSOC campaign.
    Provisional "sustainable" threshold: >= 0.5 (recalibrate after 30 days of data).

    Returns 0.0 if trend data is unavailable.
    """
    import statistics
    yoy  = float(enrichment.get("trend_yearly", 0) or 0)
    qoq  = float(enrichment.get("trend_quarterly", 0) or 0)
    monthly_searches = enrichment.get("monthly_searches", [])
    vols = [m.get("search_volume", 0) for m in (monthly_searches or [])[-12:]
            if m.get("search_volume")]
    stddev = statistics.stdev(vols) if len(vols) >= 2 else 0
    return round((yoy + qoq) / (stddev + 1), 4)


def tag_opportunity_v2(rsoc_score: float, cpc_usd: float,
                       competition: float, enrichment: dict,
                       vertical: str, country: str) -> str:
    """
    Extended opportunity tagger using rsoc_score (0-100 after soft modifiers).
    Returns one of:
        GOLDEN_OPPORTUNITY  — rsoc_score >= 65 (high CPC confirmed, deep auction)
        WATCH               — rsoc_score >= 35 (decent metrics, worth monitoring)
        EMERGING_HIGH       — classify_emerging() override (3+ trend signals)
        EMERGING            — classify_emerging() override (1-2 trend signals)
        LOW                 — rsoc_score < 40 and no emerging signals
        UNSCORED            — no metrics available
    """
    if rsoc_score >= 65:
        return "GOLDEN_OPPORTUNITY"
    if rsoc_score >= 35:
        return "WATCH"

    # —— New EMERGING detection ————————————————————————————————————————————————
    if not enrichment:
        return "LOW"

    trend_monthly  = float(enrichment.get("trend_monthly", 0) or 0)
    kd             = int(enrichment.get("kd", 0) or 0)
    kvsi           = _compute_kvsi(enrichment)

    # Look up vertical average CPC from vertical_cpc_reference.json
    # The file is a flat dict: {vertical: {tier_1: ..., avg_cpc: ...}, ...}
    vertical_data  = _VERTICAL_CPC_REF.get(vertical) or _VERTICAL_CPC_REF.get("general") or {}
    avg_cpc        = float(vertical_data.get("avg_cpc") or 4.0)

    # Core EMERGING trigger (all four must be true)
    triggers_met = (
        trend_monthly > 30
        and 25 <= kd < 50
        and cpc_usd > avg_cpc * 0.5
        and kvsi > 0
    )

    if not triggers_met:
        return "LOW"

    # Confidence boosters (each contributes +1)
    confidence = 0
    main_intent     = enrichment.get("main_intent", "")
    high_bid        = float(enrichment.get("cpc_labs") or cpc_usd or 0)
    trend_quarterly = float(enrichment.get("trend_quarterly", 0) or 0)

    if competition >= 0.50:              confidence += 1   # some advertiser density
    if high_bid > cpc_usd * 1.5:        confidence += 1   # bid ceiling rising
    if trend_quarterly > 20:             confidence += 1   # sustained multi-month growth
    if main_intent in ("commercial", "transactional"): confidence += 1

    return "EMERGING_HIGH" if confidence >= 3 else "EMERGING"
