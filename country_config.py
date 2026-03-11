# =============================================================================
# country_config.py  —  Per-country arbitrage thresholds and efficiency factors
#
# Edit this file to tune search arbitrage economics per market.
# DO NOT modify keyword_extractor.py to change thresholds — change them here.
#
# Fields per country:
#   min_cpc    (float)  — minimum CPC in USD to accept a keyword
#   min_volume (int)    — minimum monthly search volume to accept a keyword
#   efficiency (float)  — traffic acquisition efficiency multiplier vs US baseline.
#                         Higher = traffic is cheaper relative to CPC earnings.
#                         Used in: opportunity_score = cpc × volume × efficiency
#   tier       (int)    — 1=premium, 2=moderate, 3=emerging, 4=frontier
#
# opportunity_score lets you rank all keywords across all countries on one scale:
#   US: CPC $2.00 × Vol 5,000 × Eff 1.0  = 10,000
#   IN: CPC $0.08 × Vol 200k  × Eff 5.0  = 80,000   ← IN wins despite low CPC
#
# ASSUMED_AD_CTR: fraction of page visitors who click an ad.
# Used to compute estimated_rpm = cpc × ASSUMED_AD_CTR × 1000
# Typical rSOC/content-arbitrage range: 0.10 – 0.20
# Replace with your observed CTR once you have real data.
# =============================================================================

ASSUMED_AD_CTR = 0.15   # 15% assumed ad click-through rate on content pages

# ── DataForSEO cost optimization ──────────────────────────────────────────────
# Edit these to control API spend. Do NOT hardcode them in keyword_extractor.py.

CACHE_TTL_HOURS          = 168   # 7 days — CPC is monthly aggregate, stable week-to-week
CACHE_TTL_MIN_HOURS      = 24    # absolute minimum — never re-fetch within 24h even if forced
ONCE_PER_DAY_DFS         = True  # DataForSEO fires on the FIRST pipeline run each day only.
                                  # Subsequent runs (every 6h) are served from cache or deferred.
                                  # Set to False to allow multiple runs per day (watch costs!).
DAILY_API_BUDGET         = 75    # max DataForSEO keyword lookups per calendar day.
                                  # Cost guide at ~$0.08/keyword:
                                  #   40  → ~$3/day
                                  #   75  → ~$6/day  ← current
                                  #   130 → ~$10/day
BUDGET_PRIORITY_ORDER    = [1, 2, 3, 4]   # tier 1 first when trimming to budget
HIGH_CONFIDENCE_PRIORITY = True  # within same tier, prioritize "high" confidence keywords

# Default for countries not listed below
DEFAULT_COUNTRY = {
    "min_cpc":    0.10,
    "min_volume": 500,
    "efficiency": 2.0,
    "tier":       3,
}

COUNTRY_CONFIG = {

    # ── TIER 1: High CPC, expensive traffic ───────────────────────────────────
    # Premium English-speaking and Western European markets.
    # Advertisers bid aggressively. Traffic is expensive, so we need strong CPC.

    "US": {"min_cpc": 0.50, "min_volume": 500,  "efficiency": 1.0, "tier": 1},
    "GB": {"min_cpc": 0.40, "min_volume": 500,  "efficiency": 1.1, "tier": 1},
    "UK": {"min_cpc": 0.40, "min_volume": 500,  "efficiency": 1.1, "tier": 1},  # alias
    "CA": {"min_cpc": 0.40, "min_volume": 400,  "efficiency": 1.2, "tier": 1},
    "AU": {"min_cpc": 0.40, "min_volume": 300,  "efficiency": 1.2, "tier": 1},
    "DE": {"min_cpc": 0.35, "min_volume": 400,  "efficiency": 1.3, "tier": 1},
    "FR": {"min_cpc": 0.30, "min_volume": 400,  "efficiency": 1.3, "tier": 1},

    # ── TIER 2: Moderate CPC, moderate traffic costs ──────────────────────────
    # Established ad markets, lower competition than tier 1.
    # Good arbitrage: traffic noticeably cheaper while CPCs remain decent.

    "JP": {"min_cpc": 0.25, "min_volume": 400,  "efficiency": 1.8, "tier": 2},
    "KR": {"min_cpc": 0.20, "min_volume": 400,  "efficiency": 2.0, "tier": 2},
    "IT": {"min_cpc": 0.20, "min_volume": 400,  "efficiency": 2.0, "tier": 2},
    "ES": {"min_cpc": 0.18, "min_volume": 400,  "efficiency": 2.2, "tier": 2},
    "NL": {"min_cpc": 0.25, "min_volume": 300,  "efficiency": 1.8, "tier": 2},
    "SE": {"min_cpc": 0.25, "min_volume": 250,  "efficiency": 1.8, "tier": 2},
    "IL": {"min_cpc": 0.30, "min_volume": 200,  "efficiency": 1.5, "tier": 2},
    "AT": {"min_cpc": 0.25, "min_volume": 250,  "efficiency": 1.8, "tier": 2},
    "BE": {"min_cpc": 0.20, "min_volume": 250,  "efficiency": 2.0, "tier": 2},
    "CH": {"min_cpc": 0.30, "min_volume": 200,  "efficiency": 1.6, "tier": 2},
    "IE": {"min_cpc": 0.30, "min_volume": 200,  "efficiency": 1.6, "tier": 2},
    "NO": {"min_cpc": 0.25, "min_volume": 200,  "efficiency": 1.8, "tier": 2},
    "DK": {"min_cpc": 0.25, "min_volume": 200,  "efficiency": 1.8, "tier": 2},
    "FI": {"min_cpc": 0.20, "min_volume": 200,  "efficiency": 2.0, "tier": 2},
    "SG": {"min_cpc": 0.25, "min_volume": 300,  "efficiency": 1.8, "tier": 2},
    "NZ": {"min_cpc": 0.30, "min_volume": 200,  "efficiency": 1.6, "tier": 2},
    "ZA": {"min_cpc": 0.15, "min_volume": 400,  "efficiency": 2.5, "tier": 2},
    "HK": {"min_cpc": 0.20, "min_volume": 300,  "efficiency": 2.0, "tier": 2},
    "TW": {"min_cpc": 0.15, "min_volume": 400,  "efficiency": 2.2, "tier": 2},

    # ── TIER 3: Low CPC, cheap traffic — high ratio opportunity ──────────────
    # Arbitrage is strongest here. CPCs look small but traffic is nearly free.
    # $0.08 CPC with near-zero acquisition cost = pure margin at scale.
    # Higher volume floors needed because per-click revenue is low.

    "BR": {"min_cpc": 0.08, "min_volume": 1000, "efficiency": 3.0, "tier": 3},
    "MX": {"min_cpc": 0.06, "min_volume": 1000, "efficiency": 3.5, "tier": 3},
    "IN": {"min_cpc": 0.03, "min_volume": 2000, "efficiency": 5.0, "tier": 3},
    "ID": {"min_cpc": 0.03, "min_volume": 1500, "efficiency": 5.0, "tier": 3},
    "TH": {"min_cpc": 0.04, "min_volume": 1000, "efficiency": 4.5, "tier": 3},
    "PH": {"min_cpc": 0.02, "min_volume": 1500, "efficiency": 5.5, "tier": 3},
    "VN": {"min_cpc": 0.02, "min_volume": 1500, "efficiency": 5.5, "tier": 3},
    "PL": {"min_cpc": 0.10, "min_volume": 800,  "efficiency": 2.8, "tier": 3},
    "TR": {"min_cpc": 0.05, "min_volume": 1000, "efficiency": 4.0, "tier": 3},
    "CO": {"min_cpc": 0.05, "min_volume": 800,  "efficiency": 4.0, "tier": 3},
    "AR": {"min_cpc": 0.04, "min_volume": 800,  "efficiency": 4.5, "tier": 3},
    "CL": {"min_cpc": 0.08, "min_volume": 600,  "efficiency": 3.0, "tier": 3},
    "MY": {"min_cpc": 0.05, "min_volume": 800,  "efficiency": 4.0, "tier": 3},
    "PE": {"min_cpc": 0.04, "min_volume": 800,  "efficiency": 4.5, "tier": 3},
    "RO": {"min_cpc": 0.08, "min_volume": 600,  "efficiency": 3.0, "tier": 3},
    "HU": {"min_cpc": 0.08, "min_volume": 500,  "efficiency": 3.0, "tier": 3},
    "CZ": {"min_cpc": 0.10, "min_volume": 500,  "efficiency": 2.8, "tier": 3},
    "GR": {"min_cpc": 0.10, "min_volume": 400,  "efficiency": 2.8, "tier": 3},
    "PT": {"min_cpc": 0.10, "min_volume": 400,  "efficiency": 2.8, "tier": 3},
    "UA": {"min_cpc": 0.03, "min_volume": 1500, "efficiency": 5.0, "tier": 3},

    # ── TIER 4: Very low CPC, very cheap traffic ─────────────────────────────
    # Emerging digital ad markets. Need massive volume to justify effort.
    # Worth pursuing at scale — nearly zero content production cost.

    "NG": {"min_cpc": 0.02, "min_volume": 3000, "efficiency": 6.0, "tier": 4},
    "KE": {"min_cpc": 0.02, "min_volume": 2000, "efficiency": 6.0, "tier": 4},
    "EG": {"min_cpc": 0.02, "min_volume": 2000, "efficiency": 6.0, "tier": 4},
    "SA": {"min_cpc": 0.05, "min_volume": 1500, "efficiency": 4.0, "tier": 4},
    "BD": {"min_cpc": 0.01, "min_volume": 3000, "efficiency": 7.0, "tier": 4},
    "PK": {"min_cpc": 0.01, "min_volume": 3000, "efficiency": 7.0, "tier": 4},
    "GH": {"min_cpc": 0.02, "min_volume": 2000, "efficiency": 6.0, "tier": 4},
}


def get_country_tier(country_code: str) -> int:
    """Return the tier for a country code. Used by validation.py and dashboard_builder.py."""
    cfg = COUNTRY_CONFIG.get(country_code.upper(), DEFAULT_COUNTRY)
    return cfg["tier"]
