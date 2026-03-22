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

# ── Dollar-based budget (replaces DAILY_API_BUDGET) ────────────────────────────
DFS_DAILY_BUDGET_USD     = 2.00  # Hard daily dollar cap. Tune after pricing confirmed.
                                  # Expected spend with Labs-only: $0.15-0.50/day
                                  # $2.00 provides safety margin for unexpected spikes.

DAILY_API_BUDGET         = 500   # Legacy task-count budget (kept for backward compat)
BUDGET_PRIORITY_ORDER    = [1, 2, 3, 4]   # tier 1 first when trimming to budget
HIGH_CONFIDENCE_PRIORITY = True  # within same tier, prioritize "high" confidence keywords

# ── Expansion result cap (secondary guard alongside dollar cap) ─────────────────
# Labs keyword_ideas has server-side 200-result cap, but this guards against
# unexpected behavior or future endpoint changes.
DFS_EXPAND_RESULTS_DAILY_CAP = 3000
DFS_SEEDS_PER_EXPAND_TASK    = 5    # Seeds per keyword_ideas/live call (was 20, smaller = earlier cap detection)

# ── DataForSEO endpoint costs (update from billing dashboard after first run) ───
DFS_ENDPOINT_COSTS = {
    "bulk_kd":                0.00001,   # $0.01 per 1,000 keywords
    "keyword_overview":       0.0000286, # $0.02 per 700 keywords
    "keyword_ideas":          0.01,      # $0.01 base cost per call
    "keyword_ideas_per_result": 0.0001,  # $0.0001 per result (max 200 = $0.02)
}

# ── Non-English market minimums ───────────────────────────────────────────────
# Applied to non-English expansion results before they enter the pipeline.
NON_ENGLISH_MIN_VOLUME = 500    # minimum monthly volume for non-English keywords
NON_ENGLISH_MIN_CPC    = 0.30   # minimum CPC (USD) for non-English keywords

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


# ── Country-specific CPC floors (Master Plan Section 4) ───────────────────────
# Per-country measured floors based on Google Ads CPC benchmarks (WordStream, Statista, 2024-2025).
# Two floors per country:
#   cpc  = floor for keyword_info.cpc field
#   htpb = floor for high_top_of_page_bid field (set at 2.5× CPC floor)
# Confidence: H = empirical data, M = calculated %, L = estimated

COUNTRY_CPC_FLOORS = {
    # ── Premium English Markets ────────────────────────────────────────────────
    "US": {"cpc": 2.50, "htpb": 6.25, "avg_cpc": 2.69, "confidence": "H",
           "notes": "WordStream 2025 baseline. Existing floor retained."},
    "AU": {"cpc": 2.35, "htpb": 5.90, "avg_cpc": 2.56, "confidence": "H",
           "notes": "WordStream: -5% vs US."},
    "GB": {"cpc": 2.10, "htpb": 5.25, "avg_cpc": 2.34, "confidence": "H",
           "notes": "WordStream: -13% vs US. Use 'GB' not 'UK' for DFS."},
    "NZ": {"cpc": 1.80, "htpb": 4.50, "avg_cpc": 2.00, "confidence": "M",
           "notes": "WordStream: listed in 'next 10 most expensive'. ~$2.00 est."},
    "CA": {"cpc": 1.75, "htpb": 4.40, "avg_cpc": 1.91, "confidence": "H",
           "notes": "WordStream: -29% vs US."},

    # ── Western Europe ─────────────────────────────────────────────────────────
    "AT": {"cpc": 2.40, "htpb": 6.00, "avg_cpc": 2.64, "confidence": "H",
           "notes": "WordStream: -2% vs US. Very close to US market depth."},
    "CH": {"cpc": 1.90, "htpb": 4.75, "avg_cpc": 2.13, "confidence": "H",
           "notes": "WordStream: -21% vs US."},
    "IT": {"cpc": 1.90, "htpb": 4.75, "avg_cpc": 2.10, "confidence": "M",
           "notes": "WordStream: top-10 most expensive. Estimated ~$2.10."},
    "DE": {"cpc": 1.70, "htpb": 4.25, "avg_cpc": 1.86, "confidence": "H",
           "notes": "WordStream: -31% vs US."},
    "NO": {"cpc": 1.65, "htpb": 4.10, "avg_cpc": 1.78, "confidence": "H",
           "notes": "WordStream: -34% vs US."},
    "IE": {"cpc": 1.45, "htpb": 3.65, "avg_cpc": 1.61, "confidence": "H",
           "notes": "WordStream: -40% vs US. Statista: €1.22/click."},
    "SE": {"cpc": 1.25, "htpb": 3.15, "avg_cpc": 1.37, "confidence": "H",
           "notes": "WordStream: -49% vs US."},
    "ES": {"cpc": 1.20, "htpb": 3.00, "avg_cpc": 1.35, "confidence": "H",
           "notes": "WordStream: -50% vs US."},
    "NL": {"cpc": 1.10, "htpb": 2.75, "avg_cpc": 1.18, "confidence": "H",
           "notes": "WordStream: -56% vs US."},
    "DK": {"cpc": 1.05, "htpb": 2.65, "avg_cpc": 1.16, "confidence": "H",
           "notes": "WordStream: -57% vs US."},
    "FR": {"cpc": 0.90, "htpb": 2.25, "avg_cpc": 0.97, "confidence": "H",
           "notes": "WordStream: -64% vs US. Lower than expected for G7."},
    "BE": {"cpc": 0.75, "htpb": 1.90, "avg_cpc": 0.83, "confidence": "H",
           "notes": "WordStream: -69% vs US."},

    # ── Middle East ────────────────────────────────────────────────────────────
    "AE": {"cpc": 2.60, "htpb": 6.50, "avg_cpc": 2.91, "confidence": "H",
           "notes": "WordStream: +8% vs US. Only country above US avg."},
    "SA": {"cpc": 1.00, "htpb": 2.50, "avg_cpc": 1.08, "confidence": "H",
           "notes": "WordStream: -60% vs US."},

    # ── Asia-Pacific ───────────────────────────────────────────────────────────
    "SG": {"cpc": 1.05, "htpb": 2.60, "avg_cpc": 1.13, "confidence": "H",
           "notes": "WordStream: -58% vs US. Premium APAC hub."},
    "HK": {"cpc": 1.65, "htpb": 4.10, "avg_cpc": 1.80, "confidence": "L",
           "notes": "Estimated. High-income city-state. Validate vs DFS."},
    "JP": {"cpc": 1.30, "htpb": 3.25, "avg_cpc": 1.43, "confidence": "H",
           "notes": "WordStream: -47% vs US. Yahoo Japan dominates."},
    "KR": {"cpc": 0.70, "htpb": 1.75, "avg_cpc": 0.75, "confidence": "H",
           "notes": "WordStream: -72% vs US. Naver dominates."},
    "TH": {"cpc": 1.00, "htpb": 2.50, "avg_cpc": 1.13, "confidence": "H",
           "notes": "WordStream: -58% vs US."},
    "MY": {"cpc": 0.60, "htpb": 1.50, "avg_cpc": 0.67, "confidence": "H",
           "notes": "WordStream: -75% vs US."},
    "PH": {"cpc": 0.60, "htpb": 1.50, "avg_cpc": 0.67, "confidence": "H",
           "notes": "WordStream: -75% vs US."},
    "ID": {"cpc": 0.30, "htpb": 0.75, "avg_cpc": 0.32, "confidence": "H",
           "notes": "Statista: $0.32 specific. Lowest in APAC."},
    "IN": {"cpc": 0.55, "htpb": 1.40, "avg_cpc": 0.62, "confidence": "H",
           "notes": "WordStream: -77% vs US. Scale play."},

    # ── Latin America ──────────────────────────────────────────────────────────
    "BR": {"cpc": 0.65, "htpb": 1.60, "avg_cpc": 0.70, "confidence": "M",
           "notes": "LATAM range $0.20-$1.50; Brazil highest. Est. ~$0.70."},
    "MX": {"cpc": 0.30, "htpb": 0.75, "avg_cpc": 0.35, "confidence": "L",
           "notes": "'~10x less than US' → ~$0.27. Using $0.35. Validate."},
    "AR": {"cpc": 0.55, "htpb": 1.35, "avg_cpc": 0.59, "confidence": "H",
           "notes": "WordStream: -78% vs US."},
    "CO": {"cpc": 0.40, "htpb": 1.00, "avg_cpc": 0.46, "confidence": "H",
           "notes": "WordStream: -83% vs US."},

    # ── Africa ─────────────────────────────────────────────────────────────────
    "ZA": {"cpc": 0.45, "htpb": 1.15, "avg_cpc": 0.51, "confidence": "H",
           "notes": "Statista: $0.51 specific (May 2023)."},
    "NG": {"cpc": 0.60, "htpb": 1.50, "avg_cpc": 0.66, "confidence": "H",
           "notes": "Statista: $0.66 specific. Competitive vs ZA."},

    # ── Eastern Europe ─────────────────────────────────────────────────────────
    "PL": {"cpc": 0.45, "htpb": 1.15, "avg_cpc": 0.51, "confidence": "H",
           "notes": "WordStream: -81% vs US."},
}

# Default floor for countries not in the table above
COUNTRY_CPC_FLOOR_DEFAULT = {"cpc": 0.50, "htpb": 1.25}


def get_cpc_floor(country_code: str, field: str = "cpc") -> float:
    """
    Returns the minimum viable CPC floor for a given country and field.
    
    Args:
        country_code: ISO 3166-1 alpha-2 country code (e.g. 'US', 'GB', 'AU')
        field: 'cpc' for keyword_info.cpc or 'htpb' for high_top_of_page_bid
    
    Returns:
        float: minimum CPC required for RSOC viability in that market
    
    Note: 'GB' is the correct code for United Kingdom (not 'UK').
    """
    country_data = COUNTRY_CPC_FLOORS.get(country_code.upper(), COUNTRY_CPC_FLOOR_DEFAULT)
    return country_data.get(field, COUNTRY_CPC_FLOOR_DEFAULT[field])


def get_floor_confidence(country_code: str) -> str:
    """Returns data confidence level: H (empirical), M (calculated %), L (estimated)."""
    return COUNTRY_CPC_FLOORS.get(country_code.upper(), {}).get("confidence", "L")
