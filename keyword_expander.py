# =============================================================================
# keyword_expander.py  —  Stage 2.5: Free keyword expansion via Google Ads API
#
# Takes explosive trends, expands each into 10-50 commercial keyword variations
# using Google's generateKeywordIdeas endpoint (free, 15,000 ops/day on Basic Access).
# Returns pre-filtered keywords with Google CPC/volume/competition data.
#
# The 3-bucket classifier separates results into:
#   BUCKET_A — high confidence (skip DataForSEO, use Google CPC directly)
#   BUCKET_B — needs DataForSEO validation
#   BUCKET_C — discarded (CPC=0 or below volume threshold)
#
# Credentials required in ~/.openclaw/.env:
#   GOOGLE_ADS_DEVELOPER_TOKEN
#   GOOGLE_ADS_CLIENT_ID
#   GOOGLE_ADS_CLIENT_SECRET
#   GOOGLE_ADS_REFRESH_TOKEN
#   GOOGLE_ADS_CUSTOMER_ID
#   GOOGLE_ADS_LOGIN_CUSTOMER_ID  (optional, for MCC accounts)
#
# Run scripts/google_ads_setup.py once to generate the refresh token.
# =============================================================================

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

BASE      = Path("/Users/newmac/.openclaw/workspace")
ENV_FILE  = Path.home() / ".openclaw" / ".env"
sys.path.insert(0, str(BASE))

from country_config import COUNTRY_CONFIG, DEFAULT_COUNTRY

# ── Load .env ─────────────────────────────────────────────────────────────────

def _load_env(path: Path) -> dict:
    env = {}
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return env

_env = _load_env(ENV_FILE)
os.environ.update(_env)

# ── Credentials ───────────────────────────────────────────────────────────────

GADS_DEV_TOKEN       = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GADS_CLIENT_ID       = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
GADS_CLIENT_SECRET   = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
GADS_REFRESH_TOKEN   = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
GADS_CUSTOMER_ID     = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace("-", "")
GADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")


def check_credentials() -> bool:
    required = {
        "GOOGLE_ADS_DEVELOPER_TOKEN": GADS_DEV_TOKEN,
        "GOOGLE_ADS_CLIENT_ID":       GADS_CLIENT_ID,
        "GOOGLE_ADS_CLIENT_SECRET":   GADS_CLIENT_SECRET,
        "GOOGLE_ADS_REFRESH_TOKEN":   GADS_REFRESH_TOKEN,
        "GOOGLE_ADS_CUSTOMER_ID":     GADS_CUSTOMER_ID,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"  [keyword_expander] Google Ads credentials missing: {', '.join(missing)}")
        print("  Run scripts/google_ads_setup.py to configure.")
        return False
    return True


def _build_client():
    from google.ads.googleads.client import GoogleAdsClient
    cfg = {
        "developer_token":   GADS_DEV_TOKEN,
        "client_id":         GADS_CLIENT_ID,
        "client_secret":     GADS_CLIENT_SECRET,
        "refresh_token":     GADS_REFRESH_TOKEN,
        "use_proto_plus":    True,
    }
    if GADS_LOGIN_CUSTOMER_ID:
        cfg["login_customer_id"] = GADS_LOGIN_CUSTOMER_ID
    return GoogleAdsClient.load_from_dict(cfg)


# ── Geo mapping: country ISO → (geo_target_constant_id, language_constant_id) ─
# language_id: 1000=English, 1001=German, 1002=French, 1003=Spanish, 1005=Japanese,
#              1014=Portuguese, 1018=Korean, 1024=Dutch, 1025=Italian, etc.

GEO_LANG_MAP = {
    "US": (2840, 1000), "GB": (2826, 1000), "UK": (2826, 1000),
    "CA": (2124, 1000), "AU": (2036, 1000), "IE": (2372, 1000),
    "NZ": (2554, 1000), "ZA": (2710, 1000), "IN": (2356, 1000),
    "PH": (2608, 1000), "SG": (2702, 1000), "MY": (2458, 1000),
    "NG": (2566, 1000), "KE": (2404, 1000),
    "DE": (2276, 1001), "AT": (2040, 1001), "CH": (2756, 1001),
    "FR": (2250, 1002), "BE": (2056, 1002),
    "ES": (2724, 1003), "MX": (2484, 1003), "AR": (2032, 1003),
    "CO": (2170, 1003), "CL": (2152, 1003), "PE": (2604, 1003),
    "JP": (2392, 1005), "BR": (2076, 1014), "PT": (2620, 1014),
    "KR": (2410, 1018), "NL": (2528, 1024), "IT": (2380, 1025),
    "PL": (2616, 1030), "RO": (2642, 1032), "HU": (2348, 1028),
    "CZ": (2203, 1021), "GR": (2300, 1022), "TR": (2792, 1037),
    "TH": (2764, 1044), "VN": (2704, 1040), "ID": (2360, 1002),  # 1002 closest for ID
    "UA": (2804, 1036), "IL": (2376, 1027), "EG": (2818, 1019),
    "SA": (2682, 1019), "HK": (2344, 1000), "TW": (2158, 1000),
    "SE": (2752, 1035), "NO": (2578, 1013), "DK": (2208, 1009),
    "FI": (2246, 1023),
}

COMPETITION_MAP = {"LOW": 0.1, "MEDIUM": 0.5, "HIGH": 0.9, "UNSPECIFIED": None}

# Rate limiting: 1 request / 2 seconds, retry on RESOURCE_EXHAUSTED
_RATE_LIMIT_INTERVAL = 2.0
_last_call_time      = 0.0

# Max keywords per call (Google accepts up to 20 seed keywords per request)
MAX_SEEDS_PER_CALL = 20


# ── Core expansion function ───────────────────────────────────────────────────

def _expand_batch(client, seeds: list, geo_id: int, lang_id: int) -> list:
    """
    Call generateKeywordIdeas with up to MAX_SEEDS_PER_CALL seed keywords.
    Returns raw list of idea dicts.
    """
    global _last_call_time
    from google.ads.googleads.errors import GoogleAdsException

    # Rate limiting
    elapsed = time.time() - _last_call_time
    if elapsed < _RATE_LIMIT_INTERVAL:
        time.sleep(_RATE_LIMIT_INTERVAL - elapsed)
    _last_call_time = time.time()

    idea_svc = client.get_service("KeywordPlanIdeaService")
    request  = client.get_type("GenerateKeywordIdeasRequest")

    request.customer_id              = GADS_CUSTOMER_ID
    request.language                 = f"languageConstants/{lang_id}"
    request.geo_target_constants     = [f"geoTargetConstants/{geo_id}"]
    request.include_adult_keywords   = False
    request.keyword_plan_network     = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
    for seed in seeds:
        request.keyword_seed.keywords.append(seed)

    # Retry on RESOURCE_EXHAUSTED with exponential backoff
    backoff = 5
    for attempt in range(4):
        try:
            response = idea_svc.generate_keyword_ideas(request=request)
            ideas = []
            for idea in response:
                m = idea.keyword_idea_metrics
                comp_name = m.competition.name if hasattr(m.competition, "name") else str(m.competition)
                competition_val = COMPETITION_MAP.get(comp_name)

                low_bid  = round(getattr(m, "low_top_of_page_bid_micros", 0) / 1_000_000, 2)
                high_bid = round(getattr(m, "high_top_of_page_bid_micros", 0) / 1_000_000, 2)

                # Monthly search volume history
                monthly = []
                if hasattr(m, "monthly_search_volumes"):
                    for mv in m.monthly_search_volumes:
                        monthly.append({
                            "year": mv.year, "month": mv.month.name,
                            "volume": mv.monthly_searches,
                        })

                ideas.append({
                    "text":                  idea.text,
                    "avg_monthly_searches":  int(m.avg_monthly_searches or 0),
                    "competition":           comp_name,
                    "competition_index":     int(getattr(m, "competition_index", 0) or 0),
                    "cpc_low":               low_bid,
                    "cpc_high":              high_bid,
                    "estimated_cpc":         round((low_bid + high_bid) / 2, 2),
                    "monthly_searches":      monthly,
                })
            return ideas

        except GoogleAdsException as ex:
            code = ex.error.code().name
            if "RESOURCE_EXHAUSTED" in code and attempt < 3:
                print(f"  [keyword_expander] RESOURCE_EXHAUSTED — backing off {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            raise

    return []


def _is_branded(text: str) -> bool:
    """
    Heuristic: detect branded keywords by checking against a hardcoded list
    of common brand name patterns. Not exhaustive — catches obvious ones.
    """
    BRAND_SIGNALS = (
        "amazon", "google", "apple", "microsoft", "facebook", "meta",
        "instagram", "tiktok", "youtube", "twitter", "x.com",
        "netflix", "spotify", "airbnb", "uber", "lyft", "paypal",
        "ebay", "walmart", "target", "costco", "ikea", "mcdonalds",
        "starbucks", "samsung", "sony", "nike", "adidas",
    )
    lower = text.lower()
    return any(b in lower for b in BRAND_SIGNALS)


def _passes_prefilter(idea: dict, country: str) -> bool:
    """
    Pre-filter before LLM or DataForSEO:
    - CPC > 0
    - volume >= country min_volume
    - competition is known (not UNSPECIFIED)
    - not branded
    - 2+ words
    """
    cfg = COUNTRY_CONFIG.get(country.upper(), DEFAULT_COUNTRY)

    if idea["estimated_cpc"] <= 0:
        return False
    if idea["avg_monthly_searches"] < cfg["min_volume"]:
        return False
    if idea["competition"] == "UNSPECIFIED":
        return False
    if _is_branded(idea["text"]):
        return False
    if len(idea["text"].split()) < 2:
        return False
    return True


# ── 3-Bucket Classifier ───────────────────────────────────────────────────────

def classify_keyword(idea: dict, country: str) -> str:
    """
    Returns 'A', 'B', or 'C'.

    Bucket A: Google says it's golden — skip DataForSEO entirely
    Bucket B: Promising but needs DataForSEO validation
    Bucket C: Discard
    """
    cfg = COUNTRY_CONFIG.get(country.upper(), DEFAULT_COUNTRY)
    min_cpc = cfg["min_cpc"]

    if idea["estimated_cpc"] <= 0 or idea["avg_monthly_searches"] < cfg["min_volume"]:
        return "C"
    if idea["competition"] == "UNSPECIFIED":
        return "C"

    # Bucket A: high-confidence — Google competition_index ≥ 70, CPC ≥ 2× min, volume ≥ 2× min
    if (idea["competition_index"] >= 70
            and idea["estimated_cpc"] >= min_cpc * 2
            and idea["avg_monthly_searches"] >= cfg["min_volume"] * 2):
        return "A"

    # Bucket B: CPC > 0 but doesn't meet the A threshold
    if idea["estimated_cpc"] > 0:
        return "B"

    return "C"


# ── Main expansion entry point ────────────────────────────────────────────────

def expand(trends: list) -> tuple:
    """
    Take a list of explosive trend dicts, expand each into keyword ideas.

    Returns: (bucket_a, bucket_b) — two lists of enriched keyword dicts.
    Bucket C keywords are discarded silently.

    Each returned dict has:
      keyword, country, expansion_seed, google_cpc_low, google_cpc_high,
      google_estimated_cpc, google_volume, google_competition,
      google_competition_index, monthly_search_history, is_branded,
      metrics_source, needs_dataforseo_validation
    """
    if not check_credentials():
        return [], []

    try:
        client = _build_client()
    except Exception as e:
        print(f"  [keyword_expander] Failed to build GoogleAdsClient: {e}")
        return [], []

    # Group seeds by (country, geo_id, lang_id)
    by_locale: dict = {}
    for trend in trends:
        seed    = trend.get("term") or trend.get("keyword") or trend.get("title", "")
        country = (trend.get("geo") or trend.get("country") or "US").upper()
        if not seed:
            continue
        geo_id, lang_id = GEO_LANG_MAP.get(country, (2840, 1000))
        key = (country, geo_id, lang_id)
        by_locale.setdefault(key, []).append(seed)

    bucket_a, bucket_b = [], []
    total_ideas = 0
    total_passed = 0
    api_calls = 0
    errors = 0

    for (country, geo_id, lang_id), seeds in by_locale.items():
        # Batch seeds: up to MAX_SEEDS_PER_CALL per API call
        for i in range(0, len(seeds), MAX_SEEDS_PER_CALL):
            batch = seeds[i : i + MAX_SEEDS_PER_CALL]
            try:
                ideas = _expand_batch(client, batch, geo_id, lang_id)
                api_calls += 1
            except Exception as e:
                print(f"  [keyword_expander] Error [{country}] batch {i // MAX_SEEDS_PER_CALL + 1}: {e}")
                errors += 1
                continue

            total_ideas += len(ideas)

            for idea in ideas:
                if not _passes_prefilter(idea, country):
                    continue

                total_passed += 1
                bucket = classify_keyword(idea, country)
                if bucket == "C":
                    continue

                record = {
                    "keyword":                idea["text"],
                    "country":                country,
                    "expansion_seed":         batch[0] if len(batch) == 1 else ", ".join(batch[:3]),
                    "google_cpc_low":         idea["cpc_low"],
                    "google_cpc_high":        idea["cpc_high"],
                    "google_estimated_cpc":   idea["estimated_cpc"],
                    "google_volume":          idea["avg_monthly_searches"],
                    "google_competition":     idea["competition"],
                    "google_competition_index": idea["competition_index"],
                    "monthly_search_history": idea["monthly_searches"],
                    "is_branded":             _is_branded(idea["text"]),
                    "metrics_source":         "google_keyword_planner",
                    "needs_dataforseo_validation": bucket == "B",
                    "source":                 "google_keyword_planner",
                    "fetched_at":             datetime.now().isoformat(),
                }

                if bucket == "A":
                    bucket_a.append(record)
                else:
                    bucket_b.append(record)

    discarded = total_ideas - total_passed
    print(f"[keyword_expander] {api_calls} API calls "
          f"({errors} errors) → {total_ideas} ideas → "
          f"pre-filter kept {total_passed} ({discarded} discarded) → "
          f"A={len(bucket_a)}, B={len(bucket_b)}")

    return bucket_a, bucket_b


# ── CLI for standalone testing ────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    INPUT  = BASE / "explosive_trends.json"
    OUTPUT = BASE / "expanded_keywords.json"

    if not INPUT.exists():
        print(f"⚠️  {INPUT} not found — run trends_postprocess.py first")
        raise SystemExit(1)

    trends = json.loads(INPUT.read_text())
    print(f"[keyword_expander] Expanding {len(trends)} trends...")

    bucket_a, bucket_b = expand(trends)
    all_keywords = bucket_a + bucket_b

    OUTPUT.write_text(json.dumps(all_keywords, indent=2))
    print(f"✅ keyword_expander complete: {len(all_keywords)} keywords → {OUTPUT.name}")
    print(f"   Bucket A (skip DataForSEO): {len(bucket_a)}")
    print(f"   Bucket B (needs DataForSEO): {len(bucket_b)}")
