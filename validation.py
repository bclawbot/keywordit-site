# =============================================================================
# validation.py  —  Stage 4: Keyword metrics + arbitrage scoring
#
# Provider priority: Google Ads Keyword Planner → SEMrush → DataForSEO
# NO MOCK DATA. If no provider is configured, opportunities are saved as
# UNSCORED and a setup guide is printed.
#
# ── Google Ads Keyword Planner setup (free) ──────────────────────────────────
# 1. Create a Google Ads account at https://ads.google.com
# 2. Apply for API access: https://developers.google.com/google-ads/api/docs/get-started/introduction
# 3. Create OAuth2 credentials at https://console.cloud.google.com
#    — Enable "Google Ads API", create a Desktop OAuth2 Client ID
#    — One-time token flow:
#        pip install google-auth-oauthlib
#        python3 -c "
#          from google_auth_oauthlib.flow import InstalledAppFlow
#          flow = InstalledAppFlow.from_client_secrets_file('client_secrets.json',
#              scopes=['https://www.googleapis.com/auth/adwords'])
#          creds = flow.run_local_server()
#          print('REFRESH TOKEN:', creds.refresh_token)
#        "
# 4. Export:
#    export GOOGLE_ADS_CLIENT_ID=...
#    export GOOGLE_ADS_CLIENT_SECRET=...
#    export GOOGLE_ADS_REFRESH_TOKEN=...
#    export GOOGLE_ADS_DEVELOPER_TOKEN=...
#    export GOOGLE_ADS_CUSTOMER_ID=...   # digits only, no dashes
#
# ── DataForSEO setup (~$0.002/request) ───────────────────────────────────────
#    export DATAFORSEO_LOGIN=your@email.com
#    export DATAFORSEO_PASSWORD=yourpassword
# =============================================================================

import json
import os
import re
import sqlite3
import sys
import base64
import time
from pathlib import Path
from datetime import datetime, timedelta

import requests
from country_config import get_country_tier as _country_tier, get_cpc_floor, DFS_ENDPOINT_COSTS, DFS_DAILY_BUDGET_USD
from cpc_cache import pre_flight_budget_check, increment_usd_spent

# NOTE: Google Ads API versions deprecate quarterly.
# Check https://developers.google.com/google-ads/api/docs/sunset-dates
# and update the version in GoogleAdsClient calls if needed.
# We intentionally do NOT pin a version here — the SDK default tracks the latest stable.

# ── Cache & budget configuration ──────────────────────────────────────────────
CACHE_DB         = Path("/Users/newmac/.openclaw/workspace/cpc_cache.db")
CACHE_TTL_HOURS  = 168   # 7 days — CPC data is monthly aggregate, changes slowly
DAILY_API_BUDGET = int(os.environ.get("DATAFORSEO_DAILY_BUDGET", "500"))


# ── DataForSEO rate-limit state ───────────────────────────────────────────────
DFS_MAX_WORDS  = 7
DFS_MIN_DELAY  = 6.0
_dfs_last_call = 0.0
DFS_LABS_BASE  = "https://api.dataforseo.com/v3/dataforseo_labs/google"


# ── Layer 1: Keyword normalization (comparison only — never sent to API) ──────

def _normalize_keyword(text: str) -> str:
    """Normalize for deduplication comparison only. Original keyword is always sent to the API."""
    kw = text.lower().strip()
    kw = re.sub(r"\s+", " ", kw)
    # Remove trailing current year ("best vpn 2026" → "best vpn")
    kw = re.sub(r"\s+\b" + str(datetime.now().year) + r"\b\s*$", "", kw)
    # Remove leading filler articles
    kw = re.sub(r"^(the|a|an)\s+", "", kw)
    # Strip punctuation for comparison
    kw = re.sub(r"[-.,?!'\"()]", "", kw)
    return kw.strip()


def _clean_keyword(text: str) -> str:
    """Strip non-ASCII and characters DataForSEO rejects. Truncate to DFS_MAX_WORDS."""
    ascii_only = text.encode("ascii", errors="ignore").decode()
    cleaned = re.sub(r"[\"'`,;:()\[\]{}!?]", " ", ascii_only)
    cleaned = re.sub(r"[,:]", " ", cleaned)
    words = cleaned.split()
    return " ".join(words[:DFS_MAX_WORDS]).strip()


# ── Layer 2: SQLite persistent cache ──────────────────────────────────────────

def _init_cache_db():
    """Create cache tables if they don't exist, then migrate schema."""
    con = sqlite3.connect(CACHE_DB)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS keyword_cpc_cache (
            keyword        TEXT NOT NULL,
            country        TEXT NOT NULL,
            cpc            REAL,
            search_volume  INTEGER,
            competition    REAL,
            fetched_at     TEXT NOT NULL,
            PRIMARY KEY (keyword, country)
        );
        CREATE INDEX IF NOT EXISTS idx_cache_lookup
            ON keyword_cpc_cache (keyword, country, fetched_at);

        CREATE TABLE IF NOT EXISTS api_usage (
            date     TEXT PRIMARY KEY,
            lookups  INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS deferred_keyword_lookups (
            keyword     TEXT NOT NULL,
            country     TEXT NOT NULL,
            tier        INTEGER,
            deferred_at TEXT NOT NULL,
            PRIMARY KEY (keyword, country)
        );
    """)
    # Migrate api_usage: add columns that may be missing from older schema
    cols = {row[1] for row in con.execute("PRAGMA table_info(api_usage)").fetchall()}
    if "expand_results" not in cols:
        con.execute("ALTER TABLE api_usage ADD COLUMN expand_results INTEGER NOT NULL DEFAULT 0")
    if "usd_spent_today" not in cols:
        con.execute("ALTER TABLE api_usage ADD COLUMN usd_spent_today REAL NOT NULL DEFAULT 0.0")
    if "endpoint_breakdown" not in cols:
        con.execute("ALTER TABLE api_usage ADD COLUMN endpoint_breakdown TEXT DEFAULT '{}'")
    con.commit()
    con.close()


def _cache_lookup_batch(pairs: list) -> tuple:
    """
    Batch lookup (keyword, country) pairs against the persistent cache.
    Uses a temp table JOIN — single query regardless of batch size.
    Returns:
        hits:   {(keyword, country): metrics_dict}
        misses: [(keyword, country), ...]
    """
    if not pairs:
        return {}, []

    cutoff = (datetime.now() - timedelta(hours=CACHE_TTL_HOURS)).isoformat()
    con = sqlite3.connect(CACHE_DB)
    con.row_factory = sqlite3.Row

    con.execute("CREATE TEMPORARY TABLE _lookup (keyword TEXT, country TEXT)")
    con.executemany("INSERT INTO _lookup VALUES (?, ?)", pairs)

    rows = con.execute(
        "SELECT c.keyword, c.country, c.cpc, c.search_volume, c.competition, c.fetched_at "
        "FROM keyword_cpc_cache c "
        "JOIN _lookup l ON c.keyword = l.keyword AND c.country = l.country "
        "WHERE c.fetched_at > ?",
        (cutoff,)
    ).fetchall()
    con.close()

    hits = {}
    for row in rows:
        hours_ago = round(
            (datetime.now() - datetime.fromisoformat(row["fetched_at"])).total_seconds() / 3600, 1
        )
        hits[(row["keyword"], row["country"])] = {
            "search_volume": row["search_volume"] or 0,
            "cpc_usd":       row["cpc"] or 0.0,
            "competition":   row["competition"] or 0.5,
            "source":        f"cache ({hours_ago}h ago)",
        }

    hit_keys = set(hits.keys())
    misses   = [(kw, c) for kw, c in pairs if (kw, c) not in hit_keys]
    return hits, misses


def _cache_write_back(keyword: str, country: str, metrics: dict):
    """Upsert fresh metrics into the persistent cache."""
    con = sqlite3.connect(CACHE_DB)
    con.execute(
        "INSERT INTO keyword_cpc_cache "
        "(keyword, country, cpc, search_volume, competition, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(keyword, country) DO UPDATE SET "
        "  cpc          = excluded.cpc, "
        "  search_volume = excluded.search_volume, "
        "  competition  = excluded.competition, "
        "  fetched_at   = excluded.fetched_at",
        (keyword, country,
         metrics.get("cpc_usd"), metrics.get("search_volume"),
         metrics.get("competition"), datetime.now().isoformat())
    )
    con.commit()
    con.close()


def _cache_cleanup():
    """Garbage-collect cache entries older than 30 days."""
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    con = sqlite3.connect(CACHE_DB)
    deleted = con.execute(
        "DELETE FROM keyword_cpc_cache WHERE fetched_at < ?", (cutoff,)
    ).rowcount
    con.commit()
    con.close()
    return deleted


# ── Layer 3: Budget gate ───────────────────────────────────────────────────────

def _get_today_usage() -> int:
    today = datetime.now().strftime("%Y-%m-%d")
    con   = sqlite3.connect(CACHE_DB)
    row   = con.execute("SELECT lookups FROM api_usage WHERE date = ?", (today,)).fetchone()
    con.close()
    return row[0] if row else 0


def _increment_usage(count: int):
    if count <= 0:
        return
    today = datetime.now().strftime("%Y-%m-%d")
    con   = sqlite3.connect(CACHE_DB)
    con.execute(
        "INSERT INTO api_usage (date, lookups) VALUES (?, ?) "
        "ON CONFLICT(date) DO UPDATE SET lookups = api_usage.lookups + ?",
        (today, count, count)
    )
    con.commit()
    con.close()


def _budget_gate(misses: list, remaining: int) -> tuple:
    """
    Split misses into (approved, deferred) based on remaining daily budget.
    Approved list is sorted: Tier 1 countries first, Tier 4 last.
    """
    if remaining <= 0:
        return [], list(misses)

    sorted_misses = sorted(misses, key=lambda x: _country_tier(x[1]))

    if len(sorted_misses) <= remaining:
        return sorted_misses, []

    return sorted_misses[:remaining], sorted_misses[remaining:]


def _save_deferred(pairs: list):
    """Write keyword pairs to the deferred queue."""
    now = datetime.now().isoformat()
    con = sqlite3.connect(CACHE_DB)
    con.executemany(
        "INSERT OR REPLACE INTO deferred_keyword_lookups "
        "(keyword, country, tier, deferred_at) VALUES (?, ?, ?, ?)",
        [(kw, country, _country_tier(country), now) for kw, country in pairs]
    )
    con.commit()
    con.close()


def _recover_deferred() -> list:
    """
    Load deferred keywords not older than 3 days.
    Deletes expired entries automatically.
    Returns list of (keyword, country) sorted by tier.
    """
    cutoff = (datetime.now() - timedelta(days=3)).isoformat()
    con    = sqlite3.connect(CACHE_DB)
    expired = con.execute(
        "DELETE FROM deferred_keyword_lookups WHERE deferred_at < ?", (cutoff,)
    ).rowcount
    rows = con.execute(
        "SELECT keyword, country FROM deferred_keyword_lookups ORDER BY tier ASC"
    ).fetchall()
    con.commit()
    con.close()
    if expired:
        print(f"  [Deferred] Dropped {expired} expired entr{'y' if expired == 1 else 'ies'} (>3 days old)")
    return [(row[0], row[1]) for row in rows]


def _remove_from_deferred(pairs: list):
    """Remove successfully resolved keywords from the deferred queue."""
    if not pairs:
        return
    con = sqlite3.connect(CACHE_DB)
    con.executemany(
        "DELETE FROM deferred_keyword_lookups WHERE keyword = ? AND country = ?", pairs
    )
    con.commit()
    con.close()


sys.path.insert(0, str(Path("/Users/newmac/.openclaw/workspace")))
try:
    from trend_forecast import predict_persistence
    _FORECAST_AVAILABLE = True
except Exception:
    _FORECAST_AVAILABLE = False

BASE    = Path("/Users/newmac/.openclaw/workspace")
INPUT   = BASE / "vetted_opportunities.json"
OUTPUT  = BASE / "validated_opportunities.json"
GOLDEN  = BASE / "golden_opportunities.json"
HISTORY = BASE / "validation_history.jsonl"

VERTICAL_REF_PATH = BASE / "vertical_cpc_reference.json"
_VERTICAL_CPC_REF: dict = {}
try:
    _VERTICAL_CPC_REF = json.loads(VERTICAL_REF_PATH.read_text())
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
    "EVERGREEN": {
        "cpc":         0.35,
        "intent":      0.25,
        "competition": 0.10,
        "kd":          0.10,
        "volume":      0.10,
        "trend":       0.10,
    },
    "EMERGING": {
        "cpc":         0.25,
        "intent":      0.25,
        "trend":       0.30,
        "competition": 0.15,
        "volume":      0.05,
    },
}

# ── Credentials ───────────────────────────────────────────────────────────────
GADS_CLIENT_ID       = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
GADS_CLIENT_SECRET   = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
GADS_REFRESH_TOKEN   = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
GADS_DEV_TOKEN       = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GADS_CUSTOMER_ID     = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "")
GADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")

DFS_LOGIN    = os.environ.get("DATAFORSEO_LOGIN", "")
DFS_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")

GADS_READY = all([GADS_CLIENT_ID, GADS_CLIENT_SECRET, GADS_REFRESH_TOKEN,
                  GADS_DEV_TOKEN, GADS_CUSTOMER_ID])
DFS_READY  = bool(DFS_LOGIN and DFS_PASSWORD)


def check_google_ads_credentials() -> bool:
    """Returns True if all Google Ads credentials are present, False otherwise."""
    required = {
        "GOOGLE_ADS_DEVELOPER_TOKEN": GADS_DEV_TOKEN,
        "GOOGLE_ADS_CLIENT_ID":       GADS_CLIENT_ID,
        "GOOGLE_ADS_CLIENT_SECRET":   GADS_CLIENT_SECRET,
        "GOOGLE_ADS_REFRESH_TOKEN":   GADS_REFRESH_TOKEN,
        "GOOGLE_ADS_CUSTOMER_ID":     GADS_CUSTOMER_ID,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"  ⚠️  Google Ads API not configured. Missing: {', '.join(missing)}")
        print("      Keyword expansion will be skipped. Run scripts/google_ads_setup.py")
        return False
    return True


def _build_gads_client():
    """Build a GoogleAdsClient from env credentials (uses SDK default API version)."""
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

COMPETITION_MAP = {"LOW": 0.1, "MEDIUM": 0.5, "HIGH": 0.9}

GEO_MAP = {
    "US": (2840, "us",  2840), "GB": (2826, "uk",  2826),
    "AU": (2036, "au",  2036), "CA": (2124, "ca",  2124),
    "DE": (2276, "de",  2276), "FR": (2250, "fr",  2250),
    "JP": (2392, "jp",  2392), "IN": (2356, "in",  2356),
    "BR": (2076, "br",  2076), "ZA": (2710, "za",  2710),
    "NG": (2566, "ng",  2566), "KE": (2404, "ke",  2404),
    "PH": (2608, "ph",  2608), "ID": (2360, "id",  2360),
    "VN": (2704, "vn",  2704), "TH": (2764, "th",  2764),
    "MY": (2458, "my",  2458), "SG": (2702, "sg",  2702),
    "HK": (2344, "hk",  2344), "TW": (2158, "tw",  2158),
    "KR": (2410, "kr",  2410), "AR": (2032, "ar",  2032),
    "MX": (2484, "mx",  2484), "CO": (2170, "co",  2170),
    "CL": (2152, "cl",  2152), "PE": (2604, "pe",  2604),
    "PL": (2616, "pl",  2616), "CZ": (2203, "cz",  2203),
    "RO": (2642, "ro",  2642), "HU": (2348, "hu",  2348),
    "GR": (2300, "gr",  2300), "PT": (2620, "pt",  2620),
    "NO": (2578, "no",  2578), "SE": (2752, "se",  2752),
    "DK": (2208, "dk",  2208), "FI": (2246, "fi",  2246),
    "AT": (2040, "at",  2040), "BE": (2056, "be",  2056),
    "NL": (2528, "nl",  2528), "CH": (2756, "ch",  2756),
    "IE": (2372, "ie",  2372), "IL": (2376, "il",  2376),
    "EG": (2818, "eg",  2818), "SA": (2682, "sa",  2682),
    "TR": (2792, "tr",  2792), "UA": (2804, "ua",  2804),
    "NZ": (2554, "nz",  2554), "IT": (2380, "it",  2380),
    "ES": (2724, "es",  2724),
}


# DFS language codes per country (for Labs API calls)
_DFS_LANG = {
    "US": "en", "GB": "en", "AU": "en", "CA": "en", "NZ": "en",
    "IE": "en", "ZA": "en", "SG": "en", "PH": "en", "MY": "en",
    "NG": "en", "KE": "en", "IN": "en",
    "DE": "de", "AT": "de", "CH": "de",
    "FR": "fr", "BE": "nl", "NL": "nl",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "IT": "it", "PT": "pt", "BR": "pt",
    "JP": "ja", "KR": "ko", "HK": "zh", "TW": "zh",
    "PL": "pl", "CZ": "cs", "RO": "ro", "HU": "hu", "GR": "el",
    "SE": "sv", "NO": "no", "DK": "da", "FI": "fi",
    "TH": "th", "ID": "id", "VN": "vi",
    "TR": "tr", "UA": "uk", "IL": "he",
    "EG": "ar", "SA": "ar",
}


def _geo_params(country_iso):
    return GEO_MAP.get(country_iso.upper(), (2840, "us", 2840))


def _dfs_language(country_iso: str) -> str:
    """Return DataForSEO language_code for a country. Defaults to 'en'."""
    return _DFS_LANG.get(country_iso.upper(), "en")


# ── Google Ads (SDK-based, no raw REST) ───────────────────────────────────────

def fetch_google_ads(keyword, country="US"):
    """
    Fetch CPC/volume metrics for a single keyword via the google-ads Python SDK.
    Uses generateKeywordIdeas and returns the result matching the seed keyword.
    """
    from google.ads.googleads.errors import GoogleAdsException

    client = _build_gads_client()
    idea_svc = client.get_service("KeywordPlanIdeaService")
    request  = client.get_type("GenerateKeywordIdeasRequest")

    criterion_id, _, _ = _geo_params(country)
    customer_id_clean  = GADS_CUSTOMER_ID.replace("-", "")

    request.customer_id            = customer_id_clean
    request.language               = f"languageConstants/1000"
    request.geo_target_constants   = [f"geoTargetConstants/{criterion_id}"]
    request.include_adult_keywords = False
    request.keyword_plan_network   = (
        client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
    )
    request.keyword_seed.keywords.append(keyword)

    try:
        response = idea_svc.generate_keyword_ideas(request=request)
    except GoogleAdsException as ex:
        raise ValueError(f"Google Ads API error: {ex.error.code().name}") from ex

    results = list(response)
    if not results:
        raise ValueError("No results returned from Google Ads API")

    def _extract_idea_metrics(m, kw):
        # Bug 1 fix: average_cpc_micros doesn't exist in GenerateKeywordIdeas response.
        # The correct fields are low_top_of_page_bid_micros / high_top_of_page_bid_micros.
        low_bid  = round(getattr(m, "low_top_of_page_bid_micros", 0) / 1_000_000, 2)
        high_bid = round(getattr(m, "high_top_of_page_bid_micros", 0) / 1_000_000, 2)
        cpc_usd  = round((low_bid + high_bid) / 2, 2)
        comp_name = m.competition.name if hasattr(m.competition, "name") else str(m.competition)
        return {
            "keyword":        kw,
            "search_volume":  int(m.avg_monthly_searches or 0),
            "cpc_usd":        cpc_usd,
            "cpc_low_usd":    low_bid,
            "cpc_high_usd":   high_bid,
            "competition":    COMPETITION_MAP.get(comp_name, 0.5),
            "source":         "google_ads",
        }

    # Prefer an exact match on the seed keyword text
    for idea in results:
        if idea.text.lower() == keyword.lower():
            return _extract_idea_metrics(idea.keyword_idea_metrics, keyword)

    # Fall back to first result
    return _extract_idea_metrics(results[0].keyword_idea_metrics, keyword)


# ── DataForSEO ────────────────────────────────────────────────────────────────

def fetch_dataforseo(keyword, country="US"):
    global _dfs_last_call
    elapsed = time.time() - _dfs_last_call
    if elapsed < DFS_MIN_DELAY:
        time.sleep(DFS_MIN_DELAY - elapsed)

    clean_kw = _clean_keyword(keyword)
    if len(clean_kw.split()) < 2:
        raise ValueError(f"Keyword too short after cleaning: '{clean_kw}'")

    _, _, dfs_location = _geo_params(country)
    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    body    = [{"keywords": [clean_kw], "language_name": "English",
                "location_code": dfs_location, "include_serp_info": False}]
    _dfs_last_call = time.time()
    r = requests.post(
        "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live",
        headers=headers, json=body, timeout=15
    )
    r.raise_for_status()
    tasks = r.json().get("tasks", [])
    if not tasks or tasks[0].get("status_code") != 20000:
        raise ValueError(
            f"DataForSEO error: {tasks[0].get('status_message') if tasks else 'no tasks'}"
        )
    results = tasks[0].get("result", [])
    if not results:
        raise ValueError("DataForSEO returned no keyword results")
    item = results[0]
    competition_index = item.get("competition_index", 50) or 50
    return {
        "keyword":            keyword,
        "search_volume":      int(item.get("search_volume", 0) or 0),
        "cpc_usd":            round(float(item.get("cpc", 0) or 0), 2),
        "cpc_low_usd":        round(float(item.get("low_top_of_page_bid", 0) or 0), 2),
        "cpc_high_usd":       round(float(item.get("high_top_of_page_bid", 0) or 0), 2),
        "competition":        round(competition_index / 100, 2),
        "competition_index":  competition_index,
        "monthly_searches":   item.get("monthly_searches", []),
        "source":             "dataforseo",
    }


# ── KD pre-filter & Labs enrichment ──────────────────────────────────────────

def _log_error(stage: str, message: str):
    """Append a structured error to error_log.jsonl."""
    try:
        with open(BASE / "error_log.jsonl", "a") as f:
            f.write(json.dumps({
                "timestamp": datetime.now().isoformat(),
                "stage":     stage,
                "error":     message,
            }) + "\n")
    except Exception:
        pass   # never crash the pipeline over a logging failure


def _bulk_kd_gate(keyword_country_pairs: list) -> list:
    """
    Cheap pre-filter: reject keywords with keyword_difficulty < 15 using the
    bulk_keyword_difficulty endpoint ($0.01 per 1,000 keywords).

    Args:
        keyword_country_pairs: list of (keyword_str, country_iso) tuples

    Returns:
        List of (keyword_str, country_iso) tuples where KD >= 15.
        On any API error, returns the input list unchanged (fail open).
    """
    if not DFS_READY or not keyword_country_pairs:
        return keyword_country_pairs

    # Group by country because location_code differs per country
    by_country: dict = {}
    for kw, country in keyword_country_pairs:
        by_country.setdefault(country, []).append(kw)

    passing = []
    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

    for country, keywords in by_country.items():
        _, _, location_code = _geo_params(country)
        # Process in batches of 1,000 (API limit)
        for i in range(0, len(keywords), 1000):
            batch = keywords[i : i + 1000]
            # Pre-flight budget check
            estimated_cost = DFS_ENDPOINT_COSTS["bulk_kd"] * len(batch)
            if not pre_flight_budget_check(estimated_cost, DFS_DAILY_BUDGET_USD):
                print(f"  [Budget] Skipping KD gate batch — daily budget exhausted")
                passing.extend((kw, country) for kw in batch)  # fail open
                continue
            payload = [{"keywords": batch, "location_code": location_code,
                        "language_code": _dfs_language(country)}]
            try:
                r = requests.post(
                    f"{DFS_LABS_BASE}/bulk_keyword_difficulty/live",
                    headers=headers, json=payload, timeout=20
                )
                r.raise_for_status()
                # Record spend immediately after POST
                increment_usd_spent(estimated_cost, "bulk_kd")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    # API error — fail open, pass all keywords through
                    passing.extend((kw, country) for kw in batch)
                    continue
                for item in tasks[0].get("result", []):
                    kd = item.get("keyword_difficulty") or 0
                    kw = item.get("keyword", "")
                    if kd >= 15:
                        passing.append((kw, country))
                    else:
                        _log_error("validation_kd_gate",
                                   f"KD gate rejected '{kw}' [{country}]: KD={kd} (<15)")
            except Exception as e:
                # Fail open on any error — never block the pipeline
                print(f"  ⚠️  KD gate error [{country}]: {e}")
                passing.extend((kw, country) for kw in batch)

    passed  = len(passing)
    total   = len(keyword_country_pairs)
    print(f"  KD gate:  {total} → {passed} passing (KD ≥ 15), "
          f"{total - passed} rejected")
    return passing


def _fetch_dataforseo_labs_batch(keyword_country_pairs: list) -> dict:
    """
    Fetch rich keyword data from the Labs keyword_overview endpoint.
    Returns KD, search intent, SERP features, trend velocity, and CPC data
    in a single batched call (up to 700 keywords per request).

    Args:
        keyword_country_pairs: list of (keyword_str, country_iso) tuples

    Returns:
        dict keyed by (keyword_str, country_iso) → enrichment dict containing:
            kd                  int         keyword_difficulty (0-100)
            main_intent         str         commercial/transactional/informational/navigational
            secondary_intents   list[dict]  [{intent, probability}, ...]
            serp_item_types     list[str]   SERP features present
            trend_monthly       float       month-over-month % change
            trend_quarterly     float       quarter-over-quarter % change
            trend_yearly        float       year-over-year % change
            monthly_searches    list[dict]  12-month volume history
            cpc_labs            float       CPC from Labs (supplemental, not primary)
            competition_labs    float       paid competition 0-1 from Labs
            is_another_language bool        True if keyword is wrong language
    """
    if not DFS_READY or not keyword_country_pairs:
        return {}

    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    results = {}

    # Group by country; process in batches of 700
    by_country: dict = {}
    for kw, country in keyword_country_pairs:
        by_country.setdefault(country, []).append(kw)

    for country, keywords in by_country.items():
        _, _, location_code = _geo_params(country)
        for i in range(0, len(keywords), 700):
            batch = keywords[i : i + 700]
            # Pre-flight budget check
            estimated_cost = DFS_ENDPOINT_COSTS["keyword_overview"] * len(batch)
            if not pre_flight_budget_check(estimated_cost, DFS_DAILY_BUDGET_USD):
                print(f"  [Budget] Skipping Labs enrichment batch — daily budget exhausted")
                continue
            payload = [{
                "keywords":                  batch,
                "location_code":             location_code,
                "language_code":             _dfs_language(country),
                "include_serp_info":         True,
                "include_clickstream_data":  False,
            }]
            try:
                r = requests.post(
                    f"{DFS_LABS_BASE}/keyword_overview/live",
                    headers=headers, json=payload, timeout=30
                )
                r.raise_for_status()
                # Record spend immediately after POST
                increment_usd_spent(estimated_cost, "keyword_overview")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    print(f"  ⚠️  Labs batch error [{country}]: "
                          f"{tasks[0].get('status_message') if tasks else 'no tasks'}")
                    continue

                for item in (tasks[0].get("result") or []):
                    kw_text = item.get("keyword", "")
                    ki      = item.get("keyword_info", {}) or {}
                    kp      = item.get("keyword_properties", {}) or {}
                    si      = item.get("search_intent_info", {}) or {}
                    sinfo   = item.get("serp_info", {}) or {}
                    trend   = ki.get("search_volume_trend", {}) or {}

                    results[(kw_text, country)] = {
                        "kd":                   int(kp.get("keyword_difficulty") or 0),
                        "main_intent":          si.get("main_intent", ""),
                        "secondary_intents":    si.get("secondary_keyword_intents", []),
                        "serp_item_types":      sinfo.get("serp_item_types", []),
                        "trend_monthly":        float(trend.get("monthly") or 0),
                        "trend_quarterly":      float(trend.get("quarterly") or 0),
                        "trend_yearly":         float(trend.get("yearly") or 0),
                        "monthly_searches":     ki.get("monthly_searches", []),
                        "cpc_labs":             float(ki.get("cpc") or 0),
                        "competition_labs":     float(ki.get("competition") or 0),
                        "is_another_language":  bool(kp.get("is_another_language")),
                    }
            except Exception as e:
                print(f"  ⚠️  Labs enrichment error [{country}] batch {i // 700 + 1}: {e}")

    print(f"  Labs enrichment: {len(keyword_country_pairs)} requested → "
          f"{len(results)} enriched")
    return results


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
    Run all pre-scoring rejection checks.
    Gate order matches master plan Section 4 (cheapest checks first).
    Returns (passes: bool, rejection_reason: str).
    """
    kd                  = enrichment.get("kd", 0)
    main_intent         = enrichment.get("main_intent", "")
    serp_item_types     = enrichment.get("serp_item_types", [])
    is_another_language = enrichment.get("is_another_language", False)

    # Gate 1 — Wrong language for location (free check from overview response)
    if is_another_language:
        return False, "wrong_language"

    # Gate 2 — Intent filtering with nuance
    if main_intent == "navigational":
        return False, "navigational_intent"
    # Allow informational IF it has commercial secondary intent (undervalued by competitors)
    if main_intent == "informational":
        secondary_intents = enrichment.get("secondary_intents", [])
        has_commercial = any(s.get("intent") == "commercial" for s in secondary_intents)
        if not has_commercial:
            return False, "informational_without_commercial"
        # Passes gate — 0.6x multiplier applied in compute_rsoc_score via intent_score

    # Gate 3 — KD minimum (re-check in case enrichment ran on keywords that bypassed bulk gate)
    if kd < 15:
        return False, f"kd_below_15 (kd={kd})"

    # Gate 4 — SERP Saturation Risk (computed locally, no API cost)
    ssr = _compute_ssr(serp_item_types)
    if ssr > 1.5:
        return False, f"serp_saturation_risk (ssr={ssr:.2f})"

    # Gate 5 — CPC floor: country-aware, uses high_top_of_page_bid against htpb floor
    floor = get_cpc_floor(country, "htpb")
    emerging_tag = enrichment.get("emerging_tag")

    # EMERGING keywords can bypass strict CPC floor if they have volume + trend
    if emerging_tag in ["EMERGING", "EMERGING_HIGH"]:
        trend_monthly = enrichment.get("trend_monthly", 0)
        if trend_monthly >= 30 and search_volume >= 200:
            # Strong emerging trend — bypass CPC floor entirely
            pass
        elif (cpc_high_usd or 0) < floor * 0.5:
            return False, f"emerging_cpc_too_low (htpb=${cpc_high_usd:.2f}, 50%_floor=${floor*0.5:.2f})"
    else:
        if (cpc_high_usd or 0) < floor:
            return False, f"cpc_below_country_floor (htpb=${cpc_high_usd:.2f}, floor=${floor:.2f})"

    # Gate 6 — Paid competition density
    if (competition or 0) < 0.40:
        return False, f"competition_too_low ({competition:.2f})"

    # Gate 7 — Volume floor (track-aware and language-aware)
    is_english = country in ["US", "GB", "AU", "CA", "NZ", "IE"]
    if emerging_tag in ["EMERGING", "EMERGING_HIGH"]:
        min_volume = 200 if is_english else 100
    else:
        min_volume = 500 if is_english else 200

    if (search_volume or 0) < min_volume:
        return False, f"volume_below_floor ({search_volume} < {min_volume})"

    return True, ""


def compute_rsoc_score(cpc_high_usd: float, competition: float,
                       search_volume: int, enrichment: dict,
                       scoring_profile: str = "EVERGREEN") -> float:
    """
    Composite RSOC opportunity score 0-100.

    Uses enrichment data from _fetch_dataforseo_labs_batch() for intent and
    trend signals. Falls back gracefully when enrichment is empty.

    Args:
        cpc_high_usd:      high_top_of_page_bid from existing metrics (RPC ceiling proxy)
        competition:       paid competition float 0-1 from existing metrics
        search_volume:     monthly search volume from existing metrics
        enrichment:        dict from _fetch_dataforseo_labs_batch() (may be empty {})
        scoring_profile:   "EVERGREEN" or "EMERGING"

    Returns:
        float 0-100
    """
    if scoring_profile not in _RSOC_WEIGHTS:
        scoring_profile = "EVERGREEN"

    w = _RSOC_WEIGHTS[scoring_profile]

    main_intent = enrichment.get("main_intent", "")
    secondary_intents = enrichment.get("secondary_intents", [])

    intent_raw = _compute_intent_score(main_intent, secondary_intents)
    # Apply 0.6x multiplier for informational keywords with commercial secondary intent
    if main_intent == "informational":
        has_commercial = any(s.get("intent") == "commercial" for s in secondary_intents)
        if has_commercial:
            intent_raw *= 0.6

    component_scores = {
        "cpc":         _compute_cpc_score(cpc_high_usd),
        "intent":      intent_raw,
        "competition": _compute_competition_score(competition),
        "kd":          _compute_kd_score(enrichment.get("kd", 0)),
        "volume":      _compute_volume_score(search_volume),
        "trend":       _compute_trend_score(
                           enrichment.get("trend_monthly", 0),
                           enrichment.get("trend_quarterly", 0),
                           enrichment.get("trend_yearly", 0),
                       ),
    }

    composite  = sum(component_scores.get(k, 0) * w[k] for k in w)
    serp_items = enrichment.get("serp_item_types", [])
    ssr        = _compute_ssr(serp_items)

    # Apply SSR as a post-score multiplier (hard gate already ran at 1.5)
    if ssr >= 1.5:
        composite *= 0.0
    elif ssr >= 1.0:
        composite *= (1.5 - ssr) * 2.0   # linear 1.0→1.5 maps multiplier 1.0→0.0

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


def tag_opportunity_v2(ai_score: float, cpc_usd: float,
                       competition: float, enrichment: dict,
                       vertical: str, country: str) -> str:
    """
    Extended opportunity tagger. Returns one of:
        GOLDEN_OPPORTUNITY  — high arbitrage_index (backward compat threshold: > 0.8)
        WATCH               — moderate arbitrage_index (> 0.5)
        EMERGING_HIGH       — trending keyword with 4+ confidence signals
        EMERGING            — trending keyword with 1-3 confidence signals
        LOW                 — below scoring thresholds
        UNSCORED            — no metrics available (unchanged from current code)
    """
    # —— Preserve existing GOLDEN/WATCH logic (backward compat) ————————————————
    if ai_score > 0.8:
        return "GOLDEN_OPPORTUNITY"
    if ai_score > 0.5:
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


# ── Provider router ───────────────────────────────────────────────────────────

def get_keyword_metrics(keyword, country="US"):
    if GADS_READY:
        try:
            return fetch_google_ads(keyword, country)
        except Exception as e:
            print(f"  ⚠️  Google Ads error '{keyword}': {e}")

    if DFS_READY:
        try:
            return fetch_dataforseo(keyword, country)
        except Exception as e:
            print(f"  ⚠️  DataForSEO error '{keyword}': {e}")

    return None


# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_ai(cpc_usd, search_volume, competition):
    return round((cpc_usd * search_volume) / ((competition or 0.01) * 10000), 4)


def tag_opportunity(ai_score):
    if ai_score > 0.8:
        return "GOLDEN_OPPORTUNITY"
    if ai_score > 0.5:
        return "WATCH"
    return "LOW"


# ── Main ──────────────────────────────────────────────────────────────────────

if not INPUT.exists():
    print(f"⚠️  {INPUT} not found — run vetting.py first")
    raise SystemExit(1)

_init_cache_db()
_cache_cleanup()

if GADS_READY:
    print("ℹ️  Provider: Google Ads Keyword Planner")
elif DFS_READY:
    print("ℹ️  Provider: DataForSEO")
else:
    print("⚠️  No keyword API configured. Opportunities saved as UNSCORED.")
    print("    Set up one of:")
    print("      Google Ads  → export GOOGLE_ADS_CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN / DEVELOPER_TOKEN / CUSTOMER_ID")
    print("      DataForSEO  → export DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD")

vetted = json.loads(INPUT.read_text())

# ── LAYER 1: Normalize & deduplicate within batch ─────────────────────────────
_seen_norm: dict  = {}  # norm_key → representative original keyword
_unique_pairs: list = []
_pre_fetched_count  = 0

for opp in vetted:
    kw      = opp.get("keyword", "")
    country = opp.get("country", "US")
    # Skip entries that already carry CPC data (pre-fetched upstream)
    if opp.get("cpc_usd") is not None and opp.get("search_volume") is not None:
        _pre_fetched_count += 1
        continue
    norm_key = _normalize_keyword(kw) + "|" + country
    if norm_key not in _seen_norm:
        _seen_norm[norm_key] = kw
        _unique_pairs.append((kw, country))

l1_total   = len(vetted) - _pre_fetched_count
l1_dropped = l1_total - len(_unique_pairs)
print(f"\n[Cost Optimization]")
print(f"  Layer 1 — Dedupe:  {l1_total} → {len(_unique_pairs)} unique keywords "
      f"({l1_dropped} intra-batch duplicates removed)")

# ── Recover deferred keywords from previous runs ──────────────────────────────
deferred_recovered = _recover_deferred()
if deferred_recovered:
    existing_norms = set(_seen_norm.keys())
    added = 0
    for kw, country in deferred_recovered:
        norm_key = _normalize_keyword(kw) + "|" + country
        if norm_key not in existing_norms:
            _seen_norm[norm_key] = kw
            _unique_pairs.append((kw, country))
            existing_norms.add(norm_key)
            added += 1
    if added:
        print(f"  [Deferred] Injected {added} keyword(s) recovered from previous run(s)")

# ── LAYER 2: Batch cache lookup ───────────────────────────────────────────────
cache_hits, cache_misses = _cache_lookup_batch(_unique_pairs)
hit_rate = round(len(cache_hits) / len(_unique_pairs) * 100) if _unique_pairs else 0
print(f"  Layer 2 — Cache:   {len(_unique_pairs)} → {len(cache_misses)} need lookup "
      f"({len(cache_hits)} cache hits, {hit_rate}% hit rate)")

# ── LAYER 3: Budget gate ──────────────────────────────────────────────────────
today_usage     = _get_today_usage()
remaining       = DAILY_API_BUDGET - today_usage
approved_misses, deferred_misses = _budget_gate(cache_misses, remaining)

if deferred_misses:
    _save_deferred(deferred_misses)
    print(f"  Layer 3 — Budget:  {len(cache_misses)} lookups needed, {remaining} remaining "
          f"— sending {len(approved_misses)}, deferring {len(deferred_misses)}")
else:
    print(f"  Layer 3 — Budget:  {len(cache_misses)} lookups needed, {remaining} remaining — all clear")

print(f"  Daily budget usage: {today_usage}/{DAILY_API_BUDGET} (before this run)\n")

# ── API calls for approved misses ─────────────────────────────────────────────
# KD pre-filter (cheap, runs before individual API calls)
if DFS_READY:
    approved_misses = _bulk_kd_gate(approved_misses)

fresh_metrics: dict = {}  # (keyword, country) → metrics dict
api_calls_made      = 0

for kw, country in approved_misses:
    metrics = get_keyword_metrics(kw, country)
    if metrics:
        fresh_metrics[(kw, country)] = metrics
        _cache_write_back(kw, country, metrics)
        api_calls_made += 1

_increment_usage(api_calls_made)

# Remove successfully resolved deferred keywords
resolved_deferred = [(kw, c) for kw, c in deferred_recovered
                     if (kw, c) in cache_hits or (kw, c) in fresh_metrics]
_remove_from_deferred(resolved_deferred)

# ── Labs enrichment batch: fetch KD, intent, SERP, trends ────────────────────
# Run on all keywords that have (or will have) metrics: cache hits + fresh calls
_labs_enrichment_pairs = (
    list(cache_hits.keys()) + list(fresh_metrics.keys())
)
_labs_enrichment: dict = {}
if DFS_READY and _labs_enrichment_pairs:
    _labs_enrichment = _fetch_dataforseo_labs_batch(_labs_enrichment_pairs)

# ── Build unified metrics map: norm_key → metrics ────────────────────────────
_all_metrics: dict = {}
for (kw, country), m in cache_hits.items():
    _all_metrics[_normalize_keyword(kw) + "|" + country] = m
for (kw, country), m in fresh_metrics.items():
    _all_metrics[_normalize_keyword(kw) + "|" + country] = m

# ── Score all vetted opportunities ────────────────────────────────────────────
validated = []

for opp in vetted:
    keyword = opp.get("keyword", "")
    country = opp.get("country", "US")

    # Pre-fetched data check (from keyword_extractor upstream — Bucket A)
    if opp.get("cpc_usd") is not None and opp.get("search_volume") is not None:
        metrics = {
            "search_volume": opp["search_volume"],
            "cpc_usd":       opp["cpc_usd"],
            "competition":   opp.get("competition", 0.5),
            "source":        "google_keyword_planner",
        }
    # Bug 2/3 fix: Bucket B keywords already have Google metrics but cpc_usd not injected.
    # Use google_estimated_cpc directly rather than re-calling the API.
    elif (opp.get("metrics_source") == "google_keyword_planner"
          and opp.get("google_estimated_cpc")):
        metrics = {
            "search_volume": opp.get("google_volume") or 0,
            "cpc_usd":       opp.get("google_estimated_cpc", 0),
            "cpc_low_usd":   opp.get("google_cpc_low"),
            "cpc_high_usd":  opp.get("google_cpc_high"),
            "competition":   round((opp.get("google_competition_index") or 50) / 100, 2),
            "source":        "google_keyword_planner",
        }
    else:
        norm_key = _normalize_keyword(keyword) + "|" + country
        metrics  = _all_metrics.get(norm_key)

    if metrics:
        cpc_usd      = metrics["cpc_usd"]
        search_vol   = metrics["search_volume"]
        competition  = metrics["competition"]
        cpc_high     = metrics.get("cpc_high_usd") or cpc_usd

        # Existing compute_ai score — kept for backward compatibility
        ai_score       = compute_ai(cpc_usd, search_vol, competition)
        persistence    = {}
        weighted_score = ai_score
        if _FORECAST_AVAILABLE:
            try:
                persistence    = predict_persistence(keyword, country)
                weighted_score = round(ai_score * persistence.get("persistence_probability", 0.5), 4)
            except Exception:
                pass

        # New DataForSEO Labs enrichment data for this keyword
        enrich_key = (keyword, country)
        enrichment = (
            _labs_enrichment.get(enrich_key)
            or _labs_enrichment.get((_clean_keyword(keyword), country))
            or {}
        )

        # Classify emerging BEFORE hard gates so emerging_tag is available for CPC bypass
        vertical  = opp.get("vertical_match") or opp.get("vertical") or "general"
        vertical_data  = _VERTICAL_CPC_REF.get(vertical) or _VERTICAL_CPC_REF.get("general") or {}
        vert_avg_cpc   = float(vertical_data.get("avg_cpc") or 4.0)

        emerging_signals = {
            "trend_monthly":        enrichment.get("trend_monthly", 0),
            "trend_quarterly":      enrichment.get("trend_quarterly", 0),
            "trend_yearly":         enrichment.get("trend_yearly", 0),
            "kd":                   enrichment.get("kd", 0),
            "cpc":                  cpc_usd,
            "high_top_of_page_bid": cpc_high,
            "monthly_searches":     enrichment.get("monthly_searches", []),
        }
        emerging_class = classify_emerging(emerging_signals, vert_avg_cpc)
        if emerging_class:
            enrichment["emerging_tag"] = emerging_class

        # Hard gate filter (runs AFTER emerging classification so CPC bypass works)
        passes_gates, gate_reason = _apply_hard_gates(
            keyword, country, cpc_usd, cpc_high, competition, search_vol, enrichment
        )
        if not passes_gates:
            _log_error("validation_hard_gate",
                       f"Hard gate rejected '{keyword}' [{country}]: {gate_reason}")
            validated.append({
                **opp,
                "search_volume":    search_vol,
                "cpc_usd":          cpc_usd,
                "competition":      competition,
                "arbitrage_index":  0.0,
                "weighted_score":   0.0,
                "rsoc_score":       0.0,
                "tag":              "GATED",
                "gate_reason":      gate_reason,
                "metrics_source":   metrics["source"],
                "validated_at":     datetime.now().isoformat(),
            })
            continue

        # Scoring profile: use EMERGING weights if classify_emerging found signals
        tag       = tag_opportunity_v2(ai_score, cpc_usd, competition,
                                       enrichment, vertical, country)
        # Prefer classify_emerging result over tag_opportunity_v2 for track selection
        if emerging_class:
            tag = emerging_class
        profile   = "EMERGING" if tag in ("EMERGING", "EMERGING_HIGH") else "EVERGREEN"
        rsoc_score = compute_rsoc_score(cpc_high, competition, search_vol,
                                        enrichment, scoring_profile=profile)
        kvsi_val  = _compute_kvsi(enrichment)

        validated.append({
            **opp,
            "search_volume":           search_vol,
            "cpc_usd":                 cpc_usd,
            "cpc_low_usd":             metrics.get("cpc_low_usd"),
            "cpc_high_usd":            cpc_high,
            "competition":             competition,
            "competition_index":       metrics.get("competition_index"),
            "monthly_searches":        enrichment.get("monthly_searches")
                                       or metrics.get("monthly_searches", []),
            "arbitrage_index":         ai_score,       # kept for backward compat
            "weighted_score":          weighted_score,
            "rsoc_score":              rsoc_score,      # new composite score
            "persistence_score":       persistence.get("persistence_probability"),
            "predicted_halflife_days": persistence.get("predicted_halflife_days"),
            "tag":                     tag,
            "metrics_source":          metrics["source"],
            # New enrichment fields
            "kd":                      enrichment.get("kd"),
            "main_intent":             enrichment.get("main_intent"),
            "serp_item_types":         enrichment.get("serp_item_types", []),
            "ssr":                     _compute_ssr(enrichment.get("serp_item_types", [])),
            "trend_monthly":           enrichment.get("trend_monthly"),
            "trend_quarterly":         enrichment.get("trend_quarterly"),
            "kvsi":                    kvsi_val,
            "validated_at":            datetime.now().isoformat(),
        })
    else:
        vertical = opp.get("vertical", "general")
        tier = _country_tier(country)
        tier_key = f"tier_{tier}"
        vertical_ceiling = _VERTICAL_CPC_REF.get(vertical, {}).get(tier_key, 0)
        tag = "EMERGING" if vertical_ceiling >= _EMERGING_THRESHOLD else "UNSCORED"
        validated.append({
            **opp,
            "search_volume":           None,
            "cpc_usd":                 None,
            "cpc_low_usd":             None,
            "cpc_high_usd":            None,
            "competition":             None,
            "competition_index":       None,
            "monthly_searches":        [],
            "arbitrage_index":         None,
            "weighted_score":          None,
            "persistence_score":       None,
            "predicted_halflife_days": None,
            "tag":                     tag,
            "vertical_ceiling_usd":    vertical_ceiling if tag == "EMERGING" else None,
            "metrics_source":          "none_configured",
            "validated_at":            datetime.now().isoformat(),
        })

# Deduplicate: keep best-scored entry per (keyword, country)
_seen_kw: dict = {}
_deduped: list = []
for entry in sorted(validated, key=lambda x: x.get("arbitrage_index") or 0, reverse=True):
    key = (entry.get("keyword", "").lower().strip(), entry.get("country", ""))
    if key not in _seen_kw:
        _seen_kw[key] = True
        _deduped.append(entry)
validated = _deduped

OUTPUT.write_text(json.dumps(validated, indent=2))

golden_watch = [r for r in validated if r["tag"] in ("GOLDEN_OPPORTUNITY", "WATCH", "EMERGING", "EMERGING_HIGH")]
GOLDEN.write_text(json.dumps(golden_watch, indent=2))

with HISTORY.open("a") as f:
    for rec in validated:
        f.write(json.dumps(rec) + "\n")

# ── Write scored opportunities to LanceDB (Issue #4) ─────────────────────────
_lancedb_written = 0
try:
    sys.path.insert(0, str(BASE))
    from vector_store import add_opportunity as _add_opportunity
    for rec in validated:
        if rec.get("arbitrage_index") is not None:
            try:
                _add_opportunity(
                    rec.get("keyword", ""),
                    rec.get("country", "US"),
                    rec.get("arbitrage_index", 0),
                    rec.get("tag", ""),
                    rec,
                )
                _lancedb_written += 1
            except Exception as _e:
                pass  # non-fatal — JSON files are the source of truth
except Exception as _e:
    print(f"  ⚠️  LanceDB write skipped: {_e}")

golden_count        = sum(1 for r in validated if r["tag"] == "GOLDEN_OPPORTUNITY")
watch_count         = sum(1 for r in validated if r["tag"] == "WATCH")
emerging_count      = sum(1 for r in validated if r["tag"] == "EMERGING")
emerging_high_count = sum(1 for r in validated if r["tag"] == "EMERGING_HIGH")
gated_count         = sum(1 for r in validated if r["tag"] == "GATED")
unscored_count      = sum(1 for r in validated if r["tag"] == "UNSCORED")
low_count           = len(validated) - golden_count - watch_count - emerging_count - emerging_high_count - gated_count - unscored_count

print(
    f"✅ Validation complete: {len(validated)} records — "
    f"{golden_count} GOLDEN, {watch_count} WATCH, "
    f"{emerging_count} EMERGING, {emerging_high_count} EMERGING_HIGH, "
    f"{low_count} LOW, {gated_count} GATED, {unscored_count} UNSCORED → {OUTPUT.name}"
)
print(f"   API calls this run: {api_calls_made} | "
      f"Daily total: {today_usage + api_calls_made}/{DAILY_API_BUDGET} | "
      f"Cache size: {len(cache_hits)} hits served from DB")
if _lancedb_written:
    print(f"   LanceDB: {_lancedb_written} opportunities written")
