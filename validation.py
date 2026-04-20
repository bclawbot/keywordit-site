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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _URLRetry
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from country_config import get_country_tier as _country_tier, get_cpc_floor, DFS_ENDPOINT_COSTS, DFS_DAILY_BUDGET_USD
from cpc_cache import pre_flight_budget_check, increment_usd_spent
from normalize import normalize_keyword as _normalize_keyword_raw

# Phase 2.3: Config version stamp
try:
    from config import pipeline_config as _pipeline_config
    _CONFIG_VERSION = _pipeline_config.version
except Exception:
    _CONFIG_VERSION = "unknown"

# NOTE: Google Ads API versions deprecate quarterly.
# Check https://developers.google.com/google-ads/api/docs/sunset-dates
# and update the version in GoogleAdsClient calls if needed.
# We intentionally do NOT pin a version here — the SDK default tracks the latest stable.

# ── Cache & budget configuration ──────────────────────────────────────────────
CACHE_DB         = Path(__file__).resolve().parent / "cpc_cache.db"
CACHE_TTL_HOURS  = 168   # 7 days — CPC data is monthly aggregate, changes slowly
DAILY_API_BUDGET = int(os.environ.get("DATAFORSEO_DAILY_BUDGET", "500"))


# ── Persistent HTTP session for DataForSEO API calls ─────────────────────────
# Reuses TCP connections (connection pooling) to avoid per-request TLS handshakes.
_dfs_session = requests.Session()
_dfs_session.mount("https://", HTTPAdapter(
    max_retries=_URLRetry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]),
    pool_connections=5,
    pool_maxsize=10,
))


class _RateLimitError(requests.HTTPError):
    """Raised when DataForSEO returns HTTP 429 so tenacity can retry it."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout, requests.HTTPError, _RateLimitError)),
    reraise=True,
)
def _dfs_post(url: str, headers: dict, json_body, timeout: int = 30) -> requests.Response:
    """
    POST to a DataForSEO endpoint with retry + exponential backoff.
    Raises _RateLimitError on HTTP 429 so the tenacity decorator retries it.
    All other HTTP errors are raised via raise_for_status().
    """
    r = _dfs_session.post(url, headers=headers, json=json_body, timeout=timeout)
    if r.status_code == 429:
        raise _RateLimitError(f"DataForSEO rate-limited (429)", response=r)
    r.raise_for_status()
    return r


# ── DataForSEO rate-limit state ───────────────────────────────────────────────
DFS_MAX_WORDS  = 7
DFS_MIN_DELAY  = 6.0
_dfs_last_call = 0.0
DFS_LABS_BASE  = "https://api.dataforseo.com/v3/dataforseo_labs/google"


# ── Layer 1: Keyword normalization (comparison only — never sent to API) ──────

def _normalize_keyword(text: str) -> str:
    """Normalize for deduplication comparison only. Original keyword is always sent to the API."""
    return _normalize_keyword_raw(text, strip_year=True, strip_articles=True, strip_punctuation=True)


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

        CREATE TABLE IF NOT EXISTS labs_enrichment_cache (
            keyword     TEXT NOT NULL,
            country     TEXT NOT NULL,
            enrichment  TEXT NOT NULL,
            fetched_at  TEXT NOT NULL,
            PRIMARY KEY (keyword, country)
        );
        CREATE INDEX IF NOT EXISTS idx_labs_lookup
            ON labs_enrichment_cache (keyword, country, fetched_at);
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


# ── Labs enrichment cache helpers ─────────────────────────────────────────────

def _labs_cache_lookup_batch(pairs: list) -> tuple:
    """
    Batch-check (keyword, country) pairs against the labs_enrichment_cache.
    Returns (hits_dict, misses_list).
    hits_dict: {(keyword, country): enrichment_dict}
    misses_list: pairs not found in cache or expired.
    """
    if not pairs:
        return {}, []
    cutoff = (datetime.now() - timedelta(hours=CACHE_TTL_HOURS)).isoformat()
    con = sqlite3.connect(CACHE_DB)
    con.execute("CREATE TEMPORARY TABLE _llookup (keyword TEXT, country TEXT)")
    con.executemany("INSERT INTO _llookup VALUES (?, ?)", pairs)
    rows = con.execute(
        "SELECT c.keyword, c.country, c.enrichment "
        "FROM labs_enrichment_cache c "
        "JOIN _llookup l ON c.keyword = l.keyword AND c.country = l.country "
        "WHERE c.fetched_at > ?",
        (cutoff,)
    ).fetchall()
    con.close()
    hits   = {(r[0], r[1]): json.loads(r[2]) for r in rows}
    misses = [(kw, c) for kw, c in pairs if (kw, c) not in hits]
    return hits, misses


def _labs_cache_write_batch(fresh_results: dict) -> None:
    """Upsert fresh Labs enrichment results into labs_enrichment_cache."""
    if not fresh_results:
        return
    now = datetime.now().isoformat()
    con = sqlite3.connect(CACHE_DB)
    con.executemany(
        "INSERT INTO labs_enrichment_cache (keyword, country, enrichment, fetched_at) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(keyword, country) DO UPDATE SET "
        "enrichment = excluded.enrichment, fetched_at = excluded.fetched_at",
        [(kw, country, json.dumps(enrich), now)
         for (kw, country), enrich in fresh_results.items()]
    )
    con.commit()
    con.close()


# Module-level counters set by _fetch_dataforseo_labs_batch for cost reporting
_labs_cache_hit_count = 0
_labs_api_call_count  = 0

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


sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from trend_forecast import predict_persistence
    _FORECAST_AVAILABLE = True
except Exception:
    _FORECAST_AVAILABLE = False

BASE    = Path(__file__).resolve().parent
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

# ── Vertical competition fallback ─────────────────────────────────────────────
# Applied when DataForSEO returns competition=0 but keyword has meaningful CPC.
# Values are empirical mid-range estimates, not upper bounds.
VERTICAL_COMP_FALLBACK = {
    "legal":          0.45,
    "legal_services": 0.45,
    "insurance":      0.55,
    "auto_insurance": 0.55,
    "finance":        0.40,
    "loans_credit":   0.40,
    "health":         0.35,
    "medical_pharma": 0.35,
    "home":           0.30,
    "home_services":  0.30,
    "automotive":     0.40,
    "real_estate":    0.35,
    "default":        0.20,
}

# ── RPC Estimator (five-level empirical lookup) ───────────────────────────────
_RPC_ESTIMATOR: dict = {}
_RPC_PATTERNS:  dict = {}
try:
    sys.path.insert(0, str(BASE))
    from modules.rpc_estimator import (
        load_estimator  as _load_estimator,
        load_patterns   as _load_rpc_patterns,
        enrich_keyword_rpc as _enrich_keyword_rpc,
    )
    _RPC_ESTIMATOR = _load_estimator()
    _RPC_PATTERNS  = _load_rpc_patterns()
    _RPC_AVAILABLE = True
except Exception as _rpc_load_err:
    _RPC_AVAILABLE = False
    print(f"  ⚠️  RPC estimator unavailable: {_rpc_load_err}")

# ── Scoring constants (moved to modules/rsoc_scorer.py) ──────────────────────
from modules.rsoc_scorer import _EMERGING_THRESHOLD, _SERP_FRICTION, _RSOC_WEIGHTS

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
        results = list(response)   # iterate here so lazy-fetch exceptions are caught
    except Exception as ex:
        # Wrap all errors (GoogleAdsException, gRPC errors, etc.) so the caller
        # can detect test-account / fatal errors without dealing with SDK internals.
        raise ValueError(f"Google Ads API error: {str(ex)[:400]}") from ex
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

    # Budget gate — $0.01 per keyword on search_volume/live
    call_cost = DFS_ENDPOINT_COSTS["search_volume_live"]
    if not pre_flight_budget_check(call_cost, DFS_DAILY_BUDGET_USD):
        raise ValueError(f"[Budget] Daily cap reached — skipping search_volume/live for '{keyword}'")

    _, _, dfs_location = _geo_params(country)
    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    body    = [{"keywords": [clean_kw], "language_name": "English",
                "location_code": dfs_location, "include_serp_info": False}]
    _dfs_last_call = time.time()
    r = _dfs_post(
        "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live",
        headers=headers, json_body=body, timeout=15
    )
    increment_usd_spent(call_cost, "search_volume_live")
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


def _fetch_bulk_kd(keyword_country_pairs: list) -> dict:
    """
    Fetch keyword difficulty from /bulk_keyword_difficulty/live.
    Returns dict of (keyword_str, country_iso) → kd_int.
    Cost: $0.01/1000 keywords (~$0.001 for typical 95-keyword batch).
    """
    if not DFS_READY or not keyword_country_pairs:
        return {}

    by_country: dict = {}
    for kw, country in keyword_country_pairs:
        if kw.strip():
            by_country.setdefault(country, []).append(kw)

    kd_dict: dict = {}
    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

    for country, keywords in by_country.items():
        _, _, location_code = _geo_params(country)
        for i in range(0, len(keywords), 1000):
            batch = keywords[i : i + 1000]
            estimated_cost = DFS_ENDPOINT_COSTS["bulk_kd"] * len(batch)
            if not pre_flight_budget_check(estimated_cost, DFS_DAILY_BUDGET_USD):
                continue
            payload = [{"keywords": batch, "location_code": location_code,
                        "language_code": _dfs_language(country)}]
            try:
                r = _dfs_post(
                    f"{DFS_LABS_BASE}/bulk_keyword_difficulty/live",
                    headers=headers, json_body=payload, timeout=20
                )
                increment_usd_spent(estimated_cost, "bulk_kd")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    continue
                for item in tasks[0].get("result", []):
                    kd_val  = item.get("keyword_difficulty") or 0
                    kw_resp = item.get("keyword", "")
                    if kw_resp and kd_val:
                        kd_dict[(kw_resp, country)] = kd_val
            except Exception as e:
                print(f"  ⚠️  bulk_kd error [{country}]: {e}")

    if kd_dict:
        print(f"  KD enrichment: {len(kd_dict)}/{len(keyword_country_pairs)} keywords got KD data")
    return kd_dict


def _bulk_kd_gate(keyword_country_pairs: list) -> tuple:
    """
    Cheap pre-filter: reject keywords with keyword_difficulty < 15 using the
    bulk_keyword_difficulty endpoint ($0.01 per 1,000 keywords).

    Args:
        keyword_country_pairs: list of (keyword_str, country_iso) tuples

    Returns:
        Tuple of:
          - List of (keyword_str, country_iso) tuples where KD >= 15.
          - Dict mapping (keyword_str, country_iso) → kd_int for all keywords
            where DFS returned a non-zero KD value (used downstream to populate
            the kd field when Labs enrichment has no data).
        On any API error, returns the input list unchanged (fail open), kd_dict={}.
    """
    if not DFS_READY or not keyword_country_pairs:
        return keyword_country_pairs, {}

    # Group by country because location_code differs per country
    by_country: dict = {}
    for kw, country in keyword_country_pairs:
        by_country.setdefault(country, []).append(kw)

    passing    = []
    kd_dict: dict = {}   # (keyword, country) → kd_int for all DFS-known keywords
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
                r = _dfs_post(
                    f"{DFS_LABS_BASE}/bulk_keyword_difficulty/live",
                    headers=headers, json_body=payload, timeout=20
                )
                # Record spend immediately after POST
                increment_usd_spent(estimated_cost, "bulk_kd")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    # API error — fail open, pass all keywords through
                    passing.extend((kw, country) for kw in batch)
                    continue
                # Build a map from lowercased API response keyword → KD
                # so we can match back to original input keywords
                kd_map = {}
                for item in tasks[0].get("result", []):
                    kd_val  = item.get("keyword_difficulty") or 0
                    kw_resp = item.get("keyword", "")
                    if kw_resp:
                        kd_map[kw_resp.lower()] = kd_val

                for orig_kw in batch:
                    kd = kd_map.get(orig_kw.lower())
                    if kd is None:
                        # DFS doesn't know this keyword — fail open, let GKP decide
                        passing.append((orig_kw, country))
                    elif kd >= 15:
                        passing.append((orig_kw, country))
                        kd_dict[(orig_kw, country)] = kd   # store for downstream use
                    else:
                        _log_error("validation_kd_gate",
                                   f"KD gate rejected '{orig_kw}' [{country}]: KD={kd} (<15)")
            except Exception as e:
                # Fail open on any error — never block the pipeline
                print(f"  ⚠️  KD gate error [{country}]: {e}")
                passing.extend((kw, country) for kw in batch)

    passed  = len(passing)
    total   = len(keyword_country_pairs)
    print(f"  KD gate:  {total} → {passed} passing (KD ≥ 15), "
          f"{total - passed} rejected")
    return passing, kd_dict


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
    global _labs_cache_hit_count, _labs_api_call_count

    if not DFS_READY or not keyword_country_pairs:
        return {}

    # ── Check Labs cache first ────────────────────────────────────────────────
    cache_hits, pairs_to_fetch = _labs_cache_lookup_batch(keyword_country_pairs)
    _labs_cache_hit_count = len(cache_hits)
    results = dict(cache_hits)

    if not pairs_to_fetch:
        print(f"  Labs enrichment: {len(keyword_country_pairs)} from cache (0 API calls)")
        _labs_api_call_count = 0
        return results

    print(f"  Labs enrichment: {len(cache_hits)} from cache, {len(pairs_to_fetch)} need API")

    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    fresh_results = {}

    # Group by country; process in batches of 700
    # Filter empty-string keywords before grouping — they produce useless "" cache entries
    by_country: dict = {}
    for kw, country in pairs_to_fetch:
        if kw.strip():
            by_country.setdefault(country, []).append(kw)

    for country, keywords in by_country.items():
        _, _, location_code = _geo_params(country)
        for i in range(0, len(keywords), 700):
            batch = keywords[i : i + 700]
            # Pre-flight budget check (task fee + per-keyword fee)
            estimated_cost = (DFS_ENDPOINT_COSTS["keyword_overview_task_fee"]
                             + DFS_ENDPOINT_COSTS["keyword_overview_per_kw"] * len(batch))
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
                r = _dfs_post(
                    f"{DFS_LABS_BASE}/keyword_overview/live",
                    headers=headers, json_body=payload, timeout=60
                )
                # Record spend immediately after POST
                increment_usd_spent(estimated_cost, "keyword_overview")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    status_msg = tasks[0].get("status_message", "") if tasks else "no tasks"
                    # Some countries reject language_code (e.g. TW→zh, NO→no).
                    # Retry without it — DFS will use the default language for the location.
                    if "language_code" in status_msg.lower() and "invalid" in status_msg.lower():
                        payload[0].pop("language_code", None)
                        r2 = _dfs_post(
                            f"{DFS_LABS_BASE}/keyword_overview/live",
                            headers=headers, json_body=payload, timeout=60
                        )
                        increment_usd_spent(estimated_cost, "keyword_overview")
                        tasks = r2.json().get("tasks", [])
                        if not tasks or tasks[0].get("status_code") != 20000:
                            print(f"  ⚠️  Labs batch error [{country}] (no lang retry): "
                                  f"{tasks[0].get('status_message') if tasks else 'no tasks'}")
                            continue
                    else:
                        print(f"  ⚠️  Labs batch error [{country}]: {status_msg}")
                        continue

                results_list = tasks[0].get("result") or []
                for idx, item in enumerate(results_list):
                    kw_text = item.get("keyword", "")
                    # Positional fallback: DFS keyword_overview/live preserves input order.
                    # When kw_text="" (unrecognised keyword), recover from batch position.
                    if not kw_text and idx < len(batch):
                        kw_text = batch[idx]
                    if not kw_text:
                        continue  # skip if still unresolvable
                    ki      = item.get("keyword_info", {}) or {}
                    kp      = item.get("keyword_properties", {}) or {}
                    si      = item.get("search_intent_info", {}) or {}
                    sinfo   = item.get("serp_info", {}) or {}
                    trend   = ki.get("search_volume_trend", {}) or {}

                    # Primary metrics (replaces search_volume/live)
                    cpc_val       = float(ki.get("cpc") or 0)
                    sv_val        = int(ki.get("search_volume") or 0)
                    comp_val      = float(ki.get("competition") or 0)
                    comp_idx      = int(ki.get("competition_index") or 0) if ki.get("competition_index") is not None else None
                    low_bid       = float(ki.get("low_top_of_page_bid") or 0)
                    high_bid      = float(ki.get("high_top_of_page_bid") or 0)

                    fresh_results[(kw_text, country)] = {
                        # Enrichment fields (existing)
                        "kd":                   int(kp.get("keyword_difficulty") or 0),
                        "main_intent":          si.get("main_intent", ""),
                        "secondary_intents":    si.get("secondary_keyword_intents", []),
                        "serp_item_types":      sinfo.get("serp_item_types", []),
                        "trend_monthly":        float(trend.get("monthly") or 0),
                        "trend_quarterly":      float(trend.get("quarterly") or 0),
                        "trend_yearly":         float(trend.get("yearly") or 0),
                        "monthly_searches":     ki.get("monthly_searches", []),
                        "cpc_labs":             cpc_val,
                        "competition_labs":     comp_val,
                        "is_another_language":  bool(kp.get("is_another_language")),
                        # Primary metrics (replaces fetch_dataforseo / search_volume_live)
                        "search_volume":        sv_val,
                        "cpc_usd":              round(cpc_val, 2),
                        "cpc_low_usd":          round(low_bid, 2),
                        "cpc_high_usd":         round(high_bid, 2),
                        "competition":          round(comp_val, 2),
                        "competition_index":    comp_idx,
                        "source":               "dataforseo_labs",
                    }
            except Exception as e:
                print(f"  ⚠️  Labs enrichment error [{country}] batch {i // 700 + 1}: {e}")

    # Persist fresh API results to cache; merge into combined output
    _labs_cache_write_batch(fresh_results)
    _labs_api_call_count = len(fresh_results)
    results.update(fresh_results)

    _per_kw_cost = DFS_ENDPOINT_COSTS["keyword_overview_per_kw"]
    saved = len(cache_hits) * _per_kw_cost
    print(f"  Labs enrichment: {len(pairs_to_fetch)} API → {len(fresh_results)} enriched "
          f"| cache saved ${saved:.4f}")
    return results


def _fetch_dfs_gads_batch(keyword_country_pairs: list) -> dict:
    """
    Fetch CPC + competition from keywords_data/google_ads/search_volume/live.
    Called only for keywords that Labs returned cpc=0 and competition=0 for —
    these are keywords not in the Labs index but that DO have Google Ads auction data.
    Returns dict keyed by (keyword_str, country_iso).
    """
    if not DFS_READY or not keyword_country_pairs:
        return {}

    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    results = {}

    by_country: dict = {}
    for kw, country in keyword_country_pairs:
        if kw.strip():
            by_country.setdefault(country, []).append(kw)

    for country, keywords in by_country.items():
        _, _, location_code = _geo_params(country)
        for i in range(0, len(keywords), 700):
            batch = keywords[i : i + 700]
            # Use task-fee pricing (one POST = one task regardless of batch size)
            call_cost = DFS_ENDPOINT_COSTS.get("keyword_overview_task_fee", 0.01)
            if not pre_flight_budget_check(call_cost, DFS_DAILY_BUDGET_USD):
                print(f"  [Budget] Skipping search_volume fallback [{country}] — budget exhausted")
                continue
            payload = [{
                "keywords":      batch,
                "location_code": location_code,
                "language_name": "English",
                "include_serp_info": False,
            }]
            try:
                r = _dfs_post(
                    "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live",
                    headers=headers, json_body=payload, timeout=30
                )
                increment_usd_spent(call_cost, "search_volume_live")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    print(f"  ⚠️  search_volume fallback error [{country}]: "
                          f"{tasks[0].get('status_message') if tasks else 'no tasks'}")
                    continue
                for item in (tasks[0].get("result") or []):
                    kw_text = item.get("keyword", "")
                    if not kw_text:
                        continue
                    ci = int(item.get("competition_index", 0) or 0)
                    results[(kw_text, country)] = {
                        "competition":       round(ci / 100, 2),
                        "competition_index": ci,
                        "cpc_usd":           round(float(item.get("cpc", 0) or 0), 2),
                        "cpc_low_usd":       round(float(item.get("low_top_of_page_bid", 0) or 0), 2),
                        "cpc_high_usd":      round(float(item.get("high_top_of_page_bid", 0) or 0), 2),
                        "search_volume":     int(item.get("search_volume", 0) or 0),
                        "source":            "dataforseo_gads",
                    }
            except Exception as e:
                print(f"  ⚠️  search_volume fallback error [{country}] batch {i // 700 + 1}: {e}")

    priced = sum(1 for v in results.values() if v["cpc_usd"] > 0 or v["competition"] > 0)
    print(f"  search_volume fallback: {priced}/{len(keyword_country_pairs)} keywords got real data")
    return results


# ── Scoring (extracted to modules/rsoc_scorer.py) ─────────────────────────────
from modules.rsoc_scorer import (
    compute_rsoc_score,
    tag_opportunity_v2,
    classify_emerging,
    _apply_hard_gates,
    _apply_soft_modifiers,
    _compute_kvsi,
    _compute_ssr,
    _infer_intent,
)


# ── Provider router ───────────────────────────────────────────────────────────

def get_keyword_metrics(keyword, country="US"):
    """Fetch metrics for a single keyword. Google Ads only (free).
    DataForSEO keywords are handled in batch via _fetch_dataforseo_labs_batch().
    """
    if GADS_READY:
        try:
            return fetch_google_ads(keyword, country)
        except Exception as e:
            print(f"  ⚠️  Google Ads error '{keyword}': {e}")

    # NOTE: DFS per-keyword fetch_dataforseo() removed — all DFS lookups now
    # go through _fetch_dataforseo_labs_batch() in the pipeline (88x cheaper).
    return None


# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_ai(cpc_usd, search_volume, competition, serp_dominance=0.5):
    """Arbitrage Index incorporating SERP weakness from vetting stage.
    serp_dominance = ratio of HIGH_DA authority sites in SERP (0-1).
    serp_weakness = 1 - serp_dominance: thin SERP = easy to rank = good for arbitrage."""
    serp_weakness = max(1.0 - serp_dominance, 0.1)
    return round((cpc_usd * search_volume * serp_weakness) / ((competition or 0.01) * 10000), 4)


def tag_opportunity(ai_score):
    if ai_score > 0.8:
        return "GOLDEN_OPPORTUNITY"
    if ai_score > 0.5:
        return "WATCH"
    return "LOW"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
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
    # Collect pre-fetched pairs for Labs enrichment (they need kd/intent data even though CPC is known)
    _pre_fetched_pairs: list = []

    for opp in vetted:
        kw      = opp.get("keyword", "")
        country = opp.get("country", "US")
        # Skip entries that already carry valid CPC data (pre-fetched upstream)
        # cpc_usd=0 is NOT valid — it means DataForSEO returned nothing (budget exhausted, not in index, etc.)
        if (opp.get("cpc_usd") is not None and float(opp.get("cpc_usd") or 0) > 0
                and opp.get("search_volume") is not None):
            _pre_fetched_count += 1
            _pre_fetched_pairs.append((kw, country))
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
    # Consolidated approach: use keyword_overview/live for ALL DataForSEO metrics
    # in a single batch call (replaces bulk_kd_gate + per-keyword search_volume/live).
    # Google Ads path is kept as primary when credentials are available (free tier).

    fresh_metrics: dict = {}  # (keyword, country) → metrics dict
    api_calls_made      = 0

    # Google Ads keywords: still use per-keyword SDK call (free)
    _gads_misses = []
    _dfs_misses  = []
    _gads_working = True   # set False on first non-recoverable error (e.g. test token)
    _gads_total_errors = 0  # total failures — never reset (unlike consecutive counter)
    if GADS_READY:
        # Pre-filter keywords that can be submitted before any fatal error is detected
        _gads_submit = []
        for kw, country in approved_misses:
            if not _gads_working:
                _dfs_misses.append((kw, country))
            else:
                _gads_submit.append((kw, country))

        # Process Google Ads keywords in parallel (up to 5 concurrent)
        _gads_lock = __import__("threading").Lock()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            for kw, country in _gads_submit:
                futures[executor.submit(fetch_google_ads, kw, country)] = (kw, country)

            for future in as_completed(futures):
                kw, country = futures[future]
                # If GADS was disabled by another thread's fatal error, skip processing
                if not _gads_working:
                    _dfs_misses.append((kw, country))
                    continue
                try:
                    metrics = future.result()
                    if metrics:
                        fresh_metrics[(kw, country)] = metrics
                        _cache_write_back(kw, country, metrics)
                        with _gads_lock:
                            api_calls_made += 1
                except Exception as e:
                    with _gads_lock:
                        _gads_total_errors += 1
                        _current_errors = _gads_total_errors
                    # str(GoogleAdsException) is often empty — extract from the gRPC cause instead
                    _cause = getattr(e, '__cause__', None) or e
                    _cause_parts = []
                    if hasattr(_cause, 'code') and callable(_cause.code):
                        _cause_parts.append(str(_cause.code()))
                    if hasattr(_cause, 'details') and callable(_cause.details):
                        _cause_parts.append(str(_cause.details()))
                    err_str = (" ".join(_cause_parts) if _cause_parts else str(e)).lower()
                    # Disable GADS on known fatal errors OR after 3 total failures
                    if ("test account" in err_str or "not whitelisted" in err_str
                            or "developer_token" in err_str or "developer token" in err_str
                            or "permission" in err_str
                            or _current_errors >= 3):
                        print(f"  ⚠️  Google Ads: non-recoverable error ({err_str[:80]!r}) — "
                              f"switching all remaining keywords to DataForSEO")
                        _gads_working = False
                    else:
                        print(f"  ⚠️  Google Ads error '{kw}': {e}")
                    _dfs_misses.append((kw, country))
    else:
        _dfs_misses = list(approved_misses)

    _increment_usage(api_calls_made)

    # Remove successfully resolved deferred keywords (from Google Ads calls above)
    resolved_deferred = [(kw, c) for kw, c in deferred_recovered
                         if (kw, c) in cache_hits or (kw, c) in fresh_metrics]
    _remove_from_deferred(resolved_deferred)

    # ── Consolidated Labs batch: metrics + KD + intent + SERP + trends ───────
    # Single keyword_overview/live call replaces 3 old calls:
    #   - bulk_keyword_difficulty ($0.01/1000 kw)  → KD now in keyword_overview
    #   - search_volume/live ($0.01/keyword)       → CPC/SV now in keyword_overview
    #   - keyword_overview ($0.08/700 kw)          → enrichment (intent, SERP, trends)
    # All keywords that need data go through this one batch endpoint.
    _labs_enrichment_pairs = list({
        *cache_hits.keys(),         # cache hits still need enrichment (kd/intent)
        *fresh_metrics.keys(),      # Google Ads results need enrichment
        *_pre_fetched_pairs,        # pre-fetched CPC keywords need enrichment
        *_dfs_misses,               # DFS-only keywords get BOTH metrics + enrichment here
    })
    _labs_enrichment: dict = {}
    _sv_fallback: dict = {}  # search_volume/live results for keywords Labs couldn't price
    _bulk_kd_map: dict = {}  # populated from keyword_overview KD values

    if DFS_READY and _labs_enrichment_pairs:
        _per_kw = DFS_ENDPOINT_COSTS["keyword_overview_per_kw"]
        _task_fee = DFS_ENDPOINT_COSTS["keyword_overview_task_fee"]
        _n_batches = max(1, -(-len(_labs_enrichment_pairs) // 700))  # ceil division
        _est_api = _n_batches * _task_fee + len(_labs_enrichment_pairs) * _per_kw
        print(f"  DataForSEO cost estimate — keyword_overview: {len(_labs_enrichment_pairs)} keywords "
              f"in {_n_batches} batch(es) = ${_est_api:.4f} max (cache may reduce this)")
        _labs_enrichment = _fetch_dataforseo_labs_batch(_labs_enrichment_pairs)
        _hit_rate = (_labs_cache_hit_count / len(_labs_enrichment_pairs) * 100
                     if _labs_enrichment_pairs else 0)
        _actual_cost = (_labs_api_call_count * _per_kw
                        + (1 if _labs_api_call_count > 0 else 0) * _task_fee)
        print(f"  DataForSEO actual spend — keyword_overview: ${_actual_cost:.4f} "
              f"(cache hit rate: {_hit_rate:.0f}%)")

        # ── search_volume/live fallback for keywords Labs couldn't price ──────────
        # Labs returns competition=0 and cpc=0 for trending/news keywords not in its index.
        # Call Google Ads search_volume endpoint for those to get real auction data.
        _zero_priced = [
            (kw, c) for (kw, c) in _labs_enrichment_pairs
            if float((_labs_enrichment.get((kw, c)) or {}).get("cpc_usd") or 0) == 0
            and float((_labs_enrichment.get((kw, c)) or {}).get("competition") or 0) == 0
        ]
        if _zero_priced:
            print(f"  search_volume fallback: {len(_zero_priced)}/{len(_labs_enrichment_pairs)} "
                  f"keywords have cpc=0 from Labs — querying search_volume/live")
            _sv_fallback = _fetch_dfs_gads_batch(_zero_priced)
            # Patch _labs_enrichment: inject CPC/competition while preserving intent/KD/trend
            for _key, _sv in _sv_fallback.items():
                if _key in _labs_enrichment:
                    _labs_enrichment[_key].update({
                        "cpc_usd":           _sv["cpc_usd"],
                        "cpc_low_usd":       _sv["cpc_low_usd"],
                        "cpc_high_usd":      _sv["cpc_high_usd"],
                        "competition":       _sv["competition"],
                        "competition_index": _sv["competition_index"],
                        "search_volume": _sv["search_volume"] or _labs_enrichment[_key].get("search_volume", 0),
                    })
                else:
                    _labs_enrichment[_key] = _sv

        # Extract primary metrics for DFS-only keywords from the enrichment response
        for kw, country in _dfs_misses:
            enrich = (_labs_enrichment.get((kw, country))
                      or _labs_enrichment.get((_clean_keyword(kw), country)))
            if enrich and enrich.get("source") in ("dataforseo_labs", "dataforseo_gads"):
                cpc_val = float(enrich.get("cpc_usd") or 0)
                sv_val  = int(enrich.get("search_volume") or 0)
                if cpc_val > 0 or sv_val > 0:
                    fresh_metrics[(kw, country)] = {
                        "search_volume":  sv_val,
                        "cpc_usd":        enrich["cpc_usd"],
                        "cpc_low_usd":    enrich.get("cpc_low_usd", 0),
                        "cpc_high_usd":   enrich.get("cpc_high_usd", 0),
                        "competition":    enrich["competition"],
                        "source":         enrich["source"],
                    }
                    _cache_write_back(kw, country, fresh_metrics[(kw, country)])

        # Fetch real KD from bulk_keyword_difficulty endpoint
        # (keyword_overview doesn't return keyword_difficulty — it only comes from this endpoint)
        _bulk_kd_map = _fetch_bulk_kd(_labs_enrichment_pairs)
        # Merge KD into enrichment dicts so downstream scoring has it
        for (kw, country), kd_val in _bulk_kd_map.items():
            if (kw, country) in _labs_enrichment:
                _labs_enrichment[(kw, country)]["kd"] = kd_val

        # Resolve any remaining deferred keywords that DFS batch resolved
        extra_resolved = [(kw, c) for kw, c in deferred_recovered
                          if (kw, c) in fresh_metrics and (kw, c) not in resolved_deferred]
        _remove_from_deferred(extra_resolved)

    # ── Build unified metrics map: norm_key → metrics ────────────────────────
    # Only include entries with real CPC data (cpc_usd > 0). Zero-CPC entries
    # mean DataForSEO returned nothing (budget exhausted, keyword not in index, etc.).
    # Keeping cpc=0 entries causes every keyword to score LOW — they should instead
    # fall through to the EMERGING/UNSCORED classification path.
    _all_metrics: dict = {}
    for (kw, country), m in cache_hits.items():
        if float(m.get("cpc_usd") or 0) > 0:
            _all_metrics[_normalize_keyword(kw) + "|" + country] = m
    for (kw, country), m in fresh_metrics.items():
        if float(m.get("cpc_usd") or 0) > 0:
            _all_metrics[_normalize_keyword(kw) + "|" + country] = m

    # ── Score all vetted opportunities ────────────────────────────────────────────
    validated = []

    for opp in vetted:
        keyword = opp.get("keyword", "")
        country = opp.get("country", "US")

        # Pre-fetched data check (from keyword_extractor upstream — Bucket A)
        if (opp.get("cpc_usd") is not None and float(opp.get("cpc_usd") or 0) > 0
                and opp.get("search_volume") is not None):
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
            # Phase 2.1: incorporate serp_dominance from vetting stage
            _serp_dom      = float(opp.get("serp_dominance") or 0.5)
            ai_score       = compute_ai(cpc_usd, search_vol, competition, _serp_dom)
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

            # Patch competition=0 — 3-layer fallback chain:
            # Layer 1: Labs competition_labs (same endpoint, sometimes non-zero)
            if competition == 0.0 and enrichment:
                labs_comp = float(enrichment.get("competition_labs") or enrichment.get("competition") or 0.0)
                if labs_comp > 0.0:
                    competition = labs_comp

            # Layer 2: search_volume/live fallback (real Google Ads auction data)
            if competition == 0.0 or cpc_usd == 0.0:
                _sv = (_sv_fallback.get((keyword, country))
                       or _sv_fallback.get((_clean_keyword(keyword), country)))
                if _sv:
                    if competition == 0.0 and float(_sv.get("competition") or 0) > 0:
                        competition = float(_sv["competition"])
                    if cpc_usd == 0.0 and float(_sv.get("cpc_usd") or 0) > 0:
                        cpc_usd = float(_sv["cpc_usd"])
                    if cpc_high == 0.0 and float(_sv.get("cpc_high_usd") or 0) > 0:
                        cpc_high = float(_sv["cpc_high_usd"])

            # Layer 3: Vertical competition fallback
            # a) When competition=0: apply vertical average (existing behavior)
            # b) When competition is suspiciously low for a high-CPC premium vertical:
            #    floor it at the vertical fallback value. DFS often underreports competition
            #    for trending/news keywords even when advertisers are clearly bidding $20+.
            _ref_cpc = cpc_usd if cpc_usd > 0 else cpc_high
            _vert = opp.get("vertical_match") or opp.get("vertical") or "general"
            if _vert == "general":
                _vert = opp.get("commercial_category", "general") or "general"
            _vert_fallback = VERTICAL_COMP_FALLBACK.get(_vert, VERTICAL_COMP_FALLBACK["default"])
            if competition == 0.0 and _ref_cpc > 0:
                _vd   = _VERTICAL_CPC_REF.get(_vert) or _VERTICAL_CPC_REF.get("general") or {}
                _vert_avg = float(_vd.get("avg_cpc") or 4.0)
                if _ref_cpc >= _vert_avg * 0.5:
                    competition = _vert_fallback
            elif competition > 0 and competition < _vert_fallback and cpc_high >= 20.0:
                # High-CPC keyword with suspiciously low competition — floor at vertical avg
                competition = _vert_fallback

            # Patch cpc_high with Labs high_top_of_page_bid if it's a better signal
            if enrichment:
                labs_high = float(enrichment.get("cpc_high_usd") or 0)
                if labs_high > cpc_high:
                    cpc_high = labs_high

            # Classify emerging BEFORE hard gates so emerging_tag is available for CPC bypass
            vertical  = opp.get("vertical_match") or opp.get("vertical") or "general"
            # Use commercial_category as fallback when vetting SERP classifier returns "general"
            if vertical == "general":
                vertical = opp.get("commercial_category", "general") or "general"
            vertical_data  = _VERTICAL_CPC_REF.get(vertical) or _VERTICAL_CPC_REF.get("general") or {}
            vert_avg_cpc   = float(vertical_data.get("avg_cpc") or 4.0)

            # Infer intent when Labs returned nothing (~96% of keywords)
            if not enrichment.get("main_intent"):
                _inferred = _infer_intent(keyword, cpc_high, vertical)
                if _inferred:
                    enrichment = {**enrichment, "main_intent": _inferred, "intent_inferred": True}

            emerging_signals = {
                "trend_monthly":        enrichment.get("trend_monthly", 0),
                "trend_quarterly":      enrichment.get("trend_quarterly", 0),
                "trend_yearly":         enrichment.get("trend_yearly", 0),
                "kd":                   enrichment.get("kd", 0),
                "cpc":                  cpc_usd,
                "high_top_of_page_bid": cpc_high,
                "monthly_searches":     enrichment.get("monthly_searches", []),
            }
            # Only classify emerging if we have real metrics — otherwise rsoc=0 records
            # get tagged EMERGING on vertical ceiling alone (bogus signal)
            if metrics["source"] != "none_configured":
                emerging_class = classify_emerging(emerging_signals, vert_avg_cpc)
            else:
                emerging_class = None
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
                    "config_version":   _CONFIG_VERSION,
                    "validated_at":     datetime.now().isoformat(),
                })
                continue

            # Scoring profile: EMERGING weights for trend-driven keywords
            profile    = "EMERGING" if emerging_class in ("EMERGING", "EMERGING_HIGH") else "EVERGREEN"
            rsoc_score = compute_rsoc_score(cpc_high, competition, search_vol,
                                            enrichment, scoring_profile=profile,
                                            cpc_usd=cpc_usd)

            # Apply soft modifiers (replace former hard gates 3/5/6/7)
            rsoc_score_modified = _apply_soft_modifiers(
                rsoc_score,
                kd          = enrichment.get("kd", 0),
                competition = competition,
                htpb        = cpc_high,
                volume      = search_vol,
                country     = country,
                intent      = enrichment.get("main_intent", ""),
            )

            # Tier classification uses rsoc_score after soft modifiers
            tag = tag_opportunity_v2(rsoc_score_modified, cpc_usd, competition,
                                     enrichment, vertical, country)
            # Emerging signals override tier from score — but NOT GOLDEN_OPPORTUNITY.
            # A keyword that scores GOLDEN on rsoc should keep that tag even if it has
            # trend signals; GOLDEN > EMERGING/EMERGING_HIGH by definition.
            if emerging_class and tag != "GOLDEN_OPPORTUNITY":
                tag = emerging_class
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
                "rsoc_score":              rsoc_score_modified,   # composite + soft modifiers
                "persistence_score":       persistence.get("persistence_probability"),
                "predicted_halflife_days": persistence.get("predicted_halflife_days"),
                "tag":                     tag,
                "metrics_source":          metrics["source"],
                # New enrichment fields
                "kd":                      enrichment.get("kd") or _bulk_kd_map.get((keyword, country)),
                "main_intent":             enrichment.get("main_intent"),
                "intent_inferred":         enrichment.get("intent_inferred", False),
                "serp_item_types":         enrichment.get("serp_item_types", []),
                "ssr":                     _compute_ssr(enrichment.get("serp_item_types", [])),
                "trend_monthly":           enrichment.get("trend_monthly"),
                "trend_quarterly":         enrichment.get("trend_quarterly"),
                "kvsi":                    kvsi_val,
                "config_version":          _CONFIG_VERSION,
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
                "config_version":          _CONFIG_VERSION,
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

    # ── RPC enrichment pass ───────────────────────────────────────────────────────
    # Attach rpc_expected, rpc_actual, rpc_display to every validated keyword.
    # Non-fatal: if estimator not built yet, fields are silently omitted.
    if _RPC_AVAILABLE:
        _rpc_enriched = 0
        for _entry in validated:
            try:
                _enrich_keyword_rpc(_entry, _RPC_ESTIMATOR, _RPC_PATTERNS)
                _rpc_enriched += 1
            except Exception:
                pass
        if _rpc_enriched:
            print(f"  RPC enrichment: {_rpc_enriched} keywords annotated "
                  f"({'estimator ready' if _RPC_ESTIMATOR else 'patterns-only fallback'})")

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


if __name__ == "__main__":
    main()
