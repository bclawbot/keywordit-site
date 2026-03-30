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
    r = requests.post(
        "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live",
        headers=headers, json=body, timeout=15
    )
    increment_usd_spent(call_cost, "search_volume_live")
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
                r = requests.post(
                    f"{DFS_LABS_BASE}/keyword_overview/live",
                    headers=headers, json=payload, timeout=60
                )
                r.raise_for_status()
                # Record spend immediately after POST
                increment_usd_spent(estimated_cost, "keyword_overview")
                tasks = r.json().get("tasks", [])
                if not tasks or tasks[0].get("status_code") != 20000:
                    status_msg = tasks[0].get("status_message", "") if tasks else "no tasks"
                    # Some countries reject language_code (e.g. TW→zh, NO→no).
                    # Retry without it — DFS will use the default language for the location.
                    if "language_code" in status_msg.lower() and "invalid" in status_msg.lower():
                        payload[0].pop("language_code", None)
                        r2 = requests.post(
                            f"{DFS_LABS_BASE}/keyword_overview/live",
                            headers=headers, json=payload, timeout=60
                        )
                        r2.raise_for_status()
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
        WATCH               — rsoc_score >= 40 (decent metrics, worth monitoring)
        EMERGING_HIGH       — classify_emerging() override (3+ trend signals)
        EMERGING            — classify_emerging() override (1-2 trend signals)
        LOW                 — rsoc_score < 40 and no emerging signals
        UNSCORED            — no metrics available
    """
    if rsoc_score >= 65:
        return "GOLDEN_OPPORTUNITY"
    if rsoc_score >= 40:
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
    for kw, country in approved_misses:
        if not _gads_working:
            _dfs_misses.append((kw, country))
            continue
        try:
            metrics = fetch_google_ads(kw, country)
            if metrics:
                fresh_metrics[(kw, country)] = metrics
                _cache_write_back(kw, country, metrics)
                api_calls_made += 1
        except Exception as e:
            _gads_total_errors += 1
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
                    or _gads_total_errors >= 3):
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

    # Extract primary metrics for DFS-only keywords from the enrichment response
    for kw, country in _dfs_misses:
        enrich = (_labs_enrichment.get((kw, country))
                  or _labs_enrichment.get((_clean_keyword(kw), country)))
        if enrich and enrich.get("source") == "dataforseo_labs":
            fresh_metrics[(kw, country)] = {
                "search_volume":  enrich["search_volume"],
                "cpc_usd":        enrich["cpc_usd"],
                "cpc_low_usd":    enrich.get("cpc_low_usd", 0),
                "cpc_high_usd":   enrich.get("cpc_high_usd", 0),
                "competition":    enrich["competition"],
                "source":         "dataforseo_labs",
            }
            _cache_write_back(kw, country, fresh_metrics[(kw, country)])

    # Build KD map from enrichment (replaces _bulk_kd_gate output)
    for (kw, country), enrich in _labs_enrichment.items():
        kd_val = enrich.get("kd", 0)
        if kd_val:
            _bulk_kd_map[(kw, country)] = kd_val

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

        # Patch competition=0 with Labs data when pre-fetched value was zero.
        # DFS keyword_overview/live returns competition_labs which may be non-zero
        # even when the upstream keyword_extractor stored competition=0.
        if competition == 0.0 and enrichment:
            labs_comp = float(enrichment.get("competition_labs") or enrichment.get("competition") or 0.0)
            if labs_comp > 0.0:
                competition = labs_comp

        # Patch cpc_high with Labs high_top_of_page_bid if it's a better signal
        if enrichment:
            labs_high = float(enrichment.get("cpc_high_usd") or 0)
            if labs_high > cpc_high:
                cpc_high = labs_high

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
