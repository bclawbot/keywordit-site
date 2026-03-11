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
from country_config import get_country_tier as _country_tier

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
    """Create cache tables if they don't exist."""
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


def _geo_params(country_iso):
    return GEO_MAP.get(country_iso.upper(), (2840, "us", 2840))


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
        ai_score       = compute_ai(metrics["cpc_usd"], metrics["search_volume"], metrics["competition"])
        tag            = tag_opportunity(ai_score)
        persistence    = {}
        weighted_score = ai_score
        if _FORECAST_AVAILABLE:
            try:
                persistence    = predict_persistence(keyword, country)
                weighted_score = round(ai_score * persistence.get("persistence_probability", 0.5), 4)
            except Exception:
                pass
        validated.append({
            **opp,
            "search_volume":           metrics["search_volume"],
            "cpc_usd":                 metrics["cpc_usd"],
            "cpc_low_usd":             metrics.get("cpc_low_usd"),
            "cpc_high_usd":            metrics.get("cpc_high_usd"),
            "competition":             metrics["competition"],
            "competition_index":       metrics.get("competition_index"),
            "monthly_searches":        metrics.get("monthly_searches", []),
            "arbitrage_index":         ai_score,
            "weighted_score":          weighted_score,
            "persistence_score":       persistence.get("persistence_probability"),
            "predicted_halflife_days": persistence.get("predicted_halflife_days"),
            "tag":                     tag,
            "metrics_source":          metrics["source"],
            "validated_at":            datetime.now().isoformat(),
        })
    else:
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
            "tag":                     "UNSCORED",
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

golden_watch = [r for r in validated if r["tag"] in ("GOLDEN_OPPORTUNITY", "WATCH")]
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

golden_count   = sum(1 for r in validated if r["tag"] == "GOLDEN_OPPORTUNITY")
watch_count    = sum(1 for r in validated if r["tag"] == "WATCH")
unscored_count = sum(1 for r in validated if r["tag"] == "UNSCORED")
low_count      = len(validated) - golden_count - watch_count - unscored_count

print(
    f"✅ Validation complete: {len(validated)} records — "
    f"{golden_count} GOLDEN, {watch_count} WATCH, "
    f"{low_count} LOW, {unscored_count} UNSCORED → {OUTPUT.name}"
)
print(f"   API calls this run: {api_calls_made} | "
      f"Daily total: {today_usage + api_calls_made}/{DAILY_API_BUDGET} | "
      f"Cache size: {len(cache_hits)} hits served from DB")
if _lancedb_written:
    print(f"   LanceDB: {_lancedb_written} opportunities written")
