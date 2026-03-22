# =============================================================================
# cpc_cache.py  —  DataForSEO cost optimization: dedup, cache, budget gate
#
# Provides three layers that sit between LLM output and DataForSEO API calls:
#
#   Layer 1 — normalize_and_dedupe(keywords)
#       Removes duplicates within a single batch (free, no API calls).
#       Normalization is comparison-only; original keyword strings are preserved.
#
#   Layer 2 — batch_cache_lookup(keywords)
#       Checks oracle.db for CPC data fetched within CACHE_TTL_HOURS (7 days).
#       Returns (hits_dict, misses_list). Cache hits skip the API entirely.
#
#   Layer 3 — budget_gate(cache_misses)
#       If remaining daily budget < number of misses, trims to budget and saves
#       the rest to a deferred table for the next pipeline run.
#
# All state lives in oracle.db (tables: keyword_cpc_cache, api_usage,
# deferred_keyword_lookups). Tables are created on first call to init_db().
# =============================================================================

import random
import re
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path("/Users/newmac/.openclaw/oracle.db")


# ── DB bootstrap ──────────────────────────────────────────────────────────────

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    """Create the three tables if they don't already exist."""
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS keyword_cpc_cache (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword       TEXT    NOT NULL,
                country       TEXT    NOT NULL,
                cpc           REAL,
                search_volume INTEGER,
                competition   REAL,
                fetched_at    TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(keyword, country)
            );
            CREATE INDEX IF NOT EXISTS idx_cache_lookup
                ON keyword_cpc_cache (keyword, country, fetched_at);

            CREATE TABLE IF NOT EXISTS api_usage (
                date           TEXT    PRIMARY KEY DEFAULT (date('now')),
                lookups        INTEGER NOT NULL DEFAULT 0,
                expand_results INTEGER NOT NULL DEFAULT 0,
                usd_spent_today REAL NOT NULL DEFAULT 0.0,
                endpoint_breakdown TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS deferred_keyword_lookups (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword             TEXT NOT NULL,
                country             TEXT NOT NULL,
                confidence          TEXT,
                tier                INTEGER,
                source_trend        TEXT,
                commercial_category TEXT,
                deferred_at         TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(keyword, country)
            );

            CREATE TABLE IF NOT EXISTS expansion_cache (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                seed_keyword    TEXT    NOT NULL,
                location_code   INTEGER NOT NULL,
                language_code   TEXT    NOT NULL,
                result_count    INTEGER NOT NULL DEFAULT 0,
                fetched_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(seed_keyword, location_code, language_code)
            );
            CREATE INDEX IF NOT EXISTS idx_expansion_lookup
                ON expansion_cache (seed_keyword, location_code, language_code, fetched_at);
        """)
    _migrate_api_usage()


def _migrate_api_usage():
    """Add usd_spent_today and endpoint_breakdown columns if missing (safe to re-run)."""
    with _conn() as con:
        cols = {row[1] for row in con.execute("PRAGMA table_info(api_usage)").fetchall()}
        if "usd_spent_today" not in cols:
            con.execute("ALTER TABLE api_usage ADD COLUMN usd_spent_today REAL NOT NULL DEFAULT 0.0")
        if "endpoint_breakdown" not in cols:
            con.execute("ALTER TABLE api_usage ADD COLUMN endpoint_breakdown TEXT DEFAULT '{}'")


def cleanup():
    """
    Garbage-collect stale entries:
      - cache rows older than 30 days (TTL re-fetches happen at 7 days anyway)
      - deferred rows older than 3 days (trend moment has passed)
    """
    with _conn() as con:
        removed_cache = con.execute(
            "DELETE FROM keyword_cpc_cache WHERE fetched_at < datetime('now', '-30 days')"
        ).rowcount
        removed_deferred = con.execute(
            "DELETE FROM deferred_keyword_lookups WHERE deferred_at < datetime('now', '-3 days')"
        ).rowcount
    return removed_cache, removed_deferred


# ── Layer 1: Normalize & deduplicate within batch ─────────────────────────────

_CURRENT_YEAR = str(datetime.now().year)
_LEADING_ARTICLES = re.compile(r"^(the|a|an)\s+", re.IGNORECASE)
_PUNCTUATION      = re.compile(r"['\-.,?!]")
_MULTI_SPACE      = re.compile(r"\s{2,}")


def normalize_keyword(keyword: str) -> str:
    """
    Produce a normalized form for duplicate comparison only.
    Always store/send the ORIGINAL keyword to DataForSEO.
    """
    kw = keyword.lower().strip()
    # Remove trailing current year: "best vpn 2026" → "best vpn"
    if kw.endswith(" " + _CURRENT_YEAR):
        kw = kw[: -(len(_CURRENT_YEAR) + 1)]
    # Strip leading articles
    kw = _LEADING_ARTICLES.sub("", kw)
    # Strip punctuation
    kw = _PUNCTUATION.sub("", kw)
    # Collapse spaces
    kw = _MULTI_SPACE.sub(" ", kw).strip()
    return kw


def normalize_and_dedupe(keywords: list) -> list:
    """
    Layer 1 — Remove duplicates within this batch.
    Returns the deduplicated list (first occurrence of each normalized key wins).
    Logs dropped duplicates to stdout at debug level.
    """
    seen:   dict = {}   # normalized_key → original keyword string
    result: list = []

    for kw_obj in keywords:
        keyword = kw_obj.get("keyword", "")
        country = kw_obj.get("country", "US").upper()
        norm    = normalize_keyword(keyword) + "|" + country

        if norm in seen:
            print(f"  [Dedupe] Dropped '{keyword}' ({country}) — duplicate of '{seen[norm]}'")
            continue

        seen[norm] = keyword
        result.append(kw_obj)

    return result


# ── Layer 2: Cache lookup ──────────────────────────────────────────────────────

def batch_cache_lookup(keywords: list, ttl_hours: int) -> tuple:
    """
    Layer 2 — Single SQL query for all keywords. No per-keyword round trips.

    Returns:
        hits_dict  — dict mapping (keyword_lower, country_upper) →
                     {cpc_usd, search_volume, competition}  (serve from cache)
        misses     — list of keyword dicts that need a fresh API lookup
    """
    if not keywords:
        return {}, []

    # Build composite keys for the WHERE IN clause
    composite_keys = [
        f"{kw.get('keyword', '').lower()}|{kw.get('country', 'US').upper()}"
        for kw in keywords
    ]
    placeholders = ",".join("?" * len(composite_keys))

    with _conn() as con:
        rows = con.execute(
            f"""
            SELECT keyword, country, cpc, search_volume, competition, fetched_at
            FROM keyword_cpc_cache
            WHERE keyword || '|' || country IN ({placeholders})
              AND fetched_at > datetime('now', '-{ttl_hours} hours')
            """,
            composite_keys,
        ).fetchall()

    # Build a lookup set of (normalized_keyword, country) that hit the cache
    hits_dict: dict = {}
    for row in rows:
        key = (row["keyword"].lower(), row["country"].upper())
        hits_dict[key] = {
            "cpc_usd":       row["cpc"] or 0.0,
            "search_volume": row["search_volume"] or 0,
            "competition":   row["competition"] or 0.0,
            "source":        "cache",
        }
        fetched_ago_h = round(
            (datetime.now() - datetime.fromisoformat(row["fetched_at"])).total_seconds() / 3600
        )
        print(f"  [Cache] HIT  '{row['keyword']}' ({row['country']}) — cached {fetched_ago_h}h ago")

    # Separate misses
    misses = []
    for kw_obj in keywords:
        key = (kw_obj.get("keyword", "").lower(), kw_obj.get("country", "US").upper())
        if key not in hits_dict:
            misses.append(kw_obj)

    return hits_dict, misses


def cache_write_back(cpc_data: dict):
    """
    Upsert fresh DataForSEO results into keyword_cpc_cache.
    cpc_data: {(keyword_lower, country_upper): {cpc_usd, search_volume, competition}}
    """
    rows = [
        (kw, country, v["cpc_usd"], v["search_volume"], v["competition"])
        for (kw, country), v in cpc_data.items()
    ]
    if not rows:
        return
    with _conn() as con:
        con.executemany(
            """
            INSERT INTO keyword_cpc_cache (keyword, country, cpc, search_volume, competition, fetched_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(keyword, country) DO UPDATE SET
                cpc           = excluded.cpc,
                search_volume = excluded.search_volume,
                competition   = excluded.competition,
                fetched_at    = datetime('now')
            """,
            rows,
        )


# ── Budget tracking ───────────────────────────────────────────────────────────

def get_today_usage() -> int:
    with _conn() as con:
        row = con.execute(
            "SELECT lookups FROM api_usage WHERE date = date('now')"
        ).fetchone()
    return row["lookups"] if row else 0


def increment_usage(count: int):
    with _conn() as con:
        con.execute(
            """
            INSERT INTO api_usage (date, lookups) VALUES (date('now'), ?)
            ON CONFLICT(date) DO UPDATE SET lookups = api_usage.lookups + ?
            """,
            (count, count),
        )


def get_today_expand_results() -> int:
    """Return total expansion results collected today (keywords_for_keywords + Labs)."""
    try:
        with _conn() as con:
            row = con.execute(
                "SELECT expand_results FROM api_usage WHERE date = date('now')"
            ).fetchone()
        return row["expand_results"] if row else 0
    except Exception:
        # Column may not exist on old DB — migrate and return 0
        try:
            with _conn() as con:
                con.execute("ALTER TABLE api_usage ADD COLUMN expand_results INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        return 0


def increment_expand_results(count: int):
    """Track how many expansion results were returned today (for cost-cap enforcement)."""
    if count <= 0:
        return
    try:
        with _conn() as con:
            con.execute(
                """
                INSERT INTO api_usage (date, expand_results) VALUES (date('now'), ?)
                ON CONFLICT(date) DO UPDATE SET expand_results = api_usage.expand_results + ?
                """,
                (count, count),
            )
    except Exception:
        # Column may not exist on old DB — migrate then retry
        try:
            with _conn() as con:
                con.execute("ALTER TABLE api_usage ADD COLUMN expand_results INTEGER NOT NULL DEFAULT 0")
            with _conn() as con:
                con.execute(
                    """
                    INSERT INTO api_usage (date, expand_results) VALUES (date('now'), ?)
                    ON CONFLICT(date) DO UPDATE SET expand_results = api_usage.expand_results + ?
                    """,
                    (count, count),
                )
        except Exception:
            pass


# ── Layer 3: Budget gate ──────────────────────────────────────────────────────

def budget_gate(cache_misses: list, daily_budget: int,
                country_config: dict, default_country: dict,
                high_confidence_first: bool = True) -> tuple:
    """
    Layer 3 — Trim cache_misses to the remaining daily budget.
    Keywords over budget are returned as `deferred` for the next run.

    Returns:
        to_lookup  — list of keyword dicts to send to DataForSEO now
        deferred   — list of keyword dicts to save for later
    """
    today_usage = get_today_usage()
    remaining   = daily_budget - today_usage

    if remaining <= 0:
        print(f"  [Budget] Daily budget exhausted ({today_usage}/{daily_budget}). "
              f"Deferring all {len(cache_misses)} lookups.")
        return [], cache_misses

    if len(cache_misses) <= remaining:
        print(f"  [Budget] {len(cache_misses)} lookups needed, {remaining} remaining — all clear")
        return cache_misses, []

    # Over budget — sort by tier then confidence, then randomize within group
    def sort_key(kw_obj):
        country    = kw_obj.get("country", "US").upper()
        tier       = country_config.get(country, default_country)["tier"]
        confidence = 0 if (high_confidence_first and kw_obj.get("confidence") == "high") else 1
        return (tier, confidence, random.random())

    sorted_misses = sorted(cache_misses, key=sort_key)
    to_lookup     = sorted_misses[:remaining]
    deferred      = sorted_misses[remaining:]

    print(
        f"  [Budget] {len(cache_misses)} lookups needed, {remaining} remaining — "
        f"sending top {len(to_lookup)}, deferring {len(deferred)}"
    )
    return to_lookup, deferred


# ── Deferred queue ────────────────────────────────────────────────────────────

def get_deferred() -> list:
    """Return all deferred keywords as dicts compatible with LLM extractor output."""
    with _conn() as con:
        rows = con.execute(
            "SELECT keyword, country, confidence, tier, source_trend, commercial_category "
            "FROM deferred_keyword_lookups "
            "ORDER BY deferred_at ASC"
        ).fetchall()
    return [dict(row) for row in rows]


def save_deferred(keywords: list):
    """Add keywords to the deferred table (ignore if already present)."""
    rows = [
        (
            kw.get("keyword", ""),
            kw.get("country", "US").upper(),
            kw.get("confidence"),
            kw.get("country_tier"),
            kw.get("source_trend", ""),
            kw.get("commercial_category", ""),
        )
        for kw in keywords
        if kw.get("keyword")
    ]
    if not rows:
        return
    with _conn() as con:
        con.executemany(
            """
            INSERT OR IGNORE INTO deferred_keyword_lookups
                (keyword, country, confidence, tier, source_trend, commercial_category)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def remove_resolved_deferred(looked_up_keys: set):
    """
    Remove keywords from the deferred table that were successfully looked up
    (either via fresh API call or cache hit).
    looked_up_keys: set of (keyword_lower, country_upper) tuples.
    """
    if not looked_up_keys:
        return
    with _conn() as con:
        for kw_lower, country in looked_up_keys:
            con.execute(
                "DELETE FROM deferred_keyword_lookups WHERE keyword = ? AND country = ?",
                (kw_lower, country),
            )


def seed_in_expansion_cache(seed_keyword: str, location_code: int,
                             language_code: str, ttl_hours: int) -> bool:
    """
    Returns True if this seed was expanded within ttl_hours.
    If True, the expansion results are already in keyword_cpc_cache — skip API call.
    """
    with _conn() as con:
        row = con.execute(
            """
            SELECT 1 FROM expansion_cache
            WHERE seed_keyword = ?
              AND location_code = ?
              AND language_code = ?
              AND fetched_at > datetime('now', ? || ' hours')
            """,
            (seed_keyword.lower(), location_code, language_code.lower(), f"-{ttl_hours}"),
        ).fetchone()
    return row is not None


def record_expansion(seed_keyword: str, location_code: int,
                     language_code: str, result_count: int):
    """Mark a seed as expanded so it's skipped on the next run within TTL."""
    with _conn() as con:
        con.execute(
            """
            INSERT INTO expansion_cache (seed_keyword, location_code, language_code, result_count, fetched_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(seed_keyword, location_code, language_code) DO UPDATE SET
                result_count = excluded.result_count,
                fetched_at   = datetime('now')
            """,
            (seed_keyword.lower(), location_code, language_code.lower(), result_count),
        )


# ── Dollar-based budget tracking ──────────────────────────────────────────────

def get_today_usd_spent() -> float:
    """Returns total USD spent today across all DataForSEO endpoints."""
    with _conn() as con:
        row = con.execute(
            "SELECT usd_spent_today FROM api_usage WHERE date = date('now')"
        ).fetchone()
    return float(row["usd_spent_today"]) if row else 0.0


def increment_usd_spent(amount: float, endpoint: str):
    """
    Records USD spend immediately after each API call.
    Called per-task, not per-batch-completion.
    
    Args:
        amount: USD cost of the API call
        endpoint: Name of the endpoint (e.g., 'keyword_ideas', 'keyword_overview', 'bulk_kd')
    """
    import json
    with _conn() as con:
        # Get current breakdown
        row = con.execute(
            "SELECT endpoint_breakdown FROM api_usage WHERE date = date('now')"
        ).fetchone()
        
        breakdown = {}
        if row and row["endpoint_breakdown"]:
            try:
                breakdown = json.loads(row["endpoint_breakdown"])
            except:
                breakdown = {}
        
        # Update breakdown
        breakdown[endpoint] = breakdown.get(endpoint, 0.0) + amount
        
        # Update total and breakdown
        con.execute(
            """
            INSERT INTO api_usage (date, usd_spent_today, endpoint_breakdown)
            VALUES (date('now'), ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                usd_spent_today = usd_spent_today + excluded.usd_spent_today,
                endpoint_breakdown = excluded.endpoint_breakdown
            """,
            (amount, json.dumps(breakdown)),
        )


def budget_remaining(daily_budget_usd: float = 2.00) -> float:
    """
    Returns remaining budget in USD for today.
    
    Args:
        daily_budget_usd: Daily budget cap in USD (default $2.00)
    
    Returns:
        Remaining budget in USD
    """
    spent = get_today_usd_spent()
    return max(0.0, daily_budget_usd - spent)


def pre_flight_budget_check(estimated_cost: float, daily_budget_usd: float = 2.00) -> bool:
    """
    Returns False if this call would exceed the daily budget.
    
    Args:
        estimated_cost: Estimated cost of the upcoming API call in USD
        daily_budget_usd: Daily budget cap in USD (default $2.00)
    
    Returns:
        True if budget allows the call, False otherwise
    """
    remaining = budget_remaining(daily_budget_usd)
    return remaining >= estimated_cost
