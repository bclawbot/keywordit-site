# =============================================================================
# keyword_extractor.py  —  Stage 2: LLM keyword pivot + DataForSEO batch validation
#
# Takes raw explosive trends, uses an LLM to pivot them into commercial-intent
# search queries, then validates with DataForSEO CPC/volume data.
#
# Filtering uses per-country thresholds from country_config.py — edit that file
# to tune arbitrage economics. Do NOT hardcode thresholds here.
#
# New env vars (already in ~/.openclaw/.env):
#   DATAFORSEO_LOGIN    — DataForSEO account email
#   DATAFORSEO_PASSWORD — DataForSEO account password
#
# LLM: LiteLLM proxy at http://localhost:4000 (model: dwight-primary)
# Input:  explosive_trends.json
# Output: commercial_keywords.json  (includes opportunity_score, estimated_rpm)
# =============================================================================

import asyncio
import base64
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor, as_completed as _as_completed
from datetime import datetime
from pathlib import Path

import sys

import requests

BASE      = Path("/Users/newmac/.openclaw/workspace")
INPUT     = BASE / "explosive_trends.json"
OUTPUT    = BASE / "commercial_keywords.json"
ERROR_LOG = BASE / "error_log.jsonl"
EXPANDED  = BASE / "expanded_keywords.json"   # output of keyword_expander.py (may not exist)

sys.path.insert(0, str(BASE))
from country_config import (
    COUNTRY_CONFIG, DEFAULT_COUNTRY, ASSUMED_AD_CTR,
    CACHE_TTL_HOURS, DAILY_API_BUDGET, HIGH_CONFIDENCE_PRIORITY, ONCE_PER_DAY_DFS,
    NON_ENGLISH_MIN_VOLUME, NON_ENGLISH_MIN_CPC,
    DFS_EXPAND_RESULTS_DAILY_CAP,
    DFS_ENDPOINT_COSTS, DFS_DAILY_BUDGET_USD,
)
from cpc_cache import (
    init_db, cleanup,
    normalize_and_dedupe,
    batch_cache_lookup, cache_write_back,
    get_today_usage, increment_usage,
    get_today_expand_results, increment_expand_results,
    budget_gate,
    get_deferred, save_deferred, remove_resolved_deferred,
    seed_in_expansion_cache, record_expansion,
    pre_flight_budget_check, increment_usd_spent,
)

# ── Keyword normalization for DataForSEO exact-match reliability ──────────────
#
# DataForSEO search_volume/live is an exact-match lookup. LLM-generated phrases
# like "best VPN for businesses 2026" return $0 because nobody searches that
# exact string. Normalization strips the verbosity before the API call.
#
# Rules applied (in order):
#   1. Lowercase
#   2. Strip trailing year ("best vpn 2026" → "best vpn")
#   3. Strip leading articles ("the best vpn" → "best vpn")
#   4. Strip trailing filler suffixes ("buy vpn online" → "buy vpn")
#   5. Remove punctuation
#   6. Truncate to max 5 words (shorter → broader → higher volume match rate)
#
# Original keyword is preserved in "original_keyword" for display/reporting.

_RE_YEAR    = re.compile(r'\s+\b20\d{2}\b\s*$')
_RE_ARTICLE = re.compile(r'^(the|a|an)\s+', re.IGNORECASE)
_RE_FILLER  = re.compile(
    r'\s+(online|near me|for free|review|reviews|today|now|'
    r'here|guide|tutorial|explained|reddit|quora)\s*$',
    re.IGNORECASE,
)
_RE_PUNCT   = re.compile(r'[^\w\s-]')
_DFS_MAX_WORDS = 5


def _normalize_for_dfs(keyword: str) -> str:
    kw = keyword.lower().strip()
    kw = _RE_YEAR.sub('', kw)
    kw = _RE_ARTICLE.sub('', kw)
    kw = _RE_FILLER.sub('', kw)
    kw = _RE_PUNCT.sub(' ', kw)
    kw = re.sub(r'\s+', ' ', kw).strip()
    words = kw.split()
    return ' '.join(words[:_DFS_MAX_WORDS]) if len(words) > _DFS_MAX_WORDS else kw


# ── LLM ────────────────────────────────────────────────────────────────────────
LITELLM_URL    = "http://localhost:4000/v1/chat/completions"
LITELLM_MODEL  = "pipeline-extractor"
LITELLM_API_KEY = "sk-dwight-local"   # master key from litellm_config.yaml
LLM_BATCH_SIZE = 8    # trends per LLM call — smaller batches prevent 180s timeout on 30B model

# ── DataForSEO ─────────────────────────────────────────────────────────────────
DFS_LOGIN    = os.environ.get("DATAFORSEO_LOGIN", "")
DFS_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")
DFS_URL_LIVE       = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
DFS_URL_POST       = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/task_post"
DFS_URL_READY      = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/tasks_ready"
DFS_URL_GET        = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/task_get"
DFS_BATCH_SIZE           = 1000  # standard queue supports up to 1000 keywords per task
DFS_INTER_REQUEST_DELAY  = 1.0   # seconds between DataForSEO requests

# Labs API configuration (replaces keywords_for_keywords)
DFS_LABS_KEYWORDS_PER_TASK = 200  # Labs API: limit=200 results per call (server-enforced)
DFS_LABS_SEEDS_PER_CALL    = 5    # Seeds batched per Live call (same locale/language)
DFS_LABS_IDEAS_URL = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live"
# Note: Labs keyword_ideas has no Standard async queue — Live only.
# Cost: $0.01 base + $0.0001/result, capped at $0.03/call (200 result limit)
HIGH_VALUE_CATEGORIES = {"insurance", "legal", "finance", "health"}

# Country → (location_code, language_code)
GEO_MAP = {
    "US": (2840, "en"), "GB": (2826, "en"), "UK": (2826, "en"),
    "AU": (2036, "en"), "CA": (2124, "en"), "IN": (2356, "en"),
    "DE": (2276, "de"), "FR": (2250, "fr"), "ES": (2724, "es"),
    "IT": (2380, "it"), "NL": (2528, "nl"), "BR": (2076, "pt"),
    "JP": (2392, "ja"), "KR": (2410, "ko"), "MX": (2484, "es"),
    "PL": (2616, "pl"), "SE": (2752, "sv"), "NO": (2578, "no"),
    "DK": (2208, "da"), "FI": (2246, "fi"), "AT": (2040, "de"),
    "BE": (2056, "nl"), "CH": (2756, "de"), "IE": (2372, "en"),
    "ZA": (2710, "en"), "SG": (2702, "en"), "NZ": (2554, "en"),
    "HK": (2344, "zh"), "TW": (2158, "zh"), "AR": (2032, "es"),
    "CO": (2170, "es"), "CL": (2152, "es"), "PE": (2604, "es"),
    "PH": (2608, "en"), "ID": (2360, "id"), "TH": (2764, "th"),
    "VN": (2704, "vi"), "MY": (2458, "en"), "NG": (2566, "en"),
    "KE": (2404, "en"), "EG": (2818, "ar"), "SA": (2682, "ar"),
    "TR": (2792, "tr"), "UA": (2804, "uk"), "GR": (2300, "el"),
    "PT": (2620, "pt"), "CZ": (2203, "cs"), "RO": (2642, "ro"),
    "HU": (2348, "hu"), "IL": (2376, "he"),
}

SYSTEM_PROMPT = """\
You are a Search Arbitrage Seed Extractor. Your job is to convert raw trend data into \
1-3 BROAD commercial seed concepts that will be passed to Google's keyword expansion API. \
The API will generate thousands of specific search queries from your seeds — do NOT generate \
the specific queries yourself.

━━━ YOUR ONLY OUTPUT ━━━
For each trend, output the MINIMUM number of seed concepts (1-3) that represent the \
core commercial intent. Seeds must be:
  - Short (2-4 words maximum)
  - Broad enough that Google recognizes them (not invented phrases)
  - In the NATIVE LANGUAGE of the source country (see country context below)
  - Commercially viable (advertisers pay for traffic on this topic)

━━━ NATIVE LANGUAGE REQUIREMENT ━━━
This is critical. Do NOT translate English terms into other languages word-for-word.
Instead, identify the commercial concept and express it as a local user would naturally
search in their own language and idiom.

  Country DE (Germany):  "hurricane florida" → "Hausratversicherung", "Sturm Versicherung"
  Country FR (France):   "student loan" → "crédit étudiant", "prêt étudiant"
  Country JP (Japan):    "weight loss drug" → "ダイエット薬", "痩せる薬"
  Country BR (Brazil):   "car insurance" → "seguro de carro", "seguro auto"
  Country US/GB/AU:      Use English seeds as normal.

If the country is non-English and you cannot produce a culturally valid native-language seed
with high confidence, produce an English seed marked with "en_fallback": true.

━━━ DISCARD IF ━━━
- Pure celebrity gossip, sports scores, political news (unless a commercial service is clearly
  triggered by the event: flood insurance, tax filing, etc.)
- Memes, viral entertainment, death announcements
- Any trend that produces zero commercially viable seed concepts

━━━ HIGH-VALUE VERTICAL SIGNALS ━━━
These categories consistently produce high CPC. When a trend touches these, always extract
a seed even if the trend seems tangential:
  insurance, legal/attorney, finance/mortgage/loan, health/medical device, home services,
  automotive, SaaS/software, senior benefits, military benefits, housing assistance

━━━ OUTPUT FORMAT ━━━
Return ONLY a valid JSON array. No markdown, no code fences, no explanation.

[
  {
    "seed_keyword": "auto insurance",
    "source_trend": "the original raw trend string",
    "country": "US",
    "location_code": 2840,
    "language_code": "en",
    "commercial_category": "one of: insurance | legal | finance | health | home_services | automotive | saas | senior_benefits | travel | ecommerce | general",
    "confidence": "high | medium",
    "en_fallback": false
  }
]

If a trend produces zero viable seeds, omit it entirely from the array.
If the entire batch has no viable commercial seeds, return an empty array: []"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _log_error(stage: str, error: str, extra: dict = None) -> None:
    entry = {"timestamp": datetime.now().isoformat(), "stage": stage, "error": error}
    if extra:
        entry.update(extra)
    with ERROR_LOG.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def _strip_code_fences(text: str) -> str:
    return re.sub(r"^```json?\n?|\n?```$", "", text.strip(), flags=re.MULTILINE)


# ── Step 1: LLM keyword extraction ─────────────────────────────────────────────

def llm_extract_keywords(batch: list, _retry: bool = True) -> list:
    """Send a batch of raw trends to the LLM, return list of commercial keyword dicts."""
    user_message = json.dumps([
        {
            "term":          t.get("term", ""),
            "country":       t.get("geo", "US"),
            "location_code": GEO_MAP.get(t.get("geo", "US").upper(), (2840, "en"))[0],
            "language_code": GEO_MAP.get(t.get("geo", "US").upper(), (2840, "en"))[1],
            "source":        t.get("source", ""),
        }
        for t in batch
    ], ensure_ascii=False)

    for attempt in range(2):
        try:
            resp = requests.post(
                LITELLM_URL,
                headers={"Authorization": f"Bearer {LITELLM_API_KEY}"},
                json={
                    "model": LITELLM_MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": user_message},
                    ],
                    "temperature": 0.3,
                    "max_tokens":  4096,
                },
                timeout=180,
            )
            resp.raise_for_status()
            raw_content = resp.json()["choices"][0]["message"]["content"]
            clean       = _strip_code_fences(raw_content)
            keywords    = json.loads(clean)
            if not isinstance(keywords, list):
                raise ValueError("LLM response is not a JSON array")
            # Backfill location_code and language_code if LLM omitted them
            for k in keywords:
                country = k.get("country", "US").upper()
                if not k.get("location_code"):
                    k["location_code"] = GEO_MAP.get(country, (2840, "en"))[0]
                if not k.get("language_code"):
                    k["language_code"] = GEO_MAP.get(country, (2840, "en"))[1]
            # Safety: drop anything the LLM sneaked in with wrong confidence or missing seed
            return [k for k in keywords if k.get("confidence") in ("high", "medium") and k.get("seed_keyword")]

        except json.JSONDecodeError:
            if attempt == 0:
                print("  ⚠️  LLM JSON parse failed — retrying with clarification…")
                user_message = (
                    "Your previous response was not valid JSON. "
                    "Please return ONLY the JSON array with no other text.\n\n"
                    + user_message
                )
                continue
            _log_error("keyword_extractor/llm", "JSON parse failed after retry")
            return []

        except requests.exceptions.Timeout:
            # On timeout: split batch in half and retry each half once
            if _retry and len(batch) > 3:
                half = len(batch) // 2
                _log_error("keyword_extractor/llm_retry",
                            f"Timeout on batch size {len(batch)} — splitting into {half}+{len(batch)-half}")
                a = llm_extract_keywords(batch[:half],  _retry=False)
                b = llm_extract_keywords(batch[half:],  _retry=False)
                return a + b
            return []

        except Exception as e:
            _log_error("keyword_extractor/llm", str(e))
            if attempt == 0:
                continue
            return []

    return []


# ── Step 2: DataForSEO batch CPC lookup (standard async queue, 33% cheaper) ───
#
# Standard queue costs $0.05/request vs $0.075/request for /live.
# We submit all tasks first, then poll tasks_ready until all results arrive.
# Typical turnaround: 1-10 minutes. We wait up to 45 minutes.

def dfs_batch_lookup(keywords: list) -> dict:
    """
    Submit keyword batches to DataForSEO standard queue, poll for results.
    keywords: list of dicts with {keyword, country, ...}
    Returns:  dict mapping (keyword_lower, country_upper) →
              {cpc_usd, search_volume, competition}
    Saves ~33% vs the /live endpoint.
    """
    if not DFS_LOGIN or not DFS_PASSWORD:
        print("  ⚠️  DataForSEO credentials not set — skipping CPC lookup")
        return {}

    # Group keyword strings by country
    by_country: dict = {}
    for kw in keywords:
        country = kw.get("country", "US").upper()
        by_country.setdefault(country, []).append(kw["keyword"])

    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    results = {}

    # ── Step 2a: Submit all tasks ─────────────────────────────────────────────
    pending_task_ids = []

    for country, kw_list in by_country.items():
        loc_code, lang_code = GEO_MAP.get(country, (2840, "en"))

        for chunk_start in range(0, len(kw_list), DFS_BATCH_SIZE):
            chunk = kw_list[chunk_start : chunk_start + DFS_BATCH_SIZE]
            body  = [{
                "keywords":      chunk,
                "location_code": loc_code,
                "language_code": lang_code,
                "include_serp_info": False,
            }]
            print(f"  → DataForSEO queue: {len(chunk)} keywords [{country}]")

            for attempt in range(2):
                try:
                    r = requests.post(DFS_URL_POST, headers=headers, json=body, timeout=30)
                    r.raise_for_status()
                    tasks = r.json().get("tasks", [])
                    if not tasks:
                        raise ValueError("DataForSEO returned no tasks on submit")
                    task_id = tasks[0].get("id")
                    if task_id:
                        pending_task_ids.append((task_id, country))
                    break
                except Exception as e:
                    if attempt == 0:
                        print(f"  ⚠️  DataForSEO submit error [{country}] — retrying in 5s…")
                        time.sleep(5)
                        continue
                    _log_error("keyword_extractor/dataforseo_submit", str(e),
                               {"country": country, "chunk_size": len(chunk)})
                    break

            time.sleep(DFS_INTER_REQUEST_DELAY)

    if not pending_task_ids:
        return {}

    print(f"  → Submitted {len(pending_task_ids)} task(s) to DataForSEO standard queue. Polling…")

    # ── Step 2b: Poll tasks_ready until all results arrive ────────────────────
    pending_ids = {task_id: country for task_id, country in pending_task_ids}
    start_time  = time.time()

    while pending_ids and (time.time() - start_time) < DFS_ASYNC_TIMEOUT:
        try:
            r = requests.get(DFS_URL_READY, headers=headers, timeout=15)
            r.raise_for_status()
            ready_tasks = r.json().get("tasks", [])
            ready_results = (ready_tasks[0].get("result") or []) if ready_tasks else []
        except Exception as e:
            print(f"  ⚠️  tasks_ready poll failed: {e} — retrying in {DFS_ASYNC_POLL_INTERVAL}s")
            time.sleep(DFS_ASYNC_POLL_INTERVAL)
            continue

        for task_info in ready_results:
            task_id = task_info.get("id")
            if task_id not in pending_ids:
                continue

            country = pending_ids.pop(task_id)

            # Fetch the result for this task
            try:
                rg = requests.get(f"{DFS_URL_GET}/{task_id}", headers=headers, timeout=30)
                rg.raise_for_status()
                get_tasks = rg.json().get("tasks", [])
                task_result = get_tasks[0].get("result") or [] if get_tasks else []
                for item in task_result:
                    key = (item.get("keyword", "").lower(), country)
                    ci  = item.get("competition_index", 0) or 0
                    results[key] = {
                        "cpc_usd":       round(float(item.get("cpc", 0) or 0), 2),
                        "search_volume": int(item.get("search_volume", 0) or 0),
                        "competition":   round(ci / 100, 2),
                        "source":        "dataforseo",
                    }
            except Exception as e:
                _log_error("keyword_extractor/dataforseo_get", str(e), {"task_id": task_id})

        if pending_ids:
            elapsed = int(time.time() - start_time)
            print(f"  → Waiting for {len(pending_ids)} task(s)… ({elapsed}s elapsed)")
            time.sleep(DFS_ASYNC_POLL_INTERVAL)

    if pending_ids:
        print(f"  ⚠️  {len(pending_ids)} DataForSEO task(s) timed out after "
              f"{DFS_ASYNC_TIMEOUT // 60} minutes — results will be partial")
        _log_error("keyword_extractor/dataforseo_timeout",
                   f"{len(pending_ids)} tasks timed out",
                   {"timed_out_ids": list(pending_ids.keys())})

    elapsed_total = round(time.time() - start_time)
    print(f"  → DataForSEO results received in {elapsed_total}s "
          f"({len(results)} keywords with data)")

    return results


# ── Step 2: DataForSEO Labs keyword expansion from LLM seeds ──────────────────
# Uses keyword_ideas/live endpoint (replaces keywords_for_keywords).
# Server-side 200-result cap + CPC/volume/intent filters = predictable cost.
# Cost: $0.01 base + $0.0001/result, max $0.03/call.


def dfs_labs_keyword_ideas(seed_objects: list) -> list:
    """
    Labs keyword_ideas for high-CPC verticals only (Live endpoint — no Standard queue exists).
    Groups seeds by (location_code, language_code), batches up to DFS_LABS_SEEDS_PER_CALL per call.
    Filters commercial/transactional intent server-side; orders by CPC desc.
    Cost: $0.01 + $0.0001/result per call; limit=200 caps at $0.03/call.
    Only called for seeds with commercial_category in HIGH_VALUE_CATEGORIES and confidence=high.
    """
    if not seed_objects:
        return []
    if not DFS_LOGIN or not DFS_PASSWORD:
        return []

    creds   = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

    # Group seeds by (location_code, language_code, country) to batch same-locale seeds
    groups: dict = {}
    for s in seed_objects:
        country   = s.get("country", "US").upper()
        loc_code  = s.get("location_code") or GEO_MAP.get(country, (2840, "en"))[0]
        lang_code = s.get("language_code") or GEO_MAP.get(country, (2840, "en"))[1]
        key = (loc_code, lang_code, country)
        groups.setdefault(key, []).append(s)

    results: list = []

    for (loc_code, lang_code, country), seeds in groups.items():
        cfg = COUNTRY_CONFIG.get(country, DEFAULT_COUNTRY)
        for i in range(0, len(seeds), DFS_LABS_SEEDS_PER_CALL):
            chunk = seeds[i:i + DFS_LABS_SEEDS_PER_CALL]
            first_seed = chunk[0].get("seed_keyword", "")
            if first_seed and seed_in_expansion_cache(first_seed, loc_code, lang_code, CACHE_TTL_HOURS):
                print(f"  [Labs cache HIT] {first_seed} ({country}) — skipping call")
                continue

            seed_kws = [s["seed_keyword"] for s in chunk if s.get("seed_keyword")]
            if not seed_kws:
                continue

            # Pre-flight budget check: estimate $0.03 max per call (base + 200 results)
            estimated_cost = (DFS_ENDPOINT_COSTS["keyword_ideas"]
                              + DFS_ENDPOINT_COSTS["keyword_ideas_per_result"] * DFS_LABS_KEYWORDS_PER_TASK)
            if not pre_flight_budget_check(estimated_cost, DFS_DAILY_BUDGET_USD):
                print(f"  [Budget] Skipping Labs expansion — daily budget exhausted")
                break

            body = [{
                "keywords":          seed_kws,
                "location_code":     loc_code,
                "language_code":     lang_code,
                "limit":             DFS_LABS_KEYWORDS_PER_TASK,
                "include_serp_info": True,
                "filters": [
                    ["keyword_info.search_volume", ">", max(cfg["min_volume"] // 2, 50)],
                    "and",
                    ["keyword_info.cpc", ">", cfg["min_cpc"] * 0.5],
                    "and",
                    ["search_intent_info.main_intent", "in", ["commercial", "transactional"]],
                ],
                "order_by": ["keyword_info.cpc,desc"],
            }]

            try:
                r = requests.post(DFS_LABS_IDEAS_URL, headers=headers, json=body, timeout=30)
                r.raise_for_status()
                # Record spend immediately after POST (per master plan Section 6.3)
                increment_usd_spent(DFS_ENDPOINT_COSTS["keyword_ideas"], "keyword_ideas")
                tasks      = r.json().get("tasks", [])
                result_wrap = (tasks[0].get("result") or []) if tasks else []
                task_items  = result_wrap[0].get("items", []) if result_wrap else []
                # Record per-result cost
                if task_items:
                    result_cost = DFS_ENDPOINT_COSTS["keyword_ideas_per_result"] * len(task_items)
                    increment_usd_spent(result_cost, "keyword_ideas_results")

                for item in task_items:
                    intent = item.get("search_intent_info", {}).get("main_intent", "")
                    # Client-side safety net (server filter already excludes informational)
                    if intent == "informational":
                        continue
                    ki      = item.get("keyword_info", {})
                    ci      = ki.get("competition_index", 0) or 0
                    has_ads = bool((item.get("serp_info") or {}).get("se_results_count"))
                    results.append({
                        "keyword":             item.get("keyword", ""),
                        "country":             country,
                        "location_code":       loc_code,
                        "language_code":       lang_code,
                        "cpc_usd":             round(float(ki.get("cpc", 0) or 0), 2),
                        "search_volume":       int(ki.get("search_volume", 0) or 0),
                        "competition":         round(ci / 100, 2),
                        "competition_index":   ci,
                        "commercial_category": chunk[0].get("commercial_category", "general"),
                        "source_trend":        chunk[0].get("source_trend", ""),
                        "trend_source":        chunk[0].get("trend_source", ""),
                        "confidence":          "high",
                        "metrics_source":      "dataforseo_labs_ideas",
                        "original_keyword":    item.get("keyword", ""),
                        "search_intent":       intent,
                        "has_paid_ads":        has_ads,
                        "seed_keyword":        first_seed,
                    })

                # Record expansion cache for each seed in chunk
                for s in chunk:
                    sk = s.get("seed_keyword", "")
                    if sk:
                        record_expansion(sk, loc_code, lang_code, len(task_items))
                # Track Labs results toward daily cap
                increment_expand_results(len(task_items))

            except Exception as e:
                _log_error("keyword_extractor/dfs_labs_ideas", str(e),
                           {"seeds": seed_kws, "country": country})
            time.sleep(DFS_INTER_REQUEST_DELAY)

    print(f"  → Labs keyword_ideas: {len(results)} high-intent keywords from "
          f"{len(seed_objects)} high-value seeds")
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not INPUT.exists():
        print(f"⚠️  {INPUT} not found — run trends_postprocess.py first")
        raise SystemExit(1)

    # ── DB init + maintenance ──────────────────────────────────────────────────
    init_db()
    removed_cache, removed_deferred = cleanup()
    if removed_cache or removed_deferred:
        print(f"[Cache] Cleaned up {removed_cache} stale cache rows, "
              f"{removed_deferred} expired deferred rows")

    raw_trends = json.loads(INPUT.read_text())

    # ── Inject reddit intelligence seeds ──────────────────────────────────────
    REDDIT_INTEL = BASE / "reddit_intelligence.json"
    reddit_seeds = []
    if REDDIT_INTEL.exists():
        try:
            intel_posts = json.loads(REDDIT_INTEL.read_text())
            for post in intel_posts:
                if any(c in ("keyword_mention", "vertical_signal")
                       for c in post.get("categories", [])):
                    reddit_seeds.append({
                        "term":      post["title"][:120],
                        "geo":       "US",
                        "traffic":   50000,
                        "source":    "reddit_intel",
                        "subreddit": post.get("subreddit", ""),
                        "score":     post.get("score", 0),
                    })
            if reddit_seeds:
                print(f"[Pipeline] Loaded {len(reddit_seeds)} reddit intelligence seeds "
                      f"(keyword_mention/vertical_signal posts)")
        except Exception as e:
            _log_error("keyword_extractor/reddit_intel", str(e))

    raw_trends = raw_trends + reddit_seeds

    total_batches = (len(raw_trends) + LLM_BATCH_SIZE - 1) // LLM_BATCH_SIZE
    print(f"[Pipeline] Received {len(raw_trends)} raw trends "
          f"({len(raw_trends) - len(reddit_seeds)} from explosive_trends, "
          f"{len(reddit_seeds)} from reddit_intel) across "
          f"{len(set(t.get('geo','US') for t in raw_trends))} countries")

    # ── Step 1: LLM extraction → seed concepts ────────────────────────────────
    seed_objects: list = []
    for i in range(0, len(raw_trends), LLM_BATCH_SIZE):
        batch     = raw_trends[i : i + LLM_BATCH_SIZE]
        batch_num = i // LLM_BATCH_SIZE + 1
        print(f"  → LLM batch {batch_num}/{total_batches} ({len(batch)} trends)…")
        try:
            extracted = llm_extract_keywords(batch)
            # Backfill trend_source (source identifier) from the originating trend record.
            # The LLM returns source_trend (raw term) but drops the source name.
            term_to_source = {t.get("term", "").lower(): t.get("source", "") for t in batch}
            for kw in extracted:
                if not kw.get("trend_source"):
                    raw_term = (kw.get("source_trend") or "").lower()
                    kw["trend_source"] = term_to_source.get(raw_term, "")
            seed_objects.extend(extracted)
        except Exception as e:
            _log_error("keyword_extractor/llm_batch", str(e), {"batch": batch_num})
            print(f"  ⚠️  Batch {batch_num} failed — skipping")

    seed_count = len(seed_objects)
    print(f"[Pipeline] LLM produced {seed_count} seed concepts")

    # ── Merge Google Ads expanded keywords (from keyword_expander.py, if run) ──
    expanded_a_count = 0   # Bucket A — already have CPC, skip DataForSEO
    expanded_b_count = 0   # Bucket B — need DataForSEO validation
    google_ads_keywords: list = []
    if EXPANDED.exists():
        try:
            google_keywords = json.loads(EXPANDED.read_text())
            for gkw in google_keywords:
                if not gkw.get("keyword"):
                    continue
                gkw.setdefault("country", "US")
                gkw.setdefault("commercial_category", "")
                gkw.setdefault("confidence", "high" if not gkw.get("needs_dataforseo_validation") else "medium")
                gkw.setdefault("source_trend", gkw.get("expansion_seed", ""))
                if not gkw.get("needs_dataforseo_validation"):
                    gkw["cpc_usd"]       = gkw.get("google_estimated_cpc", 0)
                    gkw["search_volume"] = gkw.get("google_volume", 0)
                    gkw["competition"]   = round(gkw.get("google_competition_index", 50) / 100, 2)
                    gkw["metrics_source"] = "google_ads"
                    expanded_a_count += 1
                else:
                    expanded_b_count += 1
                google_ads_keywords.append(gkw)
            if google_keywords:
                print(f"[Pipeline] Merged {len(google_keywords)} Google-expanded keywords "
                      f"(A={expanded_a_count} pre-filled, B={expanded_b_count} need DFS)")
        except Exception as e:
            print(f"  ⚠️  Could not merge expanded_keywords.json: {e}")

    # ── Recover deferred seeds/keywords from previous runs ────────────────────
    prior_deferred = get_deferred()
    if prior_deferred:
        # Items with seed_keyword are seeds — add to seed pool for re-expansion
        deferred_seeds = [d for d in prior_deferred if d.get("seed_keyword")]
        deferred_keywords = [d for d in prior_deferred if not d.get("seed_keyword")]
        seed_objects.extend(deferred_seeds)
        google_ads_keywords.extend(deferred_keywords)
        print(f"[Pipeline] Recovering {len(prior_deferred)} deferred items "
              f"({len(deferred_seeds)} seeds, {len(deferred_keywords)} keywords)")

    # ── Step 2: Expand LLM seeds via DataForSEO Labs ──────────────────────────
    # Uses keyword_ideas/live exclusively (replaces keywords_for_keywords).
    # All seeds go through Labs with server-side filters and 200-result cap.
    expanded_keywords: list = []

    if not seed_objects:
        print("[Pipeline] No seed concepts to expand")
    elif ONCE_PER_DAY_DFS and get_today_usage() > 0:
        today_usage = get_today_usage()
        print(f"  [Expansion] DataForSEO already ran today "
              f"({today_usage}/{DAILY_API_BUDGET} tasks used) — skipping expansion")
    else:
        # Check daily result cap before expansion
        today_results = get_today_expand_results()
        if today_results >= DFS_EXPAND_RESULTS_DAILY_CAP:
            print(f"  [Expansion] Daily result cap hit ({today_results}/{DFS_EXPAND_RESULTS_DAILY_CAP}) — skipping expansion")
            save_deferred(seed_objects)
        else:
            # All seeds go through Labs (no more keywords_for_keywords)
            try:
                expanded_keywords = dfs_labs_keyword_ideas(seed_objects)
                print(f"[Pipeline] Labs expansion: {len(expanded_keywords)} candidates from {len(seed_objects)} seeds"
                      f" — daily results: {get_today_expand_results()}/{DFS_EXPAND_RESULTS_DAILY_CAP}")
            except Exception as e:
                _log_error("keyword_extractor/labs_expansion", str(e))
                print(f"  ⚠️  Labs expansion failed: {e}")
                # Defer seeds on failure
                for s in seed_objects:
                    s.setdefault("keyword", s.get("seed_keyword", ""))
                save_deferred(seed_objects)
                print(f"  [Error Recovery] Deferred {len(seed_objects)} seeds to next run")

    # ── Combine all keyword sources ────────────────────────────────────────────
    all_keywords = expanded_keywords + google_ads_keywords

    if not all_keywords:
        OUTPUT.write_text("[]")
        print("⚠️  No commercial keywords extracted — writing empty output")
        raise SystemExit(0)

    # ── Normalize keywords for DataForSEO exact-match reliability ─────────────
    # Skip normalization for expanded keywords — they come from DataForSEO's own
    # database and are already valid real search queries. Only normalize Bucket B.
    normalized_count = 0
    dropped_count    = 0
    for kw in all_keywords:
        if kw.get("metrics_source") in ("dataforseo_expansion", "dataforseo_labs_ideas"):
            # Already validated real keywords — preserve as-is
            kw.setdefault("original_keyword", kw.get("keyword", ""))
            continue
        original   = kw.get("keyword", "")
        if not original:
            dropped_count += 1
            kw["keyword"] = ""
            continue
        normalized = _normalize_for_dfs(original)
        if not normalized:
            dropped_count += 1
            kw["keyword"] = ""
            continue
        kw["original_keyword"] = original
        if normalized != original.lower().strip():
            normalized_count += 1
        kw["keyword"] = normalized
    all_keywords = [kw for kw in all_keywords if kw.get("keyword")]
    print(f"[Pipeline] Keyword normalization: {normalized_count} modified, "
          f"{dropped_count} dropped (became empty after cleaning)")

    # ── Experimental Step 1: Hard filter (pre-LLM noise gate) ─────────────────
    try:
        from modules.hard_filter import hard_filter as _hard_filter
        _FILTERED_OUT_LOG = BASE / "data" / "filtered_out.log"
        _FILTERED_OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
        _hf_passed = []
        _hf_rejected = 0
        with _FILTERED_OUT_LOG.open("a", encoding="utf-8") as _hf_fh:
            for _kw in all_keywords:
                _ok, _reason = _hard_filter(
                    _kw.get("keyword", ""),
                    _kw.get("country", "US"),
                    _kw.get("vertical"),
                )
                if _ok:
                    _hf_passed.append(_kw)
                else:
                    _hf_rejected += 1
                    _hf_fh.write(json.dumps({
                        "keyword": _kw.get("keyword"),
                        "country": _kw.get("country"),
                        "reason":  _reason,
                        "ts":      datetime.now().isoformat(),
                    }) + "\n")
        print(f"[Experimental] Hard filter: {len(all_keywords)} → {len(_hf_passed)} passed "
              f"({_hf_rejected} rejected → data/filtered_out.log)")
        all_keywords = _hf_passed
    except Exception as _e:
        print(f"  ⚠️  Hard filter failed ({_e}) — skipping, pipeline continues")

    # ── Experimental Step 2: Linguistic scorer ────────────────────────────────
    try:
        from modules.linguistic_scorer import score_linguistic_signals as _score_ling
        for _kw in all_keywords:
            _kw["linguistic_score"] = _score_ling(_kw.get("keyword", ""))
    except Exception as _e:
        print(f"  ⚠️  Linguistic scorer failed ({_e}) — skipping")

    # ── Cost optimization ──────────────────────────────────────────────────────
    print("[Pipeline] Cost optimization:")

    # Pre-populate cpc_data from embedded metrics (expanded + Bucket A)
    pre_filled_cpc: dict = {}
    for kw in all_keywords:
        ms = kw.get("metrics_source", "")
        if ms in ("dataforseo_expansion", "dataforseo_labs_ideas", "google_ads"):
            key = (kw["keyword"].lower(), kw.get("country", "US").upper())
            if key not in pre_filled_cpc:
                pre_filled_cpc[key] = {
                    "cpc_usd":       float(kw.get("cpc_usd", 0) or 0),
                    "search_volume": int(kw.get("search_volume", 0) or 0),
                    "competition":   float(kw.get("competition", 0) or 0),
                    "source":        ms,
                }

    # Layer 1: normalize & deduplicate within batch
    pre_dedupe_count = len(all_keywords)
    unique_keywords  = normalize_and_dedupe(all_keywords)
    dupes_removed    = pre_dedupe_count - len(unique_keywords)
    print(f"  Layer 1 — Dedupe:   {pre_dedupe_count} → {len(unique_keywords)} unique "
          f"({dupes_removed} duplicates removed)")

    # Layer 2: cache lookup for Bucket B only
    unique_b = [kw for kw in unique_keywords if kw.get("needs_dataforseo_validation")]
    cache_hits_dict: dict = {}
    cache_misses: list = unique_b[:]

    if unique_b:
        cache_hits_dict, cache_misses = batch_cache_lookup(unique_b, CACHE_TTL_HOURS)
        hit_rate = (len(cache_hits_dict) / len(unique_b) * 100) if unique_b else 0
        print(f"  Layer 2 — Cache:    {len(unique_b)} Bucket B → {len(cache_misses)} need lookup "
              f"({len(cache_hits_dict)} cache hits, {hit_rate:.0f}% hit rate)")
    else:
        print(f"  Layer 2 — Cache:    0 Bucket B keywords (all pre-filled from expansion)")

    # Layer 3: daily budget gate (Bucket B only)
    today_usage_b   = get_today_usage()
    newly_deferred: list = []
    to_lookup:       list = []

    if cache_misses:
        if ONCE_PER_DAY_DFS and today_usage_b > 0:
            to_lookup      = []
            newly_deferred = cache_misses
            print(f"  Layer 3 — Budget:   DataForSEO already ran today "
                  f"({today_usage_b}/{DAILY_API_BUDGET}) — "
                  f"deferring {len(cache_misses)} Bucket B keywords")
        else:
            to_lookup, newly_deferred = budget_gate(
                cache_misses, DAILY_API_BUDGET,
                COUNTRY_CONFIG, DEFAULT_COUNTRY, HIGH_CONFIDENCE_PRIORITY,
            )
            print(f"  Layer 3 — Budget:   {len(cache_misses)} Bucket B lookups needed, "
                  f"{DAILY_API_BUDGET - today_usage_b} remaining — "
                  f"sending {len(to_lookup)}"
                  + (f", deferring {len(newly_deferred)}" if newly_deferred else ""))

    # ── DataForSEO lookup for Bucket B cache misses only ──────────────────────
    fresh_cpc_data: dict = {}
    if to_lookup:
        fresh_cpc_data = dfs_batch_lookup(to_lookup)
        have_data      = sum(1 for v in fresh_cpc_data.values() if v["cpc_usd"] > 0)
        print(f"[Pipeline] Bucket B DataForSEO: {len(to_lookup)} lookups → "
              f"{have_data} with CPC data")
        cache_write_back(fresh_cpc_data)
        increment_usage(len(to_lookup))
        budget_used = today_usage_b + len(to_lookup)
        print(f"[Pipeline] Daily budget usage: {budget_used}/{DAILY_API_BUDGET} "
              f"({budget_used / DAILY_API_BUDGET * 100:.0f}%)")
    elif unique_b:
        print("[Pipeline] DataForSEO: 0 Bucket B lookups (all served from cache or budget exhausted)")
    else:
        print("[Pipeline] DataForSEO: expansion-only run (no Bucket B keywords)")

    # Save newly deferred + clean up resolved ones from previous runs
    if newly_deferred:
        save_deferred(newly_deferred)
    resolved_keys = set(cache_hits_dict.keys()) | set(fresh_cpc_data.keys())
    if prior_deferred:
        remove_resolved_deferred(resolved_keys)
        recovered = sum(
            1 for d in prior_deferred
            if (d.get("keyword", "").lower(), d.get("country", "US").upper()) in resolved_keys
        )
        if recovered:
            print(f"[Pipeline] Deferred from previous runs: {recovered} recovered")

    # Merge all CPC data: pre-filled + cache hits + fresh lookups
    full_cpc_data: dict = {**pre_filled_cpc, **cache_hits_dict, **fresh_cpc_data}

    # ── Step 3: Per-country threshold filter + opportunity scoring ─────────────
    seen:       set  = set()
    passed:     list = []
    tier_stats: dict = {}

    for kw in unique_keywords:
        country = kw.get("country", "US").upper()
        key     = (kw["keyword"].lower(), country)

        if key in seen:
            continue
        seen.add(key)

        cfg  = COUNTRY_CONFIG.get(country, DEFAULT_COUNTRY)
        tier = cfg["tier"]
        tier_stats.setdefault(tier, {"countries": set(), "total": 0, "passed": 0})
        tier_stats[tier]["countries"].add(country)
        tier_stats[tier]["total"] += 1

        metrics = full_cpc_data.get(key)
        if not metrics:
            # No DataForSEO data yet (deferred/budget exhausted).
            tier_stats[tier]["passed"] += 1
            passed.append({
                **kw,
                "cpc_usd":           0.0,
                "search_volume":     0,
                "competition":       0.5,
                "opportunity_score": 0,
                "estimated_rpm":     0,
                "country_tier":      tier,
                "efficiency_factor": cfg["efficiency"],
                "metrics_source":    kw.get("metrics_source", "deferred"),
                "processed_at":      datetime.now().isoformat(),
            })
            continue

        cpc = metrics["cpc_usd"]
        vol = metrics["search_volume"]

        efficiency        = cfg["efficiency"]
        opportunity_score = round(cpc * vol * efficiency, 2)
        estimated_rpm     = round(cpc * ASSUMED_AD_CTR * 1000, 2)

        if cpc < cfg["min_cpc"] or vol < cfg["min_volume"]:
            tier_stats[tier]["passed"] += 1
            passed.append({
                **kw,
                "cpc_usd":           cpc,
                "search_volume":     vol,
                "competition":       metrics["competition"],
                "opportunity_score": 0,
                "estimated_rpm":     estimated_rpm,
                "country_tier":      tier,
                "efficiency_factor": efficiency,
                "metrics_source":    metrics.get("source", kw.get("metrics_source", "dataforseo")),
                "processed_at":      datetime.now().isoformat(),
            })
            continue

        tier_stats[tier]["passed"] += 1
        passed.append({
            **kw,
            "cpc_usd":           cpc,
            "search_volume":     vol,
            "competition":       metrics["competition"],
            "opportunity_score": opportunity_score,
            "estimated_rpm":     estimated_rpm,
            "country_tier":      tier,
            "efficiency_factor": efficiency,
            "metrics_source":    metrics.get("source", kw.get("metrics_source", "dataforseo")),
            "processed_at":      datetime.now().isoformat(),
        })

    # ── Logging: tier breakdown + top 5 ──────────────────────────────────────
    total_passed = len(passed)
    total_seen   = len(seen)
    pass_rate    = (total_passed / total_seen * 100) if total_seen else 0

    print("[Pipeline] Filtering (tiered thresholds):")
    for tier in sorted(tier_stats):
        s         = tier_stats[tier]
        countries = ", ".join(sorted(s["countries"]))
        print(f"  Tier {tier} ({countries}): {s['total']} keywords → {s['passed']} passed")
    print(f"[Pipeline] Total: {total_passed} keywords passed ({pass_rate:.1f}% pass rate)")

    top5 = sorted(passed, key=lambda x: x["opportunity_score"], reverse=True)[:5]
    if top5:
        print("[Pipeline] Top 5 by opportunity score:")
        for i, kw in enumerate(top5, 1):
            print(f"  {i}. \"{kw['keyword']}\" ({kw['country']}) "
                  f"— score: {kw['opportunity_score']:,.0f}")

    # ── Cost summary ──────────────────────────────────────────────────────────
    dfs_b_requests = (len(to_lookup) + DFS_BATCH_SIZE - 1) // DFS_BATCH_SIZE if to_lookup else 0
    dfs_b_cost     = round(dfs_b_requests * 0.05, 2)
    print(f"[Pipeline] Cost summary:")
    print(f"  DataForSEO expansion: {len(expanded_keywords)} keywords via seeds")
    print(f"  Google Ads API:       {expanded_a_count + expanded_b_count} keywords from free expansion")
    print(f"  DataForSEO Bucket B:  {len(to_lookup)} lookups in {dfs_b_requests} request(s)"
          f" ≈ ${dfs_b_cost:.2f} (standard queue)")
    print(f"  Bucket A (free):      {expanded_a_count} keywords used Google CPC directly")
    print(f"  Bucket B (paid):      {expanded_b_count} keywords sent to DataForSEO")

    # ── Experimental Steps 3–6: Template expansion pipeline ───────────────────
    _exp_stats = {
        "keywords_extracted": total_seen,
        "passed_hard_filter": len(passed),
        "decomposed": 0, "expandable": 0,
        "raw_expansions_generated": 0, "passed_plausibility": 0,
        "passed_quality_gate": 0, "budget_used": 0,
        "track_a_count": 0, "track_b_count": 0,
        "new_entities_discovered": 0,
    }
    _exp_stats["filter_rate"] = round(
        1 - len(passed) / max(total_seen, 1), 3)

    try:
        from modules.template_decomposer import decompose_batch as _decompose
        from modules.template_expander   import expand_batch as _expand, load_registry as _load_reg
        from modules.plausibility_checker import check_batch as _check_plausibility
        from modules.cpc_router          import route_for_validation as _route, load_vertical_ref as _load_vref

        _registry    = _load_reg()
        _vert_ref    = _load_vref()

        # Step 3a: Decompose PRIORITY keywords (any keyword with real CPC data)
        _priority_kws = [kw for kw in passed if kw.get("opportunity_score", 0) > 0]
        print(f"[Experimental] Decomposing {len(_priority_kws)} PRIORITY keywords…")

        if _priority_kws:
            _decomposed = _decompose([kw["keyword"] for kw in _priority_kws],
                                     country="US")
            _exp_stats["decomposed"]  = len(_decomposed)
            _exp_stats["expandable"]  = sum(1 for d in _decomposed if d.get("expandable"))

            # Build quality score lookup
            _quality_map = {kw["keyword"]: kw.get("opportunity_score", 0) for kw in _priority_kws}

            # Step 3b: Expand
            _raw_expansions = _expand(_decomposed, _registry, "US", _quality_map)
            _exp_stats["raw_expansions_generated"] = len(_raw_expansions)
            print(f"[Experimental] {len(_raw_expansions)} raw expansions generated")

            # Step 3c: Plausibility check
            if _raw_expansions:
                _raw_expansions = _check_plausibility(_raw_expansions)
                _plausible = [e for e in _raw_expansions if e.get("plausible") is not False]
                _exp_stats["passed_plausibility"] = len(_plausible)
                print(f"[Experimental] {len(_plausible)}/{len(_raw_expansions)} expansions passed plausibility")

                # Step 5: Check for unknown entities → discovered_entities.jsonl
                _DISCOVERED_PATH = BASE / "data" / "discovered_entities.jsonl"
                _known_types     = set(_registry.keys()) - {"_promotion_rules"}
                _new_ents        = []
                for _exp in _plausible:
                    _etype  = _exp.get("swapped_slot") or _exp.get("entity_type")
                    _ename  = _exp.get("new_value")
                    if not _etype or not _ename:
                        continue
                    _pool   = _registry.get(_etype, {})
                    _all_known = []
                    for _pool_data in _pool.values():
                        if isinstance(_pool_data, dict):
                            _all_known.extend(_pool_data.get("proven", []))
                            _all_known.extend(_pool_data.get("test", []))
                            _all_known.extend(_pool_data.get("blocked", []))
                    if _ename not in _all_known:
                        _new_ents.append({
                            "entity": _ename, "entity_type": _etype,
                            "discovered_in": _exp["keyword"],
                            "ts": datetime.now().isoformat(),
                        })

                if _new_ents:
                    with _DISCOVERED_PATH.open("a", encoding="utf-8") as _df:
                        for _ne in _new_ents:
                            _df.write(json.dumps(_ne) + "\n")
                    _exp_stats["new_entities_discovered"] = len(_new_ents)
                    print(f"[Experimental] {len(_new_ents)} new entities discovered → data/discovered_entities.jsonl")

                # Step 4: CPC router — organic passed + experimental expansions
                _experimental_kws = []
                for _exp in _plausible:
                    _exp["source"] = "experimental"
                    _exp.setdefault("country", "US")
                    _experimental_kws.append(_exp)

                _all_for_routing = passed + _experimental_kws
                for _kw in passed:
                    _kw.setdefault("source", "organic")

                _track_a, _track_b = _route(_all_for_routing, _registry, _vert_ref)
                _exp_stats["track_a_count"] = sum(1 for k in _track_a if k.get("source") == "experimental")
                _exp_stats["track_b_count"] = len(_track_b)

                # Step 6: Merge plausible Track B experimental keywords into passed list
                _merged_count = 0
                for _kw in _track_b:
                    if _kw.get("source") == "experimental":
                        _kw["cpc_usd"]           = _kw.get("inherited_cpc", 0)
                        _kw["search_volume"]      = 0
                        _kw["competition"]        = 0.5
                        _kw["opportunity_score"]  = min(100.0, round(_kw.get("source_quality_score", 0), 1))
                        _kw["estimated_rpm"]      = round(_kw["cpc_usd"] * ASSUMED_AD_CTR * 1000, 2)
                        _kw["country_tier"]       = COUNTRY_CONFIG.get(
                            _kw.get("country", "US"), DEFAULT_COUNTRY)["tier"]
                        _kw["processed_at"]       = datetime.now().isoformat()
                        passed.append(_kw)
                        _merged_count += 1

                _exp_stats["passed_quality_gate"] = _merged_count
                _exp_stats["budget_used"]         = _merged_count
                print(f"[Experimental] {_merged_count} experimental keywords merged into output")

                # Write expansion results to JSONL
                _EXP_RESULTS_PATH = BASE / "data" / "expansion_results.jsonl"
                with _EXP_RESULTS_PATH.open("a", encoding="utf-8") as _ef:
                    for _exp in _plausible:
                        _ef.write(json.dumps(_exp) + "\n")

        # Write expansion log (one entry per run)
        _EXP_LOG_PATH = BASE / "data" / "expansion_log.jsonl"
        _run_log = {
            "run_id":     datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cycle_stats": _exp_stats,
            "entities_in_registry": {
                "proven": sum(
                    len(v.get(c, {}).get("proven", []))
                    for k, v in _registry.items() if not k.startswith("_")
                    for c in v if isinstance(v.get(c), dict)
                ),
                "test": sum(
                    len(v.get(c, {}).get("test", []))
                    for k, v in _registry.items() if not k.startswith("_")
                    for c in v if isinstance(v.get(c), dict)
                ),
                "blocked": sum(
                    len(v.get(c, {}).get("blocked", []))
                    for k, v in _registry.items() if not k.startswith("_")
                    for c in v if isinstance(v.get(c), dict)
                ),
            },
        }
        with _EXP_LOG_PATH.open("a", encoding="utf-8") as _lf:
            _lf.write(json.dumps(_run_log) + "\n")

    except Exception as _exp_err:
        _log_error("keyword_extractor/experimental", str(_exp_err))
        print(f"  ⚠️  Experimental pipeline failed ({_exp_err}) — organic keywords unaffected")

    # Normalize opportunity_score to 0–100 percentile rank within this batch.
    # Score=0 keywords (no CPC data) stay at 0; scored keywords get a rank from 0.0–100.0.
    _scored_idx = [(i, kw["opportunity_score"]) for i, kw in enumerate(passed) if kw.get("opportunity_score", 0) > 0]
    if _scored_idx:
        _sorted_scored = sorted(_scored_idx, key=lambda x: x[1])
        _n = len(_sorted_scored)
        for _rank, (_idx, _raw) in enumerate(_sorted_scored):
            passed[_idx]["opportunity_score"] = round((_rank / (_n - 1)) * 100, 1) if _n > 1 else 100.0

    OUTPUT.write_text(json.dumps(passed, indent=2))
    print(f"✅ Keyword extraction complete: {len(passed)} commercial keywords → {OUTPUT.name}")
