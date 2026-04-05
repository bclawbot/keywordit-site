import asyncio
import json
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from ddgs import DDGS

BASE    = Path(__file__).resolve().parent
INPUT   = BASE / "commercial_keywords.json"
OUTPUT  = BASE / "vetted_opportunities.json"
HISTORY = BASE / "vetted_history.jsonl"

LONG_FORM_PATHS = {"/blog/", "/article/", "/news/", "/learn/", "/guide/", "/how-to/", "/post/"}
LONG_FORM_MIN_WORDS = 60

SEARCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
}

# Vertical classifier
VERTICAL_RULES = [
    ("health",        r"health|medic|diet|weight|fitness|symptom|disease|cancer|drug|pharma|mental|anxiety|depression|pain|workout|vitamin|supplement|covid|vaccine"),
    ("finance",       r"financ|invest|stock|crypto|bitcoin|trading|loan|mortgage|insurance|tax|bank|saving|credit|debt|wallet|forex|fund|ipo|dividend|earn money"),
    ("politics",      r"election|vote|president|congress|senate|parliament|democrat|republican|political|government|policy|law|court|supreme|immigration|border|trump|biden|war|sanction"),
    ("sports",        r"sport|football|soccer|basketball|nba|nfl|tennis|golf|olympic|athlete|league|championship|tournament|match|score|player|team"),
    ("tech",          r"tech|software|ai |artificial intel|chatgpt|openai|google|apple|microsoft|iphone|android|app |startup|cyber|hack|data|cloud|robot|electric vehicle|ev "),
    ("entertainment", r"movie|film|music|celebrity|actor|actress|singer|album|concert|netflix|streaming|award|oscar|grammy|box office|tv show|series|episode|viral"),
    ("travel",        r"travel|flight|hotel|visa|tourism|destination|vacation|holiday|resort|booking|airline|passport|cruise|trip"),
    ("food",          r"food|recipe|restaurant|cooking|diet|nutrition|meal|eat|drink|cuisine|chef|vegan|keto|snack|coffee|tea"),
    ("real_estate",   r"real estate|property|housing|mortgage|rent|apartment|home buy|home sell|landlord|tenant|zillow|redfin"),
    ("education",     r"school|universit|college|degree|course|learn|study|exam|student|educat|online class|certif|tutor|academic"),
    ("news",          r"breaking|latest news|update|announce|report|incident|accident|arrest|protest|crisis|disaster|storm|earthquake|flood"),
]
VERTICAL_DEFAULT = "general"


def classify_vertical(keyword, lander_url=""):
    text = (keyword + " " + lander_url).lower()
    for vertical, pattern in VERTICAL_RULES:
        if re.search(pattern, text):
            return vertical
    return VERTICAL_DEFAULT


def extract_hook_theme(title):
    t = title.lower()
    if any(w in t for w in ["how to", "guide", "tutorial", "step-by-step", "step by step"]):
        return "how-to tutorial"
    if any(w in t for w in ["top ", "best ", "ranked", " list"]):
        return "listicle tease"
    if any(w in t for w in ["before", "after", "transform", "changed"]):
        return "before/after"
    if any(w in t for w in ["secret", "hidden", "nobody", "you don't know", "trick"]):
        return "curiosity gap"
    if any(w in t for w in ["%", "statistic", "study shows", "research", "data shows"]):
        return "surprising statistic"
    if any(w in t for w in ["review", "vs ", "versus", "compare", "which is better"]):
        return "social proof"
    if any(w in t for w in ["fast", "quick", "now", "today", "urgent", "deadline", "last chance"]):
        return "urgency/scarcity"
    if any(w in t for w in ["why ", "reason", "explain", "what is", "what are"]):
        return "curiosity gap"
    return "problem/solution"


def is_long_form(url, snippet):
    if any(path in url.lower() for path in LONG_FORM_PATHS):
        return True
    word_count = len(re.findall(r"\b\w+\b", snippet or ""))
    return word_count >= LONG_FORM_MIN_WORDS


def _ddg_fetch_sync(keyword):
    """Synchronous DDG fetch — wrapped in executor for async use."""
    return list(DDGS(timeout=10).text(keyword, max_results=10))


async def search_ddg(keyword: str, executor: ThreadPoolExecutor) -> list:
    """DDG search via thread executor with 8s wall-clock timeout."""
    loop = asyncio.get_event_loop()
    try:
        raw = await asyncio.wait_for(
            loop.run_in_executor(executor, _ddg_fetch_sync, keyword),
            timeout=8,
        )
        return [{"url": r["href"], "title": r["title"], "snippet": r["body"]} for r in raw]
    except (asyncio.TimeoutError, FuturesTimeout, Exception) as e:
        print(f"  ⚠️  DDG search failed for '{keyword}': {type(e).__name__}")
        return []


async def search_brave(keyword: str, session: aiohttp.ClientSession, retries: int = 2) -> list:
    """Brave HTML scrape — async fallback."""
    import urllib.parse
    url = f"https://search.brave.com/search?q={urllib.parse.quote(keyword)}&source=web"
    for attempt in range(retries):
        try:
            async with session.get(url, headers=SEARCH_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 429:
                    wait = 10 * (attempt + 1)
                    print(f"  ⚠️  Brave 429 — backing off {wait}s (attempt {attempt+1}/{retries})")
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                text = await r.text()
            soup = BeautifulSoup(text, "html.parser")
            results = []
            for item in soup.select('#results .snippet[data-type="web"]')[:10]:
                a = item.select_one("a.l1[href]")
                if not a or not a.get("href", "").startswith("http"):
                    continue
                href  = a["href"]
                title = item.select_one(".title")
                desc  = item.select_one(".snippet-description, .generic-snippet .content")
                results.append({
                    "url":     href,
                    "title":   title.get_text(strip=True) if title else "",
                    "snippet": desc.get_text(strip=True) if desc else "",
                })
            return results
        except Exception as e:
            print(f"  ⚠️  Brave search failed for '{keyword}': {e}")
            return []
    return []


async def vet_keyword(entry: dict, session: aiohttp.ClientSession,
                      executor: ThreadPoolExecutor, semaphore: asyncio.Semaphore) -> list:
    async with semaphore:
        keyword = entry.get("keyword", "")
        country = entry.get("country", "US")

        results = await search_ddg(keyword, executor)
        if not results:
            results = await search_brave(keyword, session)

        survivors = []
        # Phase 2.1: Compute SERP dominance — ratio of HIGH_DA authority sites in results
        # High serp_dominance = dominated by authority sites = HARD to rank = BAD for arbitrage
        # Low serp_dominance  = thin SERP coverage = EASY to rank = GOOD for arbitrage
        HIGH_DA_DOMAINS = (
            "wikipedia.org", "webmd.com", "forbes.com", "nytimes.com",
            "reddit.com", "amazon.com", "yelp.com", "bbc.com", "cnn.com",
            "mayoclinic.org", "healthline.com", "investopedia.com",
            "nerdwallet.com", "gov.",
        )
        noise_domains = ("youtube.com", "twitter.com", "x.com", "instagram.com",
                         "tiktok.com", "facebook.com")
        total_results  = len(results)
        dominant_hits  = 0
        for r in results:
            url = r.get("url", "").lower()
            if any(d in url for d in HIGH_DA_DOMAINS):
                dominant_hits += 1
        # serp_dominance: 0.0 (thin SERP) to 1.0 (fully dominated by authority sites)
        serp_dominance = round(dominant_hits / max(total_results, 1), 3)

        for r in results:
            url     = r.get("url", "")
            title   = r.get("title", "")
            snippet = r.get("snippet", "")

            if not is_long_form(url, snippet):
                continue

            if any(d in url.lower() for d in noise_domains):
                continue

            # Preserve all CPC and metrics data from commercial_keywords.json
            vetted_entry = {
                "keyword":         keyword,
                "country":         country,
                "vertical":        classify_vertical(keyword, url),
                "hook_theme":      extract_hook_theme(title),
                "lander_url":      url,
                "lander_title":    title,
                "ad_age_days":     90,
                "data_source":     "ddg_serp",
                "serp_dominance":  serp_dominance,
                "vetted_at":       datetime.now().isoformat(),
            }
            # Copy all CPC and metrics fields from commercial_keywords
            for key in ["cpc_usd", "search_volume", "competition", "competition_index",
                        "opportunity_score", "estimated_rpm", "metrics_source",
                        "commercial_category", "confidence", "country_tier",
                        "efficiency_factor", "processed_at", "cpc_low_usd", "cpc_high_usd",
                        # Discovery context fields — required by Stage 3a (angle_engine.py)
                        "source_trend", "language_code", "trend_source", "source",
                        "subreddit", "reddit_score", "original_keyword",
                        "expansion_seed", "is_branded"]:
                if key in entry:
                    vetted_entry[key] = entry[key]
            
            survivors.append(vetted_entry)

        return survivors


async def vet_all(trends: list) -> list:
    semaphore = asyncio.Semaphore(15)  # 15 concurrent SERP checks (3× faster)
    executor  = ThreadPoolExecutor(max_workers=15)

    async with aiohttp.ClientSession() as session:
        tasks = [vet_keyword(entry, session, executor, semaphore) for entry in trends]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    executor.shutdown(wait=True)
    all_vetted = []
    for r in results:
        if isinstance(r, list):
            all_vetted.extend(r)
    return all_vetted


def is_news_headline(keyword: str) -> bool:
    """Quick heuristic to filter non-commercial news headlines.
    Uses word-boundary matching to avoid false positives like 'fire extinguisher'."""
    import re
    # Only match as whole words to prevent substring false positives
    _NEWS_VERBS = [
        "arrested", "dies", "died", "shooting", "shot", "killed",
        "scandal", "investigation", "elected", "resigns", "explodes",
    ]
    # These are only news when NOT paired with product context
    _CONTEXT_VERBS = ["fire", "earthquake", "flood", "storm", "hurricane",
                      "weather", "forecast", "crashes", "wins", "loses", "lost"]
    # Product/commercial modifiers that override news classification
    _COMMERCIAL_CONTEXT = [
        "extinguisher", "pit", "door", "proof", "resistant", "insurance",
        "protection", "alarm", "detector", "damage", "repair", "restoration",
        "cleanup", "service", "company", "kit", "equipment", "gear", "supply",
        "sale", "buy", "best", "top", "review", "price", "cost", "cheap",
    ]
    kw_lower = keyword.lower()

    # Check hard news verbs (always filter)
    for verb in _NEWS_VERBS:
        if re.search(rf'\b{verb}\b', kw_lower):
            return True

    # Check context-dependent verbs — only filter if NO commercial modifier present
    has_commercial = any(cm in kw_lower for cm in _COMMERCIAL_CONTEXT)
    if not has_commercial:
        for verb in _CONTEXT_VERBS:
            if re.search(rf'\b{verb}\b', kw_lower):
                return True

    return False


if __name__ == "__main__":
    if not INPUT.exists():
        print(f"⚠️  {INPUT} not found — run keyword_extractor.py first")
        raise SystemExit(1)

    commercial_keywords = json.loads(INPUT.read_text())
    
    if not commercial_keywords:
        print(f"⚠️  {INPUT.name} is empty — skipping vetting (nothing to vet)")
        OUTPUT.write_text("[]")
        raise SystemExit(0)
    
    # Filter out news headlines before vetting
    original_count = len(commercial_keywords)
    commercial_keywords = [
        kw for kw in commercial_keywords
        if not is_news_headline(kw.get("keyword", ""))
    ]
    filtered_count = original_count - len(commercial_keywords)
    if filtered_count > 0:
        print(f"  [News Filter] Removed {filtered_count} news headlines from {original_count} keywords")

    # When ALL keywords have CPC=0 (DFS expansion skipped this run), only vet
    # keywords in high-value verticals with high LLM confidence — the rest have
    # no price signal and produce only UNSCORED results, wasting Brave quota.
    HIGH_VALUE_VERTICALS = {
        "finance", "insurance", "legal", "health", "real_estate", "education",
        "software", "saas", "tech", "travel", "automotive"
    }
    all_cpc_zero = all(float(kw.get("cpc_usd") or 0) == 0 for kw in commercial_keywords)
    if all_cpc_zero:
        before_zero_filter = len(commercial_keywords)
        commercial_keywords = [
            kw for kw in commercial_keywords
            if (
                (kw.get("vertical", "general") in HIGH_VALUE_VERTICALS
                 or kw.get("commercial_category", "general") in HIGH_VALUE_VERTICALS)
                and kw.get("confidence", "") in ("high", "medium")
            )
        ]
        dropped_zero = before_zero_filter - len(commercial_keywords)
        if dropped_zero > 0:
            print(f"  [Zero-CPC Filter] All {before_zero_filter} keywords have CPC=0 "
                  f"(DFS expansion skipped) — retained {len(commercial_keywords)} "
                  f"high-value-vertical keywords, dropped {dropped_zero}")
        if not commercial_keywords:
            print("  [Zero-CPC Filter] No high-value keywords to vet — writing empty output")
            OUTPUT.write_text("[]")
            print(f"✅ Vetting complete: 0 opportunities (no actionable keywords) → {OUTPUT.name}")
            raise SystemExit(0)

    # Deduplicate by (keyword, country) before SERP calls — saves DDG/Brave quota.
    # Keep the first occurrence (preserves ordering from prior sort/filter).
    _seen_kw_country = set()
    _deduped = []
    for kw in commercial_keywords:
        _key = (kw.get("keyword", "").strip().lower(), kw.get("country", "").strip().upper())
        if _key not in _seen_kw_country:
            _seen_kw_country.add(_key)
            _deduped.append(kw)
    _dedup_dropped = len(commercial_keywords) - len(_deduped)
    if _dedup_dropped > 0:
        print(f"  [Dedup] Removed {_dedup_dropped} duplicate kw+country pairs "
              f"({len(commercial_keywords)} → {len(_deduped)})")
    commercial_keywords = _deduped

    # Cap at 2000 keywords to keep vetting within the 3600s heartbeat timeout.
    # At 8s DDG timeout × 2000 kw / 15 concurrent ≈ 1067s worst-case.
    # Prioritise by CPC × volume (highest commercial value first).
    VETTING_CAP = 2000
    if len(commercial_keywords) > VETTING_CAP:
        commercial_keywords = sorted(
            commercial_keywords,
            key=lambda k: (k.get("cpc_usd") or 0) * (k.get("search_volume") or 0),
            reverse=True,
        )[:VETTING_CAP]
        print(f"  [Cap] Trimmed to top {VETTING_CAP} keywords by CPC×volume for vetting")

    all_vetted = asyncio.run(vet_all(commercial_keywords))

    OUTPUT.write_text(json.dumps(all_vetted, indent=2))

    with HISTORY.open("a") as f:
        for rec in all_vetted:
            f.write(json.dumps(rec) + "\n")

    print(f"✅ Vetting complete: {len(all_vetted)} opportunities from {len(commercial_keywords)} commercial keywords → {OUTPUT.name}")
