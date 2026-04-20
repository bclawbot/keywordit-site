#!/usr/bin/env python3
"""
subreddit_discovery.py — Stage 0a: Discover and score new RSOC subreddits.

Uses 5 strategies to find candidate subreddits, pre-scores them, sends top
candidates to LiteLLM for relevance evaluation, and persists approved subs
to subreddit_registry.json for use by reddit_intelligence.py.

Runs daily by default (controlled by discovery_last_run.txt). Can also
be run standalone: python3 subreddit_discovery.py [--force]

Outputs:
  - subreddit_registry.json      (created/updated)
  - discovery_last_run.txt       (timestamp gate)
  - discovery_rejected.jsonl     (candidates below MIN_PRE_SCORE)
"""
import asyncio
import json
import random
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

import aiohttp

BASE = Path(__file__).resolve().parent
REGISTRY_FILE       = BASE / "subreddit_registry.json"
LAST_RUN_FILE       = BASE / "discovery_last_run.txt"
REJECTED_LOG        = BASE / "discovery_rejected.jsonl"
CHAT_ID_FILE        = BASE / ".telegram_chat_id"
ENV_FILE            = Path.home() / ".openclaw" / ".env"

# LLM — centralized client (handles .env, think=False, fallback, timeouts)
from llm_client import call as _llm_call

DISCOVERY_RUN_FREQ_DAYS = 1   # only run if last run was > N days ago
REGISTRY_CAP            = 100  # max active subs in registry
MAX_NEW_PER_RUN         = 10   # cap new additions per pipeline run
MIN_PRE_SCORE           = 4.0  # below this → log to rejected, skip LLM
LLM_BATCH_SIZE          = 10   # candidates per LLM call
LLM_MIN_SCORE           = 7    # llm_score >= this to add to registry
ZERO_POST_DISABLE_THRESHOLD = 5

HEADERS = {"User-Agent": "OpenClaw/2.0 (RSOC intelligence scanner)"}

# Hardcoded subs — never re-add or re-evaluate these
KNOWN_HARDCODED: frozenset = frozenset({
    "searchArbitrage", "ppc", "adops", "domains", "seo",
    "bigseo", "affiliatemarketing", "marketing", "digital_marketing", "facebookads",
})

# Multi-theme RSOC-focused keyword queries
DISCOVERY_QUERIES: dict[str, list[str]] = {
    "rsoc_core": [
        "RSOC search arbitrage",
        "RPM arbitrage traffic",
        "search feed monetization",
        "domain parking monetization",
        "CPC optimization publisher",
    ],
    "traffic": [
        "arbitrage traffic native ads",
        "paid traffic monetization",
        "content monetization display ads",
        "media buy ROI arbitrage",
    ],
    "feed_providers": [
        "System1 Tonic Sedo feed",
        "RSOC provider payout",
        "search feed provider",
        "domain parking revenue",
    ],
    "metrics": [
        "RPC revenue per click publisher",
        "EPC affiliate arbitrage",
        "display ads RPM publisher",
    ],
    "compliance": [
        "Google Ads policy publisher compliance",
        "ad account compliance traffic quality",
        "IVT invalid traffic prevention",
    ],
    "verticals": [
        "insurance keywords high CPC",
        "legal keywords mass tort arbitrage",
        "finance keywords affiliate CPC",
        "solar roofing high CPC arbitrage",
    ],
}

# All queries flattened
ALL_QUERIES: list[str] = [q for qs in DISCOVERY_QUERIES.values() for q in qs]

RE_SUB_MENTION    = re.compile(r'r/([A-Za-z0-9_]{3,50})')
RE_REDDIT_URL_SUB = re.compile(r'reddit\.com/r/([A-Za-z0-9_]{3,50})')
VALID_NAME_RE     = re.compile(r'^[A-Za-z0-9_]{3,50}$')

# General-interest blocklist (not RSOC relevant)
BLOCKLIST: frozenset = frozenset({
    "announcements", "blog", "changelog", "help", "modnews", "redditmobile",
    "reddit", "all", "popular", "mod", "AskReddit", "funny", "pics",
    "worldnews", "news", "politics", "gaming", "videos", "todayilearned",
    "science", "technology", "sports", "movies", "music", "books",
    "food", "cooking", "travel", "personalfinance", "investing",
    "explainlikeimfive", "DIY", "LifeProTips", "dataisbeautiful",
    "programming", "learnprogramming", "webdev", "python", "javascript",
})


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class SubredditCandidate:
    name: str                            # normalized: no r/ prefix, original casing
    name_lower: str = ""                 # lowercase, for dedup
    mention_count: int = 0
    source_strategies: list = field(default_factory=list)
    source_subs: list = field(default_factory=list)
    sidebar_position: int | None = None  # 0 = first (highest signal)
    comment_post_spread: int = 0         # distinct posts mentioning this sub
    crosspost_count: int = 0
    keyword_query_hits: int = 0
    wiki_mention_count: int = 0
    raw_context_snippets: list = field(default_factory=list)  # up to 3 x 120-char
    pre_score: float = 0.0
    discovered_at: str = ""

    def __post_init__(self):
        if not self.name_lower:
            self.name_lower = self.name.lower()
        if not self.discovered_at:
            self.discovered_at = datetime.now().isoformat()


class CandidateRegistry:
    def __init__(self):
        self._store: dict[str, SubredditCandidate] = {}  # keyed on name_lower

    def upsert(
        self,
        name: str,
        strategy: str,
        source_sub: str = "",
        sidebar_position: int | None = None,
        comment_spread_inc: int = 0,
        crosspost_inc: int = 0,
        keyword_hit_inc: int = 0,
        wiki_inc: int = 0,
        context_snippet: str | None = None,
    ) -> None:
        key = name.lower()
        if key not in self._store:
            self._store[key] = SubredditCandidate(name=name)
        c = self._store[key]
        c.mention_count += 1
        if strategy not in c.source_strategies:
            c.source_strategies.append(strategy)
        if source_sub and source_sub not in c.source_subs:
            c.source_subs.append(source_sub)
        if sidebar_position is not None:
            if c.sidebar_position is None or sidebar_position < c.sidebar_position:
                c.sidebar_position = sidebar_position
        c.comment_post_spread += comment_spread_inc
        c.crosspost_count += crosspost_inc
        c.keyword_query_hits += keyword_hit_inc
        c.wiki_mention_count += wiki_inc
        if context_snippet:
            snippet = context_snippet.strip()[:120]
            if snippet and snippet not in c.raw_context_snippets:
                c.raw_context_snippets = (c.raw_context_snippets + [snippet])[:3]

    def all_candidates(self) -> list[SubredditCandidate]:
        return list(self._store.values())

    def top_n(self, n: int) -> list[SubredditCandidate]:
        return sorted(self._store.values(), key=lambda c: c.pre_score, reverse=True)[:n]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _normalize_name(raw: str) -> str:
    """Strip r/ prefix, return original casing."""
    return raw.lstrip("r/").lstrip("/")


def _is_valid_candidate(name: str, existing_registry_names: set[str]) -> bool:
    key = name.lower()
    return (
        bool(VALID_NAME_RE.match(name))
        and key not in KNOWN_HARDCODED
        and key not in BLOCKLIST
        and key not in existing_registry_names
    )


def _strip_code_fences(text: str) -> str:
    return re.sub(r"^```json?\n?|\n?```$", "", text.strip(), flags=re.MULTILINE)


def compute_pre_score(c: SubredditCandidate) -> float:
    score = 0.0
    if c.sidebar_position is not None:
        score += max(5.0, 35.0 - c.sidebar_position * 5.0)   # max 35pts
    score += min(25.0, c.comment_post_spread * 5.0)            # max 25pts
    score += min(15.0, c.crosspost_count * 5.0)                # max 15pts
    score += min(10.0, c.keyword_query_hits * 2.0)             # max 10pts
    score += min(10.0, c.wiki_mention_count * 3.0)             # max 10pts
    n_strategies = len(set(c.source_strategies))
    score += 5.0 if n_strategies >= 3 else (2.0 if n_strategies == 2 else 0.0)
    return round(score, 2)


# ── Rate limiter ───────────────────────────────────────────────────────────────

class RateLimiter:
    """Rolling window rate limiter with min delay between requests."""

    def __init__(self, req_per_min: int = 30, min_delay: float = 1.5):
        self._req_per_min = req_per_min
        self._min_delay = min_delay
        self._timestamps: list[float] = []
        self._last_req: float = 0.0

    async def acquire(self) -> None:
        now = time.monotonic()
        # Enforce min delay between consecutive requests
        since_last = now - self._last_req
        if since_last < self._min_delay:
            await asyncio.sleep(self._min_delay - since_last)
            now = time.monotonic()
        # Enforce rolling window cap
        window_start = now - 60.0
        self._timestamps = [t for t in self._timestamps if t > window_start]
        if len(self._timestamps) >= self._req_per_min:
            oldest = self._timestamps[0]
            sleep_for = (oldest + 60.0) - now + 0.1
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            now = time.monotonic()
        self._timestamps.append(now)
        self._last_req = now


async def _reddit_get(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    retries: int = 2,
) -> dict | None:
    """GET a Reddit JSON endpoint with rate limiting and backoff."""
    for attempt in range(retries):
        await rate_limiter.acquire()
        async with semaphore:
            try:
                async with session.get(
                    url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=12)
                ) as r:
                    if r.status == 429:
                        delay = 2.0 * (2 ** attempt) + random.uniform(0, 1)
                        print(f"  [429] Rate limited — sleeping {delay:.1f}s — {url[:60]}")
                        await asyncio.sleep(delay)
                        continue
                    if r.status == 403 or r.status == 404:
                        return None
                    if r.status != 200:
                        return None
                    return await r.json()
            except Exception:
                if attempt < retries - 1:
                    await asyncio.sleep(2.0 + random.uniform(0, 1))
    return None


# ── Strategy 1: Sidebar Crawling ───────────────────────────────────────────────

async def strategy_sidebar(
    session: aiohttp.ClientSession,
    sub_name: str,
    registry: CandidateRegistry,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    existing_names: set[str],
) -> int:
    """Fetch about.json for a known sub, extract related_subreddits + text mentions."""
    sub_clean = sub_name.lstrip("r/")
    url = f"https://www.reddit.com/r/{sub_clean}/about.json"
    data = await _reddit_get(session, url, semaphore, rate_limiter)
    if not data:
        return 0

    d = data.get("data", {})
    added = 0

    # related_subreddits (moderator-curated)
    related = d.get("related_subreddits") or []
    if isinstance(related, list):
        for idx, item in enumerate(related):
            name_raw = item if isinstance(item, str) else item.get("name", "")
            name = _normalize_name(name_raw)
            if name and _is_valid_candidate(name, existing_names):
                registry.upsert(
                    name=name, strategy="sidebar", source_sub=sub_name,
                    sidebar_position=idx,
                    context_snippet=f"Related subreddit of {sub_name} (position {idx})",
                )
                added += 1

    # Text mentions in description/public_description/header_title
    text_blob = " ".join(filter(None, [
        d.get("description", "") or "",
        d.get("public_description", "") or "",
        d.get("header_title", "") or "",
        d.get("submit_text_label", "") or "",
    ]))
    for m in RE_SUB_MENTION.finditer(text_blob):
        name = m.group(1)
        if _is_valid_candidate(name, existing_names):
            start = max(0, m.start() - 40)
            end   = min(len(text_blob), m.end() + 40)
            snippet = text_blob[start:end].strip()
            registry.upsert(
                name=name, strategy="sidebar", source_sub=sub_name,
                context_snippet=snippet,
            )
            added += 1

    return added


# ── Strategy 2: Comment Mining ─────────────────────────────────────────────────

async def strategy_comment_mining(
    session: aiohttp.ClientSession,
    sub_name: str,
    post_id: str,
    registry: CandidateRegistry,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    existing_names: set[str],
) -> int:
    """Fetch comments for a post, extract r/ mentions."""
    sub_clean = sub_name.lstrip("r/")
    url = f"https://www.reddit.com/r/{sub_clean}/comments/{post_id}.json?limit=50&depth=2"
    data = await _reddit_get(session, url, semaphore, rate_limiter)
    if not data or not isinstance(data, list) or len(data) < 2:
        return 0

    comment_listing = data[1]
    comments = comment_listing.get("data", {}).get("children", [])
    added = 0
    seen_in_this_post: set[str] = set()

    for comment_item in comments:
        body = comment_item.get("data", {}).get("body", "") or ""
        for m in RE_SUB_MENTION.finditer(body):
            name = m.group(1)
            if not _is_valid_candidate(name, existing_names):
                continue
            key = name.lower()
            spread_inc = 1 if key not in seen_in_this_post else 0
            seen_in_this_post.add(key)
            start = max(0, m.start() - 40)
            end   = min(len(body), m.end() + 40)
            snippet = body[start:end].strip()
            registry.upsert(
                name=name, strategy="comment_mining", source_sub=sub_name,
                comment_spread_inc=spread_inc,
                context_snippet=snippet,
            )
            added += 1

    return added


# ── Strategy 3: Cross-Post Tracking (synchronous, free) ───────────────────────

def strategy_crosspost(
    posts: list[dict],
    registry: CandidateRegistry,
    existing_names: set[str],
) -> int:
    """Extract source subreddits from cross-post metadata in already-fetched posts."""
    added = 0
    for post in posts:
        xpost_list = post.get("crosspost_parent_list") or []
        for xp in xpost_list:
            name = xp.get("subreddit") or ""
            if name and _is_valid_candidate(name, existing_names):
                registry.upsert(
                    name=name, strategy="crosspost",
                    source_sub=post.get("subreddit", ""),
                    crosspost_inc=1,
                    context_snippet=f"Cross-posted from in {post.get('subreddit', '')}",
                )
                added += 1
    return added


# ── Strategy 4: Keyword Search ─────────────────────────────────────────────────

async def strategy_keyword_search(
    session: aiohttp.ClientSession,
    registry: CandidateRegistry,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    existing_names: set[str],
) -> int:
    """Search Reddit for subreddits matching RSOC-focused queries."""
    added = 0
    for query in ALL_QUERIES:
        url = f"https://www.reddit.com/search.json?q={query}&type=sr&sort=relevance&limit=25"
        data = await _reddit_get(session, url, semaphore, rate_limiter)
        if not data:
            continue
        children = data.get("data", {}).get("children", [])
        for item in children:
            d = item.get("data", {})
            if d.get("over18"):
                continue
            if d.get("subreddit_type") not in ("public", None):
                continue
            if int(d.get("subscribers", 0)) < 500:
                continue
            name = d.get("display_name") or ""
            if not name or not _is_valid_candidate(name, existing_names):
                continue
            desc = (d.get("public_description") or "")[:200]
            registry.upsert(
                name=name, strategy="keyword_search",
                keyword_hit_inc=1,
                context_snippet=f"[query: {query[:40]}] {desc[:80]}",
            )
            added += 1

        # Also scan description text of results for r/ mentions
        for item in children:
            desc = (item.get("data", {}).get("public_description") or "")
            for m in RE_SUB_MENTION.finditer(desc):
                name = m.group(1)
                if _is_valid_candidate(name, existing_names):
                    registry.upsert(
                        name=name, strategy="keyword_search",
                        keyword_hit_inc=1,
                        context_snippet=desc[max(0, m.start()-40):m.end()+40].strip(),
                    )
                    added += 1

    return added


# ── Strategy 5: Wiki Extraction ────────────────────────────────────────────────

async def strategy_wiki(
    session: aiohttp.ClientSession,
    sub_name: str,
    registry: CandidateRegistry,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    existing_names: set[str],
    max_pages: int = 3,
) -> int:
    """Fetch wiki index for a known sub and extract r/ mentions from markdown."""
    sub_clean = sub_name.lstrip("r/")
    url = f"https://www.reddit.com/r/{sub_clean}/wiki/index.json"
    data = await _reddit_get(session, url, semaphore, rate_limiter)
    if not data:
        return 0

    content_md = data.get("data", {}).get("content_md", "") or ""
    added = 0

    for m in RE_SUB_MENTION.finditer(content_md):
        name = m.group(1)
        if _is_valid_candidate(name, existing_names):
            start = max(0, m.start() - 40)
            end   = min(len(content_md), m.end() + 40)
            registry.upsert(
                name=name, strategy="wiki", source_sub=sub_name,
                wiki_inc=1,
                context_snippet=content_md[start:end].strip(),
            )
            added += 1

    for m in RE_REDDIT_URL_SUB.finditer(content_md):
        name = m.group(1)
        if _is_valid_candidate(name, existing_names):
            registry.upsert(
                name=name, strategy="wiki", source_sub=sub_name,
                wiki_inc=1,
            )
            added += 1

    return added


# ── LLM Scoring ────────────────────────────────────────────────────────────────

def _llm_system_prompt() -> str:
    return (
        "You are a senior search arbitrage (RSOC) media buyer and data analyst with deep expertise in:\n"
        "- High-CPC keyword verticals: insurance, legal, finance, health/pharma, SaaS, home services\n"
        "  (solar, roofing, HVAC, plumbing), automotive, education, Medicare/senior care\n"
        "- RSOC economics: revenue share on search (System1, Tonic, Sedo, Bodis), RPM/RPC optimization,\n"
        "  bridge page compliance, feed payout mechanics, traffic quality requirements\n"
        "- User intent signals: the difference between informational content (low value) vs. commercial/\n"
        "  transactional intent (high value) — what makes someone click an ad vs. just read\n"
        "- Paid search arbitrage: buying cheap traffic (native, display, social) and monetizing via\n"
        "  search feeds; profitability requires users to arrive with latent purchase intent\n\n"
        "Your job: evaluate Reddit community candidates and score how likely scraping them would surface\n"
        "early signals of PROFITABLE PAID SEARCH KEYWORDS — queries real consumers type when ready to\n"
        "buy, compare, or hire.\n\n"
        "SCORING FRAMEWORK (llm_score 1-10):\n\n"
        "9-10 = RSOC GOLD — community directly centered on high-CPC buying decisions or ad industry\n"
        "  intelligence. Posts will routinely surface commercial intent keywords, CPC data, or\n"
        "  arbitrage tactics. Examples: r/insurance, r/legaladvice, r/personalfinance, r/adops.\n\n"
        "7-8 = HIGH VALUE — community frequently surfaces commercial intent or practitioner discussions\n"
        "  about paid acquisition. Overlap with known high-CPC verticals is strong. Worth scraping.\n\n"
        "5-6 = MONITOR — moderate overlap. Community occasionally surfaces relevant signals but primary\n"
        "  focus is informational or tangential. Accumulate more data before committing to scrape.\n\n"
        "1-4 = SKIP — informational, entertainment, geography-only, or zero overlap with commercial\n"
        "  intent keywords. Scraping would produce noise.\n\n"
        "KEY QUESTIONS to ask for each candidate:\n"
        "1. Do users in this sub discuss buying, hiring, comparing, or pricing services?\n"
        "2. Does the topic overlap with insurance, legal, finance, health, home services, SaaS, auto,\n"
        "   or education — the verticals that print money on search feeds?\n"
        "3. Would a post title from this sub ever resemble a high-CPC search query?\n"
        "4. Is this a consumer community (intent signal) or a hobbyist/practitioner community (low intent)?\n\n"
        "IMPORTANT: Be selective. Daily discovery runs mean a growing candidate pool — only approve subs\n"
        "where you are confident posts will contain commercial intent signals. When uncertain, use\n"
        '"monitor" not "add". Never approve geography, hobby, entertainment, or news subs.\n\n'
        "Respond ONLY with a JSON array. No explanation, no markdown fences. Each element:\n"
        '{"name": "SubredditName", "llm_score": 1-10, "llm_verdict": "add|monitor|skip",\n'
        ' "suggested_priority": 1-3, "suggested_feeds": ["hot","new"], "llm_rationale": "one sentence"}\n\n'
        "Priority guide: 1 = scrape hot+new aggressively (RSOC gold), 2 = hot+rising, 3 = hot only."
    )


def _build_llm_prompt(candidates: list[SubredditCandidate]) -> str:
    items = []
    for c in candidates:
        items.append({
            "name": c.name,
            "strategies": c.source_strategies,
            "pre_score": c.pre_score,
            "context_snippets": c.raw_context_snippets,
        })
    return json.dumps(items, ensure_ascii=False)


def score_candidates_llm(candidates: list[SubredditCandidate]) -> list[dict]:
    """Batch-score candidates via centralized LLM client. Non-fatal."""
    if not candidates:
        return []

    results = []
    system_msg = _llm_system_prompt()

    for i in range(0, len(candidates), LLM_BATCH_SIZE):
        batch = candidates[i:i + LLM_BATCH_SIZE]
        user_msg = _build_llm_prompt(batch)

        for attempt in range(2):
            try:
                raw = _llm_call(
                    [{"role": "system", "content": system_msg},
                     {"role": "user",   "content": user_msg}],
                    max_tokens=2048,
                    temperature=0.2,
                    timeout="generous",
                    stage="subreddit_discovery/scoring",
                )
                clean = _strip_code_fences(raw)
                batch_results = json.loads(clean)
                if not isinstance(batch_results, list):
                    raise ValueError("LLM response not a list")
                results.extend(batch_results)
                break

            except json.JSONDecodeError:
                if attempt == 0:
                    user_msg = (
                        "Your previous response was not valid JSON. "
                        "Return ONLY the JSON array, no other text.\n\n" + user_msg
                    )
                    continue
                print(f"  [!] LLM JSON parse failed for batch {i//LLM_BATCH_SIZE + 1}")

            except Exception as e:
                print(f"  [!] LLM error for batch {i//LLM_BATCH_SIZE + 1}: {e}")
                break

    return results


# ── Registry I/O ───────────────────────────────────────────────────────────────

def load_registry() -> dict:
    """Load registry JSON. Returns empty skeleton on any error."""
    try:
        return json.loads(REGISTRY_FILE.read_text())
    except Exception:
        return {
            "schema_version": "1",
            "last_updated": datetime.now().isoformat(),
            "subreddits": {},
        }


def save_registry(registry: dict) -> None:
    """Atomic write: write to .tmp then rename."""
    registry["last_updated"] = datetime.now().isoformat()
    tmp = REGISTRY_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(registry, indent=2))
    tmp.rename(REGISTRY_FILE)


def update_zero_post_counters(registry: dict, subs_with_zero_posts: list[str]) -> dict:
    """
    Called externally by reddit_intelligence.py after a scrape run.
    Increments times_yielded_zero_posts; disables subs at threshold.
    """
    changed = False
    for name in subs_with_zero_posts:
        if name in registry.get("subreddits", {}):
            entry = registry["subreddits"][name]
            entry["times_yielded_zero_posts"] = entry.get("times_yielded_zero_posts", 0) + 1
            if entry["times_yielded_zero_posts"] >= ZERO_POST_DISABLE_THRESHOLD:
                entry["status"] = "disabled"
                print(f"  [registry] Disabled {name} — {ZERO_POST_DISABLE_THRESHOLD} zero-post runs")
            changed = True
    if changed:
        save_registry(registry)
    return registry


def _registry_active_names(registry: dict) -> set[str]:
    """Return lowercase names of all entries (active or disabled) in the registry."""
    return {name.lstrip("r/").lower() for name in registry.get("subreddits", {}).keys()}


# ── Weekly gate ────────────────────────────────────────────────────────────────

def _should_run() -> bool:
    """Return True if last run was > DISCOVERY_RUN_FREQ_DAYS ago (or never ran)."""
    try:
        last = float(LAST_RUN_FILE.read_text().strip())
        days_ago = (time.time() - last) / 86400
        if days_ago < DISCOVERY_RUN_FREQ_DAYS:
            print(f"  Discovery last ran {days_ago:.1f} days ago — skipping (runs every {DISCOVERY_RUN_FREQ_DAYS} day(s))")
            return False
    except Exception:
        pass
    return True


def _mark_run() -> None:
    LAST_RUN_FILE.write_text(str(time.time()))


def _load_env(path: Path) -> dict:
    """Load environment variables from .env file."""
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


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    """Send a Telegram message."""
    if not token or not chat_id:
        return
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    try:
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data,
            timeout=10,
        )
    except Exception:
        pass


def _count_active(registry: dict) -> int:
    return sum(1 for e in registry.get("subreddits", {}).values() if e.get("status") == "active")


# ── Main discovery orchestrator ────────────────────────────────────────────────

async def discover_and_update(registry: dict) -> tuple[dict, int]:
    """
    Run all 5 strategies, score candidates, update registry.
    Returns (updated_registry, new_count).
    """
    existing_names = _registry_active_names(registry)
    candidate_registry = CandidateRegistry()
    semaphore = asyncio.Semaphore(3)
    rate_limiter = RateLimiter(req_per_min=30, min_delay=1.5)

    # Known subreddits list (for sidebar/comment/wiki strategies)
    known_subs = [
        "r/SearchArbitrage", "r/PPC", "r/adops", "r/Domains", "r/SEO",
        "r/bigseo", "r/Affiliatemarketing", "r/marketing", "r/digital_marketing", "r/FacebookAds",
    ]

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5)) as session:

        # ── Strategy 1: Sidebar ────────────────────────────────────────────────
        print("  [discovery] Strategy 1: Sidebar crawling...")
        sidebar_tasks = [
            strategy_sidebar(session, sub, candidate_registry, semaphore, rate_limiter, existing_names)
            for sub in known_subs
        ]
        sidebar_counts = await asyncio.gather(*sidebar_tasks, return_exceptions=True)
        sidebar_total = sum(c for c in sidebar_counts if isinstance(c, int))
        print(f"             → {sidebar_total} candidates from sidebars")

        # ── Strategy 4: Keyword Search ─────────────────────────────────────────
        # Run keyword search early so comment mining candidates are richer
        print("  [discovery] Strategy 4: Keyword search ({} queries)...".format(len(ALL_QUERIES)))
        kw_count = await strategy_keyword_search(
            session, candidate_registry, semaphore, rate_limiter, existing_names
        )
        print(f"             → {kw_count} candidates from keyword search")

        # ── Strategy 5: Wiki Extraction ────────────────────────────────────────
        print("  [discovery] Strategy 5: Wiki extraction...")
        wiki_tasks = [
            strategy_wiki(session, sub, candidate_registry, semaphore, rate_limiter, existing_names)
            for sub in known_subs
        ]
        wiki_counts = await asyncio.gather(*wiki_tasks, return_exceptions=True)
        wiki_total = sum(c for c in wiki_counts if isinstance(c, int))
        print(f"             → {wiki_total} candidates from wikis")

        # ── Strategy 2: Comment Mining (with 300s deadline) ─────────────────────
        print("  [discovery] Strategy 2: Comment mining (fetching posts)...")
        comment_total = 0

        async def _run_comment_mining():
            nonlocal comment_total
            for sub in known_subs:
                sub_clean = sub.lstrip("r/")
                url = f"https://www.reddit.com/r/{sub_clean}/hot.json?limit=25"
                data = await _reddit_get(session, url, semaphore, rate_limiter)
                if not data:
                    continue
                posts_raw = data.get("data", {}).get("children", [])
                top_posts = sorted(
                    [p.get("data", {}) for p in posts_raw if not p.get("data", {}).get("stickied")],
                    key=lambda p: p.get("score", 0),
                    reverse=True,
                )[:5]
                strategy_crosspost(
                    [p for p in top_posts if p.get("crosspost_parent_list")],
                    candidate_registry,
                    existing_names,
                )
                comment_tasks = []
                for post in top_posts:
                    if post.get("score", 0) < 20 or not post.get("num_comments", 0):
                        continue
                    post_id = post.get("id", "")
                    if post_id:
                        comment_tasks.append(
                            strategy_comment_mining(
                                session, sub, post_id, candidate_registry,
                                semaphore, rate_limiter, existing_names
                            )
                        )
                if comment_tasks:
                    counts = await asyncio.gather(*comment_tasks, return_exceptions=True)
                    comment_total += sum(c for c in counts if isinstance(c, int))

        try:
            await asyncio.wait_for(_run_comment_mining(), timeout=300)
        except asyncio.TimeoutError:
            print("  [discovery] Comment mining timed out after 300s — continuing with partial results")

        print(f"             → {comment_total} candidates from comments")

    # ── Pre-scoring ────────────────────────────────────────────────────────────
    all_candidates = candidate_registry.all_candidates()
    for c in all_candidates:
        c.pre_score = compute_pre_score(c)

    above_threshold = [c for c in all_candidates if c.pre_score >= MIN_PRE_SCORE]
    below_threshold = [c for c in all_candidates if c.pre_score < MIN_PRE_SCORE]

    # Log rejected candidates
    with REJECTED_LOG.open("a") as f:
        for c in below_threshold:
            f.write(json.dumps({
                "name": c.name, "pre_score": c.pre_score,
                "strategies": c.source_strategies, "rejected_at": datetime.now().isoformat()
            }) + "\n")

    print(f"\n  [discovery] Unique candidates: {len(all_candidates)} total, "
          f"{len(above_threshold)} above threshold, {len(below_threshold)} below")

    # ── LLM Scoring ────────────────────────────────────────────────────────────
    top_candidates = sorted(above_threshold, key=lambda c: c.pre_score, reverse=True)[:40]
    print(f"  [discovery] Sending top {len(top_candidates)} to LiteLLM for scoring...")
    llm_results = score_candidates_llm(top_candidates)

    # ── Write approved to registry ─────────────────────────────────────────────
    # Build lookup by normalized name
    llm_lookup: dict[str, dict] = {}
    for r in llm_results:
        if isinstance(r, dict) and r.get("name"):
            llm_lookup[r["name"].lower()] = r

    active_count = _count_active(registry)
    new_count = 0
    llm_add = llm_monitor = llm_skip = 0

    for c in top_candidates:
        scored = llm_lookup.get(c.name_lower) or llm_lookup.get(c.name.lower())
        if not scored:
            continue

        verdict = scored.get("llm_verdict", "skip").lower()
        llm_score = int(scored.get("llm_score", 0))

        if verdict == "add":
            llm_add += 1
        elif verdict == "monitor":
            llm_monitor += 1
        else:
            llm_skip += 1
            continue

        if verdict != "add" or llm_score < LLM_MIN_SCORE:
            continue
        if active_count >= REGISTRY_CAP:
            print(f"  [discovery] Registry cap ({REGISTRY_CAP}) reached — stopping additions")
            break
        if new_count >= MAX_NEW_PER_RUN:
            print(f"  [discovery] Max new per run ({MAX_NEW_PER_RUN}) reached")
            break

        sub_key = f"r/{c.name}"
        registry.setdefault("subreddits", {})[sub_key] = {
            "name":                    sub_key,
            "status":                  "active",
            "date_added":              datetime.now().isoformat(),
            "discovery_source":        " + ".join(
                f"{s}:{src}" for s, src in zip(c.source_strategies[:3], c.source_subs[:3])
            ) or ", ".join(c.source_strategies[:3]),
            "pre_score":               c.pre_score,
            "llm_score":               llm_score,
            "llm_rationale":           scored.get("llm_rationale", ""),
            "times_yielded_zero_posts": 0,
            "feeds":                   scored.get("suggested_feeds") or ["hot"],
            "limit":                   15,
            "priority":                scored.get("suggested_priority") or 3,
        }
        print(f"  [registry] +++ Added {sub_key} (pre={c.pre_score}, llm={llm_score}) — {scored.get('llm_rationale', '')[:60]}")
        new_count += 1
        active_count += 1

    print(f"\n  [discovery] LLM verdicts: add={llm_add}, monitor={llm_monitor}, skip={llm_skip}")
    return registry, new_count


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    force = "--force" in sys.argv

    print("=" * 56)
    print("  Subreddit Discovery Engine")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 56)

    if not force and not _should_run():
        sys.exit(0)

    try:
        registry = load_registry()
        initial_count = _count_active(registry)
        print(f"  Registry: {initial_count} active subs loaded")

        registry, new_count = asyncio.run(discover_and_update(registry))
        save_registry(registry)
        _mark_run()

        final_count = _count_active(registry)
        print(f"\n  Discovery complete.")
        print(f"  New subs added: {new_count}")
        print(f"  Registry total: {final_count} active subs")
        print(f"  Saved → {REGISTRY_FILE.name}")

        # Send Telegram notification
        env = _load_env(ENV_FILE)
        telegram_token = env.get("TELEGRAM_TOKEN", "")
        telegram_chat_id = ""
        try:
            telegram_chat_id = CHAT_ID_FILE.read_text().strip()
        except FileNotFoundError:
            pass

        if telegram_token and telegram_chat_id:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            msg = (
                f"🔍 Subreddit Discovery Complete — {now_str}\n"
                f"New subreddits added: {new_count}\n"
                f"Total active subreddits: {final_count}"
            )
            print(f"\n[notify] Sending Telegram notification...")
            _send_telegram(telegram_token, telegram_chat_id, msg)
            print(f"[notify] Notification sent")

    except Exception as exc:
        print(f"  [FATAL] Subreddit discovery failed: {exc}")
        try:
            with open(BASE / "error_log.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "timestamp": datetime.now().isoformat(),
                    "stage": "subreddit_discovery",
                    "error": str(exc),
                }) + "\n")
        except Exception:
            pass
        # Exit 0 so heartbeat pipeline continues to next stage
        sys.exit(0)
