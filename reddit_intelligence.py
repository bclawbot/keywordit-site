#!/usr/bin/env python3
"""
reddit_intelligence.py — Scrape RSOC intelligence subreddits, categorize posts.

Outputs:
  - reddit_intelligence.json  (latest run snapshot — all categorized posts)
  - reddit_intel_history.jsonl (append-only log)

This is NOT a pipeline stage. Run standalone or integrate into heartbeat.py.
Usage: python3 reddit_intelligence.py
"""
import asyncio
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
import aiohttp

BASE = Path("/Users/newmac/.openclaw/workspace")
OUTPUT = BASE / "reddit_intelligence.json"
HISTORY = BASE / "reddit_intel_history.jsonl"
REGISTRY_FILE = BASE / "subreddit_registry.json"


def _load_registry_subs() -> dict:
    """Load active subs from subreddit_registry.json. Returns {} on any error."""
    try:
        data = json.loads(REGISTRY_FILE.read_text())
        result = {}
        for name, entry in data.get("subreddits", {}).items():
            if entry.get("status") == "active":
                result[name] = {
                    "feeds":    entry.get("feeds", ["hot"]),
                    "limit":    entry.get("limit", 10),
                    "priority": entry.get("priority", 3),
                }
        return result
    except Exception:
        return {}


INTELLIGENCE_SUBREDDITS = {
    "r/SearchArbitrage":    {"feeds": ["hot", "new"], "limit": 25, "priority": 1},
    "r/PPC":                {"feeds": ["hot", "rising"], "limit": 20, "priority": 1},
    "r/adops":              {"feeds": ["hot", "new"], "limit": 20, "priority": 1},
    "r/Domains":            {"feeds": ["hot", "new"], "limit": 15, "priority": 2},
    "r/SEO":                {"feeds": ["hot", "rising"], "limit": 15, "priority": 2},
    "r/bigseo":             {"feeds": ["hot"], "limit": 10, "priority": 2},
    "r/Affiliatemarketing": {"feeds": ["hot"], "limit": 15, "priority": 3},
    "r/marketing":          {"feeds": ["hot"], "limit": 10, "priority": 3},
    "r/digital_marketing":  {"feeds": ["hot"], "limit": 10, "priority": 3},
    "r/FacebookAds":        {"feeds": ["hot"], "limit": 10, "priority": 3},
}

CATEGORY_PATTERNS = {
    "compliance_alert": [
        r"policy\s+(change|update|violation)",
        r"rac\s+(policy|requirement|update)",
        r"google\s+(ban|suspend|clamp|crack|policy|update)",
        r"adsense\s+(ban|suspend|policy|update|terminated)",
        r"ivt|invalid\s+traffic",
        r"account\s+(ban|suspend|terminat|disabled)",
    ],
    "feed_intel": [
        r"(system1|tonic|sedo|bodis|giantpanda|ads\.com|domain\s*active)",
        r"(feed|provider).*(payout|rpc|epc|payment|net-?\d+)",
        r"(rpc|epc|rpm).*(drop|increase|change|crash|surge)",
        r"search\s+feed",
        r"rsoc.*(provider|feed|payout)",
    ],
    "cpc_data": [
        r"\$\d+\.?\d*\s*/?\s*(cpc|rpc|rpm|epc|cpm)",
        r"(cpc|rpc|rpm|epc|cpm)\s*[\$:]?\s*\$?\d+",
        r"roas\s*[\$:]?\s*\d+%?",
        r"\d+%?\s*roas",
        r"(cost\s+per\s+click|revenue\s+per\s+click)\s*.*\$\d+",
    ],
    "vertical_signal": [
        r"(health|insurance|legal|finance|mortgage|loan|credit|dental|medicare|solar|roofing|hvac|auto)\s*.*(crush|profit|print|money|roi|roas|convert|scale)",
        r"(crush|profit|print|money|killing\s+it|roi|roas).*(health|insurance|legal|finance|mortgage|dental|medicare|solar|roofing|auto)",
        r"(best|top|highest)\s*(vertical|niche|keyword)s?\s*(for|in)\s*(arbitrage|rsoc|search)",
        r"(vertical|niche)\s+(is|are)\s+(hot|fire|printing|crushing)",
    ],
    "keyword_mention": [
        r"keyword.*(share|sharing|example|list|specific)",
        r"(best|top)\s+\w+\s+(near me|review|cost|price|vs|comparison|alternative)",
        r"(long.?tail|seed)\s+keyword",
        r"keyword\s+(idea|suggestion|recommendation|cluster)",
    ],
    "decay_signal": [
        r"(roas|rpc|rpm|epc|margin).*(drop|crash|collapse|tank|die|dead|dying|plummet)",
        r"(keyword|vertical|niche|campaign).*(decay|dying|dead|saturated|decline|exhausted)",
        r"(no\s+longer|stopped)\s+(profitable|converting|working)",
        r"margin\s+(compress|squeez|shrink|disappear)",
    ],
    "platform_shift": [
        r"(meta|facebook|tiktok|taboola|outbrain|snapchat|pinterest|reddit\s+ads).*(ban|policy|change|update|algorithm|restrict|limit|block)",
        r"(ban\s+wave|account\s+disabled|ad\s+account).*(meta|facebook|tiktok)",
        r"(cpm|cpc)\s+(surge|spike|increase).*(meta|facebook|tiktok|native)",
    ],
}

SCORE_THRESHOLDS = {1: 5, 2: 10, 3: 20}


async def fetch_subreddit(session, subreddit, feed, limit):
    url = f"https://www.reddit.com/{subreddit}/{feed}.json?limit={limit}"
    headers = {"User-Agent": "OpenClaw/2.0 (RSOC intelligence scanner)"}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=12)) as r:
            data = await r.json()
        posts = data.get("data", {}).get("children", [])
        results = []
        for post in posts:
            d = post.get("data", {})
            if d.get("stickied"):
                continue
            results.append({
                "subreddit": subreddit,
                "feed": feed,
                "title": d.get("title", "").strip(),
                "selftext": (d.get("selftext", "") or "")[:2000],
                "score": int(d.get("score", 0)),
                "upvote_ratio": round(float(d.get("upvote_ratio", 0)), 2),
                "num_comments": int(d.get("num_comments", 0)),
                "url": d.get("url", ""),
                "permalink": f"https://reddit.com{d.get('permalink', '')}",
                "created_utc": int(d.get("created_utc", 0)),
                "fetched_at": datetime.now().isoformat(),
            })
        return results
    except Exception as e:
        print(f"  [!] {subreddit}/{feed}: {e}")
        return []


def classify_post(post):
    text = (post["title"] + " " + post["selftext"]).lower()
    matches = []
    for category, patterns in CATEGORY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                matches.append(category)
                break
    if not matches:
        matches = ["noise"]
    return matches


def extract_mentioned_verticals(text):
    text_lower = text.lower()
    verticals = []
    vertical_keywords = {
        "health": r"health|medical|medicare|dental|pharma|weight\s*loss|supplement",
        "finance": r"financ|invest|stock|crypto|loan|mortgage|credit|debt|bank|insurance",
        "legal": r"legal|lawyer|attorney|lawsuit|mesothelioma|personal\s*injury|mass\s*tort",
        "insurance": r"insurance|auto\s+insurance|home\s+insurance|health\s+insurance|life\s+insurance",
        "real_estate": r"real\s*estate|property|housing|apartment|rent|zillow|redfin",
        "tech": r"saas|software|vpn|antivirus|hosting|cloud|cyber",
        "education": r"education|university|college|course|certification|degree|online\s+class",
        "home_improvement": r"roofing|solar|hvac|plumbing|contractor|home\s+improvement|remodel",
        "automotive": r"auto|car\s+(deal|lease|buy|insurance|loan)|vehicle",
        "travel": r"travel|flight|hotel|booking|cruise|vacation",
    }
    for vertical, pattern in vertical_keywords.items():
        if re.search(pattern, text_lower):
            verticals.append(vertical)
    return verticals


def extract_dollar_amounts(text):
    amounts = []
    for match in re.finditer(r'\$(\d+(?:\.\d{1,2})?)\s*/?\s*(cpc|rpc|rpm|epc|cpm)?', text.lower()):
        amounts.append({
            "value": float(match.group(1)),
            "metric": match.group(2) or "unknown",
        })
    return amounts


async def scrape_all():
    # Merge hardcoded subs with discovered registry subs (registry is additive)
    effective_subs = {**INTELLIGENCE_SUBREDDITS, **_load_registry_subs()}
    registry_subs = set(_load_registry_subs().keys())

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        task_subs = []
        for subreddit, config in effective_subs.items():
            for feed in config["feeds"]:
                tasks.append(fetch_subreddit(session, subreddit, feed, config["limit"]))
                task_subs.append(subreddit)
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_posts = []
    # Track which registry subs yielded posts (for zero-post counter)
    subs_with_posts: set[str] = set()
    for sub, r in zip(task_subs, results):
        if isinstance(r, list) and r:
            all_posts.extend(r)
            subs_with_posts.add(sub)

    # Update zero-post counters for registry subs that returned nothing
    zero_post_subs = [s for s in registry_subs if s not in subs_with_posts]
    if zero_post_subs:
        try:
            import subreddit_discovery as _sd
            _registry = _sd.load_registry()
            _sd.update_zero_post_counters(_registry, zero_post_subs)
        except Exception:
            pass

    return all_posts


def process_all(raw_posts):
    _effective_subs = {**INTELLIGENCE_SUBREDDITS, **_load_registry_subs()}
    processed = []
    for post in raw_posts:
        sub = post["subreddit"]
        priority = _effective_subs.get(sub, {}).get("priority", 3)
        threshold = SCORE_THRESHOLDS.get(priority, 20)
        if post["score"] < threshold:
            continue

        text = post["title"] + " " + post["selftext"]
        categories = classify_post(post)
        verticals = extract_mentioned_verticals(text)
        dollar_amounts = extract_dollar_amounts(text)

        processed.append({
            **post,
            "categories": categories,
            "mentioned_verticals": verticals,
            "dollar_amounts": dollar_amounts,
            "priority": priority,
            "is_actionable": "noise" not in categories,
        })

    processed.sort(key=lambda x: (not x["is_actionable"], -x["score"]))
    return processed


if __name__ == "__main__":
    print("=" * 56)
    print("  Reddit Intelligence Scanner")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 56)

    raw = asyncio.run(scrape_all())
    print(f"Raw posts fetched: {len(raw)}")

    processed = process_all(raw)
    actionable = [p for p in processed if p["is_actionable"]]

    cat_counts = Counter()
    for p in processed:
        for c in p["categories"]:
            cat_counts[c] += 1

    print(f"\nTotal processed: {len(processed)} ({len(actionable)} actionable)")
    print("Category breakdown:")
    for cat, count in cat_counts.most_common():
        print(f"  {cat}: {count}")

    if actionable:
        print(f"\nTop actionable posts:")
        for p in actionable[:10]:
            cats = ", ".join(p["categories"])
            verts = ", ".join(p["mentioned_verticals"]) if p["mentioned_verticals"] else "-"
            print(f"  [{cats}] [{p['subreddit']}] (^{p['score']}) {p['title'][:80]}")
            if p["dollar_amounts"]:
                for d in p["dollar_amounts"]:
                    print(f"    $ {d['value']:.2f} {d['metric']}")
            if p["mentioned_verticals"]:
                print(f"    Verticals: {verts}")

    OUTPUT.write_text(json.dumps(processed, indent=2))
    with HISTORY.open("a") as f:
        for rec in processed:
            f.write(json.dumps(rec) + "\n")

    print(f"\nSaved {len(processed)} posts -> {OUTPUT.name}")
    print(f"Appended to history -> {HISTORY.name}")
