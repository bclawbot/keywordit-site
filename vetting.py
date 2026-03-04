import json
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS

BASE    = Path("/Users/newmac/.openclaw/workspace")
INPUT   = BASE / "explosive_trends.json"
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


def search_ddg(keyword):
    """Search via DuckDuckGo with hard 15s wall-clock timeout."""
    def _fetch():
        return list(DDGS(timeout=10).text(keyword, max_results=10))
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_fetch)
            results = future.result(timeout=15)
        return [{"url": r["href"], "title": r["title"], "snippet": r["body"]} for r in results]
    except (FuturesTimeout, Exception) as e:
        print(f"  ⚠️  DDG search failed for '{keyword}': {type(e).__name__}")
        return []


def search_brave(keyword, retries=2):
    """Brave HTML scrape — used as fallback if DDG returns nothing."""
    url = f"https://search.brave.com/search?q={requests.utils.quote(keyword)}&source=web"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=SEARCH_HEADERS, timeout=12)
            if r.status_code == 429:
                wait = 10 * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⚠️  Brave 429 — backing off {wait:.0f}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            results = []
            for item in soup.select('#results .snippet[data-type="web"]')[:10]:
                a = item.select_one("a.l1[href]")
                if not a or not a.get("href", "").startswith("http"):
                    continue
                href    = a["href"]
                title   = item.select_one(".title")
                desc    = item.select_one(".snippet-description, .generic-snippet .content")
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


def search(keyword):
    """DDG first, Brave fallback if DDG returns no results."""
    results = search_ddg(keyword)
    if not results:
        results = search_brave(keyword)
    return results


def vet_keyword(entry):
    keyword = entry.get("term", "")
    country = entry.get("geo", "US")
    results = search(keyword)

    survivors = []
    for r in results:
        url     = r.get("url", "")
        title   = r.get("title", "")
        snippet = r.get("snippet", "")

        if not is_long_form(url, snippet):
            continue

        # Skip social media, video, and aggregator noise
        noise_domains = ("youtube.com", "twitter.com", "x.com", "instagram.com",
                         "tiktok.com", "facebook.com", "reddit.com", "wikipedia.org")
        if any(d in url.lower() for d in noise_domains):
            continue

        survivors.append({
            "keyword":         keyword,
            "country":         country,
            "vertical":        classify_vertical(keyword, url),
            "hook_theme":      extract_hook_theme(title),
            "lander_url":      url,
            "lander_title":    title,
            "ad_age_days":     90,   # conservative: established if ranking organically
            "explosive_score": entry.get("explosive_score", 0),
            "data_source":     "ddg_serp",
            "vetted_at":       datetime.now().isoformat(),
        })

    # Polite delay between keywords (DDG handles its own backoff internally)
    time.sleep(random.uniform(0.5, 1.5))
    return survivors


if __name__ == "__main__":
    if not INPUT.exists():
        print(f"⚠️  {INPUT} not found — run trends_postprocess.py first")
        raise SystemExit(1)

    trends = json.loads(INPUT.read_text())
    all_vetted = []

    for entry in trends:
        results = vet_keyword(entry)
        all_vetted.extend(results)

    OUTPUT.write_text(json.dumps(all_vetted, indent=2))

    with HISTORY.open("a") as f:
        for rec in all_vetted:
            f.write(json.dumps(rec) + "\n")

    print(f"✅ Vetting complete: {len(all_vetted)} opportunities from {len(trends)} trends → {OUTPUT.name}")
