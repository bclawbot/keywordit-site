import asyncio
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import aiohttp

BASE    = Path("/Users/newmac/.openclaw/workspace")
ALL_LOG = BASE / "trends_all_history.jsonl"   # append-only log
SNAP    = BASE / "latest_trends.json"         # last run snapshot

REGIONS = {
    'argentina': 'AR', 'australia': 'AU', 'austria': 'AT', 'belgium': 'BE',
    'brazil': 'BR', 'canada': 'CA', 'chile': 'CL', 'colombia': 'CO',
    'czechia': 'CZ', 'denmark': 'DK', 'egypt': 'EG', 'finland': 'FI',
    'france': 'FR', 'germany': 'DE', 'greece': 'GR', 'hong_kong': 'HK',
    'hungary': 'HU', 'india': 'IN', 'indonesia': 'ID', 'ireland': 'IE',
    'israel': 'IL', 'italy': 'IT', 'japan': 'JP', 'kenya': 'KE',
    'malaysia': 'MY', 'mexico': 'MX', 'netherlands': 'NL', 'new_zealand': 'NZ',
    'nigeria': 'NG', 'norway': 'NO', 'peru': 'PE', 'philippines': 'PH',
    'poland': 'PL', 'portugal': 'PT', 'romania': 'RO', 'saudi_arabia': 'SA',
    'singapore': 'SG', 'south_africa': 'ZA', 'south_korea': 'KR',
    'spain': 'ES', 'sweden': 'SE', 'switzerland': 'CH', 'taiwan': 'TW',
    'thailand': 'TH', 'turkey': 'TR', 'ukraine': 'UA',
    'united_kingdom': 'GB', 'united_states': 'US', 'vietnam': 'VN'
}

REDDIT_COUNTRY_MAP = {
    "united_states":  ["r/news", "r/technology", "r/worldnews"],
    "united_kingdom": ["r/unitedkingdom", "r/uknews"],
    "australia":      ["r/australia"],
    "canada":         ["r/canada"],
    "india":          ["r/india"],
    "germany":        ["r/germany"],
    "france":         ["r/france"],
    "ireland":        ["r/ireland"],
    "new_zealand":    ["r/newzealand"],
    "south_africa":   ["r/southafrica"],
    "nigeria":        ["r/Nigeria"],
    "kenya":          ["r/Kenya"],
    "philippines":    ["r/Philippines"],
    "singapore":      ["r/singapore"],
    "brazil":         ["r/brasil"],
    "mexico":         ["r/mexico"],
    "argentina":      ["r/argentina"],
    "netherlands":    ["r/thenetherlands"],
    "sweden":         ["r/sweden"],
    "norway":         ["r/norway"],
    "denmark":        ["r/denmark"],
    "poland":         ["r/poland"],
    "south_korea":    ["r/korea"],
    "japan":          ["r/japan"],
    "indonesia":      ["r/indonesia"],
    "malaysia":       ["r/malaysia"],
}

GOOGLE_NEWS_COUNTRY_MAP = {
    "united_states":  ("United States",  "en", "US"),
    "united_kingdom": ("United Kingdom", "en", "GB"),
    "australia":      ("Australia",      "en", "AU"),
    "canada":         ("Canada",         "en", "CA"),
    "germany":        ("Germany",        "de", "DE"),
    "france":         ("France",         "fr", "FR"),
    "japan":          ("Japan",          "ja", "JP"),
    "india":          ("India",          "en", "IN"),
    "brazil":         ("Brazil",         "pt", "BR"),
    "south_africa":   ("South Africa",   "en", "ZA"),
    "nigeria":        ("Nigeria",        "en", "NG"),
    "kenya":          ("Kenya",          "en", "KE"),
    "philippines":    ("Philippines",    "en", "PH"),
    "indonesia":      ("Indonesia",      "id", "ID"),
    "vietnam":        ("Vietnam",        "vi", "VN"),
    "thailand":       ("Thailand",       "th", "TH"),
    "malaysia":       ("Malaysia",       "en", "MY"),
    "singapore":      ("Singapore",      "en", "SG"),
    "hong_kong":      ("Hong Kong",      "en", "HK"),
    "taiwan":         ("Taiwan",         "zh", "TW"),
    "south_korea":    ("South Korea",    "ko", "KR"),
    "argentina":      ("Argentina",      "es", "AR"),
    "mexico":         ("Mexico",         "es", "MX"),
    "colombia":       ("Colombia",       "es", "CO"),
    "chile":          ("Chile",          "es", "CL"),
    "peru":           ("Peru",           "es", "PE"),
    "poland":         ("Poland",         "pl", "PL"),
    "czechia":        ("Czechia",        "cs", "CZ"),
    "romania":        ("Romania",        "ro", "RO"),
    "hungary":        ("Hungary",        "hu", "HU"),
    "greece":         ("Greece",         "el", "GR"),
    "portugal":       ("Portugal",       "pt", "PT"),
    "norway":         ("Norway",         "no", "NO"),
    "sweden":         ("Sweden",         "sv", "SE"),
    "denmark":        ("Denmark",        "da", "DK"),
    "finland":        ("Finland",        "fi", "FI"),
    "austria":        ("Austria",        "de", "AT"),
    "belgium":        ("Belgium",        "fr", "BE"),
    "netherlands":    ("Netherlands",    "nl", "NL"),
    "switzerland":    ("Switzerland",    "de", "CH"),
    "ireland":        ("Ireland",        "en", "IE"),
    "israel":         ("Israel",         "he", "IL"),
    "egypt":          ("Egypt",          "ar", "EG"),
    "saudi_arabia":   ("Saudi Arabia",   "ar", "SA"),
    "turkey":         ("Turkey",         "tr", "TR"),
    "ukraine":        ("Ukraine",        "uk", "UA"),
    "new_zealand":    ("New Zealand",    "en", "NZ"),
}

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# ── Async scraper functions ───────────────────────────────────────────────────

async def _scrape_google_trends_country(session: aiohttp.ClientSession,
                                         region_name: str, geo_code: str,
                                         run_id: str, semaphore: asyncio.Semaphore) -> list:
    async with semaphore:
        try:
            url = f"https://trends.google.com/trending/rss?geo={geo_code}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                content = await r.read()
            root = ET.fromstring(content)
            results = []
            for item in root.findall("./channel/item")[:5]:
                title = item.find("title")
                traffic = item.find("{https://trends.google.com/trending/rss}approx_traffic")
                results.append({
                    "run_id":     run_id,
                    "term":       title.text if title is not None else "unknown",
                    "traffic":    traffic.text if traffic is not None else "unknown",
                    "region":     region_name,
                    "geo":        geo_code,
                    "source":     "google_trends_rss",
                    "fetched_at": datetime.now().isoformat(),
                })
            return results
        except Exception as e:
            return []


async def _scrape_bing_country(session: aiohttp.ClientSession,
                                region_name: str, geo_code: str,
                                run_id: str, semaphore: asyncio.Semaphore) -> list:
    async with semaphore:
        try:
            url = f"https://www.bing.com/news/search?q=top+stories&cc={geo_code}&format=RSS"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                content = await r.read()
            if b"<rss" not in content:
                return []
            root = ET.fromstring(content)
            results = []
            for item in root.findall("./channel/item")[:5]:
                title = item.find("title")
                if title is None or not title.text:
                    continue
                results.append({
                    "run_id":     run_id,
                    "term":       title.text.strip(),
                    "traffic":    "20K+",
                    "region":     region_name,
                    "geo":        geo_code,
                    "source":     "bing_news_rss",
                    "fetched_at": datetime.now().isoformat(),
                })
            return results
        except Exception:
            return []


async def _scrape_reddit_subreddit(session: aiohttp.ClientSession,
                                    region_name: str, geo_code: str,
                                    subreddit: str, run_id: str,
                                    semaphore: asyncio.Semaphore) -> list:
    async with semaphore:
        try:
            url = f"https://www.reddit.com/{subreddit}/hot.json?limit=10"
            headers = {"User-Agent": "OpenClaw/1.0 (trend research bot)"}
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            posts = data.get("data", {}).get("children", [])
            results = []
            for post in posts:
                d = post.get("data", {})
                if d.get("stickied"):
                    continue
                title = d.get("title", "").strip()
                if title:
                    results.append({
                        "run_id":     run_id,
                        "term":       title,
                        "traffic":    "20K+",
                        "region":     region_name,
                        "geo":        geo_code,
                        "source":     "reddit_hot",
                        "fetched_at": datetime.now().isoformat(),
                    })
            return results
        except Exception:
            return []


async def _scrape_gnews_country(session: aiohttp.ClientSession,
                                 region_name: str, geo_code: str,
                                 run_id: str, semaphore: asyncio.Semaphore) -> list:
    mapping = GOOGLE_NEWS_COUNTRY_MAP.get(region_name)
    if not mapping:
        return []
    async with semaphore:
        try:
            country_name, lang, geo = mapping
            url = (f"https://news.google.com/rss/headlines/section/geo/{country_name}"
                   f"?hl={lang}&gl={geo}&ceid={geo}:{lang}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                content = await r.read()
            root = ET.fromstring(content)
            results = []
            for item in root.findall("./channel/item")[:5]:
                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                title = title_el.text.rsplit(" - ", 1)[0].strip()
                results.append({
                    "run_id":     run_id,
                    "term":       title,
                    "traffic":    "20K+",
                    "region":     region_name,
                    "geo":        geo_code,
                    "source":     "google_news_rss",
                    "fetched_at": datetime.now().isoformat(),
                })
            return results
        except Exception:
            return []


# ── Gather all sources concurrently ──────────────────────────────────────────

async def scrape_all(run_id: str) -> list:
    semaphore = asyncio.Semaphore(10)  # max 10 concurrent requests
    connector = aiohttp.TCPConnector(limit=20)
    headers = {"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9"}

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        tasks = []

        for region_name, geo_code in REGIONS.items():
            tasks.append(_scrape_google_trends_country(session, region_name, geo_code, run_id, semaphore))
            tasks.append(_scrape_bing_country(session, region_name, geo_code, run_id, semaphore))
            tasks.append(_scrape_gnews_country(session, region_name, geo_code, run_id, semaphore))

            for subreddit in REDDIT_COUNTRY_MAP.get(region_name, []):
                tasks.append(_scrape_reddit_subreddit(session, region_name, geo_code,
                                                      subreddit, run_id, semaphore))

        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_raw = []
    for r in results:
        if isinstance(r, list):
            all_raw.extend(r)
    return all_raw


# ── Deduplication (unchanged logic) ──────────────────────────────────────────

def _parse_traffic(traffic_str):
    if isinstance(traffic_str, (int, float)):
        return int(traffic_str)
    s = str(traffic_str).upper().replace("+", "").replace(",", "").strip()
    if s.endswith("M"):
        return int(float(s[:-1]) * 1_000_000)
    if s.endswith("K"):
        return int(float(s[:-1]) * 1_000)
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def deduplicate_trends(records):
    seen = {}
    deduped = []
    for rec in records:
        key = (rec.get("term", "").lower().strip(), rec.get("geo", ""))
        if key not in seen:
            entry = dict(rec)
            entry["sources"] = [rec.get("source", "")]
            seen[key] = len(deduped)
            deduped.append(entry)
        else:
            idx = seen[key]
            existing = deduped[idx]
            src = rec.get("source", "")
            if src and src not in existing["sources"]:
                existing["sources"].append(src)
            if _parse_traffic(rec.get("traffic", 0)) > _parse_traffic(existing.get("traffic", 0)):
                existing["traffic"] = rec["traffic"]
    return deduped


# ── Main ──────────────────────────────────────────────────────────────────────

run_id = datetime.now().isoformat()
all_raw = asyncio.run(scrape_all(run_id))

# Append ALL raw records to history (full provenance preserved)
with ALL_LOG.open("a") as f:
    for rec in all_raw:
        f.write(json.dumps(rec) + "\n")

# Deduplicated snapshot for postprocess
deduped = deduplicate_trends(all_raw)
SNAP.write_text(json.dumps(deduped, indent=2))

gt_count     = sum(1 for r in all_raw if r.get("source") == "google_trends_rss")
bing_count   = sum(1 for r in all_raw if r.get("source") == "bing_news_rss")
reddit_count = sum(1 for r in all_raw if r.get("source") == "reddit_hot")
gnews_count  = sum(1 for r in all_raw if r.get("source") == "google_news_rss")

print(f"Sources: GT={gt_count} Bing={bing_count} Reddit={reddit_count} GNews={gnews_count}")
print(f"Raw: {len(all_raw)} | Deduped: {len(deduped)}")
print("Snapshot:", SNAP)
print("Historical log (append-only):", ALL_LOG)
