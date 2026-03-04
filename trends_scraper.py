import requests
import json
import time
from datetime import datetime
import xml.etree.ElementTree as ET
from pathlib import Path

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


def scrape_google_trends_rss(regions, run_id, delay=1.0):
    results = []
    failed = []
    for region_name, geo_code in regions.items():
        try:
            url = f'https://trends.google.com/trending/rss?geo={geo_code}'
            r = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            root = ET.fromstring(r.content)
            items = root.findall('./channel/item')
            for item in items[:5]:
                title = item.find('title')
                traffic = item.find('{https://trends.google.com/trending/rss}approx_traffic')
                rec = {
                    'run_id':      run_id,
                    'term':        title.text if title is not None else 'unknown',
                    'traffic':     traffic.text if traffic is not None else 'unknown',
                    'region':      region_name,
                    'geo':         geo_code,
                    'source':      'google_trends_rss',
                    'fetched_at':  datetime.now().isoformat()
                }
                results.append(rec)
        except Exception as e:
            failed.append(f"{region_name}: {e}")
        if delay:
            time.sleep(delay)
    return results, failed


def scrape_bing_news_rss(regions, run_id, delay=1.5):
    results = []
    for region_name, geo_code in regions.items():
        try:
            url = f'https://www.bing.com/news/search?q=top+stories&cc={geo_code}&format=RSS'
            r = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            if b'<rss' not in r.content:
                continue  # Bing returned HTML (bot check / no RSS for this geo)
            root = ET.fromstring(r.content)
            items = root.findall('./channel/item')
            for item in items[:5]:
                title = item.find('title')
                if title is None or not title.text:
                    continue
                rec = {
                    'run_id':     run_id,
                    'term':       title.text.strip(),
                    'traffic':    '20K+',
                    'region':     region_name,
                    'geo':        geo_code,
                    'source':     'bing_news_rss',
                    'fetched_at': datetime.now().isoformat()
                }
                results.append(rec)
        except Exception as e:
            print(f"  Bing News RSS failed [{region_name}]: {e}")
        if delay:
            time.sleep(delay)
    return results


def scrape_reddit_hot(regions, run_id, delay=2.0):
    results = []
    headers = {'User-Agent': 'OpenClaw/1.0 (trend research bot)'}
    for region_name, geo_code in regions.items():
        subreddits = REDDIT_COUNTRY_MAP.get(region_name)
        if not subreddits:
            continue
        for subreddit in subreddits:
            try:
                url = f'https://www.reddit.com/{subreddit}/hot.json?limit=10'
                r = requests.get(url, timeout=10, headers=headers)
                r.raise_for_status()
                posts = r.json().get('data', {}).get('children', [])
                for post in posts:
                    data = post.get('data', {})
                    if data.get('stickied'):
                        continue
                    title = data.get('title', '').strip()
                    if not title:
                        continue
                    rec = {
                        'run_id':     run_id,
                        'term':       title,
                        'traffic':    '20K+',
                        'region':     region_name,
                        'geo':        geo_code,
                        'source':     'reddit_hot',
                        'fetched_at': datetime.now().isoformat()
                    }
                    results.append(rec)
            except Exception as e:
                print(f"  Reddit hot failed [{subreddit}]: {e}")
            if delay:
                time.sleep(delay)
    return results


def scrape_google_news_rss(regions, run_id, delay=1.0):
    results = []
    for region_name, geo_code in regions.items():
        mapping = GOOGLE_NEWS_COUNTRY_MAP.get(region_name)
        if not mapping:
            continue
        country_name, lang, geo = mapping
        try:
            url = (f'https://news.google.com/rss/headlines/section/geo/{country_name}'
                   f'?hl={lang}&gl={geo}&ceid={geo}:{lang}')
            r = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            root = ET.fromstring(r.content)
            items = root.findall('./channel/item')
            for item in items[:5]:
                title_el = item.find('title')
                if title_el is None or not title_el.text:
                    continue
                # Strip " - Publisher Name" suffix
                title = title_el.text.rsplit(' - ', 1)[0].strip()
                rec = {
                    'run_id':     run_id,
                    'term':       title,
                    'traffic':    '20K+',
                    'region':     region_name,
                    'geo':        geo_code,
                    'source':     'google_news_rss',
                    'fetched_at': datetime.now().isoformat()
                }
                results.append(rec)
        except Exception as e:
            print(f"  Google News RSS failed [{region_name}]: {e}")
        if delay:
            time.sleep(delay)
    return results


def _parse_traffic(traffic_str):
    """Convert traffic string (e.g. '1M+', '500K+', '20K+') to int for comparison."""
    if isinstance(traffic_str, (int, float)):
        return int(traffic_str)
    s = str(traffic_str).upper().replace('+', '').replace(',', '').strip()
    if s.endswith('M'):
        return int(float(s[:-1]) * 1_000_000)
    if s.endswith('K'):
        return int(float(s[:-1]) * 1_000)
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def deduplicate_trends(records):
    """
    Dedup by (term.lower().strip(), geo).
    Keeps the record with the highest traffic value.
    Adds a 'sources' list of all source names seen for that key.
    """
    seen = {}   # key -> record index in deduped list
    deduped = []
    for rec in records:
        key = (rec.get('term', '').lower().strip(), rec.get('geo', ''))
        if key not in seen:
            entry = dict(rec)
            entry['sources'] = [rec.get('source', '')]
            seen[key] = len(deduped)
            deduped.append(entry)
        else:
            idx = seen[key]
            existing = deduped[idx]
            # Track all sources
            src = rec.get('source', '')
            if src and src not in existing['sources']:
                existing['sources'].append(src)
            # Keep highest traffic
            if _parse_traffic(rec.get('traffic', 0)) > _parse_traffic(existing.get('traffic', 0)):
                existing['traffic'] = rec['traffic']
    return deduped


# ── Main ──────────────────────────────────────────────────────────────────────

run_id  = datetime.now().isoformat()
all_raw = []

gt_results, gt_failed = scrape_google_trends_rss(REGIONS, run_id)
all_raw.extend(gt_results)

bing_results = scrape_bing_news_rss(REGIONS, run_id)
all_raw.extend(bing_results)

reddit_results = scrape_reddit_hot(REGIONS, run_id)
all_raw.extend(reddit_results)

gnews_results = scrape_google_news_rss(REGIONS, run_id)
all_raw.extend(gnews_results)

# Append ALL raw records to history (full provenance preserved)
with ALL_LOG.open("a") as f:
    for rec in all_raw:
        f.write(json.dumps(rec) + "\n")

# Deduplicated snapshot for postprocess
deduped = deduplicate_trends(all_raw)
SNAP.write_text(json.dumps(deduped, indent=2))

print(f"Sources: GT={len(gt_results)} Bing={len(bing_results)} Reddit={len(reddit_results)} GNews={len(gnews_results)}")
print(f"Raw: {len(all_raw)} | Deduped: {len(deduped)} | Failures: {len(gt_failed)}")
print("Snapshot:", SNAP)
print("Historical log (append-only):", ALL_LOG)
