# =============================================================================
# validation.py  —  Stage 3: Keyword metrics + arbitrage scoring
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
import sys
import base64
from pathlib import Path
from datetime import datetime

import requests

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
GADS_CLIENT_ID     = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
GADS_CLIENT_SECRET = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
GADS_REFRESH_TOKEN = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
GADS_DEV_TOKEN     = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GADS_CUSTOMER_ID   = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "")
GADS_API_VERSION   = "v18"

SEMRUSH_KEY        = os.environ.get("SEMRUSH_API_KEY", "")
DFS_LOGIN          = os.environ.get("DATAFORSEO_LOGIN", "")
DFS_PASSWORD       = os.environ.get("DATAFORSEO_PASSWORD", "")

GADS_READY    = all([GADS_CLIENT_ID, GADS_CLIENT_SECRET, GADS_REFRESH_TOKEN,
                     GADS_DEV_TOKEN, GADS_CUSTOMER_ID])
SEMRUSH_READY = bool(SEMRUSH_KEY)
DFS_READY     = bool(DFS_LOGIN and DFS_PASSWORD)

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


# ── Google Ads ────────────────────────────────────────────────────────────────

def _gads_access_token():
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     GADS_CLIENT_ID,
        "client_secret": GADS_CLIENT_SECRET,
        "refresh_token": GADS_REFRESH_TOKEN,
        "grant_type":    "refresh_token",
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def _parse_gads_result(item):
    metrics    = item.get("keywordIdeaMetrics", {})
    cpc_micros = int(metrics.get("averageCpcMicros", 0) or 0)
    return {
        "search_volume": int(metrics.get("avgMonthlySearches", 0) or 0),
        "cpc_usd":       round(cpc_micros / 1_000_000, 2),
        "competition":   COMPETITION_MAP.get(metrics.get("competition", "MEDIUM"), 0.5),
    }


def fetch_google_ads(keyword, country="US"):
    access_token = _gads_access_token()
    url = (f"https://googleads.googleapis.com/{GADS_API_VERSION}/"
           f"customers/{GADS_CUSTOMER_ID}:generateKeywordIdeas")
    headers = {
        "Authorization":   f"Bearer {access_token}",
        "developer-token": GADS_DEV_TOKEN,
        "Content-Type":    "application/json",
    }
    criterion_id, _, _ = _geo_params(country)
    body = {
        "keywordSeed":        {"keywords": [keyword]},
        "geoTargetConstants": [f"geoTargetConstants/{criterion_id}"],
        "language":           "languageConstants/1000",
        "keywordPlanNetwork": "GOOGLE_SEARCH_AND_PARTNERS",
        "includeAdultKeywords": False,
    }
    r = requests.post(url, headers=headers, json=body, timeout=15)
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        raise ValueError("No results returned from Google Ads API")
    for item in results:
        if item.get("text", "").lower() == keyword.lower():
            return {"keyword": keyword, **_parse_gads_result(item), "source": "google_ads"}
    return {"keyword": keyword, **_parse_gads_result(results[0]), "source": "google_ads"}


# ── SEMrush ───────────────────────────────────────────────────────────────────

def fetch_semrush(keyword, country="US"):
    _, semrush_db, _ = _geo_params(country)
    params = {
        "type": "phrase_this", "key": SEMRUSH_KEY,
        "phrase": keyword, "export_columns": "Ph,Nq,Cp,Co", "database": semrush_db,
    }
    r = requests.get("https://api.semrush.com/", params=params, timeout=10)
    r.raise_for_status()
    lines = r.text.strip().splitlines()
    if len(lines) < 2:
        raise ValueError("SEMrush returned no data")
    row = dict(zip(lines[0].split("\t"), lines[1].split("\t")))
    return {
        "keyword":       row.get("Ph", keyword),
        "search_volume": int(row.get("Nq", 0) or 0),
        "cpc_usd":       float(row.get("Cp", 0) or 0),
        "competition":   float(row.get("Co", 0.5) or 0.5),
        "source":        "semrush",
    }


# ── DataForSEO ────────────────────────────────────────────────────────────────

def fetch_dataforseo(keyword, country="US"):
    _, _, dfs_location = _geo_params(country)
    creds = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    body = [{"keywords": [keyword], "language_name": "English",
             "location_code": dfs_location, "include_serp_info": False}]
    r = requests.post(
        "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live",
        headers=headers, json=body, timeout=15
    )
    r.raise_for_status()
    tasks = r.json().get("tasks", [])
    if not tasks or tasks[0].get("status_code") != 20000:
        raise ValueError(f"DataForSEO error: {tasks[0].get('status_message') if tasks else 'no tasks'}")
    results = tasks[0].get("result", [])
    if not results:
        raise ValueError("DataForSEO returned no keyword results")
    item = results[0]
    competition_index = item.get("competition_index", 50) or 50
    return {
        "keyword":       keyword,
        "search_volume": int(item.get("search_volume", 0) or 0),
        "cpc_usd":       round(float(item.get("cpc", 0) or 0), 2),
        "competition":   round(competition_index / 100, 2),
        "source":        "dataforseo",
    }


# ── Provider router ───────────────────────────────────────────────────────────

def get_keyword_metrics(keyword, country="US"):
    if GADS_READY:
        try:
            return fetch_google_ads(keyword, country)
        except Exception as e:
            print(f"  ⚠️  Google Ads error '{keyword}': {e}")

    if SEMRUSH_READY:
        try:
            return fetch_semrush(keyword, country)
        except Exception as e:
            print(f"  ⚠️  SEMrush error '{keyword}': {e}")

    if DFS_READY:
        try:
            return fetch_dataforseo(keyword, country)
        except Exception as e:
            print(f"  ⚠️  DataForSEO error '{keyword}': {e}")

    return None   # no provider available


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

if GADS_READY:
    print("ℹ️  Provider: Google Ads Keyword Planner")
elif SEMRUSH_READY:
    print("ℹ️  Provider: SEMrush")
elif DFS_READY:
    print("ℹ️  Provider: DataForSEO")
else:
    print("⚠️  No keyword API configured. Opportunities saved as UNSCORED.")
    print("    Set up one of:")
    print("      Google Ads → export GOOGLE_ADS_CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN / DEVELOPER_TOKEN / CUSTOMER_ID")
    print("      DataForSEO → export DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD")
    print("      SEMrush    → export SEMRUSH_API_KEY")

vetted    = json.loads(INPUT.read_text())
validated = []

for opp in vetted:
    keyword = opp.get("keyword", "")
    country = opp.get("country", "US")
    metrics = get_keyword_metrics(keyword, country)

    if metrics:
        ai_score = compute_ai(metrics["cpc_usd"], metrics["search_volume"], metrics["competition"])
        tag      = tag_opportunity(ai_score)
        # Trend persistence scoring
        persistence = {}
        weighted_score = ai_score
        if _FORECAST_AVAILABLE:
            try:
                persistence = predict_persistence(keyword, country)
                weighted_score = round(ai_score * persistence.get("persistence_probability", 0.5), 4)
            except Exception:
                pass
        validated.append({
            **opp,
            "search_volume":           metrics["search_volume"],
            "cpc_usd":                 metrics["cpc_usd"],
            "competition":             metrics["competition"],
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
            "competition":             None,
            "arbitrage_index":         None,
            "weighted_score":          None,
            "persistence_score":       None,
            "predicted_halflife_days": None,
            "tag":                     "UNSCORED",
            "metrics_source":          "none_configured",
            "validated_at":            datetime.now().isoformat(),
        })

OUTPUT.write_text(json.dumps(validated, indent=2))

golden_watch = [r for r in validated if r["tag"] in ("GOLDEN_OPPORTUNITY", "WATCH")]
GOLDEN.write_text(json.dumps(golden_watch, indent=2))

with HISTORY.open("a") as f:
    for rec in validated:
        f.write(json.dumps(rec) + "\n")

golden_count   = sum(1 for r in validated if r["tag"] == "GOLDEN_OPPORTUNITY")
watch_count    = sum(1 for r in validated if r["tag"] == "WATCH")
unscored_count = sum(1 for r in validated if r["tag"] == "UNSCORED")
print(
    f"✅ Validation complete: {len(validated)} records — "
    f"{golden_count} GOLDEN, {watch_count} WATCH, "
    f"{len(validated) - golden_count - watch_count - unscored_count} LOW, "
    f"{unscored_count} UNSCORED → {OUTPUT.name}"
)
