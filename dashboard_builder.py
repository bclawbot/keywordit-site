import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path("/Users/newmac/.openclaw/workspace")))
from country_config import get_country_tier

BASE    = Path("/Users/newmac/.openclaw/workspace")
INPUT   = BASE / "validated_opportunities.json"
HISTORY = BASE / "validation_history.jsonl"
OUTPUT  = BASE / "dashboard.html"

# ── Load history (accumulate all keywords across runs) ─────────────────────────
# validation_history.jsonl is the append-only permanent record.
# We deduplicate by keyword+country, keeping the latest entry for each combo.
history_map: dict = {}
if HISTORY.exists():
    for line in HISTORY.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            key = f"{(rec.get('keyword') or '').lower().strip()}|{rec.get('country', '')}"
            existing = history_map.get(key)
            if existing is None or (rec.get("validated_at", "") > existing.get("validated_at", "")):
                history_map[key] = rec
        except Exception:
            continue

# ── Merge current run on top (current run takes precedence) ────────────────────
current: list = []
if INPUT.exists():
    try:
        current = json.loads(INPUT.read_text(encoding="utf-8")) or []
    except Exception:
        current = []

for rec in current:
    key = f"{(rec.get('keyword') or '').lower().strip()}|{rec.get('country', '')}"
    history_map[key] = rec

if not history_map:
    print(f"⚠️  No data in {HISTORY.name} or {INPUT.name} — run validation.py first")
    raise SystemExit(1)

opportunities = list(history_map.values())

for opp in opportunities:
    opp.setdefault('commercial_category', None)
    opp.setdefault('confidence', 'medium')
    opp.setdefault('opportunity_score', None)
    opp.setdefault('estimated_rpm', None)
    opp.setdefault('country_tier', None)
    opp.setdefault('validated_at', None)
    opp.setdefault('processed_at', None)
    opp.setdefault('subreddit', '')
    opp.setdefault('reddit_score', None)
    opp.setdefault('hook_theme', '')
    opp.setdefault('cpc_low_usd', None)
    opp.setdefault('cpc_high_usd', None)
    opp.setdefault('competition_index', None)
    opp.setdefault('monthly_searches', [])
    # New fields from Google Ads keyword expansion
    opp.setdefault('google_cpc_low', None)
    opp.setdefault('google_cpc_high', None)
    opp.setdefault('google_estimated_cpc', None)
    opp.setdefault('google_volume', None)
    opp.setdefault('google_competition', None)
    opp.setdefault('google_competition_index', None)
    opp.setdefault('monthly_search_history', [])
    opp.setdefault('is_branded', None)
    opp.setdefault('metrics_source', 'none_configured')
    opp.setdefault('expansion_seed', None)
    opp.setdefault('trend_source', '')
    opp.setdefault('source_trend', '')

    # Add run_date for date filtering in the dashboard
    ts = opp.get('validated_at') or opp.get('processed_at') or opp.get('vetted_at') or ''
    opp['run_date'] = ts[:10] if ts else ''
    # New DataForSEO Labs enrichment fields (added 2026-03-21)
    opp.setdefault('rsoc_score', None)
    opp.setdefault('kd', None)
    opp.setdefault('main_intent', None)
    opp.setdefault('serp_item_types', [])
    opp.setdefault('ssr', None)
    opp.setdefault('trend_monthly', None)
    opp.setdefault('trend_quarterly', None)
    opp.setdefault('kvsi', None)
    opp.setdefault('emerging_tag', None)
    opp.setdefault('scoring_profile', None)
    cpc = opp.get('cpc_usd') or 0
    vol = opp.get('search_volume') or 0
    lo  = opp.get('cpc_low_usd') or 0
    hi  = opp.get('cpc_high_usd') or 0
    opp['cpc_spread'] = round(hi - lo, 2) if hi > 0 else None
    if opp['opportunity_score'] is None and cpc > 0 and vol > 0:
        opp['opportunity_score'] = round(cpc * vol / 1000, 2)
    if opp['estimated_rpm'] is None and cpc > 0:
        opp['estimated_rpm'] = round(cpc * 0.03 * 1000, 2)
    if opp['country_tier'] is None:
        c = opp.get('country', '')
        opp['country_tier'] = get_country_tier(c) if c else 4

total               = len(opportunities)
golden_count        = sum(1 for o in opportunities if o.get('tag') == 'GOLDEN_OPPORTUNITY')
emerging_count      = sum(1 for o in opportunities if o.get('tag') in ('EMERGING', 'EMERGING_HIGH'))
scored_count        = sum(1 for o in opportunities if o.get('tag') not in ('UNSCORED', None))
unscored_count      = sum(1 for o in opportunities if o.get('tag') == 'UNSCORED')
countries_cnt  = len({o.get('country') for o in opportunities if o.get('country')})
last_run       = max((o.get('validated_at') or o.get('vetted_at') or '') for o in opportunities) if opportunities else ''

google_kp_count  = sum(1 for o in opportunities if 'google_keyword_planner' in (o.get('metrics_source') or ''))
dataforseo_count = sum(1 for o in opportunities if 'dataforseo' in (o.get('metrics_source') or ''))

# Distinct run dates for date filter reference
run_dates = sorted({o['run_date'] for o in opportunities if o.get('run_date')})

meta = {
    'generated_at':          datetime.now().isoformat(),
    'total_validated':        total,
    'total_scored':           scored_count,
    'total_golden':           golden_count,
    'total_emerging':         emerging_count,
    'total_unscored':         unscored_count,
    'countries_covered':      countries_cnt,
    'last_run_at':            last_run,
    'run_id':                 'run_' + datetime.now().strftime('%Y%m%d_%H%M'),
    'metrics_from_google_kp': google_kp_count,
    'metrics_from_dataforseo':dataforseo_count,
    'run_dates':              run_dates,
    'accumulated_runs':       len(run_dates),
}

data_json = json.dumps(opportunities, ensure_ascii=False)
meta_json = json.dumps(meta, ensure_ascii=False)

template_path = BASE / 'dashboard_template.html'
if not template_path.exists():
    print(f"⚠️  {template_path} not found")
    raise SystemExit(1)

TEMPLATE = template_path.read_text(encoding='utf-8')
html = TEMPLATE.replace('__DATA__', data_json).replace('__META__', meta_json)
OUTPUT.write_text(html, encoding='utf-8')
print(f"✅ Dashboard → {OUTPUT}  ({total} opps across {len(run_dates)} run dates, {len(html)//1024}KB)")
print(f"   Sources: Google KP={google_kp_count}  DataForSEO={dataforseo_count}  unscored={unscored_count}")
if run_dates:
    print(f"   Date range: {run_dates[0]} → {run_dates[-1]}")
