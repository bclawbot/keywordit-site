import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path("/Users/newmac/.openclaw/workspace")))
from country_config import get_country_tier

BASE   = Path("/Users/newmac/.openclaw/workspace")
INPUT  = BASE / "validated_opportunities.json"
OUTPUT = BASE / "dashboard.html"

if not INPUT.exists():
    print(f"⚠️  {INPUT} not found — run validation.py first")
    raise SystemExit(1)

opportunities = json.loads(INPUT.read_text())

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

total          = len(opportunities)
golden_count   = sum(1 for o in opportunities if o.get('tag') == 'GOLDEN_OPPORTUNITY')
scored_count   = sum(1 for o in opportunities if o.get('tag') not in ('UNSCORED', None))
unscored_count = sum(1 for o in opportunities if o.get('tag') == 'UNSCORED')
countries_cnt  = len({o.get('country') for o in opportunities if o.get('country')})
last_run       = max((o.get('validated_at') or o.get('vetted_at') or '') for o in opportunities) if opportunities else ''

google_kp_count = sum(1 for o in opportunities if 'google_keyword_planner' in (o.get('metrics_source') or ''))
dataforseo_count = sum(1 for o in opportunities if 'dataforseo' in (o.get('metrics_source') or ''))

meta = {
    'generated_at': datetime.now().isoformat(),
    'total_validated': total,
    'total_scored': scored_count,
    'total_golden': golden_count,
    'total_unscored': unscored_count,
    'countries_covered': countries_cnt,
    'last_run_at': last_run,
    'run_id': 'run_' + datetime.now().strftime('%Y%m%d_%H%M'),
    'metrics_from_google_kp': google_kp_count,
    'metrics_from_dataforseo': dataforseo_count,
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
print(f"✅ Dashboard → {OUTPUT}  ({total} opps, {len(html)//1024}KB)")
