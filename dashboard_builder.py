import json
import re
from pathlib import Path
from datetime import datetime

from country_config import get_country_tier

# SECURITY: Never embed API keys in generated HTML (client-side JS).
# The dashboard's cloud-generation feature is intentionally disabled.

BASE    = Path(__file__).resolve().parent
INPUT   = BASE / "validated_opportunities.json"
HISTORY = BASE / "validation_history.jsonl"
OUTPUT  = BASE / "dashboard.html"
OUTPUT_DATA_DIR = BASE / "data"
OPPS_JSON = OUTPUT_DATA_DIR / "opportunities.json"
ANGLES_JSON_FILE = OUTPUT_DATA_DIR / "angles.json"

DASHBOARD_FIELDS = {
    'keyword', 'tag', 'country', 'vertical', 'cpc_usd', 'search_volume',
    'rpc_display', 'opportunity_score', 'metrics_source', 'source_trend',
    'country_tier', 'arbitrage_index', 'estimated_rpm', 'confidence',
    'run_date', 'expansion_seed', 'rsoc_score', 'kd', 'main_intent',
    'transformed_at', 'ssr', 'trend_monthly', 'kvsi', 'emerging_tag',
    'scoring_profile', 'cpc_high_usd', 'search_intent', 'original_keyword',
    'transformation_relationship', 'transformation_confidence', 'has_paid_ads',
    'longevity_appearances', 'seed_keyword', 'rpc_confidence', 'rpc_source',
    'competition', 'trend_source', 'commercial_category', 'hook_theme',
    'lander_url', 'lander_title', 'efficiency_factor', 'longevity_first_seen',
    'longevity_bonus', 'persistence_score', 'validated_at', 'processed_at',
    'cpc_low_usd', 'cpc_spread', 'monthly_searches', 'is_branded',
}

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
    # Transformation metadata (from commercial_keyword_transformer.py)
    opp.setdefault('original_keyword', None)
    opp.setdefault('transformation_relationship', None)
    opp.setdefault('transformation_confidence', None)
    opp.setdefault('transformed_at', None)

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
    # RPC estimator fields (populated by validation.py enrichment pass)
    opp.setdefault('rpc_actual',            None)
    opp.setdefault('rpc_actual_clicks',     0)
    opp.setdefault('rpc_actual_confidence', None)
    opp.setdefault('rpc_expected',          None)
    opp.setdefault('rpc_expected_p25',      None)
    opp.setdefault('rpc_expected_p75',      None)
    opp.setdefault('rpc_source',            None)
    opp.setdefault('rpc_confidence',        None)
    opp.setdefault('rpc_vertical',          None)
    opp.setdefault('rpc_intent_applied',    None)
    opp.setdefault('rpc_intent_modifier',   None)
    opp.setdefault('rpc_display',           None)
    opp.setdefault('rpc_display_mode',      None)
    opp.setdefault('rpc_ratio',             None)
    opp.setdefault('rpc_outlier_flag',      False)
    cpc = opp.get('cpc_usd') or 0
    vol = opp.get('search_volume') or 0
    lo  = opp.get('cpc_low_usd') or 0
    hi  = opp.get('cpc_high_usd') or 0
    opp['cpc_spread'] = round(hi - lo, 2) if hi > 0 else None
    if not opp['opportunity_score'] and cpc > 0 and vol > 0:
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

data_json = json.dumps(opportunities, ensure_ascii=False).replace('</','<\\/')
meta_json = json.dumps(meta, ensure_ascii=False).replace('</','<\\/')

# ── Load experimental data files ─────────────────────────────────────────────
def _load_jsonl(p):
    items = []
    if p.exists():
        for line in p.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if line:
                try: items.append(json.loads(line))
                except Exception: pass
    return items

def _load_json(p, default=None):
    if p.exists():
        try: return json.loads(p.read_text(encoding='utf-8'))
        except Exception: pass
    return default if default is not None else {}

exp_results    = _load_jsonl(BASE / 'data' / 'expansion_results.jsonl')
entity_reg     = _load_json(BASE / 'data' / 'entity_registry.json', {})
discovered_ent = _load_jsonl(BASE / 'data' / 'discovered_entities.jsonl')
perf_cache     = _load_json(BASE / 'data' / 'performance_cache.json', None)
exp_log        = _load_jsonl(BASE / 'data' / 'expansion_log.jsonl')

# ── Load angle candidates for inline Discovery tab expansion ─────────────────
angle_candidates = _load_json(BASE / 'angle_candidates.json', [])
angles_by_key: dict = {}
for _cluster in (angle_candidates if isinstance(angle_candidates, list) else []):
    _kw = str(_cluster.get('keyword', '')).lower().strip()
    _co = str(_cluster.get('country', '')).upper()
    if _kw:
        angles_by_key[f'{_kw}|{_co}'] = _cluster
angles_json = json.dumps(angles_by_key, ensure_ascii=False).replace('</','<\\/')

# ── Write external data files for async loading ──
OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)

def _prune(rec):
    return {k: v for k, v in rec.items() if k in DASHBOARD_FIELDS and v is not None}

opps_content = json.dumps(opportunities, ensure_ascii=False, separators=(',', ':'))
OPPS_JSON.write_text(opps_content, encoding='utf-8')

angles_content = json.dumps(angles_by_key, ensure_ascii=False, separators=(',', ':'))
ANGLES_JSON_FILE.write_text(angles_content, encoding='utf-8')

print(f"   External: {OPPS_JSON} ({len(opps_content)//1024}KB), {ANGLES_JSON_FILE} ({len(angles_content)//1024}KB)")

def esc(s):
    if s is None: return ''
    return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;').replace("'",'&#39;')

# ── Tab Navigation HTML ──────────────────────────────────────────────────────
tab_nav_html = '''<nav class="tab-nav">
  <button class="tab-btn active" data-tab="discovery">Discovery</button>
  <button class="tab-btn" data-tab="experimental">Experimental</button>
  <button class="tab-btn" data-tab="performance">Performance</button>
  <button class="tab-btn" data-tab="content">Content Engine</button>
  <button class="tab-btn" data-tab="intel">Intel</button>
  <button class="tab-btn" data-tab="pipeline">Pipeline</button>
</nav>
<script>
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
  });
});
</script>'''

# ── Intel Tab HTML (fb_intelligence competitor data) ─────────────────────────
def build_intel_tab():
    """
    Intel dashboard tab — shows competitor ad keywords with CPC and angles.
    Reads from fb_intelligence SQLite database.
    """
    import sqlite3, json
    db_path = BASE / "dwight" / "fb_intelligence" / "data" / "fb_intelligence.db"
    if not db_path.exists():
        return '<div id="tab-intel" class="tab-content"><div class="exp-empty">No intel data. Run: python3 -m dwight.fb_intelligence.scheduler full</div></div>'

    try:
        conn = sqlite3.connect(str(db_path), timeout=5)
        conn.row_factory = sqlite3.Row
    except Exception:
        return '<div id="tab-intel" class="tab-content"><div class="exp-empty">Cannot open intel database.</div></div>'

    # Summary stats
    total_ads = conn.execute("SELECT COUNT(*) FROM Ads").fetchone()[0]
    classified = conn.execute("SELECT COUNT(*) FROM Ads WHERE classification_conf IS NOT NULL").fetchone()[0]
    total_kw = conn.execute("SELECT COUNT(*) FROM Keywords").fetchone()[0]
    kw_with_cpc = conn.execute("SELECT COUNT(*) FROM Keywords WHERE cpc_usd > 0").fetchone()[0]
    total_angles = conn.execute("SELECT COUNT(*) FROM KeywordAngles").fetchone()[0]
    total_pages = conn.execute("SELECT COUNT(*) FROM FacebookPages").fetchone()[0]
    networks = conn.execute("SELECT COUNT(*) FROM Networks").fetchone()[0]

    lines = ['<div id="tab-intel" class="tab-content">']

    # Summary cards
    lines.append('<div class="intel-summary" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:24px;">')
    for label, value in [
        ("Networks", networks), ("FB Pages", total_pages), ("Ads Scraped", total_ads),
        ("Classified", classified), ("Keywords", total_kw), ("With CPC", kw_with_cpc),
        ("Angles", total_angles),
    ]:
        lines.append(f'<div style="background:#1e1e2e;border-radius:8px;padding:14px;text-align:center;">'
                     f'<div style="font-size:24px;font-weight:700;color:#cba6f7;">{value}</div>'
                     f'<div style="font-size:12px;color:#a6adc8;margin-top:4px;">{esc(label)}</div></div>')
    lines.append('</div>')

    # Keywords + angles table
    kw_rows = conn.execute("""
        SELECT k.id, k.keyword, k.cpc_usd, k.volume, k.competition
        FROM Keywords k
        JOIN KeywordQueue kq ON k.keyword = kq.keyword
        WHERE kq.source = 'url_adtitle'
        GROUP BY k.id
        ORDER BY COALESCE(k.cpc_usd, 0) DESC, k.keyword
    """).fetchall()

    if not kw_rows:
        # Fallback: show all keywords
        kw_rows = conn.execute("""
            SELECT id, keyword, cpc_usd, volume, competition
            FROM Keywords ORDER BY COALESCE(cpc_usd, 0) DESC LIMIT 100
        """).fetchall()

    lines.append('<h3 style="color:#cdd6f4;margin-bottom:12px;">Competitor Keywords & Angles</h3>')
    lines.append('<div class="intel-keywords">')

    for kw_row in kw_rows:
        kw_id = kw_row[0]
        keyword = kw_row[1]
        cpc = float(kw_row[2] or 0)
        vol = int(kw_row[3] or 0)
        comp = float(kw_row[4] or 0)

        # Get angles for this keyword
        angles = conn.execute("""
            SELECT angle_type, angle_title, source, confidence
            FROM KeywordAngles WHERE keyword_id = ?
            ORDER BY source DESC, confidence DESC
        """, (kw_id,)).fetchall()

        # Get associated network/domain
        ad_info = conn.execute("""
            SELECT DISTINCT n.name, d.domain
            FROM Ads a
            JOIN FacebookPages fp ON a.page_id = fp.id
            JOIN Domains d ON fp.domain_id = d.id
            JOIN Networks n ON d.network_id = n.id
            WHERE a.extracted_keywords LIKE ?
            LIMIT 1
        """, (f'%{keyword[:20]}%',)).fetchone()
        network = ad_info[0] if ad_info else ""
        domain = ad_info[1] if ad_info else ""

        cpc_display = f"${cpc:.2f}" if cpc > 0 else "<span style='color:#585b70;'>pending</span>"
        vol_display = f"{vol:,}" if vol > 0 else "—"

        lines.append(f'<details class="intel-kw-card" style="background:#1e1e2e;border-radius:8px;margin-bottom:8px;border:1px solid #313244;">')
        lines.append(f'<summary style="padding:12px 16px;cursor:pointer;display:flex;align-items:center;gap:12px;">')
        lines.append(f'<span style="flex:1;font-weight:600;color:#cdd6f4;">{esc(keyword[:60])}</span>')
        if network:
            lines.append(f'<span style="font-size: 12px;padding:2px 8px;border-radius:4px;background:#45475a;color:#a6adc8;">{esc(network)}</span>')
        lines.append(f'<span style="color:#a6e3a1;font-weight:700;min-width:65px;text-align:right;">{cpc_display}</span>')
        lines.append(f'<span style="color:#a6adc8;font-size:12px;min-width:60px;text-align:right;">{vol_display} vol</span>')
        lines.append(f'<span style="font-size:12px;color:#585b70;">{len(angles)} angles</span>')
        lines.append('</summary>')

        if angles:
            lines.append('<div style="padding:8px 16px 16px;border-top:1px solid #313244;">')
            for angle in angles:
                a_type = angle[0]
                a_title = angle[1]
                a_source = angle[2]
                a_conf = float(angle[3] or 0)

                if a_source == "original":
                    badge_style = "background:#f38ba8;color:#1e1e2e;"
                    badge_text = "COMPETITOR"
                else:
                    badge_style = "background:#89b4fa;color:#1e1e2e;"
                    badge_text = "ALTERNATIVE"

                lines.append(f'<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #181825;">')
                lines.append(f'<span style="font-size:10px;padding:1px 6px;border-radius:3px;{badge_style}font-weight:600;">{badge_text}</span>')
                lines.append(f'<span style="font-size:12px;color:#89b4fa;min-width:100px;">{esc(a_type)}</span>')
                lines.append(f'<span style="font-size:13px;color:#cdd6f4;flex:1;">{esc(a_title[:80])}</span>')
                lines.append('</div>')
            lines.append('</div>')

        lines.append('</details>')

    lines.append('</div>')

    # Top verticals breakdown
    vert_rows = conn.execute("""
        SELECT primary_vertical, COUNT(*) as cnt
        FROM Ads WHERE primary_vertical IS NOT NULL
        GROUP BY primary_vertical ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    if vert_rows:
        lines.append('<h3 style="color:#cdd6f4;margin:24px 0 12px;">Ad Verticals</h3>')
        lines.append('<div style="display:flex;flex-wrap:wrap;gap:8px;">')
        for v in vert_rows:
            lines.append(f'<span style="background:#313244;color:#cdd6f4;padding:6px 12px;border-radius:6px;font-size:13px;">'
                         f'{esc(v[0])} <b style="color:#cba6f7;">{v[1]}</b></span>')
        lines.append('</div>')

    # Networks breakdown
    net_rows = conn.execute("""
        SELECT n.name, COUNT(DISTINCT a.id) as ad_count, COUNT(DISTINCT d.id) as domain_count
        FROM Networks n
        JOIN Domains d ON d.network_id = n.id
        JOIN FacebookPages fp ON fp.domain_id = d.id
        JOIN Ads a ON a.page_id = fp.id
        GROUP BY n.id ORDER BY ad_count DESC
    """).fetchall()
    if net_rows:
        lines.append('<h3 style="color:#cdd6f4;margin:24px 0 12px;">Networks</h3>')
        lines.append('<table style="width:100%;border-collapse:collapse;"><thead><tr>'
                     '<th style="text-align:left;padding:8px;color:#a6adc8;border-bottom:1px solid #313244;">Network</th>'
                     '<th style="text-align:right;padding:8px;color:#a6adc8;border-bottom:1px solid #313244;">Domains</th>'
                     '<th style="text-align:right;padding:8px;color:#a6adc8;border-bottom:1px solid #313244;">Ads</th>'
                     '</tr></thead><tbody>')
        for n in net_rows:
            lines.append(f'<tr><td style="padding:8px;color:#cdd6f4;">{esc(n[0])}</td>'
                         f'<td style="text-align:right;padding:8px;color:#a6adc8;">{n[2]}</td>'
                         f'<td style="text-align:right;padding:8px;color:#cba6f7;font-weight:600;">{n[1]}</td></tr>')
        lines.append('</tbody></table>')

    lines.append('</div>')  # close tab-intel
    conn.close()
    return '\n'.join(lines)


# ── Content Engine Tab HTML ──────────────────────────────────────────────────
def build_content_tab():
    """
    Content Engine dashboard tab.
    Reads angle_candidates.json and generated_articles.json produced by Stages 3a/3b.
    Displays:
      1. Angle performance summary table
      2. Generated articles list (3-stage queue)
      3. Per-keyword angle breakdown
    """
    import collections

    angles_path   = BASE / "angle_candidates.json"
    articles_path = BASE / "generated_articles.json"

    angle_clusters  = []
    article_records = []

    if angles_path.exists():
        try:
            angle_clusters = json.loads(angles_path.read_text(encoding="utf-8")) or []
        except Exception:
            pass

    if articles_path.exists():
        try:
            article_records = json.loads(articles_path.read_text(encoding="utf-8")) or []
        except Exception:
            pass

    lines = []
    lines.append('<div id="tab-content" class="tab-content">')
    lines.append('<div style="padding:16px">')

    if not angle_clusters and not article_records:
        lines.append('<p style="color:#888;font-style:italic">No content engine data yet. '
                     'Run the pipeline (Stage 3a → 3b) to generate angle candidates and articles.</p>')
        lines.append('</div></div>')
        return '\n'.join(lines)

    # ── Summary stats ─────────────────────────────────────────────────────────
    total_clusters  = len(angle_clusters)
    total_angles    = sum(len(c.get("selected_angles", [])) for c in angle_clusters)
    total_articles  = len(article_records)
    compliant_count = sum(1 for a in article_records if a.get("raf_compliant"))
    blocked_count   = sum(1 for a in article_records
                          if a.get("compliance_risk_level") in ("CRITICAL", "HIGH"))
    avg_quality     = (sum(float(a.get("quality_score", 0)) for a in article_records)
                       / max(total_articles, 1))

    lines.append('<div style="display:flex;gap:20px;flex-wrap:wrap;margin-bottom:20px">')
    for label, value, color in [
        ("Keywords Processed", total_clusters, "#4caf50"),
        ("Angles Selected",    total_angles,   "#2196f3"),
        ("Articles Generated", total_articles, "#ff9800"),
        ("RAF Compliant",      f"{compliant_count}/{total_articles}", "#4caf50"),
        ("Avg Quality Score",  f"{avg_quality:.2f}", "#9c27b0"),
        ("Blocked",            blocked_count,  "#f44336" if blocked_count else "#4caf50"),
    ]:
        lines.append(
            f'<div style="background:#1e1e2e;border:1px solid #333;border-radius:8px;'
            f'padding:12px 18px;min-width:130px">'
            f'<div style="font-size: 12px;color:#888;text-transform:uppercase">{label}</div>'
            f'<div style="font-size:24px;font-weight:700;color:{color}">{value}</div>'
            f'</div>'
        )
    lines.append('</div>')

    # ── Angle type performance table ──────────────────────────────────────────
    if article_records:
        lines.append('<h3 style="margin:16px 0 8px;color:#e0e0e0">Angle Type Performance</h3>')
        lines.append('<table style="width:100%;border-collapse:collapse;font-size:13px">')
        lines.append('<thead><tr style="background:#2a2a3e;color:#aaa">'
                     '<th style="padding:8px;text-align:left">Angle Type</th>'
                     '<th style="padding:8px;text-align:right">Articles</th>'
                     '<th style="padding:8px;text-align:right">Avg Quality</th>'
                     '<th style="padding:8px;text-align:right">RAF Pass Rate</th>'
                     '<th style="padding:8px;text-align:right">Avg CPC</th>'
                     '</tr></thead><tbody>')

        by_angle: dict = collections.defaultdict(list)
        for a in article_records:
            by_angle[a.get("angle_type", "unknown")].append(a)

        for angle_type, recs in sorted(by_angle.items()):
            count       = len(recs)
            avg_q       = sum(float(r.get("quality_score", 0)) for r in recs) / count
            pass_rate   = sum(1 for r in recs if r.get("raf_compliant")) / count
            avg_cpc     = sum(float(r.get("cpc_usd", 0)) for r in recs) / count
            pass_color  = "#4caf50" if pass_rate >= 0.85 else "#ff9800" if pass_rate >= 0.5 else "#f44336"
            lines.append(
                f'<tr style="border-bottom:1px solid #2a2a3e">'
                f'<td style="padding:7px 8px;color:#e0e0e0">{angle_type}</td>'
                f'<td style="padding:7px 8px;text-align:right;color:#ccc">{count}</td>'
                f'<td style="padding:7px 8px;text-align:right;color:#ccc">{avg_q:.2f}</td>'
                f'<td style="padding:7px 8px;text-align:right;color:{pass_color}">{pass_rate:.0%}</td>'
                f'<td style="padding:7px 8px;text-align:right;color:#ccc">${avg_cpc:.2f}</td>'
                f'</tr>'
            )
        lines.append('</tbody></table>')

    # ── Generated articles list ───────────────────────────────────────────────
    if article_records:
        lines.append('<h3 style="margin:24px 0 8px;color:#e0e0e0">Generated Articles</h3>')
        lines.append('<table style="width:100%;border-collapse:collapse;font-size:12px">')
        lines.append('<thead><tr style="background:#2a2a3e;color:#aaa">'
                     '<th style="padding:7px;text-align:left">Keyword</th>'
                     '<th style="padding:7px;text-align:left">Angle</th>'
                     '<th style="padding:7px;text-align:left">Lang</th>'
                     '<th style="padding:7px;text-align:right">Words</th>'
                     '<th style="padding:7px;text-align:right">Quality</th>'
                     '<th style="padding:7px;text-align:right">RAF Risk</th>'
                     '<th style="padding:7px;text-align:left">File</th>'
                     '</tr></thead><tbody>')

        for rec in sorted(article_records,
                          key=lambda r: r.get("generated_at", ""), reverse=True)[:50]:
            risk      = rec.get("compliance_risk_level", "?")
            risk_col  = {"LOW": "#4caf50", "MEDIUM": "#ff9800",
                         "HIGH": "#f44336", "CRITICAL": "#b00020"}.get(risk, "#888")
            q_score   = float(rec.get("quality_score", 0))
            q_col     = "#4caf50" if q_score >= 0.75 else "#ff9800" if q_score >= 0.5 else "#f44336"
            fname     = rec.get("file_path", "").split("/")[-1]
            lines.append(
                f'<tr style="border-bottom:1px solid #1e1e2e">'
                f'<td style="padding:6px 7px;color:#ccc;max-width:200px;overflow:hidden;'
                f'text-overflow:ellipsis;white-space:nowrap" title="{esc(rec.get("keyword",""))}">'
                f'{esc(rec.get("keyword","")[:40])}</td>'
                f'<td style="padding:6px 7px;color:#aaa">{esc(rec.get("angle_type",""))}</td>'
                f'<td style="padding:6px 7px;color:#aaa">{esc(rec.get("language_code","en").upper())}</td>'
                f'<td style="padding:6px 7px;text-align:right;color:#ccc">'
                f'{rec.get("word_count",0)}</td>'
                f'<td style="padding:6px 7px;text-align:right;color:{q_col}">{q_score:.2f}</td>'
                f'<td style="padding:6px 7px;text-align:right;color:{risk_col}">{esc(risk)}</td>'
                f'<td style="padding:6px 7px;color:#888;font-size: 12px;max-width:150px;'
                f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{esc(rec.get("file_path",""))}">'
                f'{esc(fname)}</td>'
                f'</tr>'
            )
        lines.append('</tbody></table>')

    # ── Angle candidates — per-angle rows with Generate buttons ───────────────
    import re as _re
    if angle_clusters:
        # Collect all per-angle rows (flatten clusters)
        generated_keys = {
            (a.get("keyword", "").lower(), a.get("angle_type", ""))
            for a in article_records
        }
        all_angles = []
        for cluster in angle_clusters:
            kw       = cluster.get("keyword", "")
            vertical = cluster.get("vertical", "unknown")
            cpc      = float(cluster.get("cpc_usd", 0.0))
            country  = cluster.get("country", "US")
            language = cluster.get("language_code", "en")
            signal_type = cluster.get("discovery_context", {}).get("signal_type", "keyword_expansion")
            signal_text = cluster.get("discovery_context", {}).get("signal_text", "")
            for angle in cluster.get("selected_angles", []):
                a_type  = angle.get("angle_type", "")
                a_title = angle.get("angle_title", kw)
                rsoc    = float(angle.get("rsoc_score", 0.0))
                if (kw.lower(), a_type) not in generated_keys:
                    all_angles.append({
                        "keyword": kw, "vertical": vertical, "cpc": cpc,
                        "country": country, "language": language,
                        "angle_type": a_type, "angle_title": a_title,
                        "rsoc_score": rsoc,
                        "signal_type": signal_type, "signal_text": signal_text,
                    })

        if all_angles:
            lines.append(
                f'<h3 style="margin:24px 0 8px;color:#e0e0e0">'
                f'Angles Ready to Generate ({len(all_angles)})</h3>'
            )
            lines.append('<table style="width:100%;border-collapse:collapse;font-size:12px">')
            lines.append(
                '<thead><tr style="background:#2a2a3e;color:#aaa">'
                '<th style="padding:7px;text-align:left">Keyword</th>'
                '<th style="padding:7px;text-align:left">Angle Type</th>'
                '<th style="padding:7px;text-align:left">Title</th>'
                '<th style="padding:7px;text-align:right">CPC</th>'
                '<th style="padding:7px;text-align:right">Score</th>'
                '<th style="padding:7px;text-align:left">Signal</th>'
                '<th style="padding:7px;text-align:center">Generate</th>'
                '</tr></thead><tbody>'
            )

            signal_colors = {
                "google_trends":        "#4caf50",
                "reddit_discussion":    "#ff9800",
                "news_event":           "#2196f3",
                "commercial_transform": "#9c27b0",
                "keyword_expansion":    "#607d8b",
            }

            for row in all_angles[:100]:
                # Build a safe HTML id from keyword + angle_type
                raw_id  = f"{row['keyword']}_{row['angle_type']}"
                aid     = _re.sub(r"[^a-z0-9_-]", "-", raw_id.lower())[:80]
                sig_col = signal_colors.get(row["signal_type"], "#888")

                # Escape values for data-* attributes
                def _esc(v): return str(v).replace('"', "&quot;").replace("'", "&#39;")

                btn_html = (
                    f'<button id="gen-btn-{aid}" class="gen-btn"'
                    f' data-angle-id="{aid}"'
                    f' data-keyword="{_esc(row["keyword"])}"'
                    f' data-vertical="{_esc(row["vertical"])}"'
                    f' data-language="{_esc(row["language"])}"'
                    f' data-country="{_esc(row["country"])}"'
                    f' data-cpc="{row["cpc"]}"'
                    f' data-angle-type="{_esc(row["angle_type"])}"'
                    f' data-angle-title="{_esc(row["angle_title"])}"'
                    f' data-rsoc="{row["rsoc_score"]}"'
                    f' data-signal="{_esc(row["signal_text"])}"'
                    f' onclick="generateArticle(this)"'
                    f' style="cursor:pointer;background:#1565c0;color:#fff;border:none;'
                    f'border-radius:4px;padding:4px 10px;font-size: 12px;white-space:nowrap"'
                    f'>Generate</button>'
                )

                lines.append(
                    f'<tr id="angle-row-{aid}" style="border-bottom:1px solid #1e1e2e">'
                    f'<td style="padding:6px 7px;color:#ccc;max-width:140px;overflow:hidden;'
                    f'text-overflow:ellipsis;white-space:nowrap" title="{_esc(row["keyword"])}">'
                    f'{esc(row["keyword"][:38])}</td>'
                    f'<td style="padding:6px 7px;color:#90caf9">{esc(row["angle_type"])}</td>'
                    f'<td style="padding:6px 7px;color:#aaa;max-width:220px;overflow:hidden;'
                    f'text-overflow:ellipsis;white-space:nowrap" title="{_esc(row["angle_title"])}">'
                    f'{esc(row["angle_title"][:55])}</td>'
                    f'<td style="padding:6px 7px;text-align:right;color:#ccc">'
                    f'${row["cpc"]:.2f}</td>'
                    f'<td style="padding:6px 7px;text-align:right;color:#ccc">'
                    f'{row["rsoc_score"]:.2f}</td>'
                    f'<td style="padding:6px 7px;color:{sig_col}">{esc(row["signal_type"])}</td>'
                    f'<td style="padding:6px 7px;text-align:center">{btn_html}</td>'
                    f'</tr>'
                    f'<tr id="article-row-{aid}" style="display:none">'
                    f'<td colspan="7" style="padding:0">'
                    f'<details open style="background:#111;padding:12px;border-left:3px solid #1565c0">'
                    f'<summary style="cursor:pointer;color:#90caf9;font-size:12px;user-select:none">'
                    f'Article: {_esc(row["angle_title"])}</summary>'
                    f'<pre id="article-text-{aid}"'
                    f' style="color:#ccc;white-space:pre-wrap;font-size:12px;'
                    f'margin:8px 0 0;font-family:monospace">Waiting...</pre>'
                    f'</details></td></tr>'
                )
            lines.append('</tbody></table>')

    # ── JavaScript: dual-path article generation (local SSE → cloud OpenRouter) ──
    lines.append('''<script>
// ── Health probe (result cached 30s) ──────────────────────────────────────
var _localAvail = null, _localProbeAt = 0;
function _probeLocal() {
  var now = Date.now();
  if (_localAvail !== null && now - _localProbeAt < 30000) return Promise.resolve(_localAvail);
  return fetch("http://127.0.0.1:5555/api/health", {signal: AbortSignal.timeout(1500)})
    .then(function(r) { _localAvail = r.ok; _localProbeAt = Date.now(); return _localAvail; })
    .catch(function()  { _localAvail = false; _localProbeAt = Date.now(); return false; });
}

// ── localStorage helpers ───────────────────────────────────────────────────
function _artKey(btn) { return (btn.dataset.keyword||"") + "|" + (btn.dataset.angleType||""); }
function _artSave(key, text) {
  try {
    var store = JSON.parse(localStorage.getItem("kwordit_articles") || "{}");
    store[key] = {text: text, ts: new Date().toISOString()};
    localStorage.setItem("kwordit_articles", JSON.stringify(store));
  } catch(e) {}
}
function _artLoad(key) {
  try { return JSON.parse(localStorage.getItem("kwordit_articles") || "{}")[key] || null; }
  catch(e) { return null; }
}
function _artShowToolbar(aid) {
  var tb = document.getElementById("article-toolbar-" + aid);
  if (tb) tb.classList.add("visible");
}

// ── Main entry point ───────────────────────────────────────────────────────
function generateArticle(btn) {
  var aid = btn.dataset.angleId;
  var rowEl  = document.getElementById("article-row-" + aid);
  var textEl = document.getElementById("article-text-" + aid);
  var key    = _artKey(btn);

  var cached = _artLoad(key);
  if (cached) {
    rowEl.style.display = ""; textEl.style.display = ""; textEl.textContent = cached.text;
    btn.textContent = "\u2713 Cached"; btn.style.background = "#1b5e20";
    _artShowToolbar(aid); return;
  }

  btn.disabled = true; btn.textContent = "Checking..."; rowEl.style.display = "";

  _probeLocal().then(function(local) {
    if (local) { _generateLocal(btn, aid, textEl, key); }
    else       { btn.textContent = "Generating via cloud\u2026"; _generateCloud(btn, aid, textEl, key); }
  });
}

// ── Local path (SSE via Research API) ─────────────────────────────────────
function _generateLocal(btn, aid, textEl, key) {
  btn.textContent = "Generating\u2026";
  var payload = {
    keyword: btn.dataset.keyword, vertical: btn.dataset.vertical,
    language_code: btn.dataset.language, country: btn.dataset.country,
    cpc_usd: parseFloat(btn.dataset.cpc)||0, angle_type: btn.dataset.angleType,
    angle_title: btn.dataset.angleTitle, rsoc_score: parseFloat(btn.dataset.rsoc)||0,
    discovery_signal_text: btn.dataset.signal||""
  };
  fetch("http://127.0.0.1:5555/api/generate-article", {
    method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload)
  }).then(function(resp) {
    var reader = resp.body.getReader(), dec = new TextDecoder(), buf = "";
    function pump() {
      return reader.read().then(function(r) {
        if (r.done) return;
        buf += dec.decode(r.value, {stream:true});
        var parts = buf.split("\\n\\n"); buf = parts.pop();
        parts.forEach(function(line) {
          if (line.indexOf("data: ")!==0) return;
          try {
            var msg = JSON.parse(line.slice(6));
            if (msg.event==="progress") { btn.textContent=msg.message; textEl.textContent=msg.message; }
            else if (msg.event==="result" && msg.status==="ok") {
              btn.textContent="Done ("+msg.word_count+"w)"; btn.style.background="#1b5e20"; btn.disabled=false;
              textEl.style.display=""; textEl.textContent=msg.article_text;
              _artSave(key, msg.article_text); _artShowToolbar(aid);
            } else if (msg.event==="result" && msg.status==="blocked") {
              btn.textContent="BLOCKED"; btn.style.background="#b00020"; btn.disabled=false;
              textEl.innerHTML="<b style=\\"color:#f44336\\">Blocked: "+msg.compliance_risk_level+"</b><br>"+(msg.violations||[]).join("<br>");
              textEl.style.display="";
            } else if (msg.event==="error") {
              btn.textContent="Error"; btn.style.background="#b00020"; btn.disabled=false;
              textEl.style.display=""; textEl.textContent="Error: "+msg.error;
            }
          } catch(e) {}
        });
        return pump();
      });
    }
    return pump();
  }).catch(function(err) {
    btn.textContent="Error"; btn.style.background="#b00020"; btn.disabled=false;
    textEl.style.display=""; textEl.textContent="Local API error: "+err.message;
  });
}

// ── Cloud path (OpenRouter direct from browser) ────────────────────────────
function _generateCloud(btn, aid, textEl, key) {
  var orKey = window._OR_KEY||"";
  if (!orKey) {
    btn.textContent="No API key"; btn.style.background="#b00020"; btn.disabled=false;
    textEl.style.display=""; textEl.textContent="OPENROUTER_API_KEY missing — rebuild dashboard.";
    return;
  }
  var kw=btn.dataset.keyword||"", angle=btn.dataset.angleType||"",
      title=btn.dataset.angleTitle||"", vert=btn.dataset.vertical||"general",
      lang=btn.dataset.language||"en", co=btn.dataset.country||"US";
  var prompt = "Write a 700-900 word RSOC (Related Search Optimised Content) article.\\n\\n"
    +"Keyword: "+kw+"\\nAngle: "+angle.replace(/_/g," ")+"\\nTitle (H1): "+title
    +"\\nVertical: "+vert+"\\nLanguage: "+lang+" | Country: "+co+"\\n\\n"
    +"Requirements:\\n- Start directly with the H1 title\\n- 4-5 H2 sections ~150 words each\\n"
    +"- Informational neutral tone, no direct financial/legal advice\\n"
    +"- Include related searches at end under \\"Related Topics\\"\\n"
    +"- Plain markdown only, no HTML\\n- No specific brand names or direct recommendations";
  fetch("https://openrouter.ai/api/v1/chat/completions", {
    method:"POST",
    headers:{"Authorization":"Bearer "+orKey,"Content-Type":"application/json","HTTP-Referer":"https://keywordit.xyz"},
    body:JSON.stringify({model:"meta-llama/llama-3.3-70b-instruct:free",
      messages:[{role:"user",content:prompt}], stream:true, max_tokens:1400})
  }).then(function(resp) {
    if (!resp.ok) throw new Error("OpenRouter "+resp.status);
    var reader=resp.body.getReader(), dec=new TextDecoder(), buf="", full="";
    textEl.style.display="";
    function pump() {
      return reader.read().then(function(r) {
        if (r.done) {
          btn.textContent="Done ("+full.trim().split(/\\s+/).length+"w, cloud)";
          btn.style.background="#1b5e20"; btn.disabled=false;
          _artSave(key, full); _artShowToolbar(aid); return;
        }
        buf += dec.decode(r.value, {stream:true});
        var parts=buf.split("\\n"); buf=parts.pop();
        parts.forEach(function(line) {
          if (!line.startsWith("data: ")||line==="data: [DONE]") return;
          try { var d=JSON.parse(line.slice(6)).choices[0].delta.content||""; full+=d; textEl.textContent=full; }
          catch(e) {}
        });
        return pump();
      });
    }
    return pump();
  }).catch(function(err) {
    btn.textContent="Cloud error"; btn.style.background="#b00020"; btn.disabled=false;
    textEl.style.display=""; textEl.textContent="OpenRouter error: "+err.message;
  });
}

// ── Article toolbar actions (Copy / Download / Clear) ─────────────────────
document.addEventListener("click", function(e) {
  var btn = e.target.closest(".art-action-btn");
  if (!btn) return;
  e.stopPropagation();
  var action=btn.dataset.action, aid=btn.dataset.aid;
  var textEl=document.getElementById("article-text-"+aid);
  var card=btn.closest("[data-art-key]");
  var artKey=card?card.dataset.artKey:"";
  if (action==="copy") {
    navigator.clipboard.writeText(textEl?textEl.textContent:"").then(function(){
      var orig=btn.textContent; btn.textContent="\u2713 Copied!";
      setTimeout(function(){btn.textContent=orig;},1400);
    });
  } else if (action==="download") {
    var blob=new Blob([textEl?textEl.textContent:""],{type:"text/markdown"});
    var a=document.createElement("a");
    a.href=URL.createObjectURL(blob); a.download=btn.dataset.filename||"article.md";
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(a.href);
  } else if (action==="clear") {
    try {
      var store=JSON.parse(localStorage.getItem("kwordit_articles")||"{}");
      delete store[artKey]; localStorage.setItem("kwordit_articles",JSON.stringify(store));
    } catch(e) {}
    var rowEl=document.getElementById("article-row-"+aid);
    if (rowEl) rowEl.style.display="none";
    var tb=document.getElementById("article-toolbar-"+aid);
    if (tb) tb.classList.remove("visible");
    var genBtn=card?card.querySelector(".angle-gen-btn"):null;
    if (genBtn){genBtn.textContent="Generate";genBtn.style.background="";genBtn.disabled=false;}
  }
});
</script>''')

    lines.append('</div></div><!-- /tab-content -->')
    return '\n'.join(lines)


# ── Experimental Tab HTML ────────────────────────────────────────────────────
def build_experimental_tab():
    if not exp_results:
        return '<div id="tab-experimental" class="tab-content"><div class="exp-empty">No expansions this cycle. Run the pipeline to generate experimental keywords.</div></div>'

    confirmed = [e for e in exp_results if e.get('cpc_track') == 'A' or e.get('cpc_confirmed')]
    inherited = [e for e in exp_results if e.get('cpc_track') != 'A' and not e.get('cpc_confirmed')]
    n_total = len(exp_results)
    n_confirmed = len(confirmed)
    n_inherited = len(inherited)
    pct_c = round(n_confirmed / n_total * 100, 1) if n_total else 0
    pct_i = round(n_inherited / n_total * 100, 1) if n_total else 0
    source_kws = len({e.get('source_keyword','') for e in exp_results if e.get('source_keyword')})
    budget_used = n_total

    lines = []
    lines.append('<div id="tab-experimental" class="tab-content">')

    # Header
    lines.append('<div class="exp-header">')
    lines.append('<h2>Experimental — Template Multiplier Output</h2>')
    lines.append(f'<div class="exp-stats">')
    lines.append(f'This cycle: <span>{n_total}</span> expansions from <span>{source_kws}</span> source keywords<br>')
    lines.append(f'CPC Confirmed: <span>{n_confirmed}</span> ({pct_c}%) | CPC Inherited: <span>{n_inherited}</span> ({pct_i}%)<br>')
    lines.append(f'Expansion budget used: <span>{budget_used}</span>/500')
    lines.append('</div></div>')

    # Filters
    lines.append('<div class="exp-filters">')
    lines.append('<button class="exp-filter-btn active" data-exp-filter="all" aria-pressed="true">All</button>')
    lines.append('<button class="exp-filter-btn" data-exp-filter="confirmed" aria-pressed="false">Confirmed Only</button>')
    lines.append('<button class="exp-filter-btn" data-exp-filter="inherited" aria-pressed="false">Inherited Only</button>')
    lines.append('<div class="exp-sort-label">Sort by: <select id="exp-sort"><option value="revenue">Source Revenue</option><option value="cpc">Est. CPC</option><option value="entity">Entity Status</option></select></div>')
    lines.append('</div>')

    # Template Groups
    groups = {}
    for e in exp_results:
        tmpl = e.get('template', e.get('template_pattern', ''))
        if not tmpl:
            tmpl = '(no template)'
        if tmpl not in groups:
            groups[tmpl] = {'source': e.get('source_keyword', ''), 'source_rev': float(e.get('source_revenue', 0) or 0), 'items': []}
        groups[tmpl]['items'].append(e)
        if float(e.get('source_revenue', 0) or 0) > groups[tmpl]['source_rev']:
            groups[tmpl]['source_rev'] = float(e.get('source_revenue', 0) or 0)
            groups[tmpl]['source'] = e.get('source_keyword', '')

    sorted_groups = sorted(groups.items(), key=lambda x: x[1]['source_rev'], reverse=True)

    lines.append('<div class="exp-section"><div class="exp-section-title">Template Groups</div>')
    for tmpl_name, gdata in sorted_groups:
        lines.append('<div class="tmpl-group">')
        lines.append(f'<div class="tmpl-pattern">TEMPLATE: {esc(tmpl_name)}</div>')
        src_rev = f'${gdata["source_rev"]:,.2f}' if gdata['source_rev'] > 0 else '—'
        lines.append(f'<div class="tmpl-source">Source: {esc(gdata["source"])} — <span class="revenue">{src_rev} proven revenue</span></div>')
        lines.append(f'<ul class="tmpl-expansions" data-template="{esc(tmpl_name)}">')
        for item in gdata['items']:
            kw = esc(item.get('keyword', ''))
            is_confirmed = item.get('cpc_track') == 'A' or item.get('cpc_confirmed')
            cpc_val = float(item.get('cpc_usd', 0) or item.get('inherited_cpc', 0) or 0)
            badge_cls = 'badge-confirmed' if is_confirmed else 'badge-inherited'
            badge_text = 'CONFIRMED' if is_confirmed else 'INHERITED'
            cpc_str = f'${cpc_val:.2f}' if is_confirmed else f'${cpc_val:.2f}e'
            track_attr = 'confirmed' if is_confirmed else 'inherited'
            lines.append(f'<li data-cpc-track="{track_attr}"><span class="kw-name">{kw}</span><span class="{badge_cls}">{badge_text}</span><span class="cpc-val">{cpc_str}</span></li>')
        lines.append('</ul></div>')
    lines.append('</div>')

    # Entity Scoreboard
    entity_data = {}
    for e in exp_results:
        ent = e.get('new_value') or e.get('entity', '')
        if not ent: continue
        if ent not in entity_data:
            entity_data[ent] = {'type': e.get('swapped_slot') or e.get('entity_type', ''), 'status': 'test', 'expansions': 0, 'est_rev': 0.0}
        entity_data[ent]['expansions'] += 1
        entity_data[ent]['est_rev'] += float(e.get('source_revenue', 0) or 0)

    # Determine entity status from registry
    discovered_names = {d.get('entity','') for d in discovered_ent}
    for ent_name, edata in entity_data.items():
        if ent_name in discovered_names:
            edata['status'] = 'new'
        else:
            for etype, pools in entity_reg.items():
                if not isinstance(pools, dict): continue
                for country, pool in pools.items():
                    if not isinstance(pool, dict): continue
                    if ent_name in pool.get('proven', []):
                        edata['status'] = 'proven'
                    elif ent_name in pool.get('test', []):
                        edata['status'] = 'test'

    status_order = {'proven': 0, 'test': 1, 'new': 2}
    sorted_entities = sorted(entity_data.items(), key=lambda x: (status_order.get(x[1]['status'], 3), -x[1]['est_rev']))

    lines.append('<div class="exp-section"><div class="exp-section-title">Entity Scoreboard</div>')
    lines.append('<table class="entity-table"><thead><tr><th>Entity</th><th>Type</th><th>Status</th><th>Expansions</th><th>Est. Rev</th></tr></thead><tbody>')
    for ent_name, edata in sorted_entities:
        status_cls = f'entity-status-{edata["status"]}'
        status_label = edata['status'].upper()
        rev_str = f'${edata["est_rev"]:,.0f}*' if edata['est_rev'] > 0 else '—'
        lines.append(f'<tr><td>{esc(ent_name)}</td><td>{esc(edata["type"])}</td><td><span class="{status_cls}">{status_label}</span></td><td>{edata["expansions"]}</td><td>{rev_str}</td></tr>')
    lines.append('</tbody></table>')
    if sorted_entities:
        lines.append('<div style="font-family:var(--font-mono);font-size:10px;color:var(--text-tertiary);padding-top:6px;">* = revenue from source keywords, not from expansions yet</div>')
    lines.append('</div>')

    # Newly Discovered Entities
    if discovered_ent:
        lines.append('<div class="exp-section"><div class="exp-section-title">Newly Discovered Entities</div>')
        for d in discovered_ent[-20:]:
            ent = esc(d.get('entity', ''))
            etype = esc(d.get('entity_type', ''))
            ts = d.get('ts', '')[:10]
            src_kw = esc(d.get('discovered_in', ''))
            country = esc(d.get('country', ''))
            lines.append(f'<div class="disc-entity"><div class="disc-title">{ent} ({etype}) — discovered {ts}</div>')
            lines.append(f'<div class="disc-detail">Source keyword: "{src_kw}"')
            if country:
                lines.append(f'<br>Country: {country} | Auto-added to test pool | Monitoring 14 days')
            lines.append('</div></div>')
        lines.append('</div>')

    lines.append('</div><!-- /tab-experimental -->')
    return '\n'.join(lines)

# ── Performance Tab HTML ─────────────────────────────────────────────────────
def build_performance_tab():
    lines = []
    lines.append('<div id="tab-performance" class="tab-content">')

    if perf_cache is None:
        # F-009: end-user-facing empty state. The previous copy listed an
        # ops-only CLI command (`python3 scripts/csv_importer.py ...`)
        # which read like a leaked TODO. The new copy explains what the
        # tab will show and when, without leaking implementation detail.
        lines.append('<div class="perf-awaiting">')
        lines.append('<h3>Performance — coming soon</h3>')
        lines.append('<p>Weekly performance reports will appear here once the next import cycle runs.<br>')
        lines.append('We compare keyword pipeline output against your real revenue / RPC numbers and<br>')
        lines.append('flag promotion candidates and drift. Check back after the weekly import.</p>')
        lines.append('</div></div>')
        return '\n'.join(lines)

    pc = perf_cache
    ts = pc.get('import_timestamp', '')[:19].replace('T', ' ')
    csv_file = esc(pc.get('csv_file', ''))
    total_kw = pc.get('total_keywords_imported', 0)
    exp_m = pc.get('matched_to_pipeline', {}).get('experimental', {})
    org_m = pc.get('matched_to_pipeline', {}).get('organic', {})
    total_matched = exp_m.get('count', 0) + org_m.get('count', 0)
    match_pct = round(total_matched / total_kw * 100, 1) if total_kw else 0

    # Header
    lines.append('<div class="perf-header">')
    lines.append('<h2>Performance — Weekly Report</h2>')
    lines.append(f'<div class="perf-stats">')
    lines.append(f'Imported: <span>{ts}</span> | Source: <span>{csv_file}</span><br>')
    lines.append(f'Total keywords in dataset: <span>{total_kw:,}</span> | Matched to pipeline: <span>{total_matched:,}</span> ({match_pct}%)')
    lines.append('</div></div>')

    # Experimental Performance Summary
    lines.append('<div class="perf-section"><div class="perf-section-title">Experimental Performance Summary</div>')
    lines.append('<div class="perf-grid">')

    exp_count = exp_m.get('count', 0)
    exp_with_rev = exp_m.get('with_revenue', 0)
    exp_hit_rate = round(exp_m.get('hit_rate', 0) * 100, 1)
    exp_total_rev = exp_m.get('total_revenue', 0)
    exp_avg = exp_m.get('avg_rev_per_kw', 0)
    org_avg = org_m.get('avg_rev_per_kw', 0)
    multiplier = round(exp_avg / org_avg, 1) if org_avg > 0 else 0

    lines.append(f'<div class="perf-metric"><div class="pm-label">Published Experimental</div><div class="pm-value">{exp_count}</div><div class="pm-sub">keywords</div></div>')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Hit Rate</div><div class="pm-value">{exp_with_rev} ({exp_hit_rate}%)</div><div class="pm-sub">with revenue</div></div>')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Experimental Revenue</div><div class="pm-value">${exp_total_rev:,.2f}</div><div class="pm-sub">total</div></div>')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Avg Rev/KW (Exp)</div><div class="pm-value">${exp_avg:.2f}</div><div class="pm-sub">per keyword</div></div>')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Avg Rev/KW (Organic)</div><div class="pm-value">${org_avg:.2f}</div><div class="pm-sub">per keyword</div></div>')
    mult_str = f'{multiplier}x' if multiplier > 0 else '—'
    lines.append(f'<div class="perf-metric"><div class="pm-label">Multiplier</div><div class="pm-value">{mult_str}</div><div class="pm-sub">exp vs organic</div></div>')
    lines.append('</div></div>')

    # Template Hit Rate Breakdown
    tmpl_rates = pc.get('template_hit_rates', {})
    if tmpl_rates:
        sorted_tmpl = sorted(tmpl_rates.items(), key=lambda x: x[1].get('hit_rate', 0), reverse=True)
        lines.append('<div class="perf-section"><div class="perf-section-title">Template Hit Rate Breakdown</div>')
        for tmpl_name, tdata in sorted_tmpl:
            pub = tdata.get('published', 0)
            hits = tdata.get('hits', 0)
            hr = round(tdata.get('hit_rate', 0) * 100, 1)
            fill_cls = 'high' if hr >= 50 else ('mid' if hr >= 20 else 'low')
            lines.append(f'<div class="perf-bar-row"><div class="perf-bar-label">{esc(tmpl_name)}</div>')
            lines.append(f'<div style="min-width:60px;text-align:center">{pub}/{hits}</div>')
            lines.append(f'<div class="perf-bar-track"><div class="perf-bar-fill {fill_cls}" style="width:{min(hr,100)}%"></div></div>')
            lines.append(f'<div class="perf-bar-pct">{hr}%</div></div>')
        lines.append('</div>')

    # Promotion / Demotion Candidates
    promo = pc.get('promotion_candidates', [])
    demo = pc.get('demotion_candidates', [])
    drift_warnings = pc.get('drift_warnings', [])
    health = pc.get('pipeline_health', {})

    lines.append('<div class="perf-section"><div class="perf-section-title">Entity Promotion / Demotion</div>')

    if drift_warnings:
        for w in drift_warnings:
            lines.append(f'<div class="perf-alert">{esc(w)}</div>')

    if health.get('drift_detected'):
        tier_c_pct = round(health.get('tier_c_keyword_pct', 0) * 100, 1)
        tier_c_base = round(health.get('tier_c_baseline', 0.22) * 100, 0)
        lines.append(f'<div class="perf-alert">Pipeline drift detected: Tier C keywords at {tier_c_pct}% (baseline {tier_c_base}%)</div>')

    if promo:
        lines.append('<div style="margin-bottom:8px;font-family:var(--font-mono);font-size: 12px;color:var(--text-secondary);">PROMOTION CANDIDATES (entity crossed $50 threshold):</div>')
        ent_perf = pc.get('entity_performance', {})
        for ent_name in promo:
            ep = ent_perf.get(ent_name, {})
            rev = ep.get('revenue', 0)
            kw_count = ep.get('expanded_keywords', 0)
            lines.append(f'<div class="perf-promo-card"><div class="perf-promo-info">{esc(ent_name)}: ${rev:,.2f} revenue across {kw_count} keywords</div>')
            lines.append(f'<button class="perf-promo-btn" onclick="copyPromotion(\'{esc(ent_name)}\')">COPY PROMOTION JSON</button></div>')

    if demo:
        lines.append('<div style="margin-top:12px;margin-bottom:8px;font-family:var(--font-mono);font-size: 12px;color:var(--text-secondary);">DEMOTION CANDIDATES (&lt; $10 for 4 weeks):</div>')
        ent_perf = pc.get('entity_performance', {})
        for ent_name in demo:
            ep = ent_perf.get(ent_name, {})
            rev = ep.get('revenue', 0)
            kw_count = ep.get('expanded_keywords', 0)
            weeks = ep.get('weeks_below_threshold', 0)
            lines.append(f'<div class="perf-promo-card"><div class="perf-promo-info">{esc(ent_name)}: ${rev:,.2f} revenue, {kw_count} keywords ({weeks} weeks below threshold)</div>')
            lines.append(f'<button class="perf-demo-btn" onclick="copyDemotion(\'{esc(ent_name)}\')">COPY DEMOTION JSON</button></div>')

    if not promo and not demo:
        lines.append('<div style="font-family:var(--font-mono);font-size: 12px;color:var(--text-tertiary);padding:8px 0;">No promotion or demotion candidates this cycle.</div>')
    lines.append('</div>')

    # Traffic Activation Rate
    tar = pc.get('traffic_activation_rate', 0)
    tar_pct = round(tar * 100, 1)
    lines.append('<div class="perf-section"><div class="perf-section-title">Traffic Activation Rate</div>')
    lines.append('<div class="perf-grid">')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Current</div><div class="pm-value">{tar_pct}%</div><div class="pm-sub">keywords with clicks</div></div>')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Baseline</div><div class="pm-value">43.8%</div><div class="pm-sub">from CSV analysis</div></div>')
    lines.append(f'<div class="perf-metric"><div class="pm-label">Target</div><div class="pm-value">&gt; 50%</div><div class="pm-sub">goal</div></div>')
    lines.append('</div></div>')

    # Revenue Concentration
    rev_conc = pc.get('revenue_concentration', {})
    lines.append('<div class="perf-section"><div class="perf-section-title">Revenue Concentration Curve</div>')
    for key, baseline_val, label in [('top_1pct', 0.634, 'Top 1%'), ('top_5pct', 0.837, 'Top 5%'), ('top_10pct', 0.900, 'Top 10%')]:
        val = rev_conc.get(key, 0)
        val_pct = round(val * 100, 1)
        base_pct = round(baseline_val * 100, 1)
        fill_w = min(val_pct, 100)
        lines.append(f'<div class="perf-bar-row"><div class="perf-bar-label">{label}: {val_pct}% of revenue</div>')
        lines.append(f'<div class="perf-bar-track"><div class="perf-bar-fill mid" style="width:{fill_w}%"></div></div>')
        lines.append(f'<div class="perf-bar-pct">base: {base_pct}%</div></div>')
    lines.append('</div>')

    # RPC Model Calibration
    _rpc_opps = [o for o in opportunities
                 if o.get('rpc_actual') is not None and o.get('rpc_expected')]
    if _rpc_opps:
        lines.append('<div class="perf-section"><div class="perf-section-title">RPC Model Calibration</div>')

        # Overall MAPE
        _mape_opps = [o for o in _rpc_opps if (o.get('rpc_actual_clicks') or 0) >= 50
                      and o.get('rpc_expected', 0) != 0]
        if _mape_opps:
            _mape = round(sum(abs(o['rpc_actual'] - o['rpc_expected']) / o['rpc_expected']
                              for o in _mape_opps) / len(_mape_opps) * 100, 1)
            lines.append(f'<div style="font-family:var(--font-mono);font-size: 12px;color:var(--text-secondary);padding:4px 0;">Overall model MAPE (≥50 clicks): {_mape}% &nbsp;|&nbsp; Sample: {len(_mape_opps)} keywords</div>')

        # Vertical breakdown
        from collections import defaultdict as _dd
        _by_vert = _dd(lambda: {'actual': 0.0, 'clicks': 0, 'expected_sum': 0.0, 'count': 0})
        for o in _rpc_opps:
            v = o.get('rpc_vertical') or o.get('vertical_match') or o.get('vertical') or 'unknown'
            _by_vert[v]['actual']       += o['rpc_actual'] * (o.get('rpc_actual_clicks') or 1)
            _by_vert[v]['clicks']       += o.get('rpc_actual_clicks') or 1
            _by_vert[v]['expected_sum'] += o['rpc_expected']
            _by_vert[v]['count']        += 1

        _vert_ratios = []
        for v, d in _by_vert.items():
            if d['clicks'] > 0 and d['count'] > 0:
                avg_actual   = d['actual'] / d['clicks']
                avg_expected = d['expected_sum'] / d['count']
                ratio = round(avg_actual / avg_expected, 2) if avg_expected > 0 else None
                _vert_ratios.append((v, ratio, d['count']))

        if _vert_ratios:
            lines.append('<div style="font-family:var(--font-mono);font-size: 12px;color:var(--text-secondary);margin-top:6px;margin-bottom:2px;">Vertical RPC ratio (actual / expected):</div>')
            for v, ratio, count in sorted(_vert_ratios, key=lambda x: -(x[1] or 0)):
                if ratio is None:
                    continue
                if ratio >= 1.2:
                    badge, cls = '▲', 'color:#f59e0b'
                elif ratio <= 0.8:
                    badge, cls = '▼', 'color:#ef4444'
                else:
                    badge, cls = '●', 'color:var(--text-tertiary)'
                lines.append(f'<div style="font-family:var(--font-mono);font-size: 12px;padding:1px 0;">'
                              f'<span style="{cls}">{badge}</span>&nbsp;'
                              f'{esc(v)}: <b>{ratio:.2f}x</b>&nbsp;'
                              f'<span style="color:var(--text-tertiary)">({count} kw)</span></div>')

        lines.append('</div>')

    lines.append('</div><!-- /tab-performance -->')
    return '\n'.join(lines)

# ── Build extra tabs script (filter/sort JS for experimental tab) ────────────
extra_tabs_js = '''<script>
// Experimental tab filter/sort
(function(){
  document.querySelectorAll('[data-exp-filter]').forEach(btn => {
    btn.addEventListener('click', () => {
      // F-064: clear both .active and aria-pressed on every sibling so AT
      // sees the same single-active state as sighted users.
      document.querySelectorAll('[data-exp-filter]').forEach(b => {
        b.classList.remove('active');
        b.setAttribute('aria-pressed', 'false');
      });
      btn.classList.add('active');
      btn.setAttribute('aria-pressed', 'true');
      const f = btn.dataset.expFilter;
      document.querySelectorAll('.tmpl-expansions li').forEach(li => {
        if (f === 'all') li.style.display = '';
        else if (f === 'confirmed') li.style.display = li.dataset.cpcTrack === 'confirmed' ? '' : 'none';
        else if (f === 'inherited') li.style.display = li.dataset.cpcTrack === 'inherited' ? '' : 'none';
      });
    });
  });
})();
// Promotion/demotion clipboard helpers
function copyPromotion(entity) {
  const json = JSON.stringify({action:'promote',entity:entity,from:'test',to:'proven',timestamp:new Date().toISOString()},null,2);
  navigator.clipboard.writeText(json).then(() => alert('Promotion JSON copied to clipboard for: ' + entity));
}
function copyDemotion(entity) {
  const json = JSON.stringify({action:'demote',entity:entity,from:'test',to:'blocked',timestamp:new Date().toISOString()},null,2);
  navigator.clipboard.writeText(json).then(() => alert('Demotion JSON copied to clipboard for: ' + entity));
}

// ── Discovery tab inline angle expansion ──────────────────────────────────
(function() {
  var tbody = document.getElementById('table-body');
  if (!tbody) return;

  var _anglesLoading = false;
  var _anglesLoaded = false;
  var _anglesPending = [];

  function _ensureAngles(cb) {
    if (_anglesLoaded) { cb(); return; }
    _anglesPending.push(cb);
    if (_anglesLoading) return;
    _anglesLoading = true;
    var ver = '?v=' + ((window.PIPELINE_META || {}).run_id || Date.now());
    fetch('data/angles.json' + ver)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        window.ANGLES = data;
        _anglesLoaded = true;
        _anglesPending.forEach(function(fn) { fn(); });
        _anglesPending = [];
      })
      .catch(function(err) {
        console.error('Failed to load angles:', err);
        _anglesLoading = false;
        _anglesPending.forEach(function(fn) { fn(); });
        _anglesPending = [];
      });
  }

  function getCluster(tr) {
    var kw = (tr.dataset.keyword || '').toLowerCase();
    var co = (tr.dataset.country || '').toUpperCase();
    return (window.ANGLES || {})[kw + '|' + co] || null;
  }

  function removePanel(tr) {
    var sib = tr.nextElementSibling;
    if (sib && sib.classList.contains('angle-panel-row')) {
      sib.remove();
      tr.classList.remove('angle-expanded');
      return true;
    }
    return false;
  }

  function buildAngleCard(a, keyword, country, cluster, kd) {
    var aid = 'disc-' + Math.random().toString(36).slice(2, 10);
    var score = Math.round((a.rsoc_score || 0) * 100);
    var signal = a.discovery_boosted ? '\u26a1 trending' : 'standard';
    var typeLabel = (a.angle_type || '').replace(/_/g, ' ');
    function escAttr(s) { return String(s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;'); }
    var artKey = keyword.toLowerCase() + '|' + (a.angle_type || '');
    var kdStr = kd != null ? ' \u00b7 KD: ' + kd : '';
    return '<div class="angle-card" data-art-key="' + escAttr(artKey) + '">'
      + '<div class="angle-type-badge">' + typeLabel + '</div>'
      + '<div class="angle-card-title">' + escAttr(a.angle_title || '\u2014') + '</div>'
      + '<div class="angle-card-meta">Score: ' + score + kdStr + ' \u00b7 ' + signal + '</div>'
      + '<button class="angle-copy-btn" onclick="angleCopyTitle(this)" data-title="' + escAttr(a.angle_title || '') + '" title="Copy title">\u29c9 Copy</button>'
      + '<button class="angle-gen-btn"'
      +   ' data-angle-id="' + aid + '"'
      +   ' data-keyword="' + escAttr(keyword) + '"'
      +   ' data-country="' + escAttr(country) + '"'
      +   ' data-vertical="' + escAttr(cluster.vertical || '') + '"'
      +   ' data-language="' + escAttr(cluster.language_code || 'en') + '"'
      +   ' data-cpc="' + (cluster.cpc_usd || 0) + '"'
      +   ' data-angle-type="' + escAttr(a.angle_type || '') + '"'
      +   ' data-angle-title="' + escAttr(a.angle_title || '') + '"'
      +   ' data-rsoc="' + (a.rsoc_score || 0) + '"'
      +   ' data-signal="' + escAttr(signal) + '"'
      +   ' onclick="generateArticle(this)">Generate</button>'
      + '<div id="article-row-' + aid + '" style="display:none">'
      +   '<pre id="article-text-' + aid + '" class="angle-article-area"></pre>'
      +   '<div id="article-toolbar-' + aid + '" class="angle-article-toolbar">'
      +     '<button class="art-action-btn" data-action="copy" data-aid="' + aid + '">\u29c9 Copy</button>'
      +     '<button class="art-action-btn" data-action="download" data-aid="' + aid + '" data-filename="' + escAttr(keyword.replace(/\\s+/g,'_') + '_' + (a.angle_type||'') + '.md') + '">\u2b07 .md</button>'
      +     '<button class="art-action-btn" data-action="clear" data-aid="' + aid + '">\u2715 Clear</button>'
      +   '</div>'
      + '</div>'
      + '</div>';
  }

  function showPanel(tr) {
    var cluster = getCluster(tr);
    var angles = cluster ? (cluster.selected_angles || []) : [];
    var keyword = tr.dataset.keyword || '';
    var country = tr.dataset.country || '';
    var colCount = tr.querySelectorAll('td').length || 12;

    var oppKd = null;
    var items = window.OPPORTUNITIES || [];
    for (var i = 0; i < items.length; i++) {
      if ((items[i].keyword || '').toLowerCase() === keyword.toLowerCase()
          && (items[i].country || '').toUpperCase() === country.toUpperCase()) {
        oppKd = items[i].kd;
        break;
      }
    }

    var inner = angles.length
      ? angles.map(function(a) { return buildAngleCard(a, keyword, country, cluster, oppKd); }).join('')
      : '<div style="padding:8px;font-size:12px;color:var(--text-tertiary)">No angles available for this keyword</div>';

    var panelTr = document.createElement('tr');
    panelTr.className = 'angle-panel-row';
    panelTr.innerHTML = '<td colspan="' + colCount + '"><div class="angle-panel">' + inner + '</div></td>';
    var wrap = document.getElementById('table-scroll-wrap');
    if (wrap) {
      panelTr.querySelector('.angle-panel').style.width = wrap.clientWidth + 'px';
    }
    tr.insertAdjacentElement('afterend', panelTr);
    tr.classList.add('angle-expanded');

    // Pre-populate cached articles for any angle that has been generated before
    panelTr.querySelectorAll('.angle-card[data-art-key]').forEach(function(card) {
      var artKey = card.dataset.artKey;
      var cached = _artLoad(artKey);
      if (!cached) return;
      var genBtn = card.querySelector('.angle-gen-btn');
      var aid = genBtn ? genBtn.dataset.angleId : null;
      if (!aid) return;
      var rowEl = document.getElementById('article-row-' + aid);
      var textEl = document.getElementById('article-text-' + aid);
      if (rowEl) rowEl.style.display = '';
      if (textEl) { textEl.textContent = cached.text; textEl.style.display = ''; }
      _artShowToolbar(aid);
      if (genBtn) { genBtn.textContent = '\u2713 Cached'; genBtn.style.background = '#1b5e20'; }
    });
  }

  window.angleCopyTitle = function(btn) {
    var title = btn.dataset.title || '';
    navigator.clipboard.writeText(title).then(function() {
      btn.textContent = 'Copied!';
      setTimeout(function() { btn.textContent = '\u29c9 Copy'; }, 1200);
    });
  };

  tbody.addEventListener('click', function(e) {
    var tr = e.target.closest('tr[data-idx]');
    if (!tr) return;
    if (e.target.classList.contains('copy-btn') || e.target.closest('.copy-btn')) return;
    if (e.target.classList.contains('angle-gen-btn') || e.target.closest('.angle-gen-btn')) return;
    if (e.target.classList.contains('angle-copy-btn') || e.target.closest('.angle-copy-btn')) return;
    if (e.target.classList.contains('art-action-btn') || e.target.closest('.art-action-btn')) return;
    e.stopImmediatePropagation();
    if (!removePanel(tr)) _ensureAngles(function() { showPanel(tr); });
  }, true);
})();
</script>'''

def build_pipeline_tab():
    """Build the Pipeline Status tab showing stage timeline, run history, and errors."""
    import os
    # F-010: ship empty-state copy by default; the rich pipeline UI below
    # was rendering incorrectly (broken stage timeline, missing run-history
    # data) and is hidden behind this short-circuit until product re-enables.
    # Tab class corrected from "tab-panel" to "tab-content" + standard
    # display toggle so the tab strip's existing show/hide JS handles it
    # the same way as the other tabs (was inline display:none with a
    # non-matching class — the source of the "renders incorrectly" symptom).
    return (
        '<div id="tab-pipeline" class="tab-content">'
        '<div class="perf-awaiting">'
        '<h3>Pipeline — coming soon</h3>'
        '<p>Live pipeline status (stage progress, run history, error feed)<br>'
        'will appear here once the rich pipeline UI is ready. Until then,<br>'
        'the system status bar at the top of the page shows the headline numbers<br>'
        '(last run, runs total, GKP / DFS / Unscored counts, errors).</p>'
        '</div></div>'
    )

    # --- pre-Wave-4 rich pipeline UI kept below for future re-enable ---
    lines = ['<div class="tab-panel" id="tab-pipeline" style="display:none;">']
    lines.append('<div class="pipeline-grid">')

    # ── Read checkpoint data ─────────────────────────────────────────────────
    checkpoint = {}
    cp_path = Path("/tmp/openclaw_run_checkpoint.json")
    try:
        if cp_path.exists():
            checkpoint = json.loads(cp_path.read_text())
    except Exception:
        pass

    started_at = checkpoint.get("started_at", "")
    completed_stages = set(checkpoint.get("completed", []))
    run_date = checkpoint.get("run_date", "")

    # ── Determine pipeline state ─────────────────────────────────────────────
    all_stages = [
        "subreddit_discovery.py", "reddit_intelligence.py",
        "trends_scraper.py", "trends_postprocess.py",
        "keyword_expander.py", "keyword_extractor.py",
        "commercial_keyword_transformer.py", "vetting.py",
        "validation.py", "angle_engine.py",
        "dashboard_builder.py", "reflection.py",
    ]

    # Check if heartbeat is currently running
    hb_running = False
    try:
        import subprocess as _sp
        r = _sp.run(["pgrep", "-f", "heartbeat.py"], capture_output=True, text=True, timeout=5)
        hb_running = r.returncode == 0
    except Exception:
        pass

    done_count = len(completed_stages)
    if hb_running and done_count < len(all_stages):
        state = "running"
        state_label = "Running"
        state_class = "warn"
    elif done_count == len(all_stages):
        state = "idle"
        state_label = "Idle"
        state_class = "ok"
    else:
        state = "partial"
        state_label = f"Partial ({done_count}/{len(all_stages)})"
        state_class = "warn"

    # ── Read recent errors ───────────────────────────────────────────────────
    recent_errors = []
    try:
        err_path = BASE / "error_log.jsonl"
        if err_path.exists():
            err_lines = err_path.read_text().strip().split("\n")
            for line in reversed(err_lines[-20:]):
                try:
                    recent_errors.append(json.loads(line))
                except Exception:
                    pass
            recent_errors = recent_errors[:10]
    except Exception:
        pass

    if recent_errors and not hb_running:
        # Check if last error is from today's run
        last_err_date = recent_errors[0].get("timestamp", "")[:10]
        if last_err_date == run_date:
            state = "error"
            state_label = "Error"
            state_class = "error"

    # ── Compute run history (golden counts per day from validation_history) ──
    run_history = []
    try:
        vh_path = BASE / "validation_history.jsonl"
        if vh_path.exists():
            from collections import defaultdict
            by_date = defaultdict(lambda: {"golden": 0, "total": 0})
            for line in vh_path.read_text().strip().split("\n")[-5000:]:
                try:
                    rec = json.loads(line)
                    d = (rec.get("validated_at") or rec.get("processed_at", ""))[:10]
                    if d:
                        by_date[d]["total"] += 1
                        if rec.get("tag") == "GOLDEN_OPPORTUNITY":
                            by_date[d]["golden"] += 1
                except Exception:
                    pass
            for d in sorted(by_date.keys(), reverse=True)[:7]:
                run_history.append({"date": d, **by_date[d]})
    except Exception:
        pass

    # ── Duration ─────────────────────────────────────────────────────────────
    duration_str = "—"
    next_str = "—"
    if started_at:
        try:
            started_dt = datetime.fromisoformat(started_at)
            elapsed = (datetime.now() - started_dt).total_seconds()
            if state == "idle":
                duration_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
            next_dt = started_dt.replace(hour=started_dt.hour + 6)
            next_str = next_dt.strftime("%H:%M")
        except Exception:
            pass

    # ── Status Card ──────────────────────────────────────────────────────────
    lines.append('<div class="pipeline-card">')
    lines.append('<div class="pipeline-card-title">Pipeline Status</div>')
    lines.append(f'<div class="pipeline-status-value {state_class}">{esc(state_label)}</div>')
    lines.append('<div class="pipeline-meta">')
    lines.append(f'<div>Last run: <span>{esc(started_at[:19]) if started_at else "Never"}</span></div>')
    lines.append(f'<div>Duration: <span>{duration_str}</span></div>')
    lines.append(f'<div>Next scheduled: <span>{next_str}</span></div>')
    lines.append('</div>')
    lines.append('<button class="btn-run-now" id="btn-run-now" onclick="triggerPipeline()">Run Now</button>')
    lines.append('</div>')

    # ── Stage Timeline ───────────────────────────────────────────────────────
    lines.append('<div class="pipeline-card">')
    lines.append('<div class="pipeline-card-title">Stage Timeline</div>')
    lines.append('<ul class="stage-timeline">')
    for stage in all_stages:
        if stage in completed_stages:
            icon_cls = "done"
            icon = "\u2713"
        elif hb_running and stage not in completed_stages:
            # First uncompleted stage while running = currently running
            if all(s in completed_stages for s in all_stages[:all_stages.index(stage)]):
                icon_cls = "running"
                icon = "\u23F3"
            else:
                icon_cls = "pending"
                icon = "\u2014"
        else:
            icon_cls = "pending"
            icon = "\u2014"
        name = stage.replace(".py", "").replace("_", " ").title()
        lines.append(f'<li class="stage-item"><span class="stage-icon {icon_cls}">{icon}</span><span class="stage-name">{esc(name)}</span></li>')
    lines.append('</ul></div>')

    # ── Run History ──────────────────────────────────────────────────────────
    lines.append('<div class="pipeline-card" style="grid-column:span 2;">')
    lines.append('<div class="pipeline-card-title">Last 7 Runs</div>')
    lines.append('<div class="run-history">')
    if run_history:
        for rh in run_history:
            lines.append(f'<div class="run-card"><div class="run-card-date">{esc(rh["date"])}</div>'
                        f'<div class="run-card-golden">{rh["golden"]}</div>'
                        f'<div class="run-card-total">{rh["total"]} total</div></div>')
    else:
        lines.append('<div style="color:var(--text-muted);font-size:12px;padding:16px;">No run history yet.</div>')
    lines.append('</div></div>')

    # ── Error Log ────────────────────────────────────────────────────────────
    lines.append('<div class="pipeline-card" style="grid-column:span 2;">')
    lines.append('<div class="pipeline-card-title">Recent Errors</div>')
    lines.append('<div class="error-list">')
    if recent_errors:
        for err in recent_errors:
            ts = esc(err.get("timestamp", "")[:19])
            stage = esc(err.get("stage", "unknown"))
            msg = esc(str(err.get("error", ""))[:200])
            lines.append(f'<div class="error-item"><span class="error-timestamp">{ts}</span>'
                        f'<span class="error-stage">{stage}</span>'
                        f'<div class="error-msg" onclick="this.classList.toggle(\'expanded\')">{msg}</div></div>')
    else:
        lines.append('<div style="color:var(--text-muted);font-size:12px;padding:16px;">No recent errors.</div>')
    lines.append('</div></div>')

    lines.append('</div><!-- /pipeline-grid -->')

    # ── JS: Run Now handler ──────────────────────────────────────────────────
    lines.append('''<script>
function triggerPipeline() {
  var btn = document.getElementById('btn-run-now');
  btn.disabled = true;
  btn.textContent = 'Triggering...';
  fetch('/api/trigger-pipeline', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      btn.textContent = d.status === 'triggered' ? 'Triggered! Refreshing...' : 'Error: ' + (d.message || 'unknown');
      if (d.status === 'triggered') setTimeout(function() { location.reload(); }, 5000);
      else setTimeout(function() { btn.disabled = false; btn.textContent = 'Run Now'; }, 3000);
    })
    .catch(function(e) {
      btn.textContent = 'API unavailable';
      setTimeout(function() { btn.disabled = false; btn.textContent = 'Run Now'; }, 3000);
    });
}
</script>''')
    lines.append('</div><!-- /tab-pipeline -->')
    return '\n'.join(lines)


content_tab_html      = build_content_tab()
experimental_tab_html = build_experimental_tab()
performance_tab_html  = build_performance_tab()
intel_tab_html        = build_intel_tab()
pipeline_tab_html     = build_pipeline_tab()
extra_tabs_content    = content_tab_html + '\n' + experimental_tab_html + '\n' + performance_tab_html + '\n' + intel_tab_html + '\n' + pipeline_tab_html + '\n' + extra_tabs_js

exp_count = len(exp_results)
disc_count = len(discovered_ent)

# ── Assemble final HTML ──────────────────────────────────────────────────────
template_path = BASE / 'dashboard_template.html'
if not template_path.exists():
    print(f"⚠️  {template_path} not found")
    raise SystemExit(1)

TEMPLATE = template_path.read_text(encoding='utf-8')

# Load chat widget HTML
chat_widget_path = BASE / "chat_widget.html"
chat_widget_html = ""
if chat_widget_path.exists():
    chat_widget_html = chat_widget_path.read_text(encoding='utf-8')

placeholders = {
    '__DATA__': '[]',
    '__META__': meta_json,
    '__ANGLES_DATA__': '{}',
    '__OR_KEY__': '',
    '__TAB_NAV__': tab_nav_html,
    '__EXTRA_TABS__': extra_tabs_content,
}
_ph_pattern = re.compile('|'.join(re.escape(k) for k in placeholders))
html = _ph_pattern.sub(lambda m: placeholders[m.group(0)], TEMPLATE)

# Inject chat widget before closing </body> tag
if chat_widget_html:
    html = html.replace('</body>', f'{chat_widget_html}\n</body>')

OUTPUT.write_text(html, encoding='utf-8')
print(f"✅ Dashboard → {OUTPUT}  ({total} opps across {len(run_dates)} run dates, {len(html)//1024}KB)")
print(f"   Sources: Google KP={google_kp_count}  DataForSEO={dataforseo_count}  unscored={unscored_count}")
print(f"   Experimental: {exp_count} expansions | {disc_count} discovered entities | perf_cache={'loaded' if perf_cache else 'none'}")
if run_dates:
    print(f"   Date range: {run_dates[0]} → {run_dates[-1]}")
