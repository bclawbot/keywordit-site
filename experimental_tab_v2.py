"""
Enhanced experimental tab v2 for dashboard_builder.py
Builds a comprehensive interactive dashboard tab with KPIs, filtering, sorting, and grouping.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional, Tuple
from experimental_enrichment import (
    load_taboola_slim,
    match_angles,
    enrich_expansion_data,
    get_top_launches,
    compute_kpi_stats
)


def _build_enhanced_experimental_tab_v2(
    BASE: Path,
    _load_jsonl: Callable,
    _load_json: Callable,
    esc: Callable
) -> str:
    """
    Build the complete experimental tab HTML with all data, JS, and CSS inline.

    Args:
        BASE: Workspace Path
        _load_jsonl: Function to load JSONL files
        _load_json: Function to load JSON files
        esc: HTML escaping function

    Returns:
        Complete HTML string for the experimental tab
    """

    # ─── Load Data Sources ────────────────────────────────────────────────────
    exp_results = _load_jsonl(BASE / 'data' / 'expansion_results.jsonl')
    entity_reg = _load_json(BASE / 'data' / 'entity_registry.json', {})
    discovered_ent = _load_jsonl(BASE / 'data' / 'discovered_entities.jsonl')
    intelligence = _load_intelligence_report(BASE)
    snapshots = _load_daily_snapshots(BASE)
    proven_rpc = _load_proven_rpc(BASE)
    proven_entities = _load_json(BASE / 'data' / 'proven_entities.json', {})
    proven_templates = _load_json(BASE / 'data' / 'proven_templates.json', {})
    vertical_ref = _load_json(BASE / 'data' / 'vertical_reference.json', {})
    country_intel = _load_json(BASE / 'data' / 'country_intelligence.json', {})
    missed_opps_data = _load_json(BASE / 'data' / 'missed_opportunities.json', [])
    angle_templates = _load_json(BASE / 'data' / 'angle_templates.json', {})

    # ─── Load Taboola and Match Angles ──────────────────────────────────────
    expansion_kw_set = set(
        f"{r.get('keyword', '')}|{r.get('country', '')}"
        for r in exp_results
        if r.get('keyword') and r.get('country')
    )

    taboola_index = load_taboola_slim(
        str(BASE / 'data' / 'taboola_keyword_index.json'),
        expansion_kw_set
    )

    matched_angles = match_angles(
        exp_results,
        str(BASE / 'data' / 'angles.json')
    )

    # ─── Enrich Expansion Data ────────────────────────────────────────────────
    enriched = enrich_expansion_data(
        exp_results,
        proven_rpc,
        proven_entities,
        proven_templates,
        vertical_ref,
        taboola_index,
        matched_angles,
        country_intel
    )

    # ─── Compute KPIs ──────────────────────────────────────────────────────
    kpi_data = compute_kpi_stats(
        enriched,
        exp_results,
        proven_rpc,
        intelligence or {},
        missed_opps_data,
        snapshots
    )

    # ─── Compute Trend Directions from Snapshots ──────────────────────────
    _assign_trend_directions(enriched, snapshots)

    # ─── Get Top Launches ──────────────────────────────────────────────────
    top_launches = get_top_launches(enriched, n=3)

    # ─── Get Entity Performance Data ───────────────────────────────────────
    entity_perf = _compute_entity_performance(enriched, proven_entities)

    # ─── Prepare JSON data for embedding in script ────────────────────────
    exp_data_for_js = []
    for row in enriched:
        exp_data_for_js.append({
            'idx': len(exp_data_for_js),
            'keyword': row.get('keyword', ''),
            'country': row.get('country', ''),
            'country_flag': _get_country_flag(row.get('country', '')),
            'margin': round(row.get('margin') or 0, 2),
            'score_v2': round(row.get('score_v2', 0), 1),
            'cpc': round(row.get('cpc_usd', 0), 2),
            'cpc_track': row.get('cpc_track', ''),
            'cpc_track_label': row.get('cpc_track_label', ''),
            'proven_rpc': round(row.get('proven_rpc') or 0, 4),
            'proven_revenue': round(row.get('proven_revenue') or 0, 2),
            'entity_name': row.get('entity_name', '') or row.get('new_value', ''),
            'entity_type': row.get('swapped_slot', ''),
            'entity_status': row.get('entity_status', ''),
            'vertical': row.get('vertical', ''),
            'status': row.get('entity_status', 'test'),
            'template': row.get('template', 'ungrouped'),
            'source_keyword': row.get('source_keyword', ''),
            'source_revenue': round(row.get('source_revenue', 0), 2),
            'processed_at': (row.get('processed_at') or '')[:10],
            'estimated_monthly_searches': row.get('estimated_monthly_searches', 0),
            'device_skew': round(row.get('device_skew') or 0, 2),
            'entity_density': round(row.get('entity_density') or 0, 2),
            'desktop_rpc': round(row.get('desktop_rpc') or 0, 4),
            'mobile_rpc': round(row.get('mobile_rpc') or 0, 4),
            'revenue_rank': row.get('revenue_rank'),
            'quality_score': round(row.get('quality_score', 0), 1),
            'proven_exact': row.get('cpc_track_label') == 'proven_exact',
            'has_angles': row.get('has_angles', False),
            'trend_direction': row.get('trend_direction', 'stable'),
        })
        # Inject top angle data from matched_angles
        angle_key = f"{row.get('keyword', '')}|{row.get('country', '')}"
        src_angle_key = f"{row.get('source_keyword', '')}|{row.get('country', '')}"
        ma = matched_angles.get(angle_key) or matched_angles.get(src_angle_key)
        if ma and ma.get('angles'):
            top = ma['angles'][0]  # already sorted by score desc
            exp_data_for_js[-1]['top_angle'] = top.get('type', '')
            exp_data_for_js[-1]['top_angle_score'] = round(top.get('score', 0), 2)
            exp_data_for_js[-1]['top_angle_source'] = top.get('source', '')
            exp_data_for_js[-1]['has_fb_intel'] = any(
                a.get('source', '').startswith('fb_intel') for a in ma['angles']
            )
        else:
            exp_data_for_js[-1]['top_angle'] = ''
            exp_data_for_js[-1]['top_angle_score'] = 0
            exp_data_for_js[-1]['top_angle_source'] = ''
            exp_data_for_js[-1]['has_fb_intel'] = False

    # Top 3 launch candidates
    top_launches_data = []
    for launch in top_launches[:3]:
        top_launches_data.append({
            'keyword': launch.get('keyword', ''),
            'country': launch.get('country', ''),
            'country_flag': _get_country_flag(launch.get('country', '')),
            'margin': round(launch.get('margin') or 0, 2),
            'score_v2': round(launch.get('score_v2', 0), 1),
            'confidence': round(launch.get('confidence', 0), 0),
        })

    # Prepare missed opportunities
    missed_opps_for_js = []
    missed_opps_list = missed_opps_data if isinstance(missed_opps_data, list) else []
    for i, opp in enumerate(missed_opps_list[:266]):
        missed_opps_for_js.append({
            'idx': i,
            'keyword': opp.get('keyword', ''),
            'revenue': round(opp.get('revenue', 0), 2),
            'rpc': round(opp.get('rpc', 0), 4),
            'priority': opp.get('priority', 'medium'),
            'reason': opp.get('reason', ''),
        })

    # ─── Build HTML ────────────────────────────────────────────────────────

    html_parts = []

    # ─── Tab Wrapper (required for dashboard tab switching) ─────────────
    html_parts.append('<div id="tab-experimental" class="tab-content">')

    # ─── CSS ───────────────────────────────────────────────────────────────
    html_parts.append(_build_experimental_css())

    # ─── Main Container ────────────────────────────────────────────────────
    html_parts.append('<div class="exp2-container">')

    # ─── Section 1: KPI Header ────────────────────────────────────────────
    html_parts.append(_build_kpi_header(kpi_data, top_launches_data, esc))

    # ─── Section 2: Country Intelligence Cards — removed (sidebar handles this) ──

    # ─── Section 3: Filter Bar (sticky) ──────────────────────────────────
    html_parts.append(_build_filter_bar(enriched, esc, exp_data_for_js))

    # ─── Section 4: Master Expansion Table ───────────────────────────────
    html_parts.append(_build_expansion_table())

    # ─── Section 5: Missed Opportunities (collapsible) ───────────────────
    html_parts.append(_build_missed_opps_section(missed_opps_for_js, esc))

    # ─── Section 6: Entity Performance Matrix (collapsible) ──────────────
    html_parts.append(_build_entity_perf_section(entity_perf, esc))

    # ─── Section 7: Angle Generator ────────────────────────────────────
    html_parts.append(_build_angle_generator(enriched, angle_templates, esc))

    html_parts.append('</div>')  # end exp2-container

    # ─── JavaScript with Embedded Data ──────────────────────────────────────
    html_parts.append(_build_javascript(
        exp_data_for_js,
        country_intel,
        matched_angles,
        angle_templates,
        missed_opps_for_js,
        entity_perf,
        esc
    ))

    html_parts.append('</div><!-- /tab-experimental -->')

    return '\n'.join(html_parts)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS FOR DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def _load_intelligence_report(BASE: Path) -> Optional[Dict[str, Any]]:
    """Load the latest intelligence report if available."""
    report_path = BASE / 'data' / 'intelligence_reports' / 'latest.json'
    if report_path.exists():
        try:
            with open(report_path, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _load_daily_snapshots(BASE: Path, days: int = 30) -> List[Dict[str, Any]]:
    """Load daily snapshots for trend charts."""
    snapshot_path = BASE / 'data' / 'daily_snapshot.jsonl'
    if not snapshot_path.exists():
        return []
    snapshots = []
    try:
        with open(snapshot_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        snapshots.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except Exception:
        pass
    return snapshots[-days:]


def _load_proven_rpc(BASE: Path) -> Dict[str, float]:
    """Load proven RPC lookup for CPC accuracy display."""
    path = BASE / 'data' / 'proven_rpc_lookup.json'
    if path.exists():
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _load_json(p: Path, default=None):
    """Load JSON file safely."""
    if p.exists():
        try:
            with open(p, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return default if default is not None else {}


def _load_jsonl(p: Path) -> List[Dict[str, Any]]:
    """Load JSONL file safely."""
    items = []
    if p.exists():
        try:
            with open(p, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            items.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
    return items


def _get_country_flag(country_code: str) -> str:
    """Get flag emoji for country code."""
    code_map = {
        'US': '🇺🇸', 'CA': '🇨🇦', 'GB': '🇬🇧', 'AU': '🇦🇺', 'NZ': '🇳🇿',
        'DE': '🇩🇪', 'FR': '🇫🇷', 'IT': '🇮🇹', 'ES': '🇪🇸', 'NL': '🇳🇱',
        'JP': '🇯🇵', 'KR': '🇰🇷', 'SG': '🇸🇬', 'MY': '🇲🇾', 'IN': '🇮🇳',
        'BR': '🇧🇷', 'MX': '🇲🇽', 'AR': '🇦🇷', 'ZA': '🇿🇦', 'SE': '🇸🇪',
    }
    return code_map.get(country_code.upper(), '🌐')


def _compute_entity_performance(
    enriched: List[Dict[str, Any]],
    proven_entities: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Compute entity performance metrics."""
    entity_stats = {}

    for row in enriched:
        entity = row.get('entity_name') or row.get('new_value', '')
        if not entity:
            continue

        if entity not in entity_stats:
            entity_stats[entity] = {
                'entity': entity,
                'type': row.get('swapped_slot', ''),
                'status': row.get('entity_status', 'test'),
                'expansions': 0,
                'revenue': 0,
                'proven_revenue': 0,
                'best_vertical': '',
                'vertical_revenue': {},
            }

        stats = entity_stats[entity]
        stats['expansions'] += 1
        stats['revenue'] += row.get('source_revenue', 0) or 0

        if row.get('proven_revenue'):
            stats['proven_revenue'] += row.get('proven_revenue', 0) or 0

        vertical = row.get('vertical', 'unknown')
        stats['vertical_revenue'][vertical] = stats['vertical_revenue'].get(vertical, 0) + (row.get('source_revenue', 0) or 0)

    # Calculate averages and best vertical
    result = []
    for entity, stats in sorted(entity_stats.items(), key=lambda x: x[1]['revenue'], reverse=True):
        if stats['vertical_revenue']:
            stats['best_vertical'] = max(stats['vertical_revenue'], key=stats['vertical_revenue'].get)
        else:
            stats['best_vertical'] = 'N/A'

        revenue_per_keyword = stats['revenue'] / stats['expansions'] if stats['expansions'] > 0 else 0
        avg_rpc = (stats['revenue'] / max(stats['expansions'], 1)) / 1000 if stats['expansions'] > 0 else 0

        result.append({
            'entity': stats['entity'],
            'type': stats['type'],
            'status': stats['status'],
            'expansions': stats['expansions'],
            'revenue': round(stats['revenue'], 2),
            'proven_revenue': round(stats['proven_revenue'], 2),
            'avg_rpc': round(avg_rpc, 4),
            'revenue_per_keyword': round(revenue_per_keyword, 2),
            'best_vertical': stats['best_vertical'],
        })

    return result[:53]  # Top 53 entities


def _assign_trend_directions(enriched: List[Dict[str, Any]], snapshots: List[Dict[str, Any]]) -> None:
    """Assign trend_direction ('up', 'down', 'stable', 'new') to each enriched row.

    Uses daily snapshots to detect if a keyword's score has been trending up or down.
    If no snapshot history, marks as 'new'.
    """
    # Build keyword→scores from recent snapshots
    kw_history: Dict[str, List[float]] = {}
    for snap in snapshots[-14:]:  # Last 14 days
        for kw_data in snap.get('keywords', snap.get('opportunities', [])):
            key = f"{kw_data.get('keyword', '')}|{kw_data.get('country', '')}"
            score = kw_data.get('score_v2', kw_data.get('score', 0))
            if key not in kw_history:
                kw_history[key] = []
            kw_history[key].append(score)

    for row in enriched:
        key = f"{row.get('keyword', '')}|{row.get('country', '')}"
        history = kw_history.get(key, [])

        if len(history) < 2:
            row['trend_direction'] = 'new'
        else:
            recent_avg = sum(history[-3:]) / len(history[-3:])
            older_avg = sum(history[:max(1, len(history)-3)]) / max(1, len(history)-3)
            diff = recent_avg - older_avg

            if diff > 5:
                row['trend_direction'] = 'up'
            elif diff < -5:
                row['trend_direction'] = 'down'
            else:
                row['trend_direction'] = 'stable'


# ─────────────────────────────────────────────────────────────────────────────
# HTML BUILDER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _build_experimental_css() -> str:
    """Build all CSS for the experimental tab, using dashboard CSS variables for theme support."""
    return '''<style>
/* ─── Experimental Tab v2 CSS (theme-aware) ──────────────────────────────────── */

.exp2-container {
    background: var(--bg-base);
    color: var(--text-primary);
    font-family: var(--font-mono);
    font-size: 11px;
}

/* KPI Header */
.exp2-kpi-header {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 16px;
    margin-bottom: 16px;
}

.exp2-kpi-cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
}

.exp2-kpi-card {
    background: var(--bg-raised);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 12px;
    text-align: center;
}

.exp2-kpi-value {
    display: block;
    font-size: 18px;
    font-weight: bold;
    color: var(--accent);
    font-variant-numeric: tabular-nums;
    margin-bottom: 4px;
}

.exp2-kpi-label {
    display: block;
    font-size: 9px;
    color: var(--text-tertiary);
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.exp2-kpi-delta {
    font-size: 9px;
    color: var(--accent);
}

.exp2-kpi-delta.down {
    color: var(--c-red);
}

/* Top Launches Row */
.exp2-top-launches {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 12px;
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
}

.exp2-launch-card {
    background: var(--bg-hover);
    border: 1px solid var(--border-light);
    border-radius: 4px;
    padding: 10px;
}

.exp2-launch-keyword {
    font-weight: bold;
    color: var(--c-amber);
    margin-bottom: 4px;
}

.exp2-launch-metric {
    font-size: 10px;
    color: var(--text-secondary);
    margin: 2px 0;
}

/* Country Intelligence Section */
.exp2-intel-section {
    margin-bottom: 16px;
}

.exp2-intel-title {
    color: var(--c-amber);
    font-size: 12px;
    font-weight: bold;
    margin-bottom: 8px;
    text-transform: uppercase;
}

.exp2-intel-cards {
    display: flex;
    gap: 10px;
    overflow-x: auto;
    padding: 10px 0;
}

.exp2-intel-card {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 10px;
    flex-shrink: 0;
    width: 140px;
    cursor: pointer;
    transition: border-color 0.2s;
}

.exp2-intel-card:hover {
    border-color: var(--accent);
}

.exp2-intel-card.active {
    border-color: var(--accent);
    background: var(--accent-dim);
}

.exp2-intel-flag {
    font-size: 18px;
    margin-bottom: 4px;
}

.exp2-intel-code {
    font-size: 11px;
    font-weight: bold;
    color: var(--accent);
    margin-bottom: 4px;
}

.exp2-intel-stat {
    font-size: 10px;
    color: var(--text-secondary);
    margin: 2px 0;
}

/* Filter Bar */
.exp2-filter-bar {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 12px;
    margin-bottom: 16px;
    position: sticky;
    top: 0;
    z-index: 100;
}

.exp2-filter-row {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    align-items: center;
    margin-bottom: 8px;
}

.exp2-filter-row:last-child {
    margin-bottom: 0;
}

.exp2-search-input,
.exp2-select {
    background: var(--bg-raised);
    border: 1px solid var(--border-light);
    color: var(--text-primary);
    padding: 6px 8px;
    border-radius: 4px;
    font-family: inherit;
    font-size: 11px;
}

.exp2-search-input {
    flex: 1;
    min-width: 150px;
}

.exp2-select {
    min-width: 110px;
}

.exp2-search-input:focus,
.exp2-select:focus {
    outline: none;
    border-color: var(--accent);
    background: var(--accent-dim);
}

.exp2-toggle-group {
    display: flex;
    gap: 4px;
}

.exp2-toggle-btn {
    background: var(--bg-raised);
    border: 1px solid var(--border-light);
    color: var(--text-secondary);
    padding: 6px 10px;
    border-radius: 4px;
    cursor: pointer;
    font-family: inherit;
    font-size: 10px;
    text-transform: uppercase;
    transition: all 0.2s;
}

.exp2-toggle-btn:hover {
    border-color: var(--text-muted);
    color: var(--text-primary);
}

.exp2-toggle-btn.active {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--bg-base);
}

.exp2-filter-pills {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
}

.exp2-filter-pill {
    background: var(--bg-hover);
    border: 1px solid var(--border-light);
    color: var(--text-primary);
    padding: 4px 8px;
    border-radius: 3px;
    font-size: 10px;
}

.exp2-filter-pill .exp2-remove {
    margin-left: 4px;
    cursor: pointer;
    color: var(--text-secondary);
}

.exp2-filter-pill .exp2-remove:hover {
    color: var(--c-red);
}

/* Table */
.exp2-table-container {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow-x: auto;
    margin-bottom: 16px;
}

.exp2-table {
    width: 100%;
    border-collapse: collapse;
}

.exp2-table thead {
    background: var(--bg-raised);
    position: sticky;
    top: 0;
    z-index: 99;
}

.exp2-table th {
    padding: 10px 8px;
    text-align: left;
    font-weight: bold;
    color: var(--c-amber);
    border-bottom: 2px solid var(--border);
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
}

.exp2-table th:hover {
    background: var(--bg-hover);
}

.exp2-table th.sortable::after {
    content: ' ⇅';
    font-size: 8px;
    color: var(--text-muted);
}

.exp2-table th.exp2-sorted-asc::after {
    content: ' ▲';
    color: var(--accent);
}

.exp2-table th.exp2-sorted-desc::after {
    content: ' ▼';
    color: var(--accent);
}

.exp2-table tbody tr {
    border-bottom: 1px solid var(--border);
}

.exp2-table tbody tr:hover {
    background: var(--bg-raised);
}

.exp2-table tbody tr.exp2-row-proven {
    background: var(--accent-dim);
    border-left: 2px solid var(--accent);
}

.exp2-table tbody tr.exp2-row-high {
    background: var(--c-amber-dim);
    border-left: 2px solid var(--c-amber);
}

.exp2-table tbody tr.exp2-row-proven.exp2-row-high {
    background: var(--accent-dim);
    border-left: 2px solid var(--accent);
}

.exp2-detail-row td {
    padding: 0 !important;
}

.exp2-detail-field {
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 8px;
    background: var(--bg-surface);
}

.exp2-table tbody tr {
    cursor: pointer;
}

.exp2-table td {
    padding: 8px;
    color: var(--text-primary);
    white-space: nowrap;
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
}

.exp2-table td.keyword {
    color: var(--accent);
    font-weight: bold;
    white-space: normal;
}

.exp2-table td.metric {
    text-align: right;
    font-variant-numeric: tabular-nums;
}

/* Pagination */
.exp2-pagination {
    display: flex;
    gap: 8px;
    justify-content: center;
    align-items: center;
    margin-top: 12px;
    padding: 12px;
    background: var(--bg-raised);
    border-radius: 4px;
}

.exp2-page-btn {
    background: var(--bg-hover);
    border: 1px solid var(--border-light);
    color: var(--text-secondary);
    padding: 4px 8px;
    border-radius: 3px;
    cursor: pointer;
    font-size: 10px;
}

.exp2-page-btn:hover:not(:disabled) {
    border-color: var(--accent);
    color: var(--accent);
}

.exp2-page-btn:disabled {
    opacity: 0.3;
    cursor: not-allowed;
}

.exp2-page-info {
    color: var(--text-tertiary);
    font-size: 10px;
}

/* Row Detail Panel */
.exp2-detail-panel {
    background: var(--bg-raised);
    border-top: 1px solid var(--border);
    padding: 12px;
    display: none;
    grid-column: 1 / -1;
}

.exp2-detail-panel.open {
    display: block;
}

.exp2-detail-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
}

.exp2-detail-field {
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 8px;
}

.exp2-detail-label {
    font-size: 9px;
    color: var(--text-tertiary);
    text-transform: uppercase;
    margin-bottom: 2px;
}

.exp2-detail-value {
    color: var(--text-primary);
    font-size: 11px;
}

/* Collapsible Sections */
.exp2-collapsible {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    margin-bottom: 16px;
    overflow: hidden;
}

.exp2-collapsible-header {
    background: var(--bg-raised);
    padding: 12px;
    cursor: pointer;
    user-select: none;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-weight: bold;
    color: var(--c-amber);
}

.exp2-collapsible-header:hover {
    background: var(--bg-hover);
}

.exp2-collapsible-icon {
    transition: transform 0.2s;
}

.exp2-collapsible.open .exp2-collapsible-icon {
    transform: rotate(180deg);
}

.exp2-collapsible-content {
    max-height: 0;
    overflow: hidden;
    transition: max-height 0.3s ease-out;
}

.exp2-collapsible.open .exp2-collapsible-content {
    max-height: 2000px;
    transition: max-height 0.3s ease-in;
}

.exp2-collapsible-content {
    padding: 12px;
}

/* Missed Opportunities */
.exp2-missed-priority {
    font-size: 9px;
    font-weight: bold;
    padding: 2px 4px;
    border-radius: 2px;
    text-transform: uppercase;
}

.exp2-missed-priority.critical {
    background: var(--c-red);
    color: #fff;
}

.exp2-missed-priority.high {
    background: var(--c-amber);
    color: var(--bg-base);
}

.exp2-missed-priority.medium {
    background: var(--text-muted);
    color: var(--text-primary);
}

/* Angle Generator */
.exp2-angle-gen {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 12px;
    margin-bottom: 16px;
}

.exp2-angle-controls {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 8px;
    margin-bottom: 12px;
}

.exp2-angle-results {
    margin-top: 12px;
}

.exp2-angle-title {
    background: var(--bg-raised);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 8px;
    margin-bottom: 8px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.exp2-angle-text {
    color: var(--text-primary);
    font-size: 11px;
}

.exp2-angle-stars {
    color: var(--c-amber);
    font-size: 10px;
    margin-left: 8px;
}

.exp2-copy-btn {
    background: var(--accent);
    color: var(--bg-base);
    border: none;
    padding: 4px 8px;
    border-radius: 3px;
    cursor: pointer;
    font-size: 9px;
    font-weight: bold;
    margin-left: 8px;
}

.exp2-copy-btn:hover {
    opacity: 0.85;
}

.exp2-gen-btn {
    background: var(--accent);
    color: var(--bg-base);
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 11px;
    font-weight: bold;
    grid-column: 1 / -1;
}

.exp2-gen-btn:hover {
    opacity: 0.85;
}

/* Group Header Rows */
.exp2-group-header {
    background: var(--bg-hover) !important;
    border-left: 3px solid var(--c-amber);
    cursor: pointer;
}

.exp2-group-header td {
    padding: 10px 12px !important;
    color: var(--text-primary);
    font-size: 11px;
}

.exp2-group-header:hover {
    background: var(--bg-elevated) !important;
}

.exp2-group-toggle {
    font-size: 9px;
    color: var(--c-amber);
    margin-right: 4px;
}

/* Country Sidebar Layout */
.exp2-country-layout {
    display: flex;
    gap: 0;
    min-height: 500px;
}

.exp2-country-sidebar {
    width: 240px;
    min-width: 240px;
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 6px 0 0 6px;
    overflow-y: auto;
    max-height: 70vh;
    flex-shrink: 0;
}

.exp2-country-sidebar-title {
    padding: 10px 12px;
    font-size: 11px;
    font-weight: bold;
    color: var(--c-amber);
    text-transform: uppercase;
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--bg-raised);
    z-index: 10;
}

.exp2-country-item {
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    cursor: pointer;
    transition: background 0.15s;
    display: flex;
    flex-direction: column;
    gap: 3px;
}

.exp2-country-item:hover {
    background: var(--bg-hover);
}

.exp2-country-item.active {
    background: var(--accent-dim);
    border-left: 3px solid var(--accent);
}

.exp2-country-item-header {
    display: flex;
    align-items: center;
    gap: 6px;
}

.exp2-country-item-flag {
    font-size: 16px;
}

.exp2-country-item-code {
    font-weight: bold;
    color: var(--accent);
    font-size: 12px;
}

.exp2-country-item-count {
    margin-left: auto;
    font-size: 10px;
    color: var(--text-secondary);
    background: var(--bg-raised);
    padding: 1px 6px;
    border-radius: 8px;
}

.exp2-country-item-stats {
    display: flex;
    gap: 8px;
    font-size: 9px;
    color: var(--text-tertiary);
}

.exp2-country-item-stat {
    white-space: nowrap;
}

.exp2-country-item-stat.positive {
    color: var(--accent);
}

.exp2-country-item-stat.negative {
    color: var(--c-red);
}

.exp2-country-main {
    flex: 1;
    min-width: 0;
    overflow: hidden;
}

.exp2-country-main .exp2-table-container {
    border-radius: 0 6px 6px 0;
    border-left: none;
}

.exp2-country-header-bar {
    background: var(--bg-raised);
    border: 1px solid var(--border);
    border-bottom: none;
    border-radius: 0 6px 0 0;
    padding: 10px 14px;
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 12px;
    color: var(--text-primary);
}

.exp2-country-header-bar .flag { font-size: 20px; }
.exp2-country-header-bar .name { font-weight: bold; color: var(--accent); }
.exp2-country-header-bar .info { color: var(--text-secondary); font-size: 10px; margin-left: auto; }

.exp2-country-mode-active .exp2-intel-section {
    display: none;
}

/* Responsive */
@media (max-width: 1200px) {
    .exp2-kpi-cards {
        grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
    }
}

@media (max-width: 768px) {
    .exp2-filter-row {
        flex-direction: column;
    }

    .exp2-search-input,
    .exp2-select {
        width: 100%;
    }

    .exp2-country-layout {
        flex-direction: column;
    }

    .exp2-country-sidebar {
        width: 100%;
        min-width: unset;
        max-height: 200px;
        border-radius: 6px 6px 0 0;
    }

    .exp2-country-main .exp2-table-container {
        border-radius: 0 0 6px 6px;
        border-left: 1px solid var(--border);
    }
}
</style>'''


def _build_kpi_header(kpi_data: Dict[str, Any], top_launches: List[Dict[str, Any]], esc: Callable) -> str:
    """Build the KPI header section."""
    html = ['<div class="exp2-kpi-header">']

    # 8 KPI Cards
    html.append('<div class="exp2-kpi-cards">')

    # Card 1: Total Expansions
    total = kpi_data['total_expansions']
    delta = kpi_data['delta_expansions']
    delta_str = ''
    if delta is not None:
        delta_class = 'down' if delta < 0 else ''
        delta_arrow = '↓' if delta < 0 else '↑'
        delta_str = f'<div class="exp2-kpi-delta {delta_class}">{delta_arrow} {abs(delta)}</div>'

    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">{total}</span>'
        f'<span class="exp2-kpi-label">Total Expansions</span>'
        f'{delta_str}'
        f'</div>'
    )

    # Card 2: Ready to Launch
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">{kpi_data["ready_to_launch"]}</span>'
        f'<span class="exp2-kpi-label">Ready to Launch</span>'
        f'</div>'
    )

    # Card 3: Est. Revenue Potential
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">${kpi_data["est_revenue_potential"]:,.0f}</span>'
        f'<span class="exp2-kpi-label">Est. Revenue</span>'
        f'</div>'
    )

    # Card 4: Missed Revenue
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">${kpi_data["missed_revenue"]:,.0f}</span>'
        f'<span class="exp2-kpi-label">Missed Revenue</span>'
        f'</div>'
    )

    # Card 5: CPC Accuracy
    accuracy = kpi_data.get('cpc_accuracy', 0)
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">{accuracy:.0%}</span>'
        f'<span class="exp2-kpi-label">CPC Accuracy</span>'
        f'</div>'
    )

    # Card 6: Budget Used
    budget_pct = min(kpi_data['budget_used'] * 100, 100)
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">{budget_pct:.1f}%</span>'
        f'<span class="exp2-kpi-label">Budget Used</span>'
        f'</div>'
    )

    # Card 7: Track A/B Split
    track_a = kpi_data['track_a_count']
    track_b = kpi_data['track_b_count']
    total_tracks = track_a + track_b
    track_a_pct = (track_a / total_tracks * 100) if total_tracks > 0 else 0
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">{track_a_pct:.0f}%</span>'
        f'<span class="exp2-kpi-label">Track A</span>'
        f'</div>'
    )

    # Card 8: Top Vertical
    top_v = kpi_data.get('top_vertical', 'N/A')
    html.append(
        f'<div class="exp2-kpi-card">'
        f'<span class="exp2-kpi-value">{esc(str(top_v)[:10])}</span>'
        f'<span class="exp2-kpi-label">Top Vertical</span>'
        f'</div>'
    )

    html.append('</div>')  # end kpi-cards

    # Top 3 Launch Candidates
    if top_launches:
        html.append('<div class="exp2-top-launches">')
        for launch in top_launches:
            html.append(
                f'<div class="exp2-launch-card">'
                f'<div class="exp2-launch-keyword">{launch["country_flag"]} {esc(launch["keyword"])}</div>'
                f'<div class="exp2-launch-metric">Margin: ${launch["margin"]}</div>'
                f'<div class="exp2-launch-metric">Score: {launch["score_v2"]}</div>'
                f'<div class="exp2-launch-metric">Confidence: {launch["confidence"]:.0f}%</div>'
                f'</div>'
            )
        html.append('</div>')

    html.append('</div>')  # end kpi-header

    return '\n'.join(html)


def _build_country_intel_section(country_intel: Dict[str, Any], esc: Callable) -> str:
    """Build the country intelligence cards section."""
    if not country_intel:
        return ''

    html = [
        '<div class="exp2-intel-section">',
        '<div class="exp2-intel-title">Country Intelligence</div>',
        '<div class="exp2-intel-cards" id="exp2-intel-cards">'
    ]

    for code, data in list(country_intel.items())[:20]:
        flag = _get_country_flag(code)
        kw_count = data.get('keyword_count', 0)
        revenue = data.get('revenue', 0)
        avg_rpc = data.get('avg_rpc', 0)
        coverage = data.get('coverage_pct', 0)
        verticals = data.get('top_verticals', [])[:2]
        seasonal = data.get('seasonal_hook', '')

        seasonal_html = f'<div class="exp2-intel-stat" style="color:var(--c-amber);">{seasonal}</div>' if seasonal else ''

        html.append(
            f'<div class="exp2-intel-card" data-country="{code}" onclick="expFilterByCountry(\'{code}\')">'
            f'<div class="exp2-intel-flag">{flag}</div>'
            f'<div class="exp2-intel-code">{code}</div>'
            f'<div class="exp2-intel-stat">{kw_count} keywords</div>'
            f'<div class="exp2-intel-stat">${revenue:,.0f} rev</div>'
            f'<div class="exp2-intel-stat">{avg_rpc:.4f} RPC</div>'
            f'<div class="exp2-intel-stat">{coverage:.0f}% coverage</div>'
            f'{seasonal_html}'
            f'</div>'
        )

    html.append('</div>')  # end intel-cards
    html.append('</div>')  # end intel-section

    return '\n'.join(html)


def _build_filter_bar(enriched: List[Dict[str, Any]], esc: Callable, exp_data_for_js: List[Dict[str, Any]] = None) -> str:
    """Build the filter bar."""
    # Extract unique values
    countries = sorted(set(r.get('country', '') for r in enriched if r.get('country')))
    verticals = sorted(set(r.get('vertical', '') for r in enriched if r.get('vertical')))
    entity_types = sorted(set(r.get('entity_type', '') for r in enriched if r.get('entity_type')))
    statuses = sorted(set(r.get('status', '') for r in enriched if r.get('status')))
    cpc_tracks = sorted(set(r.get('cpc_track', '') for r in enriched if r.get('cpc_track')))

    html = [
        '<div class="exp2-filter-bar">',
        '<div class="exp2-filter-row">',
        '<input type="text" id="exp2-search" class="exp2-search-input" placeholder="Search keywords...">',
    ]

    # Country dropdown
    html.append('<select id="exp2-filter-country" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Countries</option>')
    for c in countries:
        html.append(f'<option value="{c}">{c}</option>')
    html.append('</select>')

    # Vertical dropdown
    html.append('<select id="exp2-filter-vertical" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Verticals</option>')
    for v in verticals:
        html.append(f'<option value="{v}">{esc(v)}</option>')
    html.append('</select>')

    # Entity Type dropdown
    html.append('<select id="exp2-filter-entity-type" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Entity Types</option>')
    for et in entity_types:
        html.append(f'<option value="{et}">{esc(et)}</option>')
    html.append('</select>')

    # Status dropdown
    html.append('<select id="exp2-filter-status" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Statuses</option>')
    for s in statuses:
        html.append(f'<option value="{s}">{esc(s)}</option>')
    html.append('</select>')

    # CPC Track dropdown
    html.append('<select id="exp2-filter-cpc-track" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Tracks</option>')
    for t in cpc_tracks:
        html.append(f'<option value="{t}">{esc(t)}</option>')
    html.append('</select>')

    # Competitor Angle filter
    html.append('<select id="exp2-filter-fb-intel" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Angles</option>')
    html.append('<option value="yes">Has Competitor Angle</option>')
    html.append('<option value="no">No Competitor Angle</option>')
    html.append('</select>')

    # Angle Type filter
    _js_data = exp_data_for_js or []
    angle_types = sorted({r.get('top_angle', '') for r in _js_data if r.get('top_angle')})
    html.append('<select id="exp2-filter-angle-type" class="exp2-select" onchange="expApplyFilters()">')
    html.append('<option value="">All Angle Types</option>')
    for at in angle_types:
        html.append(f'<option value="{esc(at)}">{esc(at.replace("_", " "))}</option>')
    html.append('</select>')

    html.append('</div>')  # end filter-row

    # Group and column toggles
    html.append(
        '<div class="exp2-filter-row">'
        '<div class="exp2-toggle-group">'
        '<button class="exp2-toggle-btn active" onclick="expSetGrouping(\'flat\')" data-group="flat">Flat</button>'
        '<button class="exp2-toggle-btn" onclick="expSetGrouping(\'template\')" data-group="template">Template</button>'
        '<button class="exp2-toggle-btn" onclick="expSetGrouping(\'vertical\')" data-group="vertical">Vertical</button>'
        '<button class="exp2-toggle-btn" onclick="expSetGrouping(\'country\')" data-group="country">Country</button>'
        '<button class="exp2-toggle-btn" onclick="expSetGrouping(\'entity_type\')" data-group="entity_type">Entity Type</button>'
        '<button class="exp2-toggle-btn" onclick="expSetGrouping(\'status\')" data-group="status">Status</button>'
        '</div>'
        '<button class="exp2-toggle-btn" onclick="expToggleColumnViz()" title="Show/hide columns">⚙️ Columns</button>'
        '<button class="exp2-toggle-btn" onclick="expExportCSV()">📥 Export</button>'
        '</div>'
    )

    html.append('<div id="exp2-filter-pills" class="exp2-filter-pills"></div>')
    html.append('</div>')  # end filter-bar

    return '\n'.join(html)


def _build_expansion_table() -> str:
    """Build the expansion table structure."""
    return '''<div id="exp2-table-area">
<div class="exp2-table-container">
    <table class="exp2-table" id="exp2-table">
        <thead>
            <tr>
                <th data-col="keyword" class="sortable" onclick="expSort('keyword')">Keyword</th>
                <th data-col="country" class="sortable" onclick="expSort('country')">Country</th>
                <th data-col="margin" class="sortable" onclick="expSort('margin')">Margin</th>
                <th data-col="score_v2" class="sortable exp2-sorted-desc" onclick="expSort('score_v2')">Score</th>
                <th data-col="cpc" class="sortable" onclick="expSort('cpc')">CPC</th>
                <th data-col="proven_rpc" class="sortable" onclick="expSort('proven_rpc')">Proven RPC</th>
                <th data-col="entity_name" class="sortable" onclick="expSort('entity_name')">Entity</th>
                <th data-col="status" class="sortable" onclick="expSort('status')">Status</th>
                <th data-col="vertical" class="sortable" onclick="expSort('vertical')">Vertical</th>
                <th data-col="trend_direction" class="sortable" onclick="expSort('trend_direction')">Trend</th>
                <th data-col="top_angle" class="sortable" onclick="expSort('top_angle')">Top Angle</th>
                <th data-col="template" class="sortable hidden" style="display:none;" onclick="expSort('template')">Template</th>
                <th data-col="entity_type" class="sortable hidden" style="display:none;" onclick="expSort('entity_type')">Entity Type</th>
                <th data-col="cpc_track_label" class="sortable hidden" style="display:none;" onclick="expSort('cpc_track_label')">CPC Track</th>
                <th data-col="proven_revenue" class="sortable hidden" style="display:none;" onclick="expSort('proven_revenue')">Proven Rev</th>
                <th data-col="desktop_rpc" class="sortable hidden" style="display:none;" onclick="expSort('desktop_rpc')">Desktop RPC</th>
                <th data-col="mobile_rpc" class="sortable hidden" style="display:none;" onclick="expSort('mobile_rpc')">Mobile RPC</th>
                <th data-col="device_skew" class="sortable hidden" style="display:none;" onclick="expSort('device_skew')">Device Skew</th>
                <th data-col="entity_density" class="sortable hidden" style="display:none;" onclick="expSort('entity_density')">Density</th>
            </tr>
        </thead>
        <tbody id="exp2-table-body">
        </tbody>
    </table>
</div>

<div class="exp2-pagination">
    <button class="exp2-page-btn" onclick="expPrevPage()">← Prev</button>
    <span class="exp2-page-info"><span id="exp2-page-current">1</span> / <span id="exp2-page-total">1</span></span>
    <button class="exp2-page-btn" onclick="expNextPage()">Next →</button>
</div>
</div>'''


def _build_missed_opps_section(missed_opps: List[Dict[str, Any]], esc: Callable) -> str:
    """Build the missed opportunities section."""
    html = [
        '<div class="exp2-collapsible">',
        '<div class="exp2-collapsible-header">',
        f'<span>Missed Opportunities ({len(missed_opps)})</span>',
        '<span class="exp2-collapsible-icon">▼</span>',
        '</div>',
        '<div class="exp2-collapsible-content">',
        '<table class="exp2-table">',
        '<thead><tr>',
        '<th>Keyword</th>',
        '<th>Revenue</th>',
        '<th>RPC</th>',
        '<th>Priority</th>',
        '<th>Action</th>',
        '</tr></thead>',
        '<tbody>'
    ]

    for opp in missed_opps:
        priority_class = f"exp2-missed-priority {opp['priority']}"
        html.append(
            f'<tr>'
            f'<td>{esc(opp["keyword"])}</td>'
            f'<td class="metric">${opp["revenue"]:,.2f}</td>'
            f'<td class="metric">{opp["rpc"]:.4f}</td>'
            f'<td><span class="{priority_class}">{opp["priority"].upper()}</span></td>'
            f'<td><button class="exp2-copy-btn" onclick="expCopyText(\'{esc(opp["keyword"])}\')">Copy</button></td>'
            f'</tr>'
        )

    html.extend([
        '</tbody>',
        '</table>',
        '</div>',
        '</div>'
    ])

    return '\n'.join(html)


def _build_entity_perf_section(entity_perf: List[Dict[str, Any]], esc: Callable) -> str:
    """Build the entity performance section."""
    html = [
        '<div class="exp2-collapsible">',
        '<div class="exp2-collapsible-header">',
        f'<span>Entity Performance ({len(entity_perf)})</span>',
        '<span class="exp2-collapsible-icon">▼</span>',
        '</div>',
        '<div class="exp2-collapsible-content">',
        '<table class="exp2-table">',
        '<thead><tr>',
        '<th>Entity</th>',
        '<th>Type</th>',
        '<th>Status</th>',
        '<th>Expansions</th>',
        '<th>Revenue</th>',
        '<th>Avg RPC</th>',
        '<th>Rev/Keyword</th>',
        '<th>Best Vertical</th>',
        '</tr></thead>',
        '<tbody>'
    ]

    for entity in entity_perf:
        html.append(
            f'<tr>'
            f'<td>{esc(entity["entity"])}</td>'
            f'<td>{esc(entity["type"])}</td>'
            f'<td>{esc(entity["status"])}</td>'
            f'<td class="metric">{entity["expansions"]}</td>'
            f'<td class="metric">${entity["revenue"]:,.2f}</td>'
            f'<td class="metric">{entity["avg_rpc"]:.4f}</td>'
            f'<td class="metric">${entity["revenue_per_keyword"]:.2f}</td>'
            f'<td>{esc(entity["best_vertical"])}</td>'
            f'</tr>'
        )

    html.extend([
        '</tbody>',
        '</table>',
        '</div>',
        '</div>'
    ])

    return '\n'.join(html)


def _build_angle_generator(enriched: List[Dict[str, Any]], angle_templates: Dict[str, Any], esc: Callable) -> str:
    """Build the angle generator section."""
    # Unique values for dropdowns
    verticals = sorted(set(r.get('vertical', '') for r in enriched if r.get('vertical')))
    countries = sorted(set(r.get('country', '') for r in enriched if r.get('country')))
    entity_names = sorted(set(r.get('entity_name') or r.get('entity_type', '') for r in enriched))

    html = [
        '<div class="exp2-angle-gen">',
        '<div class="exp2-intel-title">Angle Generator</div>',
        '<div class="exp2-angle-controls">'
    ]

    # Vertical dropdown
    html.append('<select id="exp2-angle-vertical" class="exp2-select">')
    html.append('<option value="">Select Vertical</option>')
    for v in verticals:
        html.append(f'<option value="{v}">{esc(v)}</option>')
    html.append('</select>')

    # Entity dropdown
    html.append('<select id="exp2-angle-entity" class="exp2-select">')
    html.append('<option value="">Select Entity</option>')
    for e in entity_names[:20]:  # Limit to 20
        html.append(f'<option value="{e}">{esc(e[:30])}</option>')
    html.append('</select>')

    # Country dropdown
    html.append('<select id="exp2-angle-country" class="exp2-select">')
    html.append('<option value="">Select Country</option>')
    for c in countries:
        html.append(f'<option value="{c}">{c}</option>')
    html.append('</select>')

    html.append('<button class="exp2-gen-btn" onclick="expGenerateAngles()">Generate Angles</button>')
    html.append('</div>')
    html.append('<div class="exp2-angle-results" id="exp2-angle-results"></div>')
    html.append('</div>')

    return '\n'.join(html)


# ─────────────────────────────────────────────────────────────────────────────
# JAVASCRIPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_javascript(
    exp_data: List[Dict[str, Any]],
    country_intel: Dict[str, Any],
    matched_angles: Dict[str, Any],
    angle_templates: Dict[str, Any],
    missed_opps: List[Dict[str, Any]],
    entity_perf: List[Dict[str, Any]],
    esc: Callable
) -> str:
    """Build the complete JavaScript section."""

    exp_data_json = json.dumps(exp_data, ensure_ascii=False, separators=(',', ':'))
    country_intel_json = json.dumps(country_intel, ensure_ascii=False, separators=(',', ':'))
    matched_angles_json = json.dumps(matched_angles, ensure_ascii=False, separators=(',', ':'))
    angle_templates_json = json.dumps(angle_templates, ensure_ascii=False, separators=(',', ':'))
    missed_opps_json = json.dumps(missed_opps, ensure_ascii=False, separators=(',', ':'))
    entity_perf_json = json.dumps(entity_perf, ensure_ascii=False, separators=(',', ':'))

    return f'''<script>
// Embedded data for experimental tab v2
const EXP_DATA = {exp_data_json};
const COUNTRY_INTEL = {country_intel_json};
const MATCHED_ANGLES = {matched_angles_json};
const ANGLE_TEMPLATES = {angle_templates_json};
const MISSED_OPPS = {missed_opps_json};
const ENTITY_PERF = {entity_perf_json};

// State
let expState = {{
    filteredData: [...EXP_DATA],
    currentPage: 0,
    pageSize: 50,
    sortCol: 'score_v2',
    sortAsc: false,
    grouping: 'flat',
    selectedCountry: null,
    visibleCols: new Set(['keyword', 'country', 'margin', 'score_v2', 'cpc', 'proven_rpc', 'entity_name', 'status', 'vertical', 'trend_direction', 'top_angle']),
    filters: {{
        search: '',
        country: '',
        vertical: '',
        entity_type: '',
        status: '',
        cpc_track: '',
        has_fb_intel: '',
        angle_type: ''
    }}
}};

// Initialize
document.addEventListener('DOMContentLoaded', function() {{
    const searchInput = document.getElementById('exp2-search');
    if (searchInput) {{
        searchInput.addEventListener('input', function(e) {{
            clearTimeout(searchInput._debounceTimer);
            searchInput._debounceTimer = setTimeout(() => {{
                expState.filters.search = e.target.value.toLowerCase();
                expState.currentPage = 0;
                expApplyFilters();
            }}, 200);
        }});
    }}

    // Make collapsible sections work
    document.querySelectorAll('.exp2-collapsible-header').forEach(header => {{
        header.addEventListener('click', function() {{
            this.parentElement.classList.toggle('open');
        }});
    }});

    expLoadHashState();
    expRender();
}});

// Main filter function
function expApplyFilters() {{
    expState.filters.country = document.getElementById('exp2-filter-country')?.value || '';
    expState.filters.vertical = document.getElementById('exp2-filter-vertical')?.value || '';
    expState.filters.entity_type = document.getElementById('exp2-filter-entity-type')?.value || '';
    expState.filters.status = document.getElementById('exp2-filter-status')?.value || '';
    expState.filters.cpc_track = document.getElementById('exp2-filter-cpc-track')?.value || '';
    expState.filters.has_fb_intel = document.getElementById('exp2-filter-fb-intel')?.value || '';
    expState.filters.angle_type = document.getElementById('exp2-filter-angle-type')?.value || '';

    expState.filteredData = EXP_DATA.filter(row => {{
        if (expState.filters.search && row.keyword.toLowerCase().indexOf(expState.filters.search) === -1) return false;

        // Support both single-value and array (multi-select) filters
        const checkFilter = (filterVal, rowVal) => {{
            if (!filterVal) return true;
            if (Array.isArray(filterVal)) return filterVal.length === 0 || filterVal.includes(rowVal);
            return filterVal === rowVal;
        }};

        if (!checkFilter(expState.filters.country, row.country)) return false;
        if (!checkFilter(expState.filters.vertical, row.vertical)) return false;
        if (!checkFilter(expState.filters.entity_type, row.entity_type)) return false;
        if (!checkFilter(expState.filters.status, row.status)) return false;
        if (!checkFilter(expState.filters.cpc_track, row.cpc_track)) return false;
        if (expState.filters.has_fb_intel === 'yes' && !row.has_fb_intel) return false;
        if (expState.filters.has_fb_intel === 'no' && row.has_fb_intel) return false;
        if (expState.filters.angle_type && row.top_angle !== expState.filters.angle_type) return false;

        return true;
    }});

    expState.currentPage = 0;
    expUpdateFilterPills();
    expSaveHashState();
    expRender();

    if (expState.grouping === 'country') {{
        expRebuildCountrySidebar();
    }}
}}

// Update filter pills display
function expUpdateFilterPills() {{
    const container = document.getElementById('exp2-filter-pills');
    if (!container) return;

    const pills = [];
    for (let key in expState.filters) {{
        if (expState.filters[key]) {{
            const label = key.charAt(0).toUpperCase() + key.slice(1);
            pills.push(`<div class="exp2-filter-pill">${{label}}: ${{expState.filters[key]}} <span class="exp2-remove" onclick="expClearFilter('${{key}}')">✕</span></div>`);
        }}
    }}

    container.innerHTML = pills.join('');
}}

function expClearFilter(key) {{
    expState.filters[key] = '';
    document.getElementById(`exp2-filter-${{key}}`)?.addEventListener('change', () => {{}});
    const elem = document.getElementById(`exp2-filter-${{key}}`);
    if (elem) elem.value = '';
    expApplyFilters();
}}

// Sort by column
function expSort(col) {{
    if (expState.sortCol === col) {{
        expState.sortAsc = !expState.sortAsc;
    }} else {{
        expState.sortCol = col;
        expState.sortAsc = false;
    }}

    const mult = expState.sortAsc ? 1 : -1;
    expState.filteredData.sort((a, b) => {{
        let aVal = a[col] ?? 0;
        let bVal = b[col] ?? 0;

        if (typeof aVal === 'string') {{
            return mult * aVal.localeCompare(bVal);
        }}
        return mult * (bVal - aVal);
    }});

    // Update sort indicators on headers
    document.querySelectorAll('#exp2-table th.sortable').forEach(th => {{
        th.classList.remove('exp2-sorted-asc', 'exp2-sorted-desc');
        if (th.dataset.col === col) {{
            th.classList.add(expState.sortAsc ? 'exp2-sorted-asc' : 'exp2-sorted-desc');
        }}
    }});

    expState.currentPage = 0;
    expSaveHashState();
    expRender();
}}

// Set grouping mode
function expSetGrouping(mode) {{
    const prevMode = expState.grouping;
    expState.grouping = mode;
    document.querySelectorAll('.exp2-toggle-btn[data-group]').forEach(btn => {{
        btn.classList.remove('active');
    }});
    document.querySelector(`.exp2-toggle-btn[data-group="${{mode}}"]`)?.classList.add('active');

    if (mode === 'country' && prevMode !== 'country') {{
        expActivateCountryMode();
    }} else if (mode !== 'country' && prevMode === 'country') {{
        expDeactivateCountryMode();
    }}

    expState.currentPage = 0;
    expRender();
}}

// Country sidebar mode
function expActivateCountryMode() {{
    const container = document.querySelector('.exp2-container');
    if (container) container.classList.add('exp2-country-mode-active');

    // Clear country dropdown filter to avoid double-filtering
    expState.filters.country = '';
    const countryEl = document.getElementById('exp2-filter-country');
    if (countryEl) countryEl.value = '';

    const tableArea = document.getElementById('exp2-table-area');
    if (!tableArea) return;

    // Create layout wrapper
    const layout = document.createElement('div');
    layout.className = 'exp2-country-layout';
    layout.id = 'exp2-country-layout';

    // Create sidebar
    const sidebar = document.createElement('div');
    sidebar.className = 'exp2-country-sidebar';
    sidebar.id = 'exp2-country-sidebar';
    sidebar.innerHTML = '<div class="exp2-country-sidebar-title">Countries</div>';

    // Create main panel wrapper
    const main = document.createElement('div');
    main.className = 'exp2-country-main';
    main.id = 'exp2-country-main';

    // Move table area into main panel
    tableArea.parentNode.insertBefore(layout, tableArea);
    layout.appendChild(sidebar);
    layout.appendChild(main);
    main.appendChild(tableArea);

    // Build sidebar items and auto-select first
    expRebuildCountrySidebar();
}}

function expDeactivateCountryMode() {{
    const container = document.querySelector('.exp2-container');
    if (container) container.classList.remove('exp2-country-mode-active');

    const layout = document.getElementById('exp2-country-layout');
    const tableArea = document.getElementById('exp2-table-area');
    if (layout && tableArea) {{
        layout.parentNode.insertBefore(tableArea, layout);
        layout.remove();
    }}

    expState.selectedCountry = null;
}}

function expRebuildCountrySidebar() {{
    const sidebar = document.getElementById('exp2-country-sidebar');
    if (!sidebar) return;

    // Compute country stats from current filtered data
    const countryMap = {{}};
    expState.filteredData.forEach(row => {{
        const c = row.country;
        if (!c) return;
        if (!countryMap[c]) countryMap[c] = {{ code: c, flag: row.country_flag, count: 0, margin: 0, proven: 0 }};
        countryMap[c].count++;
        countryMap[c].margin += (row.margin || 0);
        if (row.proven_exact) countryMap[c].proven++;
    }});

    // Sort by total margin descending
    const countries = Object.values(countryMap).sort((a, b) => b.margin - a.margin);

    // Clear sidebar (keep title)
    sidebar.innerHTML = '<div class="exp2-country-sidebar-title">Countries (' + countries.length + ')</div>';

    countries.forEach(c => {{
        const item = document.createElement('div');
        item.className = 'exp2-country-item';
        item.dataset.country = c.code;

        const marginClass = c.margin >= 0 ? 'positive' : 'negative';
        const marginSign = c.margin >= 0 ? '$' : '-$';
        const marginAbs = Math.abs(c.margin).toFixed(0);

        item.innerHTML = `
            <div class="exp2-country-item-header">
                <span class="exp2-country-item-flag">${{c.flag}}</span>
                <span class="exp2-country-item-code">${{c.code}}</span>
                <span class="exp2-country-item-count">${{c.count}}</span>
            </div>
            <div class="exp2-country-item-stats">
                <span class="exp2-country-item-stat ${{marginClass}}">${{marginSign}}${{marginAbs}}</span>
                <span class="exp2-country-item-stat">${{c.proven}} proven</span>
            </div>
        `;

        item.addEventListener('click', () => expSelectCountry(c.code));
        sidebar.appendChild(item);
    }});

    // Auto-select first country, or keep current if still valid
    if (countries.length > 0) {{
        const target = (expState.selectedCountry && countryMap[expState.selectedCountry])
            ? expState.selectedCountry
            : countries[0].code;
        expSelectCountry(target);
    }}
}}

function expSelectCountry(code) {{
    expState.selectedCountry = code;
    expState.currentPage = 0;

    // Update sidebar highlight
    document.querySelectorAll('.exp2-country-item').forEach(item => {{
        item.classList.toggle('active', item.dataset.country === code);
    }});

    // Scroll active item into view in sidebar
    const active = document.querySelector('.exp2-country-item.active');
    if (active) active.scrollIntoView({{ block: 'nearest', behavior: 'smooth' }});

    expSaveHashState();
    expRender();
}}

// Main render function
function expRender() {{
    const tbody = document.getElementById('exp2-table-body');
    if (!tbody) return;

    tbody.innerHTML = '';

    let data = expState.filteredData;

    // Country mode: filter to selected country, skip grouping
    if (expState.grouping === 'country' && expState.selectedCountry) {{
        data = data.filter(row => row.country === expState.selectedCountry);
    }} else if (expState.grouping !== 'flat') {{
        // Other grouping modes: insert group header pseudo-rows
        data = expGroupData(data, expState.grouping);
    }}

    // Paginate
    const start = expState.currentPage * expState.pageSize;
    const end = start + expState.pageSize;
    const pageData = data.slice(start, end);

    const frag = document.createDocumentFragment();

    pageData.forEach((row, idx) => {{
        // Group header row
        if (row._isGroupHeader) {{
            const tr = document.createElement('tr');
            tr.className = 'exp2-group-header';
            tr.setAttribute('data-group', row._groupKey);
            const colSpan = document.querySelectorAll('#exp2-table thead th:not([style*="display:none"])').length || 9;
            const td = document.createElement('td');
            td.colSpan = colSpan;
            td.innerHTML = `<span class="exp2-group-toggle">▼</span> <strong>${{row._groupKey}}</strong> &nbsp;`
                + `<span style="color:var(--text-secondary);">${{row._groupCount}} keywords</span> &nbsp;`
                + `<span style="color:var(--accent);">Avg: ${{row._groupAvgScore.toFixed(1)}}</span> &nbsp;`
                + `<span style="color:var(--accent);">Margin: $$${{row._groupTotalMargin.toFixed(0)}}</span> &nbsp;`
                + `<span style="color:var(--c-amber);">${{row._groupProven}} proven</span>`;
            tr.appendChild(td);
            tr.addEventListener('click', function() {{
                const grpKey = this.dataset.group;
                const collapsed = this.classList.toggle('collapsed');
                const toggle = this.querySelector('.exp2-group-toggle');
                if (toggle) toggle.textContent = collapsed ? '▶' : '▼';
                // Hide/show following rows until next group header
                let sibling = this.nextElementSibling;
                while (sibling && !sibling.classList.contains('exp2-group-header')) {{
                    sibling.style.display = collapsed ? 'none' : '';
                    sibling = sibling.nextElementSibling;
                }}
            }});
            frag.appendChild(tr);
            return;
        }}

        const tr = document.createElement('tr');
        tr.setAttribute('data-idx', row.idx);

        // Highlight proven or high score
        if (row.proven_exact) tr.classList.add('exp2-row-proven');
        if (row.score_v2 >= 80) tr.classList.add('exp2-row-high');

        // Build cells
        for (let col of expState.visibleCols) {{
            const td = document.createElement('td');

            if (col === 'keyword') {{
                td.className = 'keyword';
                td.textContent = row[col];
            }} else if (col === 'country') {{
                td.textContent = row.country_flag + ' ' + row[col];
            }} else if (col === 'margin' || col === 'cpc' || col === 'proven_rpc') {{
                td.className = 'metric';
                td.textContent = typeof row[col] === 'number' ? row[col].toFixed(2) : row[col];
            }} else if (col === 'score_v2') {{
                td.className = 'metric';
                const val = row[col] || 0;
                td.textContent = val.toFixed(1);
                if (val >= 80) td.style.color = 'var(--accent)';
                else if (val >= 60) td.style.color = 'var(--c-amber)';
                else td.style.color = 'var(--text-secondary)';
            }} else if (col === 'status') {{
                td.textContent = row[col] || '';
                if (row[col] === 'proven') td.style.color = 'var(--accent)';
                else if (row[col] === 'test') td.style.color = 'var(--c-amber)';
            }} else if (col === 'trend_direction') {{
                const tMap = {{ up: ['▲', 'var(--accent)'], down: ['▼', 'var(--c-red)'], stable: ['━', 'var(--text-tertiary)'], 'new': ['★', 'var(--c-amber)'] }};
                const [icon, color] = tMap[row[col]] || ['━', 'var(--text-tertiary)'];
                td.innerHTML = `<span style="color:${{color}};font-size:13px;" title="${{row[col]}}">${{icon}}</span>`;
                td.style.textAlign = 'center';
            }} else if (col === 'top_angle') {{
                const aType = row.top_angle || '';
                const aScore = row.top_angle_score || 0;
                const aSrc = row.top_angle_source || '';
                if (aType) {{
                    const qColor = aScore >= 0.85 ? 'var(--c-green)' : aScore >= 0.65 ? 'var(--c-yellow)' : 'var(--c-red)';
                    const badge = aSrc === 'fb_intel'
                        ? '<span style="color:var(--c-red);font-size:9px;margin-right:3px;font-weight:600">AD</span>'
                        : '';
                    const label = aType.replace(/_/g, ' ');
                    td.innerHTML = `<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${{qColor}};margin-right:4px;vertical-align:middle;" title="Score: ${{aScore.toFixed(2)}}"></span>${{badge}}<span style="font-size:10px;color:var(--text-primary);">${{label}}</span>`;
                }} else {{
                    td.innerHTML = '<span style="color:var(--text-secondary);font-size:10px;">—</span>';
                }}
            }} else {{
                td.textContent = row[col] || '';
            }}

            tr.appendChild(td);
        }}

        // Click to expand detail
        tr.addEventListener('click', function() {{
            expToggleDetail(row.idx);
        }});

        frag.appendChild(tr);
    }});

    tbody.appendChild(frag);

    // Update pagination
    const totalPages = Math.ceil(data.length / expState.pageSize);
    document.getElementById('exp2-page-current').textContent = expState.currentPage + 1;
    document.getElementById('exp2-page-total').textContent = totalPages;
}}

// Group data by mode — insert group header pseudo-rows
function expGroupData(data, mode) {{
    const groups = {{}};

    data.forEach(row => {{
        let key = row[mode] || 'unknown';
        if (!groups[key]) groups[key] = [];
        groups[key].push(row);
    }});

    // Sort groups by aggregate revenue desc
    const sortedGroups = Object.entries(groups).sort((a, b) => {{
        const revA = a[1].reduce((s, r) => s + (r.margin || 0), 0);
        const revB = b[1].reduce((s, r) => s + (r.margin || 0), 0);
        return revB - revA;
    }});

    // Flatten with group header rows
    const result = [];
    sortedGroups.forEach(([key, rows]) => {{
        const avgScore = rows.reduce((s, r) => s + (r.score_v2 || 0), 0) / rows.length;
        const totalMargin = rows.reduce((s, r) => s + (r.margin || 0), 0);
        const provenCount = rows.filter(r => r.proven_exact).length;
        result.push({{
            _isGroupHeader: true,
            _groupKey: key,
            _groupCount: rows.length,
            _groupAvgScore: avgScore,
            _groupTotalMargin: totalMargin,
            _groupProven: provenCount,
            _collapsed: false
        }});
        result.push(...rows);
    }});
    return result;
}}

// Pagination
function expNextPage() {{
    const maxPage = Math.ceil(expState.filteredData.length / expState.pageSize) - 1;
    if (expState.currentPage < maxPage) {{
        expState.currentPage++;
        expRender();
    }}
}}

function expPrevPage() {{
    if (expState.currentPage > 0) {{
        expState.currentPage--;
        expRender();
    }}
}}

// Filter by country (from cards)
function expFilterByCountry(country) {{
    expState.filters.country = country;
    document.getElementById('exp2-filter-country').value = country;
    document.querySelectorAll('.exp2-intel-card').forEach(card => {{
        card.classList.remove('active');
    }});
    document.querySelector(`.exp2-intel-card[data-country="${{country}}"]`)?.classList.add('active');
    expApplyFilters();
}}

// Toggle column visibility
function expToggleColumnViz() {{
    alert('Column customization coming soon');
}}

// Export to CSV
function expExportCSV() {{
    const rows = [];

    // Headers
    rows.push(Array.from(expState.visibleCols).join(','));

    // Data
    expState.filteredData.forEach(row => {{
        const vals = Array.from(expState.visibleCols).map(col => {{
            const val = row[col] || '';
            return typeof val === 'string' && val.includes(',') ? `"${{val}}"` : val;
        }});
        rows.push(vals.join(','));
    }});

    const csv = rows.join('\\n');
    const blob = new Blob([csv], {{ type: 'text/csv' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `exp-data-${{new Date().toISOString().split('T')[0]}}.csv`;
    a.click();
}}

// Copy text to clipboard
function expCopyText(text) {{
    navigator.clipboard.writeText(text).then(() => {{
        alert('Copied: ' + text);
    }});
}}

// Generate angles
function expGenerateAngles() {{
    const vertical = document.getElementById('exp2-angle-vertical')?.value;
    const entity = document.getElementById('exp2-angle-entity')?.value;
    const country = document.getElementById('exp2-angle-country')?.value;

    if (!vertical || !entity || !country) {{
        alert('Please select vertical, entity, and country');
        return;
    }}

    const results = document.getElementById('exp2-angle-results');
    results.innerHTML = '';

    // Try to find matching angles
    const key = `${{entity}}|${{country}}`;
    let angles = [];

    if (key in MATCHED_ANGLES) {{
        angles = MATCHED_ANGLES[key].angles || [];
    }} else {{
        const tpl = ANGLE_TEMPLATES.patterns?.[vertical];
        if (tpl) {{
            angles = tpl[country] || tpl['default'] || Object.values(tpl)[0] || [];
            angles = angles.slice(0, 5);
        }}
    }}

    if (angles.length === 0) {{
        results.innerHTML = '<p style="color:var(--text-secondary);">No angles found. Try different filters.</p>';
        return;
    }}

    const currentMonth = new Date().getMonth(); // 0-11
    angles.slice(0, 5).forEach((angle, ai) => {{
        const title = angle.title || angle;
        // Month-aware star rating: seasonal_score if available, else rsoc_score based
        let stars = 3;
        if (angle.rsoc_score) stars = Math.min(5, Math.max(1, Math.round(angle.rsoc_score / 20)));
        else if (angle.seasonal_relevance) stars = angle.seasonal_relevance >= 0.7 ? 5 : angle.seasonal_relevance >= 0.4 ? 4 : 3;
        const starStr = '★'.repeat(stars) + '☆'.repeat(5 - stars);

        const div = document.createElement('div');
        div.className = 'exp2-angle-title';
        div.innerHTML = `
            <span>
                <span class="exp2-angle-text">${{title}}</span>
                <span class="exp2-angle-stars">${{starStr}}</span>
            </span>
            <button class="exp2-copy-btn" onclick="expCopyText('${{title.replace(/'/g, "\\'")}}')">Copy</button>
        `;
        results.appendChild(div);
    }});
}}

// Toggle detail panel
function expToggleDetail(idx) {{
    const existing = document.getElementById('exp2-detail-' + idx);
    if (existing) {{
        existing.remove();
        return;
    }}
    // Remove any other open detail
    document.querySelectorAll('.exp2-detail-row').forEach(el => el.remove());

    const row = EXP_DATA.find(r => r.idx === idx);
    if (!row) return;

    const clickedTr = document.querySelector(`#exp2-table-body tr[data-idx="${{idx}}"]`);
    if (!clickedTr) return;

    const detailTr = document.createElement('tr');
    detailTr.id = 'exp2-detail-' + idx;
    detailTr.className = 'exp2-detail-row';
    const colSpan = document.querySelectorAll('#exp2-table thead th:not([style*="display:none"])').length || 9;
    const td = document.createElement('td');
    td.colSpan = colSpan;
    td.style.padding = '0';

    // Build detail content
    const angleKey = row.keyword + '|' + row.country;
    const angles = MATCHED_ANGLES[angleKey]?.angles || [];
    const anglesHtml = angles.length > 0
        ? angles.slice(0, 3).map(a => {{
            const title = a.title || a.type || 'Untitled';
            const titleHtml = a.url
                ? `<a href="${{a.url}}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:underline">${{title}}</a>`
                : title;
            const badge = a.source === 'fb_intel'
                ? '<span style="color:var(--c-red);font-size:9px;margin-right:4px;font-weight:600">AD</span>'
                : '';
            return `<div style="margin:2px 0;color:var(--text-primary);font-size:10px;">• ${{badge}}${{titleHtml}}</div>`;
        }}).join('')
        : '<div style="color:var(--text-tertiary);font-size:10px;">No matched angles</div>';

    td.innerHTML = `<div style="background:var(--bg-raised);border-top:1px solid var(--accent);padding:14px;display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;">
        <div class="exp2-detail-field"><div class="exp2-detail-label">Source Keyword</div><div class="exp2-detail-value">${{row.source_keyword || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Template</div><div class="exp2-detail-value">${{row.template || 'ungrouped'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Entity Type</div><div class="exp2-detail-value">${{row.entity_type || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">CPC Track</div><div class="exp2-detail-value" style="color:${{row.proven_exact?'var(--accent)':'var(--c-amber)'}}">${{row.cpc_track_label || row.cpc_track || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Desktop RPC</div><div class="exp2-detail-value">${{row.desktop_rpc?.toFixed(4) || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Mobile RPC</div><div class="exp2-detail-value">${{row.mobile_rpc?.toFixed(4) || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Device Skew</div><div class="exp2-detail-value">${{row.device_skew?.toFixed(2) || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Revenue Rank</div><div class="exp2-detail-value">${{row.revenue_rank || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Entity Density</div><div class="exp2-detail-value">${{row.entity_density?.toFixed(2) || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Quality Score</div><div class="exp2-detail-value">${{row.quality_score?.toFixed(1) || 'N/A'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Proven Revenue</div><div class="exp2-detail-value" style="color:var(--accent);">$${{row.proven_revenue?.toFixed(2) || '0.00'}}</div></div>
        <div class="exp2-detail-field"><div class="exp2-detail-label">Source Revenue</div><div class="exp2-detail-value">$${{row.source_revenue?.toFixed(2) || '0.00'}}</div></div>
        <div class="exp2-detail-field" style="grid-column:1/-1;"><div class="exp2-detail-label">Matched Angles</div>${{anglesHtml}}</div>
    </div>`;

    detailTr.appendChild(td);
    clickedTr.after(detailTr);
}}

// Hash state management
function expSaveHashState() {{
    const params = new URLSearchParams();
    if (expState.filters.country) params.set('c', expState.filters.country);
    if (expState.filters.vertical) params.set('v', expState.filters.vertical);
    if (expState.filters.entity_type) params.set('et', expState.filters.entity_type);
    if (expState.filters.status) params.set('st', expState.filters.status);
    if (expState.filters.cpc_track) params.set('ct', expState.filters.cpc_track);
    if (expState.filters.search) params.set('q', expState.filters.search);
    if (expState.sortCol !== 'score_v2') params.set('sc', expState.sortCol);
    if (expState.sortAsc) params.set('sa', '1');
    if (expState.grouping !== 'flat') params.set('g', expState.grouping);
    if (expState.selectedCountry) params.set('sc2', expState.selectedCountry);
    if (expState.currentPage > 0) params.set('p', expState.currentPage);
    const hash = params.toString();
    if (hash) window.location.hash = 'exp2?' + hash;
}}

function expLoadHashState() {{
    const hash = window.location.hash;
    if (!hash.startsWith('#exp2?')) return;
    const params = new URLSearchParams(hash.slice(6));
    if (params.get('c')) {{ expState.filters.country = params.get('c'); const el = document.getElementById('exp2-filter-country'); if (el) el.value = params.get('c'); }}
    if (params.get('v')) {{ expState.filters.vertical = params.get('v'); const el = document.getElementById('exp2-filter-vertical'); if (el) el.value = params.get('v'); }}
    if (params.get('et')) {{ expState.filters.entity_type = params.get('et'); const el = document.getElementById('exp2-filter-entity-type'); if (el) el.value = params.get('et'); }}
    if (params.get('st')) {{ expState.filters.status = params.get('st'); const el = document.getElementById('exp2-filter-status'); if (el) el.value = params.get('st'); }}
    if (params.get('ct')) {{ expState.filters.cpc_track = params.get('ct'); const el = document.getElementById('exp2-filter-cpc-track'); if (el) el.value = params.get('ct'); }}
    if (params.get('q')) {{ expState.filters.search = params.get('q'); const el = document.getElementById('exp2-search'); if (el) el.value = params.get('q'); }}
    if (params.get('sc')) expState.sortCol = params.get('sc');
    if (params.get('sa')) expState.sortAsc = true;
    if (params.get('sc2')) expState.selectedCountry = params.get('sc2');
    if (params.get('g')) expSetGrouping(params.get('g'));
    if (params.get('p')) expState.currentPage = parseInt(params.get('p')) || 0;
    expApplyFilters();
}}

// Column visibility toggle
function expToggleColumnViz() {{
    let overlay = document.getElementById('exp2-col-overlay');
    if (overlay) {{ overlay.remove(); return; }}

    const allCols = [
        ['keyword','Keyword'],['country','Country'],['margin','Margin'],['score_v2','Score'],
        ['cpc','CPC'],['proven_rpc','Proven RPC'],['entity_name','Entity'],['status','Status'],
        ['vertical','Vertical'],['trend_direction','Trend'],['template','Template'],['entity_type','Entity Type'],
        ['cpc_track_label','CPC Track'],['proven_revenue','Proven Rev'],['desktop_rpc','Desktop RPC'],
        ['mobile_rpc','Mobile RPC'],['device_skew','Device Skew'],['entity_density','Entity Density']
    ];

    overlay = document.createElement('div');
    overlay.id = 'exp2-col-overlay';
    overlay.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--bg-surface);border:1px solid var(--accent);border-radius:8px;padding:16px;z-index:10000;min-width:220px;box-shadow:var(--shadow-dropdown);';
    overlay.innerHTML = '<div style="color:var(--c-amber);font-weight:bold;margin-bottom:10px;font-size:12px;">Toggle Columns</div>' +
        allCols.map(([key, label]) => {{
            const checked = expState.visibleCols.has(key) ? 'checked' : '';
            return `<label style="display:block;color:var(--text-primary);font-size:11px;margin:4px 0;cursor:pointer;"><input type="checkbox" ${{checked}} onchange="expColToggle('${{key}}',this.checked)" style="margin-right:6px;"> ${{label}}</label>`;
        }}).join('') +
        '<button onclick="document.getElementById(\\'exp2-col-overlay\\').remove()" style="margin-top:10px;background:var(--accent);color:var(--bg-base);border:none;padding:6px 12px;border-radius:4px;cursor:pointer;font-weight:bold;width:100%;">Done</button>';

    document.body.appendChild(overlay);
}}

function expColToggle(col, visible) {{
    if (visible) {{
        expState.visibleCols.add(col);
    }} else {{
        expState.visibleCols.delete(col);
    }}
    // Show/hide <th> and rebuild
    document.querySelectorAll(`#exp2-table th[data-col="${{col}}"]`).forEach(th => {{
        th.style.display = visible ? '' : 'none';
        th.classList.toggle('hidden', !visible);
    }});
    expRender();
}}
</script>'''


if __name__ == '__main__':
    # Test imports
    print("experimental_tab_v2.py is ready to be imported")
