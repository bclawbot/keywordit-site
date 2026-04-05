"""
csv_importer.py — Phase 11: Weekly performance feedback loop.

Usage:
    python3 scripts/csv_importer.py /path/to/keywords_report.csv

Input CSV columns (KeywordIt export):
    keyword, country, network, revenue, clicks

Actions (in order):
    1.  Parse CSV
    2.  Match rows against expansion_results.jsonl (exact keyword+country)
    3.  Calculate hit rates by entity / vertical / country
    4.  Flag entity promotion candidates (no auto-promote; flag only)
    5.  Update entity_registry.json promotion_flag
    6.  Update vertical_cpc_reference.json avg_rpc (rolling 70/30)
    7.  Maintain winner_dna.json: winners (top 200 by revenue) + anti-patterns (bottom 10)
    8.  Detect pipeline drift
    9.  Write data/performance_cache.json

Promotion criteria (flag only, human approves):
    - Entity total revenue across expanded keywords ≥ $50 AND
    - avg RPC of those expansions ≥ vertical avg_rpc × 0.8
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict

_WORKSPACE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _WORKSPACE)

_EXPANSION_RESULTS  = os.path.join(_WORKSPACE, 'data', 'expansion_results.jsonl')
_VALIDATION_HISTORY = os.path.join(_WORKSPACE, 'validation_history.jsonl')
_VALIDATED_OPPS     = os.path.join(_WORKSPACE, 'validated_opportunities.json')
_ENTITY_REGISTRY    = os.path.join(_WORKSPACE, 'data', 'entity_registry.json')
_VERTICAL_REF       = os.path.join(_WORKSPACE, 'data', 'vertical_cpc_reference.json')
_WINNER_DNA         = os.path.join(_WORKSPACE, 'data', 'winner_dna.json')
_PERFORMANCE_CACHE  = os.path.join(_WORKSPACE, 'data', 'performance_cache.json')

WINNER_TARGET    = 200  # keep top N winners (spec: exactly 200)
ANTI_PAT_TARGET  = 10   # keep bottom N anti-patterns
ROLLING_WEIGHT   = 0.30  # new data weight (historical gets 0.70)
PROMO_MIN_REVENUE = 50.0  # minimum revenue across expansions to flag ($50 per spec)
PROMO_RPC_RATIO  = 0.80  # fraction of vertical avg_rpc to qualify
DRIFT_TIER_C_PCT = 0.10  # allowed Tier C % above baseline before drift alarm
TIER_C_BASELINE  = 0.22  # baseline Tier C keyword % (from CSV analysis)
DRIFT_SCORE_DROP = 1.5   # score drop that triggers drift alarm
DRIFT_REV_DROP   = 0.20  # 20% revenue drop triggers drift alarm


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _load_jsonl(path: str) -> list:
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


def _save_json(path: str, data) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _load_json(path: str, default=None):
    if not os.path.exists(path):
        return default
    with open(path, encoding='utf-8') as f:
        return json.load(f)


# ──────────────────────────────────────────────
# Step 1: Parse CSV
# ──────────────────────────────────────────────

def parse_csv(filepath: str) -> list:
    """
    Parse KeywordIt CSV. Required columns: keyword, country, revenue, clicks.
    Optional: network.  Returns list of dicts.
    """
    rows = []
    with open(filepath, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = [h.strip().lower() for h in reader.fieldnames or []]
        for raw in reader:
            row = {h.strip().lower(): v.strip() for h, v in raw.items()}
            try:
                keyword = row.get('keyword', '').strip()
                country = row.get('country', 'US').strip().upper()
                revenue = float(row.get('revenue', 0) or 0)
                clicks  = int(float(row.get('clicks', 0) or 0))
                if not keyword:
                    continue
                rows.append({
                    'keyword': keyword,
                    'country': country,
                    'network': row.get('network', ''),
                    'revenue': revenue,
                    'clicks':  clicks,
                    'rpc':     round(revenue / clicks, 4) if clicks > 0 else 0.0,
                })
            except (ValueError, TypeError):
                continue
    return rows


# ──────────────────────────────────────────────
# Step 1b: Load organic pipeline keywords (validation_history + current run)
# ──────────────────────────────────────────────

def _load_organic_keywords() -> list:
    """Load validated keywords as the 'organic' keyword pool.
    Merges validation_history.jsonl + validated_opportunities.json,
    deduplicating by (keyword, country) with latest validated_at winning."""
    index = {}
    # Load history
    for rec in _load_jsonl(_VALIDATION_HISTORY):
        key = (rec.get('keyword', '').lower(), rec.get('country', 'US'))
        existing = index.get(key)
        if existing is None or rec.get('validated_at', '') >= existing.get('validated_at', ''):
            index[key] = rec
    # Overlay current run
    if os.path.exists(_VALIDATED_OPPS):
        try:
            with open(_VALIDATED_OPPS, encoding='utf-8') as f:
                for rec in json.load(f):
                    key = (rec.get('keyword', '').lower(), rec.get('country', 'US'))
                    index[key] = rec
        except Exception:
            pass
    # Mark all as organic source
    for rec in index.values():
        rec.setdefault('source', 'organic')
    return list(index.values())


# ──────────────────────────────────────────────
# Step 2: Match CSV rows against expansion_results + organic keywords
# ──────────────────────────────────────────────

def match_expansions(csv_rows: list, expansions: list, organic: list = None) -> tuple:
    """
    Returns (matched, unmatched_csv_rows).
    matched: list of dicts merging CSV performance data onto pipeline metadata.
    Matches against experimental expansions first (priority), then organic keywords.
    """
    # Index expansions by (keyword.lower(), country) — experimental has priority
    exp_index = {}
    for exp in expansions:
        key = (exp.get('keyword', '').lower(), exp.get('country', 'US'))
        exp_index[key] = exp

    # Index organic keywords (lower priority — only used if not in exp_index)
    org_index = {}
    for org in (organic or []):
        key = (org.get('keyword', '').lower(), org.get('country', 'US'))
        if key not in exp_index:
            org_index[key] = org

    matched   = []
    unmatched = []
    for row in csv_rows:
        key = (row['keyword'].lower(), row['country'])
        pipeline_rec = exp_index.get(key) or org_index.get(key)
        if pipeline_rec:
            matched.append({**pipeline_rec, **row})  # CSV values take precedence for perf fields
        else:
            unmatched.append(row)

    return matched, unmatched


# ──────────────────────────────────────────────
# Step 3: Compute hit rates
# ──────────────────────────────────────────────

def compute_hit_rates(matched: list, total_csv: int) -> dict:
    """Hit rates by entity, vertical, country."""
    entity_stats   = defaultdict(lambda: {'hits': 0, 'revenue': 0.0, 'rpcs': []})
    vertical_stats = defaultdict(lambda: {'hits': 0, 'revenue': 0.0, 'rpcs': []})
    country_stats  = defaultdict(lambda: {'hits': 0, 'revenue': 0.0})

    for m in matched:
        ent  = m.get('new_value') or m.get('entity', '')
        vert = m.get('vertical_match') or m.get('vertical', 'general')
        ctry = m.get('country', 'US')
        rev  = float(m.get('revenue', 0))
        rpc  = float(m.get('rpc', 0))

        if ent:
            entity_stats[ent]['hits']    += 1
            entity_stats[ent]['revenue'] += rev
            entity_stats[ent]['rpcs'].append(rpc)

        vertical_stats[vert]['hits']    += 1
        vertical_stats[vert]['revenue'] += rev
        vertical_stats[vert]['rpcs'].append(rpc)

        country_stats[ctry]['hits']    += 1
        country_stats[ctry]['revenue'] += rev

    # Compute avg RPC per entity/vertical
    for stats in entity_stats.values():
        rpcs = stats.pop('rpcs')
        stats['avg_rpc'] = round(sum(rpcs) / len(rpcs), 4) if rpcs else 0.0

    for stats in vertical_stats.values():
        rpcs = stats.pop('rpcs')
        stats['avg_rpc'] = round(sum(rpcs) / len(rpcs), 4) if rpcs else 0.0

    overall_hit_rate = round(len(matched) / total_csv, 4) if total_csv else 0.0

    return {
        'total_csv_rows':    total_csv,
        'total_matched':     len(matched),
        'overall_hit_rate':  overall_hit_rate,
        'by_entity':         dict(entity_stats),
        'by_vertical':       dict(vertical_stats),
        'by_country':        dict(country_stats),
    }


# ──────────────────────────────────────────────
# Step 4+5: Flag entity promotion candidates + update registry
# ──────────────────────────────────────────────

def flag_promotion_candidates(hit_rates: dict, vertical_ref: dict, entity_registry: dict) -> list:
    """
    Flag test entities that qualify for promotion. Returns list of candidate dicts.
    Does NOT modify entity_registry in place here — caller does that.
    """
    verticals     = vertical_ref.get('verticals', {})
    by_entity     = hit_rates.get('by_entity', {})
    by_vertical   = hit_rates.get('by_vertical', {})
    candidates    = []

    for ent_name, stats in by_entity.items():
        if stats['revenue'] < PROMO_MIN_REVENUE:
            continue

        # Find this entity in registry to confirm it is 'test'
        found_type    = None
        found_country = None
        for etype, pools in entity_registry.items():
            if etype in ('version', 'last_updated') or not isinstance(pools, dict):
                continue
            for country, pool in pools.items():
                if not isinstance(pool, dict):
                    continue
                if ent_name in pool.get('test', []):
                    found_type    = etype
                    found_country = country
                    break
            if found_type:
                break

        if not found_type:
            continue  # not a test entity, skip

        # Check RPC vs vertical baseline
        # We don't know which vertical this entity belongs to from stats alone —
        # look up the vertical avg_rpc for each vertical this entity hit
        ent_avg_rpc = stats['avg_rpc']
        qualifies   = False
        for vert_name, vdata in verticals.items():
            vert_avg_rpc = vdata.get('avg_rpc', 0)
            if vert_avg_rpc > 0 and ent_avg_rpc >= vert_avg_rpc * PROMO_RPC_RATIO:
                qualifies = True
                break

        if qualifies:
            candidates.append({
                'entity':       ent_name,
                'entity_type':  found_type,
                'country':      found_country,
                'hits':         stats['hits'],
                'revenue':      round(stats['revenue'], 2),
                'avg_rpc':      stats['avg_rpc'],
                'status':       'test',
                'promote_to':   'proven',
            })

    return candidates


def flag_demotion_candidates(hit_rates: dict, entity_registry: dict) -> list:
    """
    Flag proven entities with < $10 revenue for demotion back to test.
    Per _promotion_rules: "proven_to_test: < $10 revenue for 4 consecutive weeks".
    Since we only have one week's data per import, flag entities below threshold
    and track consecutive weeks via demotion_streak counter in the registry.
    """
    by_entity  = hit_rates.get('by_entity', {})
    candidates = []

    # Collect all proven entities across the registry
    for etype, pools in entity_registry.items():
        if etype in ('version', 'last_updated', '_promotion_rules') or not isinstance(pools, dict):
            continue
        for country, pool in pools.items():
            if not isinstance(pool, dict):
                continue
            for ent in pool.get('proven', []):
                stats = by_entity.get(ent, {})
                revenue = float(stats.get('revenue', 0))
                if revenue < 10.0:
                    # Increment demotion streak
                    streaks = pool.setdefault('demotion_streaks', {})
                    streak = streaks.get(ent, 0) + 1
                    streaks[ent] = streak
                    if streak >= 4:
                        candidates.append({
                            'entity':      ent,
                            'entity_type': etype,
                            'country':     country,
                            'revenue':     round(revenue, 2),
                            'streak':      streak,
                            'status':      'proven',
                            'demote_to':   'test',
                        })
                else:
                    # Reset streak on good week
                    streaks = pool.get('demotion_streaks', {})
                    if ent in streaks:
                        streaks[ent] = 0

    return candidates


def update_entity_registry(entity_registry: dict, promo_candidates: list, demo_candidates: list = None) -> dict:
    """
    Set promotion_flag=True on qualifying test entities.
    Set demotion_flag=True on underperforming proven entities.
    Does NOT move them — that requires human approval.
    """
    flagged_promo = {c['entity'] for c in promo_candidates}
    flagged_demo  = {c['entity'] for c in (demo_candidates or [])}

    for etype, pools in entity_registry.items():
        if etype in ('version', 'last_updated', '_promotion_rules') or not isinstance(pools, dict):
            continue
        for country, pool in pools.items():
            if not isinstance(pool, dict):
                continue
            # Promotion flags for test entities
            if 'promotion_flags' not in pool:
                pool['promotion_flags'] = {}
            for ent in pool.get('test', []):
                if ent in flagged_promo:
                    pool['promotion_flags'][ent] = True
            # Demotion flags for proven entities
            if 'demotion_flags' not in pool:
                pool['demotion_flags'] = {}
            for ent in pool.get('proven', []):
                if ent in flagged_demo:
                    pool['demotion_flags'][ent] = True

    entity_registry['last_updated'] = datetime.now(timezone.utc).isoformat()
    return entity_registry


# ──────────────────────────────────────────────
# Step 6: Update vertical_cpc_reference.json (rolling 70/30)
# ──────────────────────────────────────────────

def update_vertical_ref(vertical_ref: dict, hit_rates: dict) -> dict:
    """
    For each vertical with new matched data, update avg_rpc:
        new_avg_rpc = old × 0.70 + observed × 0.30
    Also increment keyword_count and total_revenue.
    """
    verticals  = vertical_ref.get('verticals', {})
    by_vertical = hit_rates.get('by_vertical', {})

    for vert_name, new_stats in by_vertical.items():
        if vert_name not in verticals:
            continue  # only update known verticals
        vdata = verticals[vert_name]
        old_rpc = float(vdata.get('avg_rpc', 1.0))
        obs_rpc = float(new_stats.get('avg_rpc', old_rpc))
        if obs_rpc > 0:
            vdata['avg_rpc'] = round(old_rpc * (1 - ROLLING_WEIGHT) + obs_rpc * ROLLING_WEIGHT, 4)

        vdata['keyword_count']  = int(vdata.get('keyword_count', 0)) + new_stats['hits']
        vdata['total_revenue']  = round(float(vdata.get('total_revenue', 0)) + new_stats['revenue'], 2)

    vertical_ref['generated'] = datetime.now(timezone.utc).isoformat()
    return vertical_ref


# ──────────────────────────────────────────────
# Step 7: Maintain winner_dna.json
# ──────────────────────────────────────────────

def update_winner_dna(winner_dna: list, matched: list, csv_rows: list) -> list:
    """
    Merge new performance data into winner_dna.
    - Top WINNER_TARGET unique keywords by revenue → winners
    - Bottom ANTI_PAT_TARGET unique keywords by revenue (with clicks > 0 but low RPC) → anti-patterns

    Strategy:
    1. Build candidate pool: existing winners + new matched expansions + unmatched CSV
    2. Deduplicate by keyword (keep highest revenue version)
    3. Rank by revenue desc → take top WINNER_TARGET as winners
    4. Of remaining, take lowest-RPC with clicks>0 as anti-patterns (keep existing anti-pattern count)
    """
    # Index existing entries for dedup
    existing = {}
    existing_anti = []
    for entry in winner_dna:
        kw = entry.get('keyword', '').lower()
        if 'why_it_fails' in entry:
            existing_anti.append(entry)
        else:
            if kw not in existing or entry.get('revenue', 0) > existing[kw].get('revenue', 0):
                existing[kw] = entry

    # Add new matched expansions as candidates
    for m in matched:
        kw  = m.get('keyword', '').lower()
        rev = float(m.get('revenue', 0))
        if kw not in existing or rev > existing[kw].get('revenue', 0):
            existing[kw] = {
                'keyword':         m.get('keyword', ''),
                'country':         m.get('country', 'US'),
                'revenue':         rev,
                'clicks':          m.get('clicks', 0),
                'rpc':             m.get('rpc', 0.0),
                'vertical':        m.get('vertical_match') or m.get('vertical', 'general'),
                'entity':          m.get('new_value') or m.get('entity', ''),
                'entity_type':     m.get('swapped_slot') or m.get('entity_type', ''),
                'intent_signals':  m.get('intent_signals', []),
                'demographic':     m.get('demographic', ''),
                'linguistic_flags': m.get('linguistic_flags', []),
                'why_it_works':    f"Experimental expansion hit: {m.get('expansion_type','')} via {m.get('swapped_slot','')}",
            }

    # Sort by revenue desc; take top WINNER_TARGET as winners
    all_candidates = sorted(existing.values(), key=lambda x: float(x.get('revenue', 0)), reverse=True)
    new_winners = all_candidates[:WINNER_TARGET]

    # Anti-patterns: lowest RPC with positive clicks (from remaining + existing anti-patterns)
    remaining = all_candidates[WINNER_TARGET:]

    # Combine existing anti-patterns with low-performers from CSV (unmatched low-RPC)
    anti_candidates = list(existing_anti)
    for entry in remaining:
        if float(entry.get('clicks', 0)) > 0 and float(entry.get('rpc', 0)) < 0.5:
            # Add as anti-pattern candidate
            anti_entry = {**entry}
            anti_entry.pop('why_it_works', None)
            anti_entry['why_it_fails'] = f"Low RPC ({entry.get('rpc',0):.3f}) despite {entry.get('clicks',0)} clicks"
            anti_candidates.append(anti_entry)

    # Sort by RPC asc, take worst ANTI_PAT_TARGET
    anti_candidates.sort(key=lambda x: float(x.get('rpc', 0)))
    new_anti = anti_candidates[:ANTI_PAT_TARGET]

    return new_winners + new_anti


# ──────────────────────────────────────────────
# Step 8: Detect pipeline drift
# ──────────────────────────────────────────────

def detect_drift(hit_rates: dict, prev_cache: dict, csv_rows: list = None) -> dict:
    """
    Compare current metrics against previous run.
    Returns drift dict with 'detected' bool and 'reasons' list.
    """
    drift = {'detected': False, 'reasons': [], 'tier_c_keyword_pct': 0.0,
             'tier_c_baseline': TIER_C_BASELINE}

    # Tier C country % check (spec's primary drift signal)
    if csv_rows:
        _hard_filters_path = os.path.join(_WORKSPACE, 'data', 'hard_filters.json')
        _tier_c_countries = set()
        if os.path.exists(_hard_filters_path):
            with open(_hard_filters_path, encoding='utf-8') as f:
                _hf = json.load(f)
                _tier_c_countries = set(_hf.get('tier_c_countries', []))
        if _tier_c_countries and csv_rows:
            _tier_c_count = sum(1 for r in csv_rows if r.get('country', '') in _tier_c_countries)
            _tier_c_pct = _tier_c_count / len(csv_rows) if csv_rows else 0
            drift['tier_c_keyword_pct'] = round(_tier_c_pct, 4)
            if _tier_c_pct > TIER_C_BASELINE + DRIFT_TIER_C_PCT:
                drift['detected'] = True
                drift['reasons'].append(
                    f"Tier C country keywords at {_tier_c_pct:.1%} "
                    f"(baseline {TIER_C_BASELINE:.0%} + {DRIFT_TIER_C_PCT:.0%} threshold)"
                )

    if not prev_cache:
        return drift  # no baseline for historical comparisons yet

    prev_hit_rate = prev_cache.get('overall_hit_rate', hit_rates.get('overall_hit_rate', 0))
    curr_hit_rate = hit_rates.get('overall_hit_rate', 0)

    # Revenue drop check
    prev_rev = prev_cache.get('total_revenue', 0)
    curr_rev = sum(e.get('revenue', 0) for e in hit_rates.get('by_entity', {}).values())
    if prev_rev > 0 and curr_rev < prev_rev * (1 - DRIFT_REV_DROP):
        drift['detected'] = True
        drift['reasons'].append(
            f"Revenue dropped {(1-curr_rev/prev_rev)*100:.1f}% vs previous run "
            f"(${curr_rev:.2f} vs ${prev_rev:.2f})"
        )

    # Hit rate drop check (score proxy)
    if prev_hit_rate > 0 and (prev_hit_rate - curr_hit_rate) >= 0.10:
        drift['detected'] = True
        drift['reasons'].append(
            f"Hit rate dropped from {prev_hit_rate:.1%} to {curr_hit_rate:.1%}"
        )

    # Vertical performance drop
    prev_vert = prev_cache.get('by_vertical', {})
    curr_vert = hit_rates.get('by_vertical', {})
    for vert, curr_stats in curr_vert.items():
        prev_stats = prev_vert.get(vert, {})
        if not prev_stats:
            continue
        prev_rpc = float(prev_stats.get('avg_rpc', 0))
        curr_rpc = float(curr_stats.get('avg_rpc', 0))
        if prev_rpc > 0 and curr_rpc < prev_rpc * (1 - DRIFT_REV_DROP):
            drift['detected'] = True
            drift['reasons'].append(
                f"Vertical '{vert}' avg_rpc dropped {(1-curr_rpc/prev_rpc)*100:.1f}% "
                f"(${curr_rpc:.2f} vs ${prev_rpc:.2f})"
            )

    return drift


# ──────────────────────────────────────────────
# Step 9: Write performance_cache.json
# ──────────────────────────────────────────────

def write_performance_cache(
    path: str,
    csv_path: str,
    hit_rates: dict,
    promotion_candidates: list,
    drift: dict,
    matched: list,
    csv_rows: list = None,
    demotion_candidates: list = None,
) -> None:
    csv_rows = csv_rows or []

    # Split matched into experimental vs organic
    exp_matched = [m for m in matched if m.get('source') == 'experimental']
    org_matched = [m for m in matched if m.get('source') != 'experimental']

    def _track_stats(rows):
        count = len(rows)
        with_rev = sum(1 for r in rows if float(r.get('revenue', 0)) > 0)
        total_rev = sum(float(r.get('revenue', 0)) for r in rows)
        return {
            'count': count,
            'with_revenue': with_rev,
            'hit_rate': round(with_rev / count, 4) if count else 0,
            'total_revenue': round(total_rev, 2),
            'avg_rev_per_kw': round(total_rev / count, 2) if count else 0,
        }

    # Template hit rates (group by template pattern)
    template_hits = defaultdict(lambda: {'published': 0, 'hits': 0, 'total_revenue': 0.0})
    for m in matched:
        tmpl = m.get('template', '')
        if not tmpl:
            continue
        template_hits[tmpl]['published'] += 1
        if float(m.get('revenue', 0)) > 0:
            template_hits[tmpl]['hits'] += 1
            template_hits[tmpl]['total_revenue'] += float(m.get('revenue', 0))
    for tmpl, stats in template_hits.items():
        stats['hit_rate'] = round(stats['hits'] / stats['published'], 4) if stats['published'] else 0
        stats['total_revenue'] = round(stats['total_revenue'], 2)

    # Entity performance for dashboard
    entity_perf = {}
    for ent_name, stats in hit_rates.get('by_entity', {}).items():
        entity_perf[ent_name] = {
            'expanded_keywords': stats.get('hits', 0),
            'revenue': round(stats.get('revenue', 0), 2),
            'status': 'test',
            'promotion_candidate': any(c['entity'] == ent_name for c in promotion_candidates),
        }

    # Revenue concentration (from full CSV)
    rev_conc = {'top_1pct': 0, 'top_5pct': 0, 'top_10pct': 0}
    if csv_rows:
        sorted_by_rev = sorted(csv_rows, key=lambda x: float(x.get('revenue', 0)), reverse=True)
        total_csv_rev = sum(float(r.get('revenue', 0)) for r in sorted_by_rev)
        if total_csv_rev > 0:
            n = len(sorted_by_rev)
            for pct_key, pct_val in [('top_1pct', 0.01), ('top_5pct', 0.05), ('top_10pct', 0.10)]:
                top_n = max(1, int(n * pct_val))
                top_rev = sum(float(r.get('revenue', 0)) for r in sorted_by_rev[:top_n])
                rev_conc[pct_key] = round(top_rev / total_csv_rev, 3)

    # Traffic activation rate (% of CSV keywords with >= 1 click)
    traffic_act = 0.0
    if csv_rows:
        clicked = sum(1 for r in csv_rows if int(float(r.get('clicks', 0) or 0)) > 0)
        traffic_act = round(clicked / len(csv_rows), 4) if csv_rows else 0

    # Top expansions by revenue
    top = sorted(matched, key=lambda x: float(x.get('revenue', 0)), reverse=True)[:20]
    top_expansions = [
        {
            'keyword':      m.get('keyword'),
            'country':      m.get('country'),
            'revenue':      m.get('revenue'),
            'rpc':          m.get('rpc'),
            'entity':       m.get('new_value') or m.get('entity'),
            'entity_status': m.get('entity_status', ''),
            'vertical':     m.get('vertical_match') or m.get('vertical'),
            'cpc_track':    m.get('cpc_track', ''),
        }
        for m in top
    ]

    cache = {
        'import_timestamp':    datetime.now(timezone.utc).isoformat(),
        'csv_file':            os.path.basename(csv_path),
        'total_keywords_imported': len(csv_rows),
        'matched_to_pipeline': {
            'experimental': _track_stats(exp_matched),
            'organic':      _track_stats(org_matched),
        },
        'template_hit_rates':  dict(template_hits),
        'entity_performance':  entity_perf,
        'pipeline_health': {
            'tier_c_keyword_pct':  drift.get('tier_c_keyword_pct', 0),
            'tier_c_baseline':     drift.get('tier_c_baseline', TIER_C_BASELINE),
            'drift_detected':      drift['detected'],
            'filter_rate':         hit_rates.get('filter_rate', 0),
            'expansion_budget_used': hit_rates.get('total_matched', 0),
        },
        'revenue_concentration': rev_conc,
        'traffic_activation_rate': traffic_act,
        'promotion_candidates':  [c['entity'] for c in promotion_candidates],
        'demotion_candidates':   [c['entity'] for c in (demotion_candidates or [])],
        'drift_warnings':        drift.get('reasons', []),
        'top_expansions':        top_expansions,
    }

    _save_json(path, cache)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main(csv_path: str) -> None:
    if not os.path.exists(csv_path):
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[csv_importer] Parsing {csv_path}…")
    csv_rows = parse_csv(csv_path)
    if not csv_rows:
        print("ERROR: no rows parsed from CSV (check column names: keyword, country, revenue, clicks)", file=sys.stderr)
        sys.exit(1)
    print(f"  Parsed {len(csv_rows)} rows")

    # Load expansion results (experimental) + organic pipeline keywords
    expansions = _load_jsonl(_EXPANSION_RESULTS)
    organic = _load_organic_keywords()
    print(f"  Loaded {len(expansions)} expansion records + {len(organic)} organic keywords")

    # Match against both pools
    matched, unmatched_csv = match_expansions(csv_rows, expansions, organic)
    exp_count = sum(1 for m in matched if m.get('source') == 'experimental')
    org_count = len(matched) - exp_count
    print(f"  Matched {len(matched)}/{len(csv_rows)} CSV rows "
          f"({exp_count} experimental, {org_count} organic)")

    # Hit rates
    hit_rates = compute_hit_rates(matched, len(csv_rows))

    # Load data files
    entity_registry = _load_json(_ENTITY_REGISTRY, {})
    vertical_ref    = _load_json(_VERTICAL_REF, {'verticals': {}})
    winner_dna      = _load_json(_WINNER_DNA, [])
    prev_cache      = _load_json(_PERFORMANCE_CACHE)

    # Promotion candidates
    promo_candidates = flag_promotion_candidates(hit_rates, vertical_ref, entity_registry)
    print(f"  Promotion candidates: {len(promo_candidates)}")
    for c in promo_candidates:
        print(f"    ↑ {c['entity']} ({c['entity_type']}, {c['country']}) "
              f"hits={c['hits']} avg_rpc=${c['avg_rpc']:.2f}")

    # Demotion candidates
    demo_candidates = flag_demotion_candidates(hit_rates, entity_registry)
    print(f"  Demotion candidates: {len(demo_candidates)}")
    for c in demo_candidates:
        print(f"    ↓ {c['entity']} ({c['entity_type']}, {c['country']}) "
              f"revenue=${c['revenue']:.2f} streak={c['streak']}wk")

    # Update registry (flags only — human approves actual moves)
    if promo_candidates or demo_candidates:
        entity_registry = update_entity_registry(entity_registry, promo_candidates, demo_candidates)
        _save_json(_ENTITY_REGISTRY, entity_registry)
        print(f"  Updated entity_registry.json (flags set)")

    # Update vertical CPC reference
    vertical_ref = update_vertical_ref(vertical_ref, hit_rates)
    _save_json(_VERTICAL_REF, vertical_ref)
    print(f"  Updated vertical_cpc_reference.json (rolling averages)")

    # Update winner_dna
    new_dna = update_winner_dna(winner_dna, matched, csv_rows)
    _save_json(_WINNER_DNA, new_dna)
    winners_count = sum(1 for e in new_dna if 'why_it_works' in e)
    anti_count    = sum(1 for e in new_dna if 'why_it_fails' in e)
    print(f"  Updated winner_dna.json: {winners_count} winners + {anti_count} anti-patterns")

    # Drift detection
    drift = detect_drift(hit_rates, prev_cache, csv_rows=csv_rows)
    if drift['detected']:
        print(f"  ⚠ PIPELINE DRIFT DETECTED:")
        for r in drift['reasons']:
            print(f"    - {r}")
    else:
        print(f"  No pipeline drift detected")

    # Write performance cache
    write_performance_cache(
        _PERFORMANCE_CACHE, csv_path, hit_rates, promo_candidates, drift, matched,
        csv_rows=csv_rows, demotion_candidates=demo_candidates,
    )
    print(f"  Wrote data/performance_cache.json")

    # Update RPC performance cache + rebuild estimator
    try:
        from modules.rpc_estimator import update_rpc_cache, build_rpc_estimator, load_patterns
        update_rpc_cache(csv_rows)
        print(f"  Updated data/keyword_rpc_cache.json ({len(csv_rows)} rows merged)")
        patterns  = load_patterns()
        from modules.rpc_estimator import load_rpc_cache
        cache     = load_rpc_cache()
        flat_rows = [
            {'keyword': e['keyword'], 'country': e['country'],
             'revenue': e['total_revenue'], 'clicks': e['total_clicks']}
            for e in cache.get('entries', {}).values()
            if e.get('total_clicks', 0) > 0
        ]
        estimator = build_rpc_estimator(flat_rows, patterns)
        if estimator:
            cv = len(estimator.get('country_x_vertical', {}))
            print(f"  Rebuilt data/rpc_estimator.json ({cv} country×vertical buckets)")
    except Exception as _rpc_err:
        print(f"  ⚠ RPC estimator update skipped: {_rpc_err}")

    # Summary
    hit_rate_pct = hit_rates['overall_hit_rate'] * 100
    total_rev = sum(float(m.get('revenue', 0)) for m in matched)
    print(f"\n✅ Import complete: {len(matched)} hits ({hit_rate_pct:.1f}%) | "
          f"${total_rev:,.2f} attributed revenue | "
          f"{len(promo_candidates)} promotions / {len(demo_candidates)} demotions | "
          f"drift={'YES' if drift['detected'] else 'no'}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/csv_importer.py /path/to/keywords_report.csv", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
