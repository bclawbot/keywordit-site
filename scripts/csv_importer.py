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
    7.  Maintain winner_dna.json: winners (top 50 by revenue) + anti-patterns (bottom 10)
    8.  Detect pipeline drift
    9.  Write data/performance_cache.json

Promotion criteria (flag only, human approves):
    - Entity appears in ≥3 matched expansions AND
    - avg RPC of those expansions ≥ vertical avg_rpc × 0.8
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict

_WORKSPACE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_EXPANSION_RESULTS  = os.path.join(_WORKSPACE, 'data', 'expansion_results.jsonl')
_ENTITY_REGISTRY    = os.path.join(_WORKSPACE, 'data', 'entity_registry.json')
_VERTICAL_REF       = os.path.join(_WORKSPACE, 'data', 'vertical_cpc_reference.json')
_WINNER_DNA         = os.path.join(_WORKSPACE, 'data', 'winner_dna.json')
_PERFORMANCE_CACHE  = os.path.join(_WORKSPACE, 'data', 'performance_cache.json')

WINNER_TARGET    = 50   # keep top N winners
ANTI_PAT_TARGET  = 10   # keep bottom N anti-patterns
ROLLING_WEIGHT   = 0.30  # new data weight (historical gets 0.70)
PROMO_MIN_HITS   = 3     # minimum matched expansions to flag
PROMO_RPC_RATIO  = 0.80  # fraction of vertical avg_rpc to qualify
DRIFT_TIER_C_PCT = 0.10  # allowed Tier C % above baseline before drift alarm
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
# Step 2: Match CSV rows against expansion_results.jsonl
# ──────────────────────────────────────────────

def match_expansions(csv_rows: list, expansions: list) -> tuple:
    """
    Returns (matched, unmatched_csv_rows).
    matched: list of dicts merging CSV performance data onto expansion metadata.
    """
    # Index expansions by (keyword.lower(), country)
    exp_index = {}
    for exp in expansions:
        key = (exp.get('keyword', '').lower(), exp.get('country', 'US'))
        exp_index[key] = exp

    matched   = []
    unmatched = []
    for row in csv_rows:
        key = (row['keyword'].lower(), row['country'])
        exp = exp_index.get(key)
        if exp:
            matched.append({**exp, **row})  # CSV values take precedence for perf fields
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
        if stats['hits'] < PROMO_MIN_HITS:
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


def update_entity_registry(entity_registry: dict, candidates: list) -> dict:
    """
    Set promotion_flag=True on qualifying test entities.
    Does NOT move them to proven — that requires human approval.
    """
    flagged_names = {c['entity'] for c in candidates}

    for etype, pools in entity_registry.items():
        if etype in ('version', 'last_updated') or not isinstance(pools, dict):
            continue
        for country, pool in pools.items():
            if not isinstance(pool, dict):
                continue
            # Add promotion_flags dict if not present
            if 'promotion_flags' not in pool:
                pool['promotion_flags'] = {}
            for ent in pool.get('test', []):
                if ent in flagged_names:
                    pool['promotion_flags'][ent] = True

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

def detect_drift(hit_rates: dict, prev_cache: dict) -> dict:
    """
    Compare current metrics against previous run.
    Returns drift dict with 'detected' bool and 'reasons' list.
    """
    drift = {'detected': False, 'reasons': []}

    if not prev_cache:
        return drift  # no baseline yet

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
) -> None:
    total_rev = sum(float(m.get('revenue', 0)) for m in matched)
    total_clicks = sum(int(m.get('clicks', 0)) for m in matched)
    avg_rpc = round(total_rev / total_clicks, 4) if total_clicks > 0 else 0.0

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
        'generated':           datetime.now(timezone.utc).isoformat(),
        'source_csv':          os.path.basename(csv_path),
        'total_csv_rows':      hit_rates['total_csv_rows'],
        'total_matched':       hit_rates['total_matched'],
        'overall_hit_rate':    hit_rates['overall_hit_rate'],
        'total_revenue':       round(total_rev, 2),
        'total_clicks':        total_clicks,
        'avg_rpc':             avg_rpc,
        'by_vertical':         hit_rates['by_vertical'],
        'by_country':          hit_rates['by_country'],
        'by_entity':           hit_rates['by_entity'],
        'top_expansions':      top_expansions,
        'promotion_candidates': promotion_candidates,
        'drift_detected':      drift['detected'],
        'drift_reasons':       drift['reasons'],
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

    # Load expansion results
    expansions = _load_jsonl(_EXPANSION_RESULTS)
    print(f"  Loaded {len(expansions)} expansion records")

    # Match
    matched, unmatched_csv = match_expansions(csv_rows, expansions)
    print(f"  Matched {len(matched)}/{len(csv_rows)} CSV rows to expansions")

    # Hit rates
    hit_rates = compute_hit_rates(matched, len(csv_rows))

    # Load data files
    entity_registry = _load_json(_ENTITY_REGISTRY, {})
    vertical_ref    = _load_json(_VERTICAL_REF, {'verticals': {}})
    winner_dna      = _load_json(_WINNER_DNA, [])
    prev_cache      = _load_json(_PERFORMANCE_CACHE)

    # Promotion candidates
    candidates = flag_promotion_candidates(hit_rates, vertical_ref, entity_registry)
    print(f"  Promotion candidates: {len(candidates)}")
    for c in candidates:
        print(f"    → {c['entity']} ({c['entity_type']}, {c['country']}) "
              f"hits={c['hits']} avg_rpc=${c['avg_rpc']:.2f}")

    # Update registry (flags only)
    if candidates:
        entity_registry = update_entity_registry(entity_registry, candidates)
        _save_json(_ENTITY_REGISTRY, entity_registry)
        print(f"  Updated entity_registry.json (promotion flags set)")

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
    drift = detect_drift(hit_rates, prev_cache)
    if drift['detected']:
        print(f"  ⚠ PIPELINE DRIFT DETECTED:")
        for r in drift['reasons']:
            print(f"    - {r}")
    else:
        print(f"  No pipeline drift detected")

    # Write performance cache
    write_performance_cache(
        _PERFORMANCE_CACHE, csv_path, hit_rates, candidates, drift, matched
    )
    print(f"  Wrote data/performance_cache.json")

    # Summary
    hit_rate_pct = hit_rates['overall_hit_rate'] * 100
    total_rev = sum(float(m.get('revenue', 0)) for m in matched)
    print(f"\n✅ Import complete: {len(matched)} hits ({hit_rate_pct:.1f}%) | "
          f"${total_rev:,.2f} attributed revenue | "
          f"{len(candidates)} promotion candidates | "
          f"drift={'YES' if drift['detected'] else 'no'}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/csv_importer.py /path/to/keywords_report.csv", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
