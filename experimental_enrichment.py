"""
Experimental enrichment module for dashboard_builder.py
Loads and matches external data sources (Taboola, angles) and computes composite scores.
"""

import json
import logging
from typing import Dict, List, Optional, Set, Any, Tuple
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def load_taboola_slim(taboola_path: str, expansion_keywords_set: Set[str]) -> Dict[str, Any]:
    """
    Load taboola_keyword_index.json but only return entries matching expansion keywords.

    Args:
        taboola_path: Path to data/taboola_keyword_index.json
        expansion_keywords_set: Set of "keyword|country" strings from expansion_results

    Returns:
        Dict of matching entries only, keyed by "keyword|country"
    """
    result = {}

    if not expansion_keywords_set:
        logger.warning("load_taboola_slim: expansion_keywords_set is empty")
        return result

    try:
        path = Path(taboola_path)
        if not path.exists():
            logger.warning(f"load_taboola_slim: taboola file not found at {taboola_path}")
            return result

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # data is expected to be a dict keyed by "keyword|country"
        if isinstance(data, dict):
            for key in expansion_keywords_set:
                if key in data:
                    result[key] = data[key]

        logger.info(f"load_taboola_slim: loaded {len(result)} matching entries from {len(expansion_keywords_set)} keywords")
        return result

    except Exception as e:
        logger.error(f"load_taboola_slim: error loading taboola data: {e}")
        return result


def match_angles(exp_results: List[Dict[str, Any]], angles_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Match expansion keywords to real angles from angles.json.

    Args:
        exp_results: List of expansion result dicts
        angles_path: Path to angles.json

    Returns:
        Dict keyed by "keyword|country" with matched angles (top 3 by rsoc_score)
    """
    result = {}

    if not exp_results:
        logger.warning("match_angles: exp_results is empty")
        return result

    try:
        path = Path(angles_path)
        if not path.exists():
            logger.warning(f"match_angles: angles file not found at {angles_path}")
            return result

        with open(path, 'r', encoding='utf-8') as f:
            angles_data = json.load(f)

        # Process each expansion result
        for exp in exp_results:
            keyword = exp.get('keyword', '')
            country = exp.get('country', '')

            if not keyword or not country:
                continue

            key = f"{keyword}|{country}"

            # Look up in angles_data — try expansion keyword first, then source keyword
            source_key = f"{exp.get('source_keyword', '')}|{country}"
            lookup_key = key if key in angles_data else (source_key if source_key in angles_data else None)
            if lookup_key:
                angle_entry = angles_data[lookup_key]

                # Extract vertical and angles
                vertical = angle_entry.get('vertical', 'unknown')
                angles_list = angle_entry.get('selected_angles', [])

                # Sort by rsoc_score (descending) and take top 3
                sorted_angles = sorted(
                    angles_list,
                    key=lambda x: x.get('rsoc_score', 0),
                    reverse=True
                )[:3]

                # Build result entry
                result[key] = {
                    'vertical': vertical,
                    'angles': [
                        {
                            'type': a.get('angle_type', ''),
                            'score': a.get('rsoc_score', 0),
                            'category': a.get('ad_category', ''),
                            'title': a.get('angle_title', ''),
                            'url': a.get('article_url', ''),
                            'source': a.get('source', ''),
                        }
                        for a in sorted_angles
                    ]
                }

        logger.info(f"match_angles: matched {len(result)} angles from {len(exp_results)} expansions")
        return result

    except Exception as e:
        logger.error(f"match_angles: error matching angles: {e}")
        return result


def compute_composite_score(
    row: Dict[str, Any],
    proven_entities: Dict[str, Dict[str, Any]],
    vertical_ref: Dict[str, Dict[str, Any]]
) -> float:
    """
    Compute the opportunity_score_v2 composite (0-100) based on 5 weighted factors.

    Weights:
    - profitability (30%): margin = (proven_rpc or estimated_rpm/1000) - cpc_usd
    - confidence (25%): proven_exact → 95, proven_template → 65, estimated → 30
    - entity_quality (20%): entity_density = entity.revenue / entity.keywords_count
    - market_signal (15%): vertical_tier and country_tier averages
    - execution_readiness (10%): angles, cpc_track, entity proof

    Args:
        row: Enriched expansion dict
        proven_entities: Dict of entity data keyed by entity_name
        vertical_ref: Dict of vertical tier data

    Returns:
        Float 0-100
    """
    score = 0.0

    # 1. Profitability (30%)
    cpc = row.get('cpc_usd', 0)
    proven_rpc = row.get('proven_rpc')
    estimated_rpm = row.get('estimated_rpm', 0)

    margin = 0
    if proven_rpc is not None:
        margin = proven_rpc - cpc
    elif estimated_rpm > 0:
        margin = (estimated_rpm / 1000) - cpc

    profitability_score = min(100, max(0, (margin / 4.0) * 100)) if margin > 0 else 0
    score += profitability_score * 0.30

    # 2. Confidence (25%)
    confidence_score = 30  # default: estimated
    if row.get('proven_rpc') is not None:
        confidence_score = 95  # proven_exact
    elif row.get('proven_template'):
        confidence_score = 65  # proven_template

    score += confidence_score * 0.25

    # 3. Entity Quality (20%)
    entity_quality_score = 25  # default if not found
    entity_name = row.get('entity_name')
    if entity_name and entity_name in proven_entities:
        entity = proven_entities[entity_name]
        revenue = entity.get('revenue', 0)
        keywords_count = entity.get('keywords_count', 1)
        if keywords_count > 0:
            entity_density = revenue / keywords_count
            entity_quality_score = min(100, max(0, (entity_density / 30) * 100))

    score += entity_quality_score * 0.20

    # 4. Market Signal (15%)
    vertical = row.get('vertical', '')
    country = row.get('country', '')

    vertical_tier_score = 50  # default
    if vertical and vertical in vertical_ref:
        tier = vertical_ref[vertical].get('tier', 'C')
        tier_map = {'S': 100, 'A': 85, 'B': 65, 'C': 40}
        vertical_tier_score = tier_map.get(tier, 50)

    country_tier_score = 50  # default
    # Assume country_tier is 1, 2, or 3
    country_tier = row.get('country_tier', 2)
    country_tier_map = {1: 90, 2: 70, 3: 50}
    country_tier_score = country_tier_map.get(country_tier, 50)

    market_signal_score = (vertical_tier_score + country_tier_score) / 2
    score += market_signal_score * 0.15

    # 5. Execution Readiness (10%)
    execution_score = 0

    # has_angle → +50
    if row.get('has_angles', False):
        execution_score += 50

    # cpc_track A → +30, B → +15, est → +5
    cpc_track = row.get('cpc_track', '')
    if cpc_track == 'A':
        execution_score += 30
    elif cpc_track == 'B':
        execution_score += 15
    else:
        execution_score += 5

    # entity proven → +20, test → +10
    if row.get('proven_rpc') is not None:
        execution_score += 20
    elif row.get('proven_template'):
        execution_score += 10

    # Cap at 100
    execution_score = min(100, execution_score)
    score += execution_score * 0.10

    return min(100, max(0, score))


def enrich_expansion_data(
    exp_results: List[Dict[str, Any]],
    proven_rpc: Dict[str, float],
    proven_entities: Dict[str, Dict[str, Any]],
    proven_templates: Dict[str, bool],
    vertical_ref: Dict[str, Dict[str, Any]],
    taboola_index: Dict[str, Dict[str, Any]],
    matched_angles: Dict[str, Dict[str, Any]],
    country_intel: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Enrich expansion results with external data and compute scores.

    Args:
        exp_results: List of base expansion result dicts
        proven_rpc: Dict keyed by "keyword|country" → rpc value
        proven_entities: Dict keyed by entity_name → {revenue, keywords_count, ...}
        proven_templates: Dict keyed by "keyword|country" → bool
        vertical_ref: Dict keyed by vertical → {tier, ...}
        taboola_index: Loaded taboola index from load_taboola_slim
        matched_angles: Matched angles from match_angles
        country_intel: Country intelligence dict

    Returns:
        List of enriched dicts, sorted by score_v2 descending
    """
    enriched = []

    for exp in exp_results:
        if not isinstance(exp, dict):
            continue

        keyword = exp.get('keyword', '')
        country = exp.get('country', '')
        key = f"{keyword}|{country}"

        # Start with base fields
        enriched_row = dict(exp)

        # Add proven_rpc and proven_revenue
        enriched_row['proven_rpc'] = proven_rpc.get(key)
        if enriched_row['proven_rpc'] is not None:
            enriched_row['proven_revenue'] = enriched_row['proven_rpc'] * enriched_row.get('estimated_monthly_searches', 0) / 1000
        else:
            enriched_row['proven_revenue'] = None

        # Add taboola data
        if key in taboola_index:
            taboola = taboola_index[key]
            enriched_row['desktop_rpc'] = taboola.get('desktop_rpc')
            enriched_row['mobile_rpc'] = taboola.get('mobile_rpc')
            enriched_row['revenue_rank'] = taboola.get('revenue_rank')

            # Compute device_skew
            desktop = taboola.get('desktop_rpc')
            mobile = taboola.get('mobile_rpc')
            if desktop and mobile and mobile > 0:
                enriched_row['device_skew'] = desktop / mobile
            else:
                enriched_row['device_skew'] = None
        else:
            enriched_row['desktop_rpc'] = None
            enriched_row['mobile_rpc'] = None
            enriched_row['revenue_rank'] = None
            enriched_row['device_skew'] = None

        # Compute margin
        cpc = enriched_row.get('cpc_usd', 0)
        if enriched_row['proven_rpc'] is not None:
            enriched_row['margin'] = enriched_row['proven_rpc'] - cpc
        elif enriched_row.get('estimated_rpm', 0) > 0:
            enriched_row['margin'] = (enriched_row['estimated_rpm'] / 1000) - cpc
        else:
            enriched_row['margin'] = None

        # Add entity_density
        entity_name = enriched_row.get('entity_name')
        if entity_name and entity_name in proven_entities:
            entity = proven_entities[entity_name]
            revenue = entity.get('revenue', 0)
            keywords_count = entity.get('keywords_count', 1)
            if keywords_count > 0:
                enriched_row['entity_density'] = revenue / keywords_count
            else:
                enriched_row['entity_density'] = None
        else:
            enriched_row['entity_density'] = None

        # Add has_angles
        enriched_row['has_angles'] = key in matched_angles

        # Add proven_template flag
        enriched_row['proven_template'] = proven_templates.get(key, False)

        # Add country_tier from country_intel
        if country in country_intel:
            enriched_row['country_tier'] = country_intel[country].get('tier', 2)
        else:
            enriched_row['country_tier'] = 2

        # Compute score_v2
        enriched_row['score_v2'] = compute_composite_score(
            enriched_row,
            proven_entities,
            vertical_ref
        )

        enriched.append(enriched_row)

    # Sort by score_v2 descending
    enriched.sort(key=lambda x: x.get('score_v2', 0), reverse=True)

    logger.info(f"enrich_expansion_data: enriched {len(enriched)} expansions")
    return enriched


def get_top_launches(enriched: List[Dict[str, Any]], n: int = 3) -> List[Dict[str, Any]]:
    """
    Get the top N enriched rows by score_v2.

    Args:
        enriched: List of enriched dicts
        n: Number of top results to return

    Returns:
        List of top N enriched dicts
    """
    if not enriched:
        return []

    return enriched[:n]


def compute_kpi_stats(
    enriched: List[Dict[str, Any]],
    exp_results: List[Dict[str, Any]],
    proven_rpc: Dict[str, float],
    intelligence_report: Dict[str, Any],
    missed_opps: List[Dict[str, Any]],
    snapshots: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Compute all 8 KPI card values for the dashboard.

    Args:
        enriched: List of enriched expansion dicts
        exp_results: Original expansion results
        proven_rpc: Proven RPC lookup dict
        intelligence_report: Intelligence report dict
        missed_opps: List of missed opportunity dicts
        snapshots: Optional list of historical snapshots

    Returns:
        Dict with all KPI values
    """
    total_expansions = len(exp_results)

    # delta_expansions: compare to yesterday's snapshot
    delta_expansions = None
    if snapshots and len(snapshots) > 0:
        # Assume snapshots are sorted chronologically
        # Compare today's count to most recent previous snapshot
        prev_snapshot = snapshots[-1]
        prev_count = prev_snapshot.get('total_expansions', 0)
        delta_expansions = total_expansions - prev_count

    # ready_to_launch: count where score_v2 >= 70
    ready_to_launch = sum(1 for e in enriched if e.get('score_v2', 0) >= 70)

    # validated_by_proven: count where proven_rpc is not None
    validated_by_proven = sum(1 for e in enriched if e.get('proven_rpc') is not None)

    # validated_pct
    validated_pct = (validated_by_proven / total_expansions * 100) if total_expansions > 0 else 0

    # est_revenue_potential: sum of proven_revenue for validated rows
    est_revenue_potential = sum(
        e.get('proven_revenue', 0) or 0
        for e in enriched
        if e.get('proven_rpc') is not None
    )

    # missed_revenue: sum of revenue from missed_opps
    missed_revenue = sum(
        op.get('revenue', 0) or 0
        for op in missed_opps
    )

    # missed_count
    missed_count = len(missed_opps)

    # cpc_accuracy: from intelligence_report
    cpc_accuracy = 0
    if intelligence_report:
        sections = intelligence_report.get('sections', {})
        cross_ref = sections.get('cross_reference', {})
        cpc_data = cross_ref.get('cpc_accuracy', {})
        cpc_accuracy = cpc_data.get('rate', 0)

    # budget_used: total_expansions / 500
    budget_used = total_expansions / 500 if total_expansions > 0 else 0

    # track_a_count and track_b_count
    track_a_count = sum(1 for e in enriched if e.get('cpc_track') == 'A')
    track_b_count = sum(1 for e in enriched if e.get('cpc_track') != 'A' and e.get('cpc_track'))

    # top_vertical and top_vertical_revenue
    top_vertical = None
    top_vertical_revenue = 0

    vertical_revenue = {}
    for e in enriched:
        vertical = e.get('vertical', 'unknown')
        source_revenue = e.get('source_revenue', 0) or 0
        vertical_revenue[vertical] = vertical_revenue.get(vertical, 0) + source_revenue

    if vertical_revenue:
        top_vertical = max(vertical_revenue, key=vertical_revenue.get)
        top_vertical_revenue = vertical_revenue[top_vertical]

    return {
        'total_expansions': total_expansions,
        'delta_expansions': delta_expansions,
        'ready_to_launch': ready_to_launch,
        'validated_by_proven': validated_by_proven,
        'validated_pct': round(validated_pct, 2),
        'est_revenue_potential': round(est_revenue_potential, 2),
        'missed_revenue': round(missed_revenue, 2),
        'missed_count': missed_count,
        'cpc_accuracy': cpc_accuracy,
        'budget_used': round(budget_used, 4),
        'track_a_count': track_a_count,
        'track_b_count': track_b_count,
        'top_vertical': top_vertical,
        'top_vertical_revenue': round(top_vertical_revenue, 2)
    }
