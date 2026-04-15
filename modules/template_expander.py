"""
template_expander.py — Phase 6a: Deterministic entity-substitution expansion.

Takes decomposed keywords and entity_registry.json, generates expanded keyword
variants by swapping entities within the same type.

Pure Python — no LLM calls.

Exposed API:
    load_registry() -> dict
    expand_batch(decomposed, registry, country, source_quality_scores) -> list[dict]

Output schema per expansion:
    {
        "keyword":             str,   # expanded keyword
        "source_keyword":      str,
        "source_revenue":      float,
        "source_quality_score": float,
        "expansion_type":      "entity_swap",
        "swapped_slot":        str,   # entity_type that was swapped
        "new_value":           str,   # new entity name
        "entity_status":       str,   # "proven" | "test"
        "country":             str,
        "template":            str,
        "plausible":           None,  # filled by plausibility_checker
        "cpc_track":           None,  # filled by cpc_router
    }
"""

import json
import os

_BASE_DIR = os.path.dirname(__file__)
_REGISTRY_PATH = os.path.join(_BASE_DIR, '..', 'data', 'entity_registry.json')

EXPANSION_BUDGET = {
    "per_cycle_max":        500,
    "per_priority_keyword": 8,    # source_quality_score >= 14
    "per_pass_keyword":     4,    # source_quality_score >= 4 and < 14
    "per_entity_max":       25,   # no single new_value dominates
}


def load_registry() -> dict:
    with open(_REGISTRY_PATH, encoding='utf-8') as f:
        return json.load(f)


def _get_entity_pool(registry: dict, entity_type: str, country: str) -> tuple:
    """
    Return (proven_list, test_list) for entity_type in country.
    Falls back to '*' wildcard if no country-specific entry.
    """
    type_data = registry.get(entity_type, {})
    if not type_data:
        return [], []

    # Try country-specific first, then wildcard
    pool = type_data.get(country) or type_data.get("*") or {}
    return pool.get("proven", []), pool.get("test", [])


def _substitute(template: str, entity_type: str, new_entity: str) -> str:
    """Replace {entity_type} placeholder in template with new_entity."""
    return template.replace(f"{{{entity_type}}}", new_entity)


def expand_batch(
    decomposed: list,
    registry: dict,
    country: str,
    source_quality_scores: dict = None,
    source_trends: dict = None,
) -> list:
    """
    Generate expanded keyword variants from decomposed keywords.

    Args:
        decomposed:            list of decomposition dicts from template_decomposer
        registry:              loaded entity_registry.json dict
        country:               ISO country code for entity pool lookup
        source_quality_scores: optional dict {keyword -> quality_score float}.
                               If provided, used to determine expansion budget per keyword.
                               If absent, all keywords get per_pass_keyword budget.

    Returns:
        list of expansion dicts (unsorted, pre-plausibility).
    """
    if source_quality_scores is None:
        source_quality_scores = {}
    if source_trends is None:
        source_trends = {}

    cycle_total   = 0
    entity_counts = {}   # new_value -> count  (per-entity cap)
    expansions    = []

    priority_threshold = 14.0
    pass_threshold     = 4.0

    for decomp in decomposed:
        if not decomp.get("expandable") or not decomp.get("template"):
            continue

        keyword     = decomp["keyword"]
        entity_type = decomp.get("entity_type")
        template    = decomp["template"]
        source_ent  = decomp.get("entity")   # entity to exclude (no self-expansion)

        if not entity_type or not template:
            continue

        quality_score = source_quality_scores.get(keyword, 0.0)

        if quality_score >= priority_threshold:
            per_kw_limit = EXPANSION_BUDGET["per_priority_keyword"]
        elif quality_score >= pass_threshold:
            per_kw_limit = EXPANSION_BUDGET["per_pass_keyword"]
        else:
            # Quality too low — skip expansion
            continue

        if cycle_total >= EXPANSION_BUDGET["per_cycle_max"]:
            break

        proven, test = _get_entity_pool(registry, entity_type, country)
        # Try wildcard if nothing country-specific
        if not proven and not test:
            proven, test = _get_entity_pool(registry, entity_type, "*")

        # Prioritize proven entities over test
        candidates = [(e, "proven") for e in proven] + [(e, "test") for e in test]

        kw_count = 0
        for entity_name, status in candidates:
            if kw_count >= per_kw_limit:
                break
            if cycle_total >= EXPANSION_BUDGET["per_cycle_max"]:
                break

            # Skip self-expansion
            if source_ent and entity_name.lower() == source_ent.lower():
                continue

            # Per-entity cap
            if entity_counts.get(entity_name, 0) >= EXPANSION_BUDGET["per_entity_max"]:
                continue

            expanded_kw = _substitute(template, entity_type, entity_name)
            if not expanded_kw or expanded_kw == keyword:
                continue

            expansion = {
                "keyword":              expanded_kw,
                "source_keyword":       keyword,
                "source_revenue":       0.0,   # caller can enrich from performance data
                "source_quality_score": quality_score,
                "expansion_type":       "entity_swap",
                "swapped_slot":         entity_type,
                "new_value":            entity_name,
                "entity_status":        status,
                "country":              country,
                "template":             template,
                "vertical":             decomp.get("vertical", "general"),
                "vertical_match":       decomp.get("vertical", "general"),
                "plausible":            None,  # filled by plausibility_checker
                "cpc_track":            None,  # filled by cpc_router
                "source_trend":         source_trends.get(keyword, ""),
            }
            expansions.append(expansion)
            kw_count              += 1
            cycle_total           += 1
            entity_counts[entity_name] = entity_counts.get(entity_name, 0) + 1

    return expansions


if __name__ == "__main__":
    from modules.template_decomposer import decompose_batch

    registry = load_registry()

    test_keywords = [
        "Sam's Club Auto Insurance Cost",
        "SSI Apartments for Rent Near Me",
        "VA Veterans Discounts",
        "I Can't Find a Lawyer to Take My Case",
    ]

    # Simulate quality scores: first two are PRIORITY, last one is not expandable
    scores = {
        "Sam's Club Auto Insurance Cost": 45.0,
        "SSI Apartments for Rent Near Me": 23.0,
        "VA Veterans Discounts": 36.0,
        "I Can't Find a Lawyer to Take My Case": 8.0,
    }

    print("Decomposing…")
    decomposed = decompose_batch(test_keywords, "US")

    print("\nExpanding…")
    expansions = expand_batch(decomposed, registry, "US", scores)

    print(f"\nGenerated {len(expansions)} expansions:\n")
    by_source = {}
    for e in expansions:
        by_source.setdefault(e["source_keyword"], []).append(e)

    for src, exps in by_source.items():
        print(f"  Source: {src!r} (score={scores.get(src, 0):.0f})")
        for ex in exps:
            print(f"    → {ex['keyword']!r} [{ex['entity_status']}] via {ex['swapped_slot']}")
        print()

    # Test: PRIORITY keyword should generate up to 8 expansions
    sams_exps = by_source.get("Sam's Club Auto Insurance Cost", [])
    print(f"Sam's Club → {len(sams_exps)} expansions (expect ≤8)")

    # Test: no self-expansion
    self_exp = [e for e in expansions if e["new_value"].lower() in e["source_keyword"].lower()]
    print(f"Self-expansions (should be 0): {len(self_exp)}")
