"""
cpc_router.py — Phase 7: Two-track CPC validation router.

Decides whether each keyword goes to Track A (real DataForSEO/Google Ads API call)
or Track B (inherited CPC estimate from vertical_cpc_reference.json).

Track A = spend API budget on it.
Track B = use statistical estimate, no API call needed.

Exposed API:
    load_vertical_ref() -> dict
    route_for_validation(keywords, registry) -> tuple[list[dict], list[dict]]
      Returns: (track_a_list, track_b_list)

Track B keywords get two extra fields:
    inherited_cpc: float   — estimated CPC in USD
    cpc_display:   str     — formatted as "$X.XXe" ('e' suffix = estimate)
"""

import json
import os

_BASE_DIR     = os.path.dirname(__file__)
_VERT_REF_PATH = os.path.join(_BASE_DIR, '..', 'data', 'vertical_cpc_reference.json')

MAX_TRACK_A_PER_CYCLE = 150   # hard cap: if Track A would exceed this, overflow to Track B
TRACK_A_MAX_PCT       = 0.30  # soft target: ≤30% of total expanded keywords go Track A


def load_vertical_ref() -> dict:
    with open(_VERT_REF_PATH, encoding='utf-8') as f:
        return json.load(f)


def _get_entity_status(registry: dict, entity_type: str, entity_name: str, country: str) -> str:
    """Return 'proven', 'test', or 'unknown' for an entity in the registry."""
    if not entity_type or not entity_name:
        return "unknown"

    type_data = registry.get(entity_type, {})
    # Try country-specific pool first, then wildcard
    pool = type_data.get(country) or type_data.get("*") or {}

    if entity_name in pool.get("proven", []):
        return "proven"
    if entity_name in pool.get("test", []):
        return "test"
    return "unknown"


def should_query_api(keyword: dict, registry: dict) -> bool:
    """
    Determine if a keyword should be sent to the real API (Track A).

    Rules (in order, first match wins):
    1. Organic (non-experimental) keywords → always Track A
    2. Government programs → always Track A (real search volume exists)
    3. PRIORITY source (score ≥14) + proven entity → Track A
    4. All other expansions → Track B
    """
    # 1. Organic keywords always get real lookup
    if keyword.get("source") != "experimental":
        return True

    entity_type = keyword.get("swapped_slot") or keyword.get("entity_type")
    entity_name = keyword.get("new_value")    or keyword.get("entity")
    country     = keyword.get("country", "US")

    # 2. Government programs always get real data
    if entity_type == "government_program":
        return True

    # 3. PRIORITY + proven entity
    if keyword.get("source_quality_score", 0) >= 14:
        status = _get_entity_status(registry, entity_type, entity_name, country)
        if status == "proven":
            return True

    # 4. Default: Track B
    return False


def calculate_inherited_cpc(keyword: dict, vertical_ref: dict, registry: dict) -> float:
    """
    Estimate CPC for a Track B keyword using vertical averages + brand multipliers.

    Formula:
        inherited_cpc = base_rpc × brand_trust_multiplier × country_multiplier

    Where:
        base_rpc = vertical country override avg_rpc if exists, else vertical avg_rpc
        brand_trust_multiplier = 1.5 for proven entities, 1.0 for test/unknown
        country_multiplier = from vertical country_overrides if present, else 1.0
    """
    vertical = keyword.get("vertical_match") or keyword.get("vertical") or "general"
    country  = keyword.get("country", "US")

    verticals    = vertical_ref.get("verticals", {})
    vertical_data = verticals.get(vertical, {})

    # Base RPC: use country-specific avg_rpc if the override provides one, else vertical avg_rpc.
    # Note: country_overrides may only have 'multiplier' (for scoring), not 'avg_rpc'.
    # In that case we fall back to the vertical-level avg_rpc — this is intentional.
    country_overrides = vertical_data.get("country_overrides", {})
    base_rpc = (country_overrides.get(country, {}).get("avg_rpc")
                or vertical_data.get("avg_rpc", 1.0))

    # Brand trust multiplier
    entity_status = keyword.get("entity_status", "test")
    brand_mult    = 1.5 if entity_status == "proven" else 1.0

    return round(float(base_rpc) * brand_mult, 2)


def route_for_validation(
    keywords: list,
    registry: dict,
    vertical_ref: dict = None,
) -> tuple:
    """
    Split keywords into Track A (API) and Track B (inherited CPC).

    Args:
        keywords:     list of keyword dicts (mix of organic + experimental)
        registry:     loaded entity_registry.json
        vertical_ref: loaded vertical_cpc_reference.json (loaded from disk if None)

    Returns:
        (track_a, track_b) — two lists of keyword dicts.
        Track B entries have `inherited_cpc` and `cpc_display` fields added.
        All entries get `cpc_track` field set to "A" or "B".
    """
    if vertical_ref is None:
        vertical_ref = load_vertical_ref()

    track_a = []
    track_b = []

    # First pass: classify without cap
    for kw in keywords:
        if should_query_api(kw, registry):
            kw["cpc_track"] = "A"
            track_a.append(kw)
        else:
            kw["cpc_track"] = "B"
            track_b.append(kw)

    # Apply Track A hard cap: overflow excess to Track B
    # Prioritize: government_program > proven_priority > others
    if len(track_a) > MAX_TRACK_A_PER_CYCLE:
        def priority_key(kw):
            if kw.get("entity_type") == "government_program":
                return 0
            if kw.get("entity_status") == "proven" and kw.get("source_quality_score", 0) >= 14:
                return 1
            return 2

        track_a.sort(key=priority_key)
        overflow   = track_a[MAX_TRACK_A_PER_CYCLE:]
        track_a    = track_a[:MAX_TRACK_A_PER_CYCLE]

        for kw in overflow:
            kw["cpc_track"] = "B"
        track_b.extend(overflow)

    # Enrich Track B with inherited CPC
    for kw in track_b:
        icpc = calculate_inherited_cpc(kw, vertical_ref, registry)
        kw["inherited_cpc"] = icpc
        kw["cpc_display"]   = f"${icpc:.2f}e"

    return track_a, track_b


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from modules.template_expander import load_registry

    registry     = load_registry()
    vertical_ref = load_vertical_ref()

    test_keywords = [
        # Organic keywords — always Track A
        {"keyword": "Ssi Apartments for Rent Near Me", "country": "US", "source": "organic",
         "vertical": "housing_ssi"},
        # Government program expansion — Track A
        {"keyword": "Section 8 Apartments for Rent Near Me", "country": "US", "source": "experimental",
         "entity_type": "government_program", "swapped_slot": "government_program",
         "new_value": "Section 8", "entity_status": "test",
         "source_quality_score": 23.0, "vertical": "housing_ssi"},
        # Test entity, PRIORITY source — Track B
        {"keyword": "BJ's Auto Insurance Cost", "country": "US", "source": "experimental",
         "swapped_slot": "membership_retailer", "new_value": "BJ's Wholesale",
         "entity_status": "test", "source_quality_score": 45.0,
         "vertical": "auto_insurance"},
        # Proven entity, PRIORITY source — Track A
        {"keyword": "State Farm Auto Insurance Cost", "country": "US", "source": "experimental",
         "swapped_slot": "insurance_carrier", "new_value": "State Farm",
         "entity_status": "proven", "source_quality_score": 45.0,
         "vertical": "auto_insurance"},
        # Test entity, low source score — Track B
        {"keyword": "AAA Auto Insurance Cost", "country": "US", "source": "experimental",
         "swapped_slot": "insurance_carrier", "new_value": "AAA",
         "entity_status": "test", "source_quality_score": 6.0,
         "vertical": "auto_insurance"},
    ]

    track_a, track_b = route_for_validation(test_keywords, registry, vertical_ref)

    print(f"Track A ({len(track_a)} keywords — will query API):")
    for kw in track_a:
        print(f"  {kw['keyword']!r} [{kw.get('entity_status', 'organic')}]")

    print(f"\nTrack B ({len(track_b)} keywords — inherited CPC):")
    for kw in track_b:
        print(f"  {kw['keyword']!r} [{kw.get('entity_status', 'organic')}] → {kw['cpc_display']}")

    # Spec checks
    sec8 = next((k for k in track_a if "Section 8" in k["keyword"]), None)
    bjs  = next((k for k in track_b if "BJ's" in k["keyword"]), None)
    aaa  = next((k for k in track_b if "AAA" in k["keyword"]), None)

    print("\n--- Spec checks ---")
    print(f"  [{'OK' if sec8 else 'FAIL'}] Section 8 → Track A (government_program)")
    print(f"  [{'OK' if bjs  else 'FAIL'}] BJ's → Track B (test entity)")
    print(f"  [{'OK' if aaa  else 'FAIL'}] AAA → Track B (test entity, low score)")

    # Inherited CPC check: auto_insurance avg_rpc=3.74, test entity → 3.74 × 1.0 × US_mult
    if bjs:
        print(f"  [INFO] BJ's inherited_cpc={bjs['inherited_cpc']} (auto_insurance avg_rpc=3.74 × brand×country)")
