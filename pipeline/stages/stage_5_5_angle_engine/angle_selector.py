"""
angle_selector.py — Angle selection with diversity rule.

select_angles() scores all 10 canonical angle types for a keyword,
then selects the top N using a diversity rule that ensures no two
selected angles share the same primary ad category — maximising
ad inventory coverage per keyword cluster.

Minimum top_n is 5 (enforced internally). Never bypass this minimum.
"""
from .angle_scorer import (
    ALL_ANGLE_TYPES,
    ANGLE_PRIMARY_AD_CATEGORY,
    DISCOVERY_SIGNAL_BOOST,
    angle_rsoc_score,
    classify_vertical_fine,
    get_discovery_boosts,
    map_discovery_context,
)


def score_all_angles(
    keyword: str,
    vertical: str,           # fine-grained vertical key (or "unknown")
    language: str,
    cpc_usd: float,
    intent_classification: str,
    competitor_saturation: float,
    discovery_signal_type: str,
) -> list:
    """
    Score all 10 angle types for a keyword.
    Returns list of dicts sorted by rsoc_score descending.
    """
    boosts = get_discovery_boosts(discovery_signal_type)
    results = []
    for angle_type in ALL_ANGLE_TYPES:
        boost = boosts.get(angle_type, 0.0)
        score = angle_rsoc_score(
            angle_type=angle_type,
            vertical=vertical,
            language=language,
            cpc_usd=cpc_usd,
            intent_classification=intent_classification,
            competitor_saturation=competitor_saturation,
            discovery_boost=boost,
        )
        results.append({
            "angle_type":        angle_type,
            "rsoc_score":        score,
            "ad_category":       ANGLE_PRIMARY_AD_CATEGORY.get(angle_type, "other"),
            "discovery_boosted": boost > 0.0,
            "selected":          False,
        })
    results.sort(key=lambda x: x["rsoc_score"], reverse=True)
    return results


def select_angles(
    keyword: str,
    coarse_vertical: str,
    language: str,
    cpc_usd: float,
    intent_classification: str,
    competitor_saturation: float = 0.5,
    top_n: int = 5,
    diversity_rule: bool = True,
    discovery_context: dict = None,
) -> list:
    """
    Score all 10 angles and return the top N (minimum 5, enforced).

    diversity_rule: if True, no two selected angles share the same primary ad_category.
    Backfills from remaining candidates if diversity rule reduces selection below top_n.

    Returns full list with selected=True on chosen angles.
    """
    top_n = max(top_n, 5)  # spec: always ≥5 angles per keyword

    if discovery_context is None:
        discovery_context = {"signal_type": "keyword_expansion", "signal_text": ""}
    discovery_signal_type = discovery_context.get("signal_type", "keyword_expansion")

    # Resolve fine-grained vertical
    fine_vertical = classify_vertical_fine(coarse_vertical, keyword)

    candidates = score_all_angles(
        keyword=keyword,
        vertical=fine_vertical,
        language=language,
        cpc_usd=cpc_usd,
        intent_classification=intent_classification,
        competitor_saturation=competitor_saturation,
        discovery_signal_type=discovery_signal_type,
    )

    if not diversity_rule:
        for c in candidates[:top_n]:
            c["selected"] = True
        return candidates

    # Apply diversity rule: select top-scoring angle per ad_category
    seen_categories: set = set()
    selected: list = []

    for c in candidates:
        if len(selected) >= top_n:
            break
        cat = c["ad_category"]
        if cat not in seen_categories:
            c["selected"] = True
            selected.append(c)
            seen_categories.add(cat)

    # Backfill: if diversity rule left us with < top_n, add best remaining
    if len(selected) < top_n:
        for c in candidates:
            if len(selected) >= top_n:
                break
            if not c["selected"]:
                c["selected"] = True
                selected.append(c)

    return candidates  # full list; caller filters on selected=True
