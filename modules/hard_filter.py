"""
hard_filter.py — Pre-LLM noise gate for the experimental pipeline.

Loads data/hard_filters.json and applies:
  1. Country whitelist check (with Tier C handling)
  2. Word count bounds
  3. Blocklist words
  4. Blocklist verticals
  5. Tier C country: require Tier S vertical match

Usage:
    from modules.hard_filter import hard_filter
    passed, reason = hard_filter("Ssi Apartments for Rent Near Me", "US")
"""

import json
import os
import re

_FILTERS = None
_FILTERS_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'hard_filters.json')


def _load_filters():
    global _FILTERS
    if _FILTERS is None:
        with open(_FILTERS_PATH, encoding='utf-8') as f:
            _FILTERS = json.load(f)
    return _FILTERS


def hard_filter(keyword: str, country: str, vertical_hint: str = None) -> tuple:
    """
    Returns: (True, "PASS") or (False, REASON) where REASON is one of:
        COUNTRY_BLOCKED, WORD_COUNT_TOO_SHORT, WORD_COUNT_TOO_LONG,
        BLOCKLIST_WORD, BLOCKLIST_VERTICAL, TIER_C_NO_TIER_S_VERTICAL
    """
    filters = _load_filters()
    kl = keyword.lower().strip()

    # 1. Country check
    whitelist = [c.upper() for c in filters.get('country_whitelist', [])]
    tier_c = [c.upper() for c in filters.get('tier_c_countries', [])]
    country_upper = country.upper()

    is_whitelisted = country_upper in whitelist
    is_tier_c = country_upper in tier_c

    if not is_whitelisted and not is_tier_c:
        # Completely unknown country — treat as Tier C
        is_tier_c = True

    if not is_whitelisted and not is_tier_c:
        return (False, 'COUNTRY_BLOCKED')

    # 2. Word count
    words = kl.split()
    wc_min = filters.get('word_count', {}).get('min', 1)
    wc_max = filters.get('word_count', {}).get('max', 10)

    if len(words) < wc_min:
        return (False, 'WORD_COUNT_TOO_SHORT')
    if len(words) > wc_max:
        return (False, 'WORD_COUNT_TOO_LONG')

    # 3. Blocklist words (any single token match)
    blocklist_words = [w.lower() for w in filters.get('blocklist_words', [])]
    for bw in blocklist_words:
        if bw in kl:
            return (False, 'BLOCKLIST_WORD')

    # 4. Blocklist verticals (phrase match)
    blocklist_verticals = [v.lower() for v in filters.get('blocklist_verticals', [])]
    for bv in blocklist_verticals:
        if bv in kl:
            return (False, 'BLOCKLIST_VERTICAL')

    # 5. Tier C requires Tier S vertical
    tier_c_requires = filters.get('tier_c_requires_tier_s_vertical', True)
    if is_tier_c and not is_whitelisted and tier_c_requires:
        tier_s_triggers = [t.lower() for t in filters.get('tier_s_verticals', [])]
        matched = False

        # Check vertical_hint first
        if vertical_hint:
            vh = vertical_hint.lower()
            # auto_insurance and life_insurance are Tier S
            if any(s in vh for s in ['auto_insurance', 'life_insurance', 'car_insurance']):
                matched = True

        # Check keyword against Tier S trigger phrases
        if not matched:
            for trigger in tier_s_triggers:
                if trigger in kl:
                    matched = True
                    break

        if not matched:
            return (False, 'TIER_C_NO_TIER_S_VERTICAL')

    return (True, 'PASS')


if __name__ == '__main__':
    # Quick self-test
    tests = [
        ("Ssi Apartments for Rent Near Me", "US", None, True),
        ("Laser Cutting Machine Price", "IN", None, False),
        ("Ausbildung für Ausländer mit B1", "DE", None, True),
        ("Free Prop Firm Challenge 2026", "TJ", None, False),
        ("Auto Insurance", "ZA", "auto_insurance", True),
    ]
    for kw, co, vh, expected_pass in tests:
        result, reason = hard_filter(kw, co, vh)
        status = "OK" if (result == expected_pass) else "FAIL"
        print(f"[{status}] {kw!r} / {co} -> ({result}, {reason!r})")
