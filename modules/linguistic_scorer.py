"""
linguistic_scorer.py — Regex-based structural signal detection.

Runs BEFORE LLM scoring. Detects high-value linguistic patterns that
correlate with elevated RPC based on empirical keyword performance data.

The compound signal effect is non-linear: when signal_count >= 2, an
additional 1.2x interaction bonus is applied (from GPT intelligence report:
0 signals = $3.78/kw, 1 = $6.96, 2 = $14.17, 3 = $16.21).

Usage:
    from modules.linguistic_scorer import score_linguistic_signals
    result = score_linguistic_signals("Bad Credit Loans Guaranteed Approval Near Me")
    # {'signals': ['negative_qualifier', 'urgency_frame'], 'bonus_multiplier': 1.584, 'signal_count': 2}
"""

import re

LINGUISTIC_PATTERNS = {
    "first_person_desperation": {
        "pattern": re.compile(
            r"\b(i\s+can'?t|i\s+need|i\s+have\s+no|can'?t\s+afford|need\s+help|i'?m\s+looking\s+for)\b",
            re.IGNORECASE
        ),
        "multiplier": 1.25,
    },
    "negative_qualifier": {
        "pattern": re.compile(
            r"\b(without\s+insurance|no\s+credit\s+check|bad\s+credit|take\s+over\s+payments|despite|trotz|sans|sin\s+rechazo|no\s+deposit|no\s+down\s+payment|sin\s+cr[eé]dito|poor\s+credit)\b",
            re.IGNORECASE
        ),
        "multiplier": 1.20,
    },
    "price_anchor": {
        "pattern": re.compile(
            r"(\$\d+|\d+\s+dollars?|\bfor\s+free\b|at\s+\$|how\s+much|cotizacion|precio)",
            re.IGNORECASE
        ),
        "multiplier": 1.15,
    },
    "urgency_frame": {
        "pattern": re.compile(
            r"\b(guaranteed|instant|today|right\s+now|same\s+day|immediately|urgent|emergency|fast\s+approval|quick\s+approval)\b",
            re.IGNORECASE
        ),
        "multiplier": 1.10,
    },
}

COMPOUND_BONUS = 1.20   # Applied when signal_count >= 2
MAX_BONUS = 2.50


def score_linguistic_signals(keyword: str) -> dict:
    """
    Detect structural patterns using regex only (no LLM).

    Returns:
        {
            "signals": list[str],           # names of detected signals
            "bonus_multiplier": float,      # product of multipliers, capped at MAX_BONUS
            "signal_count": int             # number of distinct signals detected
        }
    """
    detected = []

    for signal_name, cfg in LINGUISTIC_PATTERNS.items():
        if cfg["pattern"].search(keyword):
            detected.append(signal_name)

    signal_count = len(detected)

    if signal_count == 0:
        return {"signals": [], "bonus_multiplier": 1.0, "signal_count": 0}

    # Product of individual multipliers
    multiplier = 1.0
    for sig in detected:
        multiplier *= LINGUISTIC_PATTERNS[sig]["multiplier"]

    # Non-linear compound bonus when 2+ signals fire
    if signal_count >= 2:
        multiplier *= COMPOUND_BONUS

    # Cap
    multiplier = min(round(multiplier, 4), MAX_BONUS)

    return {
        "signals": detected,
        "bonus_multiplier": multiplier,
        "signal_count": signal_count,
    }


if __name__ == '__main__':
    tests = [
        ("I Can't Find a Lawyer to Take My Case",
         {"signals_contains": ["first_person_desperation"], "bonus_min": 1.25, "bonus_max": 1.26}),
        ("Bad Credit Motorcycle Loans Guaranteed Approval Near Me",
         {"signals_contains": ["negative_qualifier", "urgency_frame"], "bonus_min": 1.44, "bonus_max": 2.50}),
        ("Sam's Club Auto Insurance Cost",
         {"signals_contains": [], "bonus_min": 1.0, "bonus_max": 1.0}),
    ]

    for kw, expected in tests:
        result = score_linguistic_signals(kw)
        ok_signals = all(s in result["signals"] for s in expected["signals_contains"])
        ok_bonus = expected["bonus_min"] <= result["bonus_multiplier"] <= expected["bonus_max"]
        status = "OK" if (ok_signals and ok_bonus) else "FAIL"
        print(f"[{status}] {kw!r}")
        print(f"       signals={result['signals']}, bonus={result['bonus_multiplier']}, count={result['signal_count']}")
        if not ok_signals:
            print(f"       EXPECTED signals to contain: {expected['signals_contains']}")
        if not ok_bonus:
            print(f"       EXPECTED bonus in [{expected['bonus_min']}, {expected['bonus_max']}]")
