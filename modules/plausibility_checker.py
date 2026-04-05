"""
plausibility_checker.py — Phase 6b: LLM plausibility validation for expanded keywords.

Filters expanded keywords by asking the LLM whether the entity+vertical
combination is plausible for a real-world search query.

Exposed API:
    check_batch(expansions) -> list[dict]

Adds `plausible: bool` (and optionally `plausibility_reason: str` on False)
to each expansion dict. Returns the same list with those fields populated.
"""

import json
import re
import sys
import time

from llm_client import call as _llm_call, LLMError

PLAUSIBILITY_BATCH_SIZE = 50   # expansions per LLM call
MAX_RETRIES             = 3
RETRY_DELAY             = 2.0

# ── Prompt ───────────────────────────────────────────────────────────────────
_PLAUSIBILITY_SYSTEM = """\
You are a plausibility checker for keyword expansions in digital advertising.

For each expansion below, determine if it is a plausible, real-world keyword that a user might actually search for.

Return a JSON array where each object has:
- source_keyword: (copy from input)
- expanded_keyword: (copy from input)
- plausible: true or false
- reason: one sentence only if false (omit if true)

Rules for plausible: false
- The entity and vertical don't logically combine (e.g., "IKEA Auto Insurance", "Diabetes Inhalers")
- The brand doesn't operate in that space in real life
- The combination sounds nonsensical to an average user

Rules for plausible: true
- The combination is a real service/product the company offers OR
- It's a service/product adjacent enough that users would realistically search for it AND
- Government programs + any housing/financial vertical = always plausible
- Membership retailers (Sam's Club, Costco, BJ's) + insurance = always plausible (they sell it)
- Any insurance carrier + auto/life/home insurance = always plausible
- Pharma drugs + "without insurance" / "patient assistance" = always plausible

Return ONLY a valid JSON array, same order as input. No markdown, no explanation."""


def _strip_code_fences(text: str) -> str:
    return re.sub(r'^```json?\n?|\n?```$', '', text.strip(), flags=re.MULTILINE)


def check_batch(expansions: list) -> list:
    """
    Check plausibility of expanded keywords.

    Args:
        expansions: list of expansion dicts (from template_expander.expand_batch)

    Returns:
        Same list with `plausible` bool and optional `plausibility_reason` str added.
        On total LLM failure, marks all as plausible=True (fail-open to preserve recall).
    """
    if not expansions:
        return expansions

    # Build index for result alignment
    exp_by_idx = {i: exp for i, exp in enumerate(expansions)}

    for batch_start in range(0, len(expansions), PLAUSIBILITY_BATCH_SIZE):
        batch = expansions[batch_start : batch_start + PLAUSIBILITY_BATCH_SIZE]

        user_content = "Expansions to check:\n" + json.dumps(
            [{"source_keyword": e["source_keyword"], "expanded_keyword": e["keyword"]}
             for e in batch],
            ensure_ascii=False, indent=2
        )

        parsed = None
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                raw = _llm_call(
                    [{"role": "system", "content": _PLAUSIBILITY_SYSTEM},
                     {"role": "user", "content": user_content}],
                    max_tokens=3000,
                    temperature=0.1,
                    timeout="normal",
                    stage="plausibility_checker",
                )

                cleaned = _strip_code_fences(raw)
                cleaned = re.sub(r'<think>.*?</think>', '', cleaned, flags=re.DOTALL).strip()
                parsed  = json.loads(cleaned)

                if not isinstance(parsed, list):
                    raise ValueError(f"Expected list, got {type(parsed).__name__}")

                break

            except (Exception, LLMError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if parsed is None:
            # Fail-open: mark all in batch as plausible to preserve recall
            print(f"  [plausibility] FAILED batch {batch_start//PLAUSIBILITY_BATCH_SIZE + 1} "
                  f"after {MAX_RETRIES} attempts: {last_error}. Marking all as plausible.",
                  file=sys.stderr)
            for exp in batch:
                exp.setdefault("plausible", True)
            continue

        # Align results back to expansion dicts
        result_map = {}
        for item in parsed:
            if not isinstance(item, dict):
                continue
            key = (item.get("source_keyword", ""), item.get("expanded_keyword", ""))
            result_map[key] = item

        for exp in batch:
            key    = (exp["source_keyword"], exp["keyword"])
            result = result_map.get(key)
            if result:
                exp["plausible"] = bool(result.get("plausible", True))
                if not exp["plausible"] and result.get("reason"):
                    exp["plausibility_reason"] = result["reason"]
                else:
                    exp.setdefault("plausible", True)
            else:
                exp.setdefault("plausible", True)  # not found in result → fail-open

    return expansions


if __name__ == "__main__":
    test_expansions = [
        {
            "keyword":        "BJ's Auto Insurance Cost",
            "source_keyword": "Sam's Club Auto Insurance Cost",
            "swapped_slot":   "membership_retailer",
            "new_value":      "BJ's Wholesale",
            "entity_status":  "test",
            "country":        "US",
        },
        {
            "keyword":        "IKEA Auto Insurance Cost",
            "source_keyword": "Sam's Club Auto Insurance Cost",
            "swapped_slot":   "membership_retailer",
            "new_value":      "IKEA",
            "entity_status":  "test",
            "country":        "US",
        },
        {
            "keyword":        "Section 8 Apartments for Rent Near Me",
            "source_keyword": "SSI Apartments for Rent Near Me",
            "swapped_slot":   "government_program",
            "new_value":      "Section 8",
            "entity_status":  "test",
            "country":        "US",
        },
        {
            "keyword":        "Ozempic Without Insurance",
            "source_keyword": "Mounjaro Without Insurance",
            "swapped_slot":   "pharma_drug",
            "new_value":      "Ozempic",
            "entity_status":  "test",
            "country":        "US",
        },
    ]

    print(f"Checking plausibility of {len(test_expansions)} expansions…\n")
    results = check_batch(test_expansions)

    for r in results:
        status = "✓ plausible" if r.get("plausible") else "✗ implausible"
        reason = f" — {r.get('plausibility_reason', '')}" if not r.get("plausible") else ""
        print(f"  {status}: {r['keyword']!r}{reason}")

    # Expected: IKEA Auto Insurance → implausible; others → plausible
    ikea = next((r for r in results if "IKEA" in r["keyword"]), None)
    if ikea:
        label = "OK" if not ikea["plausible"] else "WARN (expected implausible)"
        print(f"\n[{label}] IKEA Auto Insurance: plausible={ikea['plausible']}")
