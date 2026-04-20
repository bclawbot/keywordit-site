"""
template_decomposer.py — Phase 5: LLM-based keyword structure analysis.

Decomposes keywords into their structural components and determines if they
are expandable via entity substitution. Runs in batches against the local
qwen model via LiteLLM proxy (falls back to direct Ollama).

Exposed API:
    decompose_batch(keywords, country) -> list[dict]

Output schema per keyword:
    {
        "keyword":      str,
        "entity":       str|None,
        "entity_type":  str|None,
        "vertical":     str,
        "intent":       str,
        "demographic":  str|None,
        "template":     str|None,
        "expandable":   bool,
        "confidence":   "high"|"medium"|"low"
    }
"""

import json
import re
import sys
import time

from llm_client import call as _llm_call, LLMError

BATCH_SIZE      = 30    # keywords per LLM call (20-50 per spec)
MAX_RETRIES     = 3
RETRY_DELAY     = 2.0   # seconds

# ── Decomposition prompt ─────────────────────────────────────────────────────
_DECOMPOSE_SYSTEM = """\
You are a keyword structure analyzer for a digital advertising pipeline.

Given a list of keywords, extract the structural components of each and determine if it is expandable via entity substitution.

For EACH keyword, return a JSON object with these exact fields:
- keyword: the original keyword (string)
- entity: the specific brand/entity name in the keyword, or null if generic (string|null)
- entity_type: category of the entity — one of: membership_retailer, insurance_carrier, government_program, employer, gig_platform, demographic_org, pharmaceutical, auction_program, education_format, or null (string|null)
- vertical: the topic vertical — one of: auto_insurance, life_insurance, legal_services, housing_ssi, veterans_military, medical_pharma, german_ausbildung, latam_delivery, cruises_travel, home_services, loans_credit, seniors_demographics, membership_retail, general (string)
- intent: user intent — one of: cost_comparison, eligibility_check, application_start, information_seeking, near_me_search, desperation_signal (string)
- demographic: specific demographic targeted, or null (string|null)
- template: the keyword with the entity replaced by {entity_type} placeholder, or null if not expandable (string|null)
- expandable: true if there are other plausible entities that could fill the same slot (boolean)
- confidence: "high" | "medium" | "low"

Rules:
- Generic keywords with no specific entity are NOT expandable (expandable: false)
- First-person desperation keywords are NOT expandable
- Only create a template if you are confident other real entities fit the same slot
- Government programs (Section 8, SNAP, Medicare, SSI, VA) ARE expandable via government_program type
- Return ONLY a valid JSON array containing one object per input keyword, in the same order
- No explanation, no markdown, no code fences

Examples:
Input: ["Sam's Club Auto Insurance Cost", "I Can't Find a Lawyer to Take My Case", "Ausbildung für Ausländer mit B1"]
Output: [
  {"keyword":"Sam's Club Auto Insurance Cost","entity":"Sam's Club","entity_type":"membership_retailer","vertical":"auto_insurance","intent":"cost_comparison","demographic":null,"template":"{membership_retailer} Auto Insurance Cost","expandable":true,"confidence":"high"},
  {"keyword":"I Can't Find a Lawyer to Take My Case","entity":null,"entity_type":null,"vertical":"legal_services","intent":"desperation_signal","demographic":null,"template":null,"expandable":false,"confidence":"high"},
  {"keyword":"Ausbildung für Ausländer mit B1","entity":null,"entity_type":"employer","vertical":"german_ausbildung","intent":"eligibility_check","demographic":"immigrants","template":"Ausbildung für {nationality} mit {language_level}","expandable":true,"confidence":"high"}
]"""


def _strip_code_fences(text: str) -> str:
    return re.sub(r'^```json?\n?|\n?```$', '', text.strip(), flags=re.MULTILINE)


def _safe_fields(item: dict, keyword: str) -> dict:
    """Ensure all required fields exist with safe defaults."""
    return {
        "keyword":     item.get("keyword", keyword),
        "entity":      item.get("entity"),
        "entity_type": item.get("entity_type"),
        "vertical":    item.get("vertical", "general"),
        "intent":      item.get("intent", "information_seeking"),
        "demographic": item.get("demographic"),
        "template":    item.get("template"),
        "expandable":  bool(item.get("expandable", False)),
        "confidence":  item.get("confidence", "low"),
    }


def _fallback_entries(keywords: list) -> list:
    """Return non-expandable fallback dicts when LLM fails entirely."""
    return [
        {
            "keyword": kw, "entity": None, "entity_type": None,
            "vertical": "general", "intent": "information_seeking",
            "demographic": None, "template": None, "expandable": False,
            "confidence": "low",
        }
        for kw in keywords
    ]


def decompose_batch(keywords: list, country: str = "US") -> list:
    """
    Decompose a list of keyword strings into structural components.

    Args:
        keywords: list of keyword strings
        country:  ISO country code (used for context in future prompt variants)

    Returns:
        list of decomposition dicts, one per keyword (same order).
        On total failure, returns non-expandable fallback dicts.
    """
    if not keywords:
        return []

    results = []

    # Process in batches
    for batch_start in range(0, len(keywords), BATCH_SIZE):
        batch = keywords[batch_start : batch_start + BATCH_SIZE]
        batch_json = json.dumps(batch, ensure_ascii=False)

        parsed = None
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                raw = _llm_call(
                    [{"role": "system", "content": _DECOMPOSE_SYSTEM},
                     {"role": "user", "content": f"Analyze these keywords:\n{batch_json}"}],
                    max_tokens=4096,
                    temperature=0.1,
                    timeout="normal",
                    stage="template_decomposer",
                )

                cleaned = _strip_code_fences(raw)

                # Handle <think>...</think> tags from qwen3
                cleaned = re.sub(r'<think>.*?</think>', '', cleaned, flags=re.DOTALL).strip()

                parsed = json.loads(cleaned)

                # Validate it's a list of the right length
                if not isinstance(parsed, list):
                    raise ValueError(f"Expected list, got {type(parsed).__name__}")
                if len(parsed) != len(batch):
                    print(f"  [decomposer] WARNING: got {len(parsed)} results for {len(batch)} keywords "
                          f"(batch {batch_start//BATCH_SIZE + 1})", file=sys.stderr)

                break  # success

            except (Exception, LLMError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if parsed is None:
            print(f"  [decomposer] FAILED batch {batch_start//BATCH_SIZE + 1} "
                  f"after {MAX_RETRIES} attempts: {last_error}", file=sys.stderr)
            results.extend(_fallback_entries(batch))
            continue

        # Align parsed results to batch keywords (handle length mismatches)
        for i, kw in enumerate(batch):
            if i < len(parsed):
                item = parsed[i]
                if not isinstance(item, dict):
                    results.append(_fallback_entries([kw])[0])
                else:
                    results.append(_safe_fields(item, kw))
            else:
                results.append(_fallback_entries([kw])[0])

    return results


if __name__ == "__main__":
    test_keywords = [
        "Sam's Club Auto Insurance Cost",
        "I Can't Find a Lawyer to Take My Case",
        "Ausbildung für Ausländer mit B1",
        "SSI Apartments for Rent Near Me",
        "Va Veterans Discounts",
        "Mounjaro Without Insurance",
        "Bad Credit Motorcycle Loans",
        "Cheap Flights to Las Vegas",
        "Welding Machine Price India",
        "Amazon Delivery Driver Jobs Near Me",
    ]

    print(f"Testing decompose_batch with {len(test_keywords)} keywords…\n")
    results = decompose_batch(test_keywords, "US")

    for r in results:
        expandable = "✓ expandable" if r["expandable"] else "✗ not expandable"
        entity_str = f"entity={r['entity']!r} ({r['entity_type']})" if r["entity"] else "no entity"
        print(f"  [{r['confidence']}] {r['keyword']!r}")
        print(f"    {expandable} | vertical={r['vertical']} | intent={r['intent']}")
        print(f"    {entity_str}")
        if r["template"]:
            print(f"    template={r['template']!r}")
        print()
