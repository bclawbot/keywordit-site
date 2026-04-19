"""
money_flow_classifier.py — Stage 1c

Classifies trend headlines into money-flow archetypes that predict where
advertiser ad spend will increase. Enriches explosive_trends.json in-place.

Two layers:
  Layer 1: Regex archetype matching (fast, deterministic, zero-cost).
  Layer 2: Local LLM fallback for ambiguous headlines (free via Ollama).

Integration: Does NOT create a separate output file. Writes enriched
records back to explosive_trends.json so downstream stages (keyword_expander,
keyword_extractor, vetting) automatically see the money_flow metadata.
"""

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

from llm_client import call as _llm_call, LLMError  # noqa: E402

TARGET = BASE / "explosive_trends.json"
ERROR_LOG = BASE / "error_log.jsonl"

# ── Archetype definitions ────────────────────────────────────────────────────

ARCHETYPES = {
    "regulatory_policy": re.compile(
        r"\b(new law|regulation|ban on|visa program|policy change|executive order|"
        r"tariff|sanction|mandate|compliance|rule change)\b", re.IGNORECASE),
    "natural_disaster": re.compile(
        r"\b(hurricane|earthquake|flood|wildfire|tornado|storm damage|"
        r"tsunami|cyclone|typhoon|landslide|volcano)\b", re.IGNORECASE),
    "corporate_market": re.compile(
        r"\b(merger|acquisition|layoff|layoffs|IPO|bankruptcy|stock crash|"
        r"buyout|earnings miss|delisting|chapter 11)\b", re.IGNORECASE),
    "health_medical": re.compile(
        r"\b(outbreak|FDA approval|recall|pandemic|drug|vaccine|"
        r"clinical trial|side effect|epidemic|health advisory)\b", re.IGNORECASE),
    "technology": re.compile(
        r"\b(launch|data breach|AI|cybersecurity|hack|vulnerability|"
        r"ransomware|zero-day|exploit|leak|cyberattack)\b", re.IGNORECASE),
    "economic": re.compile(
        r"\b(inflation|interest rates?|recession|housing market|unemployment|"
        r"gdp|fed rate|mortgage rates?|stimulus|federal reserve)\b", re.IGNORECASE),
}

ARCHETYPE_VERTICALS = {
    "regulatory_policy": ["legal", "insurance", "finance", "government_benefits"],
    "natural_disaster":  ["insurance", "home_services", "legal", "health"],
    "corporate_market":  ["finance", "legal", "software", "education"],
    "health_medical":    ["health", "insurance", "legal", "education"],
    "technology":        ["cybersecurity", "software", "education", "insurance"],
    "economic":          ["finance", "real_estate", "insurance", "education"],
}

# Urgency = publish-speed strategy. Breaking → first-mover wins. Developing → depth wins.
# There are NO delay fields — auto-bidding responds in minutes, score immediately.
ARCHETYPE_URGENCY = {
    "regulatory_policy": "developing",
    "natural_disaster":  "breaking",
    "corporate_market":  "developing",
    "health_medical":    "breaking",
    "technology":        "breaking",
    "economic":          "developing",
}

VALID_VERTICALS = {
    "finance", "insurance", "legal", "health", "real_estate", "education",
    "software", "saas", "tech", "travel", "automotive", "home_services",
    "cybersecurity", "government_benefits",
}

# ── LLM fallback ──────────────────────────────────────────────────────────────

LLM_SYSTEM_PROMPT = (
    "You are a media buying analyst. Determine if a news headline will cause "
    "advertisers to increase spending on any commercial vertical. "
    "Respond with JSON only — no prose, no code fences."
)

LLM_USER_TEMPLATE = """Headline: "{headline}"
Country: {country}

Answer in EXACTLY this JSON format (no other text):
{{
  "has_money_flow": true/false,
  "archetype": "regulatory_policy|natural_disaster|corporate_market|health_medical|technology|economic|other",
  "predicted_verticals": ["vertical1", "vertical2"],
  "confidence": 0.0-1.0,
  "reasoning": "one sentence why"
}}

Valid verticals: finance, insurance, legal, health, real_estate, education, software, saas, tech, travel, automotive, home_services, cybersecurity, government_benefits

If the headline is about celebrity gossip, sports scores, entertainment, or has NO commercial implications, set has_money_flow to false.
"""


def _log_error(stage: str, error: str):
    try:
        with ERROR_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "timestamp": datetime.now().isoformat(),
                "stage": stage,
                "error": str(error)[:500],
            }) + "\n")
    except Exception:
        pass


def _parse_llm_json(text: str) -> dict:
    """Extract JSON object from LLM response (tolerates surrounding prose)."""
    text = text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    # Find the outermost JSON object
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("no JSON object found in response")
    return json.loads(text[start:end + 1])


def _llm_classify(headline: str, country: str) -> dict:
    """Fallback classification via local Ollama. Cost: $0, ~2-3 seconds."""
    try:
        raw = _llm_call(
            [
                {"role": "system", "content": LLM_SYSTEM_PROMPT},
                {"role": "user", "content": LLM_USER_TEMPLATE.format(
                    headline=headline, country=country)},
            ],
            max_tokens=256,
            temperature=0.1,
            timeout="bg",
            stage="money_flow_classifier/llm",
            local_only=True,
        )
        parsed = _parse_llm_json(raw)
        # Validate archetype
        arche = parsed.get("archetype")
        if arche not in ARCHETYPE_VERTICALS and arche != "other":
            arche = "other"
        # Filter to valid verticals
        verticals = [v for v in parsed.get("predicted_verticals", [])
                     if v in VALID_VERTICALS]
        return {
            "has_money_flow": bool(parsed.get("has_money_flow", False)),
            "archetype": arche,
            "predicted_verticals": verticals,
            "confidence": min(1.0, max(0.0, float(parsed.get("confidence", 0.5)))),
            "reasoning": str(parsed.get("reasoning", ""))[:200],
        }
    except (LLMError, ValueError, json.JSONDecodeError, KeyError) as e:
        _log_error("money_flow_classifier/llm", f"{type(e).__name__}: {e}")
        return {"has_money_flow": False, "archetype": None, "confidence": 0}
    except Exception as e:
        _log_error("money_flow_classifier/llm", f"unexpected: {type(e).__name__}: {e}")
        return {"has_money_flow": False, "archetype": None, "confidence": 0}


# ── Main classification ──────────────────────────────────────────────────────

def classify(headline: str, country: str = "US", use_llm: bool = True) -> dict:
    """
    Classify a headline into a money-flow archetype.

    Returns:
        {
            "archetype": str or None,
            "predicted_verticals": list[str],
            "confidence": float,
            "urgency": "breaking" | "developing" | None,
            "money_flow_score": float (0-1),
            "classification_method": "regex" | "llm" | "none"
        }
    """
    text = (headline or "").strip()
    if not text:
        return {
            "archetype": None,
            "predicted_verticals": [],
            "confidence": 0,
            "urgency": None,
            "money_flow_score": 0,
            "classification_method": "none",
        }

    # Layer 1: Regex (fast, deterministic)
    for name, pattern in ARCHETYPES.items():
        if pattern.search(text):
            return {
                "archetype": name,
                "predicted_verticals": ARCHETYPE_VERTICALS[name],
                "confidence": 0.8,
                "urgency": ARCHETYPE_URGENCY[name],
                "money_flow_score": 0.8,
                "classification_method": "regex",
            }

    # Layer 2: LLM fallback (optional)
    if use_llm:
        llm_result = _llm_classify(text, country)
        if llm_result.get("has_money_flow") and llm_result.get("archetype"):
            conf = llm_result.get("confidence", 0.5)
            return {
                "archetype": llm_result["archetype"],
                "predicted_verticals": llm_result.get("predicted_verticals", []),
                "confidence": conf,
                "urgency": "developing",  # Conservative default for LLM-classified
                "money_flow_score": conf,
                "classification_method": "llm",
                "reasoning": llm_result.get("reasoning", ""),
            }

    return {
        "archetype": None,
        "predicted_verticals": [],
        "confidence": 0,
        "urgency": None,
        "money_flow_score": 0,
        "classification_method": "none",
    }


# ── Budget for LLM fallback ──────────────────────────────────────────────────
# Cap LLM calls per run so a bad day on Ollama can't stall the pipeline.
LLM_MAX_CALLS_PER_RUN = 100
CHECKPOINT_EVERY = 200  # persist partial classifications to survive a kill


def _atomic_save(target: Path, data):
    """Write JSON atomically via tmpfile + os.replace so a mid-write kill
    never leaves a partial/corrupt file."""
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                   encoding="utf-8")
    os.replace(tmp, target)


def run(input_path=None):
    """Read explosive_trends.json, enrich each record, write back to same file."""
    target = Path(input_path) if input_path else TARGET

    if not target.exists():
        print(f"[money-flow] Input not found: {target}")
        return

    try:
        trends = json.loads(target.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[money-flow] Could not parse {target.name}: {e}")
        return

    if not isinstance(trends, list):
        print(f"[money-flow] Expected list in {target.name}, got {type(trends).__name__}")
        return

    classified_regex = 0
    classified_llm = 0
    llm_calls = 0
    llm_skipped_budget = 0
    no_flow = 0

    for i, trend in enumerate(trends, 1):
        headline = trend.get("term") or trend.get("keyword", "")
        country = trend.get("geo") or trend.get("country", "US")

        # Gate LLM by per-run budget
        use_llm = llm_calls < LLM_MAX_CALLS_PER_RUN
        if not use_llm:
            llm_skipped_budget += 1

        result = classify(headline, country, use_llm=use_llm)
        if result["classification_method"] == "llm":
            llm_calls += 1
        trend["money_flow"] = result

        if result["archetype"]:
            if result["classification_method"] == "regex":
                classified_regex += 1
            else:
                classified_llm += 1
        else:
            no_flow += 1

        if i % CHECKPOINT_EVERY == 0:
            _atomic_save(target, trends)
            print(f"[money-flow] checkpoint {i}/{len(trends)} saved")

    _atomic_save(target, trends)

    total = len(trends)
    classified = classified_regex + classified_llm
    print(f"[money-flow] {total} trends processed: "
          f"{classified} classified ({classified_regex} regex, "
          f"{classified_llm} LLM), {no_flow} no money flow")
    if llm_skipped_budget:
        print(f"[money-flow] {llm_skipped_budget} trends skipped LLM "
              f"(budget {LLM_MAX_CALLS_PER_RUN}/run)")


if __name__ == "__main__":
    run()
