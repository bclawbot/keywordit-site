"""
keyword_scorer.py — LLM-based keyword quality scorer (Phase 4 upgrade).

Replaces simple extraction confidence scoring with a few-shot prompted
quality score (1-10) enriched with vertical classification and intent signals.
Loaded from data/winner_dna.json at runtime to stay current without code changes.

Output schema per keyword:
    {
        "keyword":       str,
        "score":         int,           # 1-10
        "vertical_match": str,          # e.g. "auto_insurance", "housing_ssi"
        "intent_signals": list[str],    # e.g. ["cost_price_inquiry", "membership_brand"]
        "reasoning":     str            # 1-2 sentence explanation
    }

Usage:
    from modules.keyword_scorer import score_keywords_batch
    results = score_keywords_batch([
        {"keyword": "Sam's Club Auto Insurance Cost", "country": "US"},
        {"keyword": "Laser Cutting Machine Price", "country": "IN"},
    ])
"""

import json
import os
import re
import requests

# ── Config ──────────────────────────────────────────────────────────────────
_BASE_DIR = os.path.dirname(__file__)
_WINNER_DNA_PATH = os.path.join(_BASE_DIR, '..', 'data', 'winner_dna.json')

LITELLM_URL    = os.environ.get("LITELLM_URL",   "http://localhost:4000/v1/chat/completions")
LITELLM_API_KEY = os.environ.get("LITELLM_API_KEY", "sk-dwight")
LITELLM_MODEL  = os.environ.get("LITELLM_MODEL",  "dwight-primary")

_FEW_SHOT_CACHE = None


def _load_few_shot_examples() -> tuple:
    """Load positive (top 15) and negative (last 10 anti-patterns) examples from winner_dna.json."""
    global _FEW_SHOT_CACHE
    if _FEW_SHOT_CACHE is not None:
        return _FEW_SHOT_CACHE

    with open(_WINNER_DNA_PATH, encoding='utf-8') as f:
        dna = json.load(f)

    positive = [e for e in dna if 'why_it_works' in e][:15]
    negative = [e for e in dna if 'why_it_fails' in e][-10:]

    _FEW_SHOT_CACHE = (positive, negative)
    return _FEW_SHOT_CACHE


def _build_system_prompt() -> str:
    positive, negative = _load_few_shot_examples()

    pos_lines = []
    for e in positive:
        signals = ", ".join(e.get("intent_signals", [])) or "none"
        pos_lines.append(
            f'  Keyword: "{e["keyword"]}" | Country: {e["country"]} | '
            f'Revenue: ${e["revenue"]} | Vertical: {e["vertical"]}\n'
            f'  Intent: {signals}\n'
            f'  Why it works: {e["why_it_works"]}\n'
            f'  → Score: 9\n'
        )

    neg_lines = []
    for e in negative:
        signals = ", ".join(e.get("intent_signals", [])) or "none"
        neg_lines.append(
            f'  Keyword: "{e["keyword"]}" | Country: {e["country"]} | '
            f'Revenue: ${e["revenue"]} | Vertical: {e.get("vertical", "unknown")}\n'
            f'  Intent: {signals}\n'
            f'  Why it fails: {e["why_it_fails"]}\n'
            f'  → Score: 2\n'
        )

    pos_block = "\n".join(pos_lines)
    neg_block = "\n".join(neg_lines)

    return f"""You are a keyword quality scorer for RSOC (Redirected Search on Content) arbitrage.
Your job: score keywords 1-10 based on revenue potential in the RSOC model.

━━━ RSOC ECONOMICS ━━━
Traffic source: Facebook native ads (CPM ~$3-8). Monetization: RSOC search feeds (RPM ~$40-400).
Profit = feed RPM − traffic acquisition CPM. Only keywords with HIGH advertiser CPC generate positive RSOC RPM.

━━━ SCORING RUBRIC ━━━
10 — Exceptional: Tier S vertical (auto/life insurance, legal) + Tier S country (US/DE/JP) + strong intent signal
9  — Excellent: Proven vertical + qualified intent + good country tier
8  — Strong: Proven vertical or strong intent, minor country/signal gap
7  — Above average: Known commercial vertical, passable country, some intent
6  — Average: Broad commercial intent, no clear vertical match
5  — Borderline: Informational lean with some commercial adjacency
4  — Weak: Low-CPC vertical OR Tier C country with no vertical override
3  — Poor: Generic or low-intent in weak market
2  — Very poor: Known anti-pattern (B2B machinery, prop firm, wholesale)
1  — Reject: Zero commercial value (celebrity gossip, memes, no advertiser match)

━━━ KEY RULES ━━━
- Branded keywords (Sam's, Costco, USAA, AARP) get +1 bonus for brand trust transfer
- First-person desperation ("I can't find…", "I need help…") → strong positive signal
- Negative qualifiers ("without insurance", "bad credit") → strong positive signal
- B2B industrial (machine, welding, CNC, forklift) → always score 1-3
- Year qualifiers (2026, 2027) → slight positive, do not over-weight
- "Near me" → positive local intent signal
- India (IN) + non-insurance → score ≤ 4 (RPC $0.07, almost never profitable)
- Tier C countries (TJ, FJ, PK, NG, etc.) → score ≤ 4 unless auto/life insurance

━━━ KNOWN VERTICALS (use exact names) ━━━
auto_insurance, life_insurance, legal_services, housing_ssi, membership_retail,
veterans_military, german_ausbildung, medical_pharma, latam_delivery, cruises_travel,
home_services, loans_credit, seniors_demographics, general

━━━ KNOWN INTENT SIGNALS (use exact names) ━━━
cost_price_inquiry, near_me_local, membership_brand, year_current, discount_deal,
demographic_targeted, guaranteed_approval, first_person_desperation, negative_qualifier,
branded, informational_generic, generic_best

━━━ POSITIVE EXAMPLES (real data, $899K dataset) ━━━
{pos_block}
━━━ ANTI-PATTERN EXAMPLES ━━━
{neg_block}
━━━ OUTPUT FORMAT ━━━
Return ONLY a valid JSON array. No markdown, no code fences, no explanation.

[
  {{
    "keyword": "keyword string exactly as input",
    "score": 9,
    "vertical_match": "auto_insurance",
    "intent_signals": ["cost_price_inquiry", "membership_brand"],
    "reasoning": "One or two sentences explaining the score."
  }}
]

If a batch has N keywords, return exactly N objects in the same order."""


def _strip_code_fences(text: str) -> str:
    return re.sub(r'^```json?\n?|\n?```$', '', text.strip(), flags=re.MULTILINE)


def score_keywords_batch(keywords: list, timeout: int = 120) -> list:
    """
    Score a batch of keyword dicts via LLM.

    Args:
        keywords: list of dicts with at minimum {"keyword": str, "country": str}
        timeout:  HTTP timeout in seconds

    Returns:
        list of scored dicts with fields: keyword, score, vertical_match,
        intent_signals, reasoning. Falls back to score=0 on parse failure.
    """
    if not keywords:
        return []

    system_prompt = _build_system_prompt()
    user_message = json.dumps([
        {"keyword": k.get("keyword", ""), "country": k.get("country", "US")}
        for k in keywords
    ], ensure_ascii=False)

    try:
        resp = requests.post(
            LITELLM_URL,
            headers={"Authorization": f"Bearer {LITELLM_API_KEY}"},
            json={
                "model": LITELLM_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_message},
                ],
                "temperature": 0.1,
                "max_tokens":  2048,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        cleaned = _strip_code_fences(raw)
        scored = json.loads(cleaned)

        # Validate and clamp scores
        results = []
        for item in scored:
            item["score"] = max(1, min(10, int(item.get("score", 5))))
            item.setdefault("vertical_match", "general")
            item.setdefault("intent_signals", [])
            item.setdefault("reasoning", "")
            results.append(item)
        return results

    except Exception as e:
        # Graceful fallback: return input with score=0 (caller can handle)
        fallback = []
        for k in keywords:
            fallback.append({
                "keyword": k.get("keyword", ""),
                "score": 0,
                "vertical_match": "unknown",
                "intent_signals": [],
                "reasoning": f"Scoring failed: {e}",
            })
        return fallback


def score_single(keyword: str, country: str = "US") -> dict:
    """Convenience wrapper for a single keyword."""
    results = score_keywords_batch([{"keyword": keyword, "country": country}])
    return results[0] if results else {}


if __name__ == '__main__':
    # Dry-run: print the prompt (no LLM call) + test signal detection
    positive, negative = _load_few_shot_examples()
    print(f"Few-shot loaded: {len(positive)} positive, {len(negative)} negative examples")
    print("\n--- SYSTEM PROMPT PREVIEW (first 800 chars) ---")
    prompt = _build_system_prompt()
    print(prompt[:800])
    print("...(truncated)...")

    # Show what a result should look like for our test keywords
    expected = [
        ("Sam's Club Auto Insurance Cost",     "US", (9, 10)),
        ("Ausbildung für Ausländer mit B1",     "DE", (8, 10)),
        ("Laser Cutting Machine Price",          "IN", (1, 3)),
        ("I Can't Find a Lawyer to Take My Case","US",(8, 10)),
        ("Free Prop Firm Challenge 2026",        "FJ", (1, 2)),
    ]
    print("\n--- Expected score ranges ---")
    for kw, co, (lo, hi) in expected:
        print(f"  [{lo}-{hi}] '{kw}' / {co}")
