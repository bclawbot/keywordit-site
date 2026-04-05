# =============================================================================
# commercial_keyword_transformer.py — Transform CPC=$0 keywords into commercial variants
#
# Takes keywords with CPC=$0 (from expanded_keywords.json or validation_history.jsonl)
# and transforms them into commercial-intent variants while preserving semantic
# connection to the source trending topic.
#
# Process:
# 1. Load keywords with CPC=$0
# 2. Join with explosive_trends.json to get trend context
# 3. Use LLM to generate commercial variant (1 per keyword)
# 4. Output transformed keywords for DataForSEO validation
#
# LLM: LiteLLM proxy at http://localhost:4000 (model: dwight-primary)
# Fallback: OpenRouter (gpt-4o-mini)
# =============================================================================

import json
import re
import time
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
EXPANDED_KEYWORDS = BASE / "expanded_keywords.json"
EXPLOSIVE_TRENDS = BASE / "explosive_trends.json"
OUTPUT = BASE / "transformed_keywords.json"
ERROR_LOG = BASE / "error_log.jsonl"

# ── Phase 2.2: Keyword Planner Probe Gate ────────────────────────────────────

def _probe_keyword_planner(keyword: str, country: str = "US") -> dict:
    """Quick probe via Google Ads Keyword Planner (free) to validate a keyword
    has real search volume before sending to DataForSEO (paid).
    Returns dict with probe_passed, search_volume, cpc_usd or empty on failure."""
    import os
    gads_ready = all([
        os.environ.get('GOOGLE_ADS_CLIENT_ID'),
        os.environ.get('GOOGLE_ADS_CLIENT_SECRET'),
        os.environ.get('GOOGLE_ADS_REFRESH_TOKEN'),
        os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN'),
        os.environ.get('GOOGLE_ADS_CUSTOMER_ID'),
    ])
    if not gads_ready:
        return {'probe_passed': None, 'reason': 'no_gads_credentials'}

    try:
        from validation import fetch_google_ads
        metrics = fetch_google_ads(keyword, country)
        if not metrics:
            return {'probe_passed': False, 'reason': 'no_results'}
        sv  = metrics.get('search_volume', 0)
        cpc = metrics.get('cpc_usd', 0)
        competition = metrics.get('competition', '')
        return {
            'probe_passed': (sv > 0 or cpc > 0) and competition != 'LOW_UNSET',
            'search_volume': sv,
            'cpc_usd': cpc,
            'competition': competition,
            'reason': 'gads_probe',
        }
    except Exception as e:
        return {'probe_passed': None, 'reason': f'probe_error: {str(e)[:100]}'}


_FALSE_POS_LOG = BASE / "false_positive_log.jsonl"

def probe_gate(transformed: list, max_probes: int = 50) -> list:
    """Run Keyword Planner probe on transformed keywords. Annotates each with
    probe_passed field. Only probes up to max_probes to stay within free tier.
    Logs killed variants to false_positive_log.jsonl per plan spec."""
    probed = 0
    passed = 0
    killed = 0
    for kw in transformed:
        if probed >= max_probes:
            break
        variant = kw.get('keyword', '')
        country = kw.get('country', 'US')
        if not variant or kw.get('transformation_failed'):
            continue
        result = _probe_keyword_planner(variant, country)
        kw['probe_result'] = result
        kw['probe_passed'] = result.get('probe_passed')
        if result.get('probe_passed') is True:
            passed += 1
            # Inject probe metrics so DataForSEO can be skipped for these
            if result.get('cpc_usd', 0) > 0:
                kw['cpc_usd'] = result['cpc_usd']
                kw['search_volume'] = result.get('search_volume', 0)
                kw['metrics_source'] = 'google_keyword_planner_probe'
        elif result.get('probe_passed') is False:
            kw['needs_dataforseo_validation'] = False  # skip paid validation
            killed += 1
            # Log killed variant to false_positive_log.jsonl
            try:
                with _FALSE_POS_LOG.open('a', encoding='utf-8') as _fplog:
                    _fplog.write(json.dumps({
                        'keyword': variant,
                        'country': country,
                        'source': 'stage_2a5_planner_kill',
                        'cpc': result.get('cpc_usd', 0),
                        'competition': result.get('competition'),
                        'ts': datetime.now().isoformat(),
                    }) + '\n')
            except Exception:
                pass
        probed += 1
    if probed > 0:
        print(f"  [Probe Gate] {passed}/{probed} keywords passed GKP probe, {killed} killed")
    return transformed

# LLM — centralized client (handles .env, think=False, fallback, timeouts)
from llm_client import call as _llm_call

# Auto-skip patterns (non-RSOC keywords)
SKIP_PATTERNS = [
    r"lyrics$",
    r"login$",
    r"^facebook|^gmail|^youtube|^instagram",
    r"god was one of us",
    r"what if book|what if movie",
    r"song$",
    r"^how to download",
]


def should_skip(keyword: str) -> bool:
    """Check if keyword matches non-RSOC patterns."""
    lower = keyword.lower()
    return any(re.search(pattern, lower) for pattern in SKIP_PATTERNS)


def log_error(stage: str, error: str, context: dict = None):
    """Append error to error_log.jsonl."""
    try:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "timestamp": datetime.now().isoformat(),
                "stage": stage,
                "error": error,
                "context": context or {}
            }) + "\n")
    except Exception:
        pass


def call_llm(messages: list) -> str:
    """Call LLM via centralized client (LiteLLM → Ollama → OpenRouter)."""
    return _llm_call(
        messages,
        max_tokens=500,
        temperature=0.3,
        timeout="generous",
        stage="commercial_transformer/llm",
    )


def transform_keyword(keyword: str, country: str, trend_context: dict) -> dict:
    """
    Transform an informational keyword into a commercial variant.
    
    Returns dict with:
    - variant: the commercial keyword
    - relationship: explanation of trend-to-commercial connection
    - confidence: 0-1 score
    """
    source_trend = trend_context.get("term", keyword)
    traffic = trend_context.get("traffic", "unknown")
    
    system_prompt = """You are an RSOC keyword strategist. Transform informational keywords into commercial variants while PRESERVING the semantic connection to the trending topic.

CRITICAL: The commercial variant must be naturally related to why the trend is happening.

Analyze:
1. What is the trending topic about?
2. What user need does this trend signal?
3. What commercial services naturally address this need?
4. What keyword would someone use when ready to take action?

Return ONLY a JSON object with these fields:
{
  "variant": "the commercial keyword (in same language as original)",
  "relationship": "1-sentence explanation of trend-to-commercial connection",
  "confidence": 0.8
}"""

    user_prompt = f"""Original trending topic: "{source_trend}"
Traffic: {traffic}
Country: {country}

Informational keyword: "{keyword}"

Transform this into ONE commercial keyword variant that:
1. Maintains semantic connection to the trending topic
2. Addresses the user need signaled by the trend
3. Has advertiser demand (services/products people pay for)
4. Is a natural search query for someone ready to take action

Examples of good transformations:
- "明日天気 東京" → "天気予報 アプリ おすすめ" (weather checking → weather app discovery)
- "looking for travel buddies" → "travel buddy finder app" (social need → commercial service)
- "fuel excise halved" → "fuel price comparison australia" (policy news → cost comparison tool)

Return ONLY the JSON object, no other text."""

    try:
        response = call_llm([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ])
        
        # Extract JSON from response
        json_match = re.search(r'\{[^}]+\}', response, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return result
        else:
            # Fallback: try to parse entire response
            return json.loads(response)
            
    except Exception as e:
        log_error("commercial_transformer/transform", str(e), {
            "keyword": keyword,
            "country": country,
            "trend": source_trend
        })
        # Return original keyword as fallback
        return {
            "variant": keyword,
            "relationship": "transformation failed - kept original",
            "confidence": 0.0
        }


def main():
    print("========================================================")
    print("  Commercial Keyword Transformer")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================================")
    
    # Load expanded keywords
    if not EXPANDED_KEYWORDS.exists():
        print(f"⚠️  {EXPANDED_KEYWORDS.name} not found — nothing to transform")
        return
    
    with open(EXPANDED_KEYWORDS, "r", encoding="utf-8") as f:
        expanded = json.load(f)
    
    print(f"Loaded {len(expanded)} expanded keywords")
    
    # Filter: only CPC=$0 keywords
    zero_cpc = [
        kw for kw in expanded
        if kw.get("google_estimated_cpc", 0) == 0
        and kw.get("google_cpc_low", 0) == 0
        and kw.get("google_cpc_high", 0) == 0
    ]
    
    print(f"Found {len(zero_cpc)} keywords with CPC=$0")
    
    if not zero_cpc:
        print("No keywords to transform — clearing stale output file")
        OUTPUT.write_text("[]")
        raise SystemExit(0)
    
    # Load explosive trends for context
    trend_lookup = {}
    if EXPLOSIVE_TRENDS.exists():
        with open(EXPLOSIVE_TRENDS, "r", encoding="utf-8") as f:
            trends = json.load(f)
            for trend in trends:
                term = trend.get("term", "")
                geo = trend.get("geo", "")
                if term and geo:
                    trend_lookup[(term, geo)] = trend
    
    print(f"Loaded {len(trend_lookup)} trends for context")
    
    # Cap batch size to stay within 3600s stage timeout (heartbeat.py)
    TRANSFORM_CAP = 300
    WALL_LIMIT = 3000  # seconds — stop before 3600s heartbeat timeout
    HIGH_VALUE_VERTICALS = {"finance", "insurance", "legal", "health", "real_estate", "education"}
    if len(zero_cpc) > TRANSFORM_CAP:
        priority = [k for k in zero_cpc if k.get("vertical") in HIGH_VALUE_VERTICALS]
        rest = [k for k in zero_cpc if k.get("vertical") not in HIGH_VALUE_VERTICALS]
        zero_cpc = (priority + rest)[:TRANSFORM_CAP]
        print(f"  [Cap] Trimmed to {TRANSFORM_CAP} keywords ({len(priority)} high-value-vertical priority)")

    # Transform keywords in batches of 5 (reduces LLM round-trips by ~5×)
    BATCH_SIZE = 5
    transformed = []
    skipped = 0
    failed = 0

    # Pre-filter skippable keywords
    to_transform = []
    for kw in zero_cpc:
        keyword = kw.get("keyword", "")
        if should_skip(keyword):
            skipped += 1
            continue
        to_transform.append(kw)
    if skipped:
        print(f"  [Skip] {skipped} non-RSOC keywords skipped")

    _wall_start = time.monotonic()

    for batch_start in range(0, len(to_transform), BATCH_SIZE):
        # Early exit if approaching wall-clock limit
        elapsed = time.monotonic() - _wall_start
        if elapsed > WALL_LIMIT:
            print(f"  [Wall] Stopping at {elapsed:.0f}s / {WALL_LIMIT}s — "
                  f"{len(transformed)} done, {len(to_transform) - batch_start} deferred")
            break

        batch = to_transform[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = -(-len(to_transform) // BATCH_SIZE)
        print(f"  [Batch {batch_num}/{total_batches}] Transforming {len(batch)} keywords...")

        # Build batch prompt
        batch_items = []
        for kw in batch:
            keyword = kw.get("keyword", "")
            country = kw.get("country", "US")
            seed = kw.get("expansion_seed", keyword)
            trend_context = trend_lookup.get((seed, country), {"term": seed})
            batch_items.append({
                "keyword": keyword,
                "country": country,
                "trend": trend_context.get("term", keyword),
                "traffic": trend_context.get("traffic", "unknown"),
            })

        batch_system = """You are an RSOC keyword strategist. Transform informational keywords into commercial variants while PRESERVING the semantic connection to the trending topic.

For each keyword, analyze:
1. What is the trending topic about?
2. What user need does this trend signal?
3. What commercial services naturally address this need?
4. What keyword would someone use when ready to take action?

Return ONLY a JSON array of objects, one per input keyword, each with:
{"variant": "commercial keyword", "relationship": "1-sentence explanation", "confidence": 0.8}"""

        batch_user = "Transform these keywords into commercial variants:\n\n"
        for idx, item in enumerate(batch_items, 1):
            batch_user += f'{idx}. Keyword: "{item["keyword"]}" | Country: {item["country"]} | Trend: "{item["trend"]}" | Traffic: {item["traffic"]}\n'
        batch_user += "\nReturn a JSON array with one object per keyword, in the same order."

        try:
            response = call_llm([
                {"role": "system", "content": batch_system},
                {"role": "user", "content": batch_user}
            ])

            # Parse batch response
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                results = json.loads(json_match.group())
            else:
                results = json.loads(response)

            if not isinstance(results, list):
                results = [results]

            for idx, kw in enumerate(batch):
                result = results[idx] if idx < len(results) else {"variant": kw.get("keyword", ""), "relationship": "batch parse error", "confidence": 0.0}
                keyword = kw.get("keyword", "")
                transformed_kw = {
                    **kw,
                    "keyword": result.get("variant", keyword),
                    "original_keyword": keyword,
                    "transformation_relationship": result.get("relationship", ""),
                    "transformation_confidence": result.get("confidence", 0.5),
                    "transformed_at": datetime.now().isoformat(),
                    "needs_dataforseo_validation": True,
                }
                transformed.append(transformed_kw)
                if result.get("confidence", 0) > 0.5:
                    print(f"    {keyword} → {result.get('variant', '?')}")
                else:
                    failed += 1

        except Exception as e:
            print(f"    ✗ Batch error: {e} — falling back to originals")
            for kw in batch:
                failed += 1
                transformed.append({
                    **kw,
                    "transformation_failed": True,
                    "transformation_error": str(e),
                    "needs_dataforseo_validation": True,
                })

        time.sleep(0.5)
    
    # Phase 2.2: Run Keyword Planner probe gate before saving
    transformed = probe_gate(transformed)

    # Save transformed keywords
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(transformed, f, indent=2, ensure_ascii=False)
    
    print()
    print(f"✅ Transformation complete:")
    print(f"   Total processed: {len(zero_cpc)}")
    print(f"   Successfully transformed: {len(transformed) - failed}")
    print(f"   Skipped (non-RSOC): {skipped}")
    print(f"   Failed/kept original: {failed}")
    print(f"   Output: {OUTPUT}")


if __name__ == "__main__":
    main()
