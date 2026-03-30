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
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

BASE = Path("/Users/newmac/.openclaw/workspace")
EXPANDED_KEYWORDS = BASE / "expanded_keywords.json"
EXPLOSIVE_TRENDS = BASE / "explosive_trends.json"
OUTPUT = BASE / "transformed_keywords.json"
ERROR_LOG = BASE / "error_log.jsonl"

sys.path.insert(0, str(BASE))

# LLM endpoints
OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3:14b"

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
    """
    Call LLM with fallback chain: LiteLLM proxy → direct Ollama → OpenRouter.
    All callers must use num_ctx=32768 to prevent Ollama model reload on each call.
    """
    # 1. Try LiteLLM proxy (port 4000)
    try:
        from dotenv import load_dotenv
        load_dotenv(Path.home() / ".openclaw" / ".env", override=False)
    except Exception:
        pass

    import os

    try:
        resp = requests.post(
            "http://localhost:4000/v1/chat/completions",
            headers={"Authorization": "Bearer dummy", "Content-Type": "application/json"},
            json={
                "model": "dwight-primary",
                "messages": messages,
                "temperature": 0.3,
                "max_tokens": 500,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception:
        pass

    # 2. Direct Ollama (num_ctx=32768 required — avoids model reload per system rules)
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "messages": messages,
                "stream": False,
                "options": {"num_ctx": 32768, "temperature": 0.3, "num_predict": 500},
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"]
    except Exception:
        pass

    # 3. OpenRouter fallback
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if api_key:
        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek/deepseek-v3.2",
                    "messages": messages,
                    "temperature": 0.3,
                    "max_tokens": 500,
                },
                timeout=45,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            log_error("commercial_transformer/llm_openrouter", str(e))

    raise Exception("All LLM backends failed (LiteLLM, Ollama, OpenRouter)")


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
    
    # Cap batch size to avoid exceeding 1800s stage timeout
    TRANSFORM_CAP = 500
    HIGH_VALUE_VERTICALS = {"finance", "insurance", "legal", "health", "real_estate", "education"}
    if len(zero_cpc) > TRANSFORM_CAP:
        priority = [k for k in zero_cpc if k.get("vertical") in HIGH_VALUE_VERTICALS]
        rest = [k for k in zero_cpc if k.get("vertical") not in HIGH_VALUE_VERTICALS]
        zero_cpc = (priority + rest)[:TRANSFORM_CAP]
        print(f"  [Cap] Trimmed to {TRANSFORM_CAP} keywords ({len(priority)} high-value-vertical priority)")

    # Transform keywords
    transformed = []
    skipped = 0
    failed = 0

    for i, kw in enumerate(zero_cpc, 1):
        keyword = kw.get("keyword", "")
        country = kw.get("country", "US")
        seed = kw.get("expansion_seed", keyword)
        
        # Skip non-RSOC keywords
        if should_skip(keyword):
            print(f"  [{i}/{len(zero_cpc)}] Skipped: {keyword} (non-RSOC pattern)")
            skipped += 1
            continue
        
        # Get trend context
        trend_context = trend_lookup.get((seed, country), {"term": seed})
        
        print(f"  [{i}/{len(zero_cpc)}] Transforming: {keyword} ({country})")
        
        try:
            result = transform_keyword(keyword, country, trend_context)
            
            # Create transformed keyword object
            transformed_kw = {
                **kw,  # Keep all original fields
                "keyword": result["variant"],
                "original_keyword": keyword,
                "transformation_relationship": result["relationship"],
                "transformation_confidence": result.get("confidence", 0.5),
                "transformed_at": datetime.now().isoformat(),
                "needs_dataforseo_validation": True,
            }
            
            transformed.append(transformed_kw)
            
            if result["confidence"] > 0.5:
                print(f"      → {result['variant']}")
                print(f"      Relationship: {result['relationship'][:80]}...")
            else:
                print(f"      → (kept original - low confidence)")
                failed += 1
                
        except Exception as e:
            print(f"      ✗ Error: {e}")
            failed += 1
            # Keep original keyword
            transformed.append({
                **kw,
                "transformation_failed": True,
                "transformation_error": str(e),
                "needs_dataforseo_validation": True,
            })
        
        # Rate limiting
        if i < len(zero_cpc):
            time.sleep(0.5)
    
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
