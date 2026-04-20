#!/usr/bin/env python3
"""
research_api.py — Pattern Research API for Keywordit Dashboard

Provides a conversational AI interface to query the pattern knowledge base.
Uses local Qwen3:14b via Ollama with fallback to free OpenRouter models.
"""

from flask import Flask, request, jsonify, session, Response, stream_with_context
from flask_cors import CORS
import concurrent.futures
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
import requests

try:
    from dotenv import load_dotenv
    load_dotenv(Path.home() / ".openclaw" / ".env", override=False)
except Exception:
    pass

BASE_WS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_WS))

from services.intelligence_api import intel_bp

# Protects concurrent reads/writes to generated_articles.json
_MANIFEST_LOCK = threading.Lock()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dwight-research-secret-2026")
# Allow all origins for development (file://, localhost, etc.)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

# Config
BASE = Path(__file__).resolve().parent.parent
KNOWLEDGE_BASE = BASE / "data" / "pattern_knowledge_base.json"
HISTORICAL_DATA = BASE / "data" / "historical_keywords.json"
VALIDATED_OPPS = BASE / "validated_opportunities.json"

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

# Load knowledge base on startup
kb = {}
if KNOWLEDGE_BASE.exists():
    try:
        kb = json.loads(KNOWLEDGE_BASE.read_text())
        print(f"[research_api] Loaded knowledge base: {len(kb)} sections")
    except Exception as e:
        print(f"[research_api] Failed to load knowledge base: {e}")
else:
    print(f"[research_api] Knowledge base not found at {KNOWLEDGE_BASE}")


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "ok",
        "knowledge_base_loaded": len(kb) > 0,
        "timestamp": datetime.now().isoformat()
    })


@app.route("/api/trigger-pipeline", methods=["POST"])
def trigger_pipeline():
    """Trigger a manual pipeline run via launchctl kickstart."""
    import subprocess as _sp
    try:
        result = _sp.run(
            ["launchctl", "kickstart", "-k", f"user/{os.getuid()}/ai.openclaw.heartbeat"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return jsonify({"status": "triggered", "message": "Pipeline run kicked"})
        else:
            return jsonify({"status": "error", "message": result.stderr.strip() or "Unknown error"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/chat", methods=["POST"])
def chat():
    """Process research query and return AI response."""
    # For development, allow all requests
    # In production, check session: if not session.get("logged_in"): return 401
    
    data = request.json
    if not data or "query" not in data:
        return jsonify({"error": "Missing query parameter"}), 400
    
    query = data["query"].strip()
    if not query:
        return jsonify({"error": "Empty query"}), 400
    
    # Build context from knowledge base
    context = build_context(query, kb)
    
    # Call LLM with context
    response = query_llm(query, context)
    
    return jsonify({
        "response": response,
        "timestamp": datetime.now().isoformat()
    })


@app.route("/api/knowledge", methods=["GET"])
def knowledge_summary():
    """Get knowledge base summary."""
    summary = generate_summary(kb)
    return jsonify({
        "summary": summary,
        "sections": list(kb.keys()),
        "timestamp": datetime.now().isoformat()
    })


def build_context(query: str, kb: dict) -> str:
    """Extract relevant knowledge base sections for query."""
    if not kb:
        return "Knowledge base is empty. Please run pattern analysis first."
    
    context = []
    query_lower = query.lower()
    
    # Smart context selection based on query keywords
    if "entit" in query_lower or "brand" in query_lower:
        entities = kb.get("entities", {})
        if entities:
            # Limit to top entities by performance
            entity_summary = {}
            for entity_type, data in entities.items():
                if isinstance(data, dict) and "performance" in data:
                    entity_summary[entity_type] = {
                        "proven": data.get("proven", [])[:10],  # Top 10
                        "top_performers": dict(list(data.get("performance", {}).items())[:5])
                    }
            context.append(f"Entities:\n{json.dumps(entity_summary, indent=2)}")
    
    if "vertical" in query_lower or "tier" in query_lower:
        verticals = kb.get("verticals", {})
        if verticals:
            context.append(f"Verticals:\n{json.dumps(verticals, indent=2)}")
    
    if "template" in query_lower or "pattern" in query_lower:
        templates = kb.get("templates", {})
        if templates:
            # Limit to top 10 templates
            top_templates = dict(list(templates.items())[:10])
            context.append(f"Templates:\n{json.dumps(top_templates, indent=2)}")
    
    if "intent" in query_lower or "signal" in query_lower:
        intents = kb.get("intent_signals", {})
        if intents:
            context.append(f"Intent Signals:\n{json.dumps(intents, indent=2)}")
    
    # If no specific section matched, include summary
    if not context:
        context.append(f"Knowledge Base Summary:\n{generate_summary(kb)}")
    
    return "\n\n".join(context)


def generate_summary(kb: dict) -> str:
    """Generate a concise summary of the knowledge base."""
    if not kb:
        return "Knowledge base is empty."
    
    summary_parts = []
    
    if "entities" in kb:
        entity_count = sum(len(data.get("proven", [])) for data in kb["entities"].values() if isinstance(data, dict))
        summary_parts.append(f"- {entity_count} proven entities across {len(kb['entities'])} types")
    
    if "verticals" in kb:
        summary_parts.append(f"- {len(kb['verticals'])} verticals analyzed")
    
    if "templates" in kb:
        summary_parts.append(f"- {len(kb['templates'])} proven templates")
    
    if "intent_signals" in kb:
        summary_parts.append(f"- {len(kb['intent_signals'])} intent signals tracked")
    
    return "\n".join(summary_parts) if summary_parts else "Knowledge base contains data but structure is unexpected."


def query_llm(query: str, context: str) -> str:
    """Call LLM with fallback chain: Ollama → OpenRouter Free models."""
    
    system_prompt = f"""You are Dwight, a media buying intelligence assistant. You have access to a comprehensive knowledge base of keyword performance data.

Answer the user's question based on the following data:

{context}

Provide specific, data-driven answers. Include numbers (RPC, revenue, counts) when available. Be concise but thorough."""

    # Try 1: Ollama Qwen3:14b (local, free)
    try:
        resp = requests.post("http://localhost:11434/api/generate", json={
            "model": "qwen3:14b",
            "prompt": f"{system_prompt}\n\nUser: {query}\n\nAssistant:",
            "stream": False,
            "options": {"temperature": 0.3, "num_predict": 512}
        }, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json().get("response", "")
            if result:
                print(f"[research_api] Response from Ollama Qwen3:14b")
                return result
    except Exception as e:
        print(f"[research_api] Ollama failed: {e}")
    
    # Try 2: OpenRouter Llama 3.3 70B (free tier)
    try:
        resp = requests.post("https://openrouter.ai/api/v1/chat/completions", 
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
            json={
                "model": "meta-llama/llama-3.3-70b-instruct:free",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": query}
                ],
                "temperature": 0.3,
                "max_tokens": 512
            }, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()["choices"][0]["message"]["content"]
            print(f"[research_api] Response from Llama 3.3 70B")
            return result
    except Exception as e:
        print(f"[research_api] Llama 3.3 70B failed: {e}")
    
    # Try 3: OpenRouter Qwen3 Coder (free tier)
    try:
        resp = requests.post("https://openrouter.ai/api/v1/chat/completions", 
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
            json={
                "model": "qwen/qwen3-coder:free",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": query}
                ],
                "temperature": 0.3,
                "max_tokens": 512
            }, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()["choices"][0]["message"]["content"]
            print(f"[research_api] Response from Qwen3 Coder")
            return result
    except Exception as e:
        print(f"[research_api] Qwen3 Coder failed: {e}")
    
    return "Sorry, all LLM services are currently unavailable. Please try again later."


@app.route("/api/generate-article", methods=["POST"])
def generate_article():
    """
    Generate one RSOC article on demand via Ollama.
    Returns a Server-Sent Events stream:
      {"event":"started"}
      {"event":"progress","message":"..."}   — every 8s while Ollama runs
      {"event":"result","status":"ok"|"blocked"|"error",...}
    """
    data = request.json
    required = ["keyword", "vertical", "language_code", "angle_type", "angle_title"]
    for field in required:
        if not data or field not in data:
            return jsonify({"status": "error", "error": f"Missing field: {field}"}), 400

    def generate_stream():
        # Lazy imports — keeps API server alive even if pipeline modules not ready
        try:
            from content_generator import (
                _load_config, _generate_article,
                _write_article_file, _validate_utf8_encoding,
            )
            from pipeline.stages.stage_5_5_angle_engine.prompts import build_prompt
            from pipeline.stages.stage_5_5_angle_engine.quality_gates import (
                validate_rsoc_article, validate_spanish_article,
            )
            from pipeline.stages.stage_5_5_angle_engine.compliance import compliance_scan
            from pipeline.stages.stage_5_5_angle_engine.content_store import write_article_record
        except ImportError as e:
            yield f"data: {json.dumps({'event': 'error', 'error': str(e)})}\n\n"
            return

        yield f"data: {json.dumps({'event': 'started', 'message': 'Building prompt...'})}\n\n"

        cfg  = _load_config()
        year = datetime.now().year

        try:
            prompt = build_prompt(
                angle_type=data["angle_type"],
                keyword=data["keyword"],
                vertical=data["vertical"],
                language_code=data["language_code"],
                year=year,
                title=data["angle_title"],
                audience=f"people exploring {data['keyword']}",
                discovery_signal_text=data.get("discovery_signal_text", ""),
                source_trend=data.get("source_trend", ""),
            )
        except ValueError as e:
            yield f"data: {json.dumps({'event': 'error', 'error': str(e)})}\n\n"
            return

        yield f"data: {json.dumps({'event': 'progress', 'message': 'Calling Ollama (3-5 min)...'})}\n\n"

        # Run blocking Ollama call in a thread; yield keepalive pings while waiting
        t0 = time.time()
        result_holder = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_generate_article, prompt, cfg)
            while not future.done():
                time.sleep(8)
                elapsed = int(time.time() - t0)
                yield f"data: {json.dumps({'event': 'progress', 'message': f'Generating... {elapsed}s elapsed'})}\n\n"
            try:
                article_text, model_used = future.result()
                result_holder["text"]  = article_text
                result_holder["model"] = model_used
            except Exception as e:
                yield f"data: {json.dumps({'event': 'error', 'error': str(e)})}\n\n"
                return

        elapsed      = round(time.time() - t0, 1)
        article_text = result_holder["text"]
        model_used   = result_holder["model"]
        language     = data["language_code"]

        _validate_utf8_encoding(article_text, language)

        # Quality gates
        if language.lower() == "es":
            q_result = validate_spanish_article(article_text, data["vertical"])
        else:
            q_result = validate_rsoc_article(
                article_text, data["angle_type"],
                data["keyword"], language, data["vertical"],
            )

        # Compliance scan
        c_result   = compliance_scan(article_text)
        comp_cfg   = cfg.get("compliance", {})
        block_high = bool(comp_cfg.get("block_high", True))

        if c_result.risk_level == "CRITICAL" or (c_result.risk_level == "HIGH" and block_high):
            yield f"data: {json.dumps({'event': 'result', 'status': 'blocked', 'compliance_risk_level': c_result.risk_level, 'violations': c_result.violations})}\n\n"
            return

        # Write .md file
        articles_dir = BASE_WS / cfg.get("content_generation", {}).get("articles_dir", "articles")
        rel_path = _write_article_file(
            article_text, data["keyword"], data["angle_type"], language, articles_dir,
        )

        word_count = len(article_text.split())
        record = {
            "keyword":               data["keyword"],
            "country":               data.get("country", "US"),
            "angle_type":            data["angle_type"],
            "angle_title":           data["angle_title"],
            "language_code":         language,
            "vertical":              data["vertical"],
            "word_count":            word_count,
            "file_path":             rel_path,
            "raf_compliant":         c_result.passed,
            "quality_score":         q_result.score,
            "compliance_risk_level": c_result.risk_level,
            "generated_at":          datetime.now().isoformat(),
            "model_used":            model_used,
            "generation_time_secs":  elapsed,
            "cpc_usd":               float(data.get("cpc_usd", 0.0)),
            "rsoc_score":            float(data.get("rsoc_score", 0.0)),
            "warnings":              q_result.warnings + c_result.yellow_flags,
        }

        # Append to generated_articles.json (thread-safe)
        manifest_path = BASE_WS / "generated_articles.json"
        with _MANIFEST_LOCK:
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else []
            except Exception:
                manifest = []
            manifest.append(record)
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        # LanceDB (non-fatal)
        try:
            write_article_record(record)
        except Exception:
            pass

        yield f"data: {json.dumps({'event': 'result', 'status': 'ok', 'article_text': article_text, 'word_count': word_count, 'quality_score': q_result.score, 'compliance_risk_level': c_result.risk_level, 'raf_compliant': c_result.passed, 'model_used': model_used, 'generation_time_secs': elapsed, 'warnings': q_result.warnings + c_result.yellow_flags, 'file_path': rel_path})}\n\n"

    return Response(
        stream_with_context(generate_stream()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


app.register_blueprint(intel_bp)

if __name__ == "__main__":
    print("=" * 60)
    print("  Research API Server")
    print("  http://127.0.0.1:5555")
    print("=" * 60)
    app.run(host="127.0.0.1", port=5555, debug=False, threaded=True)
