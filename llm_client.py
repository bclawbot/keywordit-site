"""
llm_client.py — Centralized LLM client for all OpenClaw pipeline scripts.

Single source of truth for:
  - .env loading (once, at import time)
  - Ollama / LiteLLM / OpenRouter routing with fallback
  - think=False enforcement (pipeline never uses thinking mode)
  - num_ctx=32768 consistency (prevents Ollama model reload)
  - Timeout presets (fast / normal / generous)
  - Health checking (cached LiteLLM probe)
  - Structured error logging to error_log.jsonl

Usage:
    from llm_client import call, generate

    # Chat-style (messages list)
    text = call(
        [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}],
        max_tokens=4096,
        temperature=0.3,
        timeout="normal",
        stage="keyword_extractor/llm",
    )

    # Generate-style (single prompt string)
    text = generate("Is this commercial? Answer yes or no.", timeout="fast")
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

# ── Persistent session with explicit pool (prevents connection starvation) ─────
_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, pool_block=False)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

# ── Load .env once at import time ──────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path.home() / ".openclaw" / ".env", override=False)
except Exception:
    pass

# ── Configuration ──────────────────────────────────────────────────────────────

OLLAMA_URL   = "http://localhost:11434"
OLLAMA_MODEL = "qwen3:14b"
OLLAMA_NUM_CTX = 32768

LITELLM_URL = "http://localhost:4000/v1/chat/completions"
LITELLM_KEY = "sk-dwight-local"
LITELLM_MODEL = "pipeline-extractor"

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = "deepseek/deepseek-v3.2"

_BASE = Path(__file__).resolve().parent
ERROR_LOG = _BASE / "error_log.jsonl"

TIMEOUTS = {
    "bg":       15,   # pipeline stages, Ollama direct expected
    "fast":     30,
    "normal":   120,
    "generous":  300,
}

# ── Health check cache ─────────────────────────────────────────────────────────

_litellm_ok_cache: dict = {"ok": None, "ts": 0.0}
_LITELLM_CACHE_TTL = 30.0  # seconds


def _litellm_ok() -> bool:
    """Cached 2-second health probe for LiteLLM proxy."""
    now = time.monotonic()
    if now - _litellm_ok_cache["ts"] < _LITELLM_CACHE_TTL and _litellm_ok_cache["ok"] is not None:
        return _litellm_ok_cache["ok"]
    try:
        r = _session.get("http://localhost:4000/v1/models", timeout=2,
                         headers={"Authorization": f"Bearer {LITELLM_KEY}"})
        r.raise_for_status()
        _litellm_ok_cache.update(ok=True, ts=now)
        return True
    except Exception:
        _litellm_ok_cache.update(ok=False, ts=now)
        return False


# ── Error logging ──────────────────────────────────────────────────────────────

def _log_error(stage: str, error: str, context: dict | None = None):
    """Append structured error to error_log.jsonl."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "error": str(error)[:500],
    }
    if context:
        entry["context"] = context
    try:
        with ERROR_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


# ── Timeout resolution ─────────────────────────────────────────────────────────

def _resolve_timeout(timeout) -> int:
    """Convert timeout preset name or int to seconds."""
    if isinstance(timeout, str):
        return TIMEOUTS.get(timeout, TIMEOUTS["normal"])
    return int(timeout)


# ── Exception ──────────────────────────────────────────────────────────────────

class LLMError(Exception):
    """Raised when all LLM tiers fail."""
    pass


# ── Core: chat-style call ──────────────────────────────────────────────────────

def call(
    messages: list,
    max_tokens: int = 4096,
    temperature: float = 0.3,
    timeout: str | int = "normal",
    num_predict: int | None = None,
    stage: str = "",
    local_only: bool = False,
) -> str:
    """
    Call LLM with 3-tier fallback: LiteLLM → Ollama → OpenRouter.

    Args:
        messages:    OpenAI-format messages list
        max_tokens:  Max tokens for cloud tiers (LiteLLM, OpenRouter)
        temperature: Sampling temperature
        timeout:     "fast" (30s), "normal" (120s), "generous" (300s), or int
        num_predict: Override max output tokens for Ollama (defaults to max_tokens)
        stage:       Error logging context (e.g. "keyword_extractor/llm")
        local_only:  Skip LiteLLM/OpenRouter tiers — use Ollama only.

    Returns:
        str — Response content text.

    Raises:
        LLMError — When all three tiers fail.
    """
    secs = _resolve_timeout(timeout)
    errors = []

    # ── Tier 1: LiteLLM proxy ────────────────────────────────────────────────
    if not local_only and _litellm_ok():
        try:
            resp = _session.post(
                LITELLM_URL,
                headers={
                    "Authorization": f"Bearer {LITELLM_KEY}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       LITELLM_MODEL,
                    "messages":    messages,
                    "temperature": temperature,
                    "max_tokens":  max_tokens,
                },
                timeout=secs,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            if content:
                return content
        except Exception as e:
            errors.append(("litellm", e))

    # ── Tier 2: Direct Ollama (3 retries, exponential backoff) ─────────────
    ollama_timeout = max(secs, 10)
    for attempt in range(3):
        try:
            resp = _session.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model":      OLLAMA_MODEL,
                    "messages":   messages,
                    "stream":     False,
                    "think":      False,
                    "keep_alive": -1,
                    "options": {
                        "num_ctx":     OLLAMA_NUM_CTX,
                        "temperature": temperature,
                        "num_predict": num_predict or max_tokens,
                    },
                },
                timeout=(10, ollama_timeout),
            )
            resp.raise_for_status()
            content = resp.json().get("message", {}).get("content", "")
            if content:
                return content
        except Exception as e:
            errors.append(("ollama", e))
            if attempt < 2:
                time.sleep(min(5 * (2 ** attempt), 30))

    # ── Tier 3: Direct OpenRouter ────────────────────────────────────────────
    if not local_only and OPENROUTER_KEY:
        try:
            resp = _session.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {OPENROUTER_KEY}",
                    "HTTP-Referer":  "https://github.com/openclaw",
                    "X-Title":       "OpenClaw-Pipeline",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       OPENROUTER_MODEL,
                    "messages":    messages,
                    "temperature": temperature,
                    "max_tokens":  max_tokens,
                },
                timeout=secs,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            if content:
                return content
        except Exception as e:
            errors.append(("openrouter", e))

    # ── All tiers failed ─────────────────────────────────────────────────────
    error_summary = "; ".join(f"{tier}: {type(e).__name__}: {e}" for tier, e in errors)
    if stage:
        _log_error(stage, error_summary)
    raise LLMError(f"All LLM backends failed — {error_summary}")


# ── Core: generate-style call (for /api/generate compatibility) ────────────────

def generate(
    prompt: str,
    max_tokens: int = 4096,
    temperature: float = 0.3,
    timeout: str | int = "normal",
    stage: str = "",
    local_only: bool = False,
) -> str:
    """
    Single-prompt generation with 3-tier fallback.

    For scripts that use Ollama's /api/generate endpoint (single prompt, no messages).
    Internally converts to messages format for LiteLLM/OpenRouter tiers,
    and uses /api/generate for Ollama tier.

    Args:
        prompt:      The prompt string
        max_tokens:  Max output tokens
        temperature: Sampling temperature
        timeout:     Preset or int
        stage:       Error logging context

    Returns:
        str — Response text.

    Raises:
        LLMError — When all tiers fail.
    """
    secs = _resolve_timeout(timeout)
    messages = [{"role": "user", "content": prompt}]
    errors = []

    # ── Tier 1: LiteLLM proxy (via messages) ─────────────────────────────────
    if not local_only and _litellm_ok():
        try:
            resp = _session.post(
                LITELLM_URL,
                headers={
                    "Authorization": f"Bearer {LITELLM_KEY}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       LITELLM_MODEL,
                    "messages":    messages,
                    "temperature": temperature,
                    "max_tokens":  max_tokens,
                },
                timeout=secs,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            if content:
                return content
        except Exception as e:
            errors.append(("litellm", e))

    # ── Tier 2: Direct Ollama /api/generate (3 retries, exponential backoff)
    ollama_timeout = max(secs, 10)
    for attempt in range(3):
        try:
            resp = _session.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model":      OLLAMA_MODEL,
                    "prompt":     prompt,
                    "stream":     False,
                    "think":      False,
                    "keep_alive": -1,
                    "options": {
                        "num_ctx":     OLLAMA_NUM_CTX,
                        "temperature": temperature,
                        "num_predict": max_tokens,
                    },
                },
                timeout=(10, ollama_timeout),
            )
            resp.raise_for_status()
            content = resp.json().get("response", "")
            if content:
                return content
        except Exception as e:
            errors.append(("ollama", e))
            if attempt < 2:
                time.sleep(min(5 * (2 ** attempt), 30))

    # ── Tier 3: Direct OpenRouter (via messages) ─────────────────────────────
    if not local_only and OPENROUTER_KEY:
        try:
            resp = _session.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {OPENROUTER_KEY}",
                    "HTTP-Referer":  "https://github.com/openclaw",
                    "X-Title":       "OpenClaw-Pipeline",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       OPENROUTER_MODEL,
                    "messages":    messages,
                    "temperature": temperature,
                    "max_tokens":  max_tokens,
                },
                timeout=secs,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            if content:
                return content
        except Exception as e:
            errors.append(("openrouter", e))

    error_summary = "; ".join(f"{tier}: {type(e).__name__}: {e}" for tier, e in errors)
    if stage:
        _log_error(stage, error_summary)
    raise LLMError(f"All LLM backends failed — {error_summary}")


# ══════════════════════════════════════════════════════════════════════════════
# Async variants — used by telegram_bot.py and other async consumers.
# Same 4-tier fallback logic, but using httpx.AsyncClient.
# ══════════════════════════════════════════════════════════════════════════════

# Bot-specific config (can be overridden before first call)
LITELLM_BOT_MODEL = "dwight-primary"
OPENROUTER_MODEL2 = "stepfun/step-3.5-flash:free"

# httpx timeouts matching the bot's existing config
try:
    import httpx
    _HTTPX_OLLAMA_TIMEOUT = httpx.Timeout(connect=10, read=90, write=10, pool=5)
    _HTTPX_CLOUD_TIMEOUT = httpx.Timeout(connect=10, read=120, write=10, pool=5)
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False


async def _async_litellm_ok() -> bool:
    """Async cached health probe for LiteLLM proxy."""
    import time as _time
    now = _time.monotonic()
    if now - _litellm_ok_cache["ts"] < _LITELLM_CACHE_TTL and _litellm_ok_cache["ok"] is not None:
        return _litellm_ok_cache["ok"]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(2.0)) as client:
            await client.get("http://localhost:4000/health")
        _litellm_ok_cache.update(ok=True, ts=now)
        return True
    except Exception:
        _litellm_ok_cache.update(ok=False, ts=now)
        return False


async def async_call(
    messages: list,
    max_tokens: int = 4096,
    temperature: float = 0.3,
    timeout: str | int = "normal",
    stage: str = "",
    local_only: bool = False,
) -> str:
    """
    Async LLM call with 4-tier fallback: LiteLLM → Ollama → OpenRouter primary → OpenRouter free.

    Same semantics as sync call() but for async contexts (telegram_bot.py, etc.).

    Returns:
        str — Response content text.

    Raises:
        LLMError — When all tiers fail.
    """
    if not _HTTPX_AVAILABLE:
        raise LLMError("httpx not installed — async LLM calls unavailable")

    secs = _resolve_timeout(timeout)
    errors = []

    # ── Tier 1: LiteLLM proxy ────────────────────────────────────────────────
    if not local_only and await _async_litellm_ok():
        try:
            async with httpx.AsyncClient(timeout=_HTTPX_CLOUD_TIMEOUT) as client:
                r = await client.post(
                    "http://localhost:4000/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {LITELLM_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": LITELLM_BOT_MODEL,
                        "messages": messages,
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    },
                )
                r.raise_for_status()
                content = r.json()["choices"][0]["message"]["content"]
                if content:
                    return content
        except Exception as e:
            errors.append(("litellm", e))

    # ── Tier 2: Direct Ollama ────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=_HTTPX_OLLAMA_TIMEOUT) as client:
            r = await client.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": OLLAMA_MODEL,
                    "messages": messages,
                    "stream": False,
                    "think": False,
                    "options": {"num_ctx": OLLAMA_NUM_CTX, "temperature": temperature},
                },
            )
            r.raise_for_status()
            content = r.json()["message"]["content"]
            if content:
                return content
    except Exception as e:
        errors.append(("ollama", e))

    # ── Tier 3: OpenRouter primary ───────────────────────────────────────────
    or_headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "HTTP-Referer": "https://github.com/openclaw",
        "X-Title": "Dwight-Bot",
        "Content-Type": "application/json",
    }
    if not local_only and OPENROUTER_KEY:
        try:
            async with httpx.AsyncClient(timeout=_HTTPX_CLOUD_TIMEOUT) as client:
                r = await client.post(
                    OPENROUTER_URL,
                    headers=or_headers,
                    json={"model": OPENROUTER_MODEL, "messages": messages, "max_tokens": max_tokens},
                )
                r.raise_for_status()
                content = r.json()["choices"][0]["message"]["content"]
                if content:
                    return content
        except Exception as e:
            errors.append(("openrouter", e))

        # ── Tier 4: OpenRouter free fallback ─────────────────────────────────
        try:
            async with httpx.AsyncClient(timeout=_HTTPX_CLOUD_TIMEOUT) as client:
                r = await client.post(
                    OPENROUTER_URL,
                    headers=or_headers,
                    json={"model": OPENROUTER_MODEL2, "messages": messages, "max_tokens": max_tokens},
                )
                r.raise_for_status()
                content = r.json()["choices"][0]["message"]["content"]
                if content:
                    return content
        except Exception as e:
            errors.append(("openrouter-free", e))

    error_summary = "; ".join(f"{tier}: {type(e).__name__}: {e}" for tier, e in errors)
    if stage:
        _log_error(stage, error_summary)
    raise LLMError(f"All async LLM backends failed — {error_summary}")
