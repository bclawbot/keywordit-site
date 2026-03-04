#!/bin/bash
# Sources .env so OPENROUTER_API_KEY is available to LiteLLM proxy
set -a
source /Users/newmac/.openclaw/.env 2>/dev/null || true
set +a

# Force asyncio loop — uvloop is incompatible with Python 3.14
export UVICORN_LOOP=asyncio

exec /opt/homebrew/bin/litellm \
    --config /Users/newmac/.openclaw/litellm_config.yaml \
    --port 4000
