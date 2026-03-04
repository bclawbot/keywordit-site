#!/bin/bash
# Sources .env so OPENROUTER_API_KEY is available to LiteLLM proxy
set -a
source /Users/newmac/.openclaw/.env 2>/dev/null || true
set +a

exec /opt/homebrew/bin/python3 -m litellm \
    --config /Users/newmac/.openclaw/litellm_config.yaml \
    --port 4000
