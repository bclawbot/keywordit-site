#!/bin/bash
# Wait for Ollama to be available (max 60s)
for i in {1..30}; do
    curl -s http://localhost:11434/api/tags > /dev/null 2>&1 && break
    sleep 2
done

# Pre-warm primary LLM
curl -s -X POST http://localhost:11434/api/generate \
    -d '{"model":"qwen3:14b","prompt":"hi","stream":false,"options":{"num_predict":1}}' \
    > /dev/null 2>&1

# Pre-warm embedding model
curl -s -X POST http://localhost:11434/api/generate \
    -d '{"model":"bge-m3","prompt":"hi","stream":false,"options":{"num_predict":1}}' \
    > /dev/null 2>&1

echo "$(date): Models preloaded" >> ~/.openclaw/logs/preload.log
