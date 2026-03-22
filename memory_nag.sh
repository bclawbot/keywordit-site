#!/bin/bash
# memory_nag.sh — Sends a Telegram reminder if no session log was written today.
# Scheduled via cron: 0 23 * * * ~/.openclaw/workspace/memory_nag.sh

TODAY=$(date +%Y-%m-%d)
LOG_FILE="$HOME/.openclaw/workspace/memory/logs/${TODAY}.md"
ENV_FILE="$HOME/.openclaw/.env"
CHAT_ID_FILE="$HOME/.openclaw/workspace/.telegram_chat_id"

# Read token from .env
TOKEN=$(grep TELEGRAM_TOKEN "$ENV_FILE" 2>/dev/null | cut -d= -f2 | tr -d '"' | tr -d "'")
CHAT_ID=$(cat "$CHAT_ID_FILE" 2>/dev/null)

if [ -z "$CHAT_ID" ] || [ -z "$TOKEN" ]; then
    exit 0
fi

if [ ! -f "$LOG_FILE" ]; then
    curl -s -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
        -d chat_id="$CHAT_ID" \
        -d text="⚠️ No session log for today (${TODAY}). If you used Claude Code, flush the memory." \
        > /dev/null
fi
