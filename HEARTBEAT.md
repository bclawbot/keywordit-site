# HEARTBEAT.md — Autonomous Schedule

## ON EVERY WAKEUP (scheduled or user-triggered)
Run this immediately — no planning, no asking:

```
python3 /Users/newmac/.openclaw/workspace/heartbeat.py
```

Then report to the user:
- Total trends fetched, explosive count
- Golden opportunities table (keyword, country, AI score, CPC)
- Any stage failures with the actual error

## INTERVALS

### Every 60 Minutes
- Run heartbeat.py
- If GOLDEN count > 0, send summary to Telegram chat 7431110958

### Every 6 Hours
- Run heartbeat.py
- Read golden_opportunities.json
- For each GOLDEN keyword, search DuckDuckGo for top 3 content angles
- Report angles directly — do not ask if you should

### Every 12 Hours
- Run reflection.py
- Read the false positive summary from MEMORY.md
- Identify which countries have signal_weight < 0.2 and flag them

## RULES
- Never describe what you are about to do. Do it.
- Never ask "should I run the pipeline?" — run it.
- If heartbeat.py fails at a stage, check the error, fix if possible, re-run that stage only.
- If you cannot fix it, report the exact error message and stop.
