## Automation Rules

### 1. Daily Heartbeat
- Run `python3 /Users/newmac/.openclaw/workspace/heartbeat.py` every 60 minutes
- Send Telegram alerts for GOLDEN opportunities

### 2. Memory Maintenance
- Create daily memory files: `memory/$(date +\%Y-\%m-\%d).md`
- Flag countries with signal_weight < 0.2 weekly

### 3. Process Cleanup
- Kill orphaned Python processes via `ps aux | grep python` and `kill -9 <PID>`