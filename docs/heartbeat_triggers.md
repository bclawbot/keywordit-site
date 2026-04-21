# heartbeat.py — legitimate triggers

`heartbeat.py` can be spawned by three distinct sources. The logging line
added in Sprint 3 (`[heartbeat] start pid=… ppid=… parent=…`) makes every
invocation self-attributing.

| Source | PPID parent command | When |
|---|---|---|
| **launchd** | `launchd` | `StartCalendarInterval` 08:30 daily, set in `~/Library/LaunchAgents/ai.openclaw.heartbeat.plist` |
| **operator shell** | `zsh`, `bash`, `python3` | Manual `python3 heartbeat.py` during debugging |
| **Dwight bot / gateway exec** | `openclaw-gateway` → node process | Dwight bot runs shell commands via the openclaw-gateway exec pipe. `HEARTBEAT.md` schedules "full pipeline every 6h" and Dwight may decide to run it in response to an operator Telegram message (`/run`, "check pipeline", etc.) |

## R2-C8 context

The Apr 21 QA run observed a second `heartbeat.py` (PID 72100) at 04:29
with PPID 67529 (openclaw-gateway). Investigation found:

1. The gateway's Node source (`/opt/homebrew/lib/node_modules/openclaw/dist/`)
   does NOT spawn `heartbeat.py`. The only "heartbeat" reference is to
   `HEARTBEAT.md` (the agent's identity file).
2. The Dwight bot talks to the gateway's exec pipe and can run arbitrary
   shell commands — so a bot-issued `python3 heartbeat.py` would appear
   with `ppid=<gateway>`.
3. `/tmp/openclaw_heartbeat.lock` via `fcntl.flock(LOCK_EX | LOCK_NB)` is
   already the serialization barrier; a second invocation attempting to
   take the lock exits early (see `heartbeat.py:106-114`).

## Decision

Outcome **A — legitimate on-demand trigger**. No gateway/launchd config
change needed. The attribution log line plus the existing lockfile make
duplicate invocations safe (second caller exits within seconds) and now
self-documenting (grep `[heartbeat] start` in `heartbeat.log`).

If a future run finds an unexpected PPID (not launchd, not a shell,
not openclaw-gateway), expand this table rather than reach for code.
