# Operator switches — files that toggle pipeline behavior

When a Telegram alert fires, start here. Each row is one-sentence per switch
plus the exact toggle command. No state bleeds beyond what's listed.

| Switch | What it does | Toggle |
|---|---|---|
| `config/sync.muted` | Skips the Railway backend sync block so it stops hammering the remote. Heartbeat fires one `railway-sync-muted` alert the first run after the mute, then stays silent. | `touch config/sync.muted` to mute, `rm config/sync.muted` to restore. |
| `logs/stale/<stage>.stale` | Written by `heartbeat._exec_stage` when a stage times out or exits non-zero. Read by `dashboard_builder.py` into `meta.stale_stages` so the dashboard shows a banner. | Cleared automatically the next time the stage completes cleanly. Force-clear via `rm -rf ~/.openclaw/logs/stale/`. |
| `~/.openclaw/logs/<stage>.alive` | Written by the stage itself on progress (batch boundary, checkpoint save). Monitored by `pipeline_watchdog` which warns when mtime exceeds the stage's SLA. | Managed automatically. Delete only when investigating a false alert. |
| `/tmp/openclaw_heartbeat.lock` | fcntl lock file that keeps two `heartbeat.py` instances from running concurrently. | Cleared automatically on process exit. If it lingers after a crash, `rm /tmp/openclaw_heartbeat.lock`. |
| `~/.openclaw/logs/.alerts_last_fired.json` | Rate-limit state for `lib.alerts`. Same `code` within `dedupe_window_minutes` is silent. | Delete the file to force the next alert through. |
| `~/.openclaw/logs/.sync_last_status.json` | Prior backend-sync status (`ok`/`degraded`/`failed`/`muted`). Used to alert only on state transitions. | Delete to force the next run's result to be treated as a transition. |

Alerts are always appended to `~/.openclaw/logs/alerts.jsonl` even when the
Telegram network call fails, so grep that file for an audit trail.
