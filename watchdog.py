"""
watchdog.py — OpenClaw process & service health monitor
Runs every 5 minutes via ai.openclaw.watchdog launchd plist.

Checks:
  - heartbeat.py log staleness (>2h = alert)
  - Zombie pipeline processes (running >2× their timeout)
  - heartbeat.py stuck (running >8h)
  - Core services dead (gateway, dwight-bot, litellm)
  - Ollama unresponsive
  - LiteLLM proxy unresponsive

Auto-remediation: kills zombies (processes running >2× timeout).
Alert dedup: re-alerts only after 30 min of continued issue.
"""

import json
import os
import re
import signal
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────

BASE          = Path("/Users/newmac/.openclaw/workspace")
ENV_FILE      = Path.home() / ".openclaw" / ".env"
CHAT_ID_FILE  = BASE / ".telegram_chat_id"
HEARTBEAT_LOG = Path.home() / ".openclaw" / "logs" / "heartbeat.log"
ALERT_STATE   = Path("/tmp/watchdog_alerted.json")

# Seconds before a stale heartbeat.log triggers alert (hourly run + ~90 min max exec)
HEARTBEAT_STALE_THRESHOLD = 7200   # 2 hours

# Max total heartbeat.py run time before declaring it stuck
HEARTBEAT_MAX_RUNTIME = 28800      # 8 hours

# Re-alert interval for a persistent issue
REALERT_INTERVAL = 1800            # 30 minutes

# Stage timeouts (must match heartbeat.py STAGE_TIMEOUTS)
STAGE_TIMEOUTS = {
    "subreddit_discovery.py": 900,
    "reddit_intelligence.py": 300,
    "trends_scraper.py":      1800,
    "trends_postprocess.py":  3600,
    "keyword_expander.py":    600,
    "keyword_extractor.py":   21600,
    "vetting.py":             3600,
    "validation.py":          900,
    "dashboard_builder.py":   120,
    "reflection.py":          300,
    "deploy_dashboard.sh":    120,
}

# Services to monitor via launchctl (label → friendly name)
SERVICES = {
    "ai.openclaw.gateway":    "Gateway",
    "ai.openclaw.dwight-bot": "Dwight bot",
    "ai.openclaw.litellm":    "LiteLLM proxy",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_env(path: Path) -> dict:
    env = {}
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return env


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        print("[notify] Skipped — TELEGRAM_TOKEN or chat_id not available")
        return
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    try:
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data,
            timeout=10,
        )
    except Exception as e:
        print(f"[notify] Telegram send failed: {e}")


def _load_alert_state() -> dict:
    try:
        return json.loads(ALERT_STATE.read_text())
    except Exception:
        return {}


def _save_alert_state(state: dict) -> None:
    ALERT_STATE.write_text(json.dumps(state))


def _should_alert(state: dict, key: str) -> bool:
    """Return True if we should fire an alert for this issue key."""
    now = time.time()
    last = state.get(key, 0)
    return (now - last) >= REALERT_INTERVAL


def _mark_alerted(state: dict, key: str) -> None:
    state[key] = time.time()


def _clear_alert(state: dict, key: str) -> None:
    state.pop(key, None)


# ── Process inspection ─────────────────────────────────────────────────────────

def _parse_elapsed(elapsed_str: str) -> int:
    """
    Convert ps elapsed time string to seconds.
    Formats: "1:23" (mm:ss), "1:23:45" (hh:mm:ss), "1-02:03:04" (days-hh:mm:ss)
    """
    elapsed_str = elapsed_str.strip()
    # days-hh:mm:ss
    m = re.match(r"^(\d+)-(\d+):(\d+):(\d+)$", elapsed_str)
    if m:
        d, h, mn, s = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
        return d * 86400 + h * 3600 + mn * 60 + s
    # hh:mm:ss
    m = re.match(r"^(\d+):(\d+):(\d+)$", elapsed_str)
    if m:
        h, mn, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return h * 3600 + mn * 60 + s
    # mm:ss
    m = re.match(r"^(\d+):(\d+)$", elapsed_str)
    if m:
        mn, s = int(m.group(1)), int(m.group(2))
        return mn * 60 + s
    return 0


def _get_workspace_processes() -> list[dict]:
    """
    Return list of {pid, script, elapsed_sec} for Python processes running
    workspace scripts (excluding watchdog itself).
    """
    procs = []
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "pid,etime,command"],
            text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        return procs

    workspace_str = str(BASE)
    my_pid = str(os.getpid())

    for line in out.splitlines():
        line = line.strip()
        if workspace_str not in line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, cmd = parts[0], parts[1], parts[2]
        if pid == my_pid:
            continue
        # Extract script name from command
        m = re.search(r"workspace/([^/\s]+\.py)", cmd)
        if not m:
            continue
        script = m.group(1)
        procs.append({
            "pid": int(pid),
            "script": script,
            "elapsed_sec": _parse_elapsed(etime),
            "cmd": cmd,
        })
    return procs


# ── Checks ─────────────────────────────────────────────────────────────────────

def check_heartbeat_staleness(state: dict, alerts: list, token: str, chat_id: str) -> None:
    """Alert if heartbeat.log hasn't been updated in >2h AND heartbeat is not currently running."""
    key = "heartbeat_stale"
    try:
        age = time.time() - HEARTBEAT_LOG.stat().st_mtime
    except FileNotFoundError:
        age = float("inf")

    if age > HEARTBEAT_STALE_THRESHOLD:
        # Suppress false alarm if heartbeat.py is actively running right now
        # (log only updates when the full cycle completes — keyword_extractor can take 6h)
        procs = _get_workspace_processes()
        heartbeat_running = any(p["script"] == "heartbeat.py" for p in procs)
        if heartbeat_running:
            _clear_alert(state, key)
            print(f"[watchdog] heartbeat.log stale ({int(age//60)}m) but heartbeat.py is running — OK")
            return

        age_h = int(age // 3600)
        age_m = int((age % 3600) // 60)
        msg = (
            f"🚨 Watchdog: heartbeat.log not updated in {age_h}h {age_m}m "
            f"and heartbeat.py is NOT running.\n"
            f"Heartbeat may be stuck or launchd plist unloaded."
        )
        print(f"[watchdog] ALERT — {msg}")
        if _should_alert(state, key):
            _send_telegram(token, chat_id, msg)
            _mark_alerted(state, key)
        alerts.append(key)
        # Auto-restart: kick launchd to schedule the next heartbeat run immediately
        try:
            result = subprocess.run(
                ["launchctl", "kickstart", "-k", "user/501/ai.openclaw.heartbeat"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                print("[watchdog] Kicked ai.openclaw.heartbeat via launchctl")
            else:
                print(f"[watchdog] kickstart failed: {result.stderr.strip()}")
        except Exception as _ke:
            print(f"[watchdog] kickstart error: {_ke}")
    else:
        _clear_alert(state, key)
        print(f"[watchdog] heartbeat.log OK (age {int(age//60)}m)")


def check_zombie_processes(state: dict, alerts: list, token: str, chat_id: str) -> None:
    """Kill and alert on pipeline processes running >2× their known timeout."""
    procs = _get_workspace_processes()
    for p in procs:
        script = p["script"]
        elapsed = p["elapsed_sec"]
        pid = p["pid"]

        # Special case: heartbeat.py itself
        if script == "heartbeat.py":
            key = "heartbeat_stuck"
            if elapsed > HEARTBEAT_MAX_RUNTIME:
                h = int(elapsed // 3600)
                msg = (
                    f"🚨 Watchdog: heartbeat.py (PID {pid}) has been running {h}h "
                    f"(limit {HEARTBEAT_MAX_RUNTIME//3600}h). "
                    f"Killing and clearing lock."
                )
                print(f"[watchdog] KILLING stuck heartbeat.py PID {pid}")
                try:
                    os.kill(pid, signal.SIGTERM)
                    # Clear lock file so next run can proceed
                    lock = Path("/tmp/openclaw_heartbeat.lock")
                    lock.unlink(missing_ok=True)
                except Exception as e:
                    msg += f"\n(kill failed: {e})"
                if _should_alert(state, key):
                    _send_telegram(token, chat_id, msg)
                    _mark_alerted(state, key)
                alerts.append(key)
            else:
                _clear_alert(state, key)
                print(f"[watchdog] heartbeat.py OK (running {int(elapsed//60)}m)")
            continue

        # Pipeline stage processes
        if script not in STAGE_TIMEOUTS:
            continue

        timeout = STAGE_TIMEOUTS[script]
        key = f"zombie_{script}_{pid}"

        if elapsed > timeout * 2:
            h = int(elapsed // 3600)
            m = int((elapsed % 3600) // 60)
            msg = (
                f"🚨 Watchdog: {script} (PID {pid}) zombie — running {h}h {m}m "
                f"(limit {timeout//60}m). Killing."
            )
            print(f"[watchdog] KILLING zombie {script} PID {pid}")
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception as e:
                msg += f"\n(kill failed: {e})"
            if _should_alert(state, key):
                _send_telegram(token, chat_id, msg)
                _mark_alerted(state, key)
            alerts.append(key)
        elif elapsed > timeout:
            # Running over limit but within 2× — warn only
            key_warn = f"slow_{script}_{pid}"
            m_elapsed = int(elapsed // 60)
            msg = (
                f"⚠️ Watchdog: {script} (PID {pid}) running {m_elapsed}m "
                f"(timeout {timeout//60}m) — slow but alive."
            )
            print(f"[watchdog] WARN — {msg}")
            if _should_alert(state, key_warn):
                _send_telegram(token, chat_id, msg)
                _mark_alerted(state, key_warn)
            alerts.append(key_warn)
        else:
            elapsed_m = int(elapsed // 60)
            print(f"[watchdog] {script} OK (running {elapsed_m}m / {timeout//60}m limit)")


def check_services(state: dict, alerts: list, token: str, chat_id: str) -> None:
    """Alert if core launchd services are not running."""
    try:
        out = subprocess.check_output(
            ["launchctl", "list"], text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        print("[watchdog] launchctl list failed")
        return

    for label, name in SERVICES.items():
        key = f"service_dead_{label}"
        # launchctl list row: "PID\tExitCode\tLabel" — PID="-" means not running
        match = re.search(rf"^(-|\d+)\s+\S+\s+{re.escape(label)}$", out, re.MULTILINE)
        if not match:
            msg = f"🚨 Watchdog: {name} ({label}) not found in launchctl — may be unloaded."
            print(f"[watchdog] ALERT — {msg}")
            if _should_alert(state, key):
                _send_telegram(token, chat_id, msg)
                _mark_alerted(state, key)
            alerts.append(key)
        elif match.group(1) == "-":
            msg = f"🚨 Watchdog: {name} ({label}) is loaded but NOT running (no PID)."
            print(f"[watchdog] ALERT — {msg}")
            if _should_alert(state, key):
                _send_telegram(token, chat_id, msg)
                _mark_alerted(state, key)
            alerts.append(key)
        else:
            _clear_alert(state, key)
            print(f"[watchdog] {name} OK (PID {match.group(1)})")


def check_new_entity_discoveries(state: dict, alerts: list, token: str, chat_id: str) -> None:
    """Alert on newly discovered entities that haven't been sent via Telegram yet."""
    key_prefix = "new_entity_"
    disc_path = BASE / "data" / "discovered_entities.jsonl"
    if not disc_path.exists():
        return

    entries = []
    try:
        for line in disc_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    except Exception:
        return

    unsent = [e for e in entries if not e.get("telegram_sent", False)]
    if not unsent:
        return

    sent_count = 0
    for entry in unsent:
        ent = entry.get("entity", "?")
        etype = entry.get("entity_type", "?")
        country = entry.get("country", "?")
        src_kw = entry.get("discovered_in", "?")
        key = f"{key_prefix}{ent}_{country}"

        msg = (
            f"⚡ New entity discovered: {ent} ({etype}) in {country}. "
            f"Found in: \"{src_kw}\". Auto-added to test pool."
        )
        print(f"[watchdog] NEW_ENTITY — {msg}")
        if _should_alert(state, key):
            _send_telegram(token, chat_id, msg)
            _mark_alerted(state, key)
        alerts.append(key)
        entry["telegram_sent"] = True
        sent_count += 1

    if sent_count > 0:
        try:
            with open(disc_path, "w", encoding="utf-8") as f:
                for entry in entries:
                    f.write(json.dumps(entry) + "\n")
            print(f"[watchdog] Marked {sent_count} discovered entities as telegram_sent")
        except Exception as e:
            print(f"[watchdog] Failed to update discovered_entities.jsonl: {e}")


def check_promotion_candidates(state: dict, alerts: list, token: str, chat_id: str) -> None:
    """Alert on entity promotion candidates from performance_cache.json."""
    perf_path = BASE / "data" / "performance_cache.json"
    if not perf_path.exists():
        return

    try:
        cache = json.loads(perf_path.read_text(encoding="utf-8"))
    except Exception:
        return

    promo = cache.get("promotion_candidates", [])
    ent_perf = cache.get("entity_performance", {})

    for ent_name in promo:
        key = f"promo_candidate_{ent_name}"
        ep = ent_perf.get(ent_name, {})
        rev = ep.get("revenue", 0)
        msg = (
            f"📈 Promotion candidate: {ent_name} crossed $50 threshold "
            f"(${rev:.2f} revenue). Approve in Performance dashboard."
        )
        print(f"[watchdog] PROMOTION_CANDIDATE — {msg}")
        if _should_alert(state, key):
            _send_telegram(token, chat_id, msg)
            _mark_alerted(state, key)
        alerts.append(key)


def check_pipeline_drift(state: dict, alerts: list, token: str, chat_id: str) -> None:
    """Alert if pipeline drift is detected in performance_cache.json."""
    perf_path = BASE / "data" / "performance_cache.json"
    if not perf_path.exists():
        return

    try:
        cache = json.loads(perf_path.read_text(encoding="utf-8"))
    except Exception:
        return

    key = "pipeline_drift"
    health = cache.get("pipeline_health", {})
    drift_warnings = cache.get("drift_warnings", [])

    if health.get("drift_detected", False) or drift_warnings:
        tier_c_pct = health.get("tier_c_keyword_pct", 0)
        tier_c_base = health.get("tier_c_baseline", 0.22)
        msg = (
            f"⚠️ Pipeline drift detected: {tier_c_pct:.0%} of keywords in "
            f"Tier C countries (baseline {tier_c_base:.0%}). Review hard filter settings."
        )
        if drift_warnings:
            msg += "\nWarnings:\n" + "\n".join(f"  - {w}" for w in drift_warnings)
        print(f"[watchdog] PIPELINE_DRIFT — {msg}")
        if _should_alert(state, key):
            _send_telegram(token, chat_id, msg)
            _mark_alerted(state, key)
        alerts.append(key)
    else:
        _clear_alert(state, key)


def check_http(state: dict, alerts: list, token: str, chat_id: str,
               url: str, name: str, key: str, headers: dict | None = None) -> None:
    """Alert if an HTTP endpoint is unresponsive."""
    try:
        req = urllib.request.Request(url, headers=headers or {})
        urllib.request.urlopen(req, timeout=5)
        _clear_alert(state, key)
        print(f"[watchdog] {name} OK")
    except Exception as e:
        msg = f"🚨 Watchdog: {name} ({url}) unresponsive — {type(e).__name__}"
        print(f"[watchdog] ALERT — {msg}")
        if _should_alert(state, key):
            _send_telegram(token, chat_id, msg)
            _mark_alerted(state, key)
        alerts.append(key)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[watchdog] === Run at {now} ===")

    _env     = _load_env(ENV_FILE)
    token    = _env.get("TELEGRAM_TOKEN", "")
    chat_id  = ""
    try:
        chat_id = CHAT_ID_FILE.read_text().strip()
    except FileNotFoundError:
        pass

    state  = _load_alert_state()
    alerts = []

    check_heartbeat_staleness(state, alerts, token, chat_id)
    check_zombie_processes(state, alerts, token, chat_id)
    check_services(state, alerts, token, chat_id)
    check_http(state, alerts, token, chat_id,
               "http://127.0.0.1:11434/", "Ollama", "ollama_down")
    check_http(state, alerts, token, chat_id,
               "http://localhost:4000/", "LiteLLM proxy", "litellm_down",
               headers={"Authorization": "Bearer sk-dwight-local"})

    # Experimental pipeline alerts (Phase 12)
    check_new_entity_discoveries(state, alerts, token, chat_id)
    check_promotion_candidates(state, alerts, token, chat_id)
    check_pipeline_drift(state, alerts, token, chat_id)

    # Prune stale alert keys (issues that have been gone for >1h)
    cutoff = time.time() - 3600
    state = {k: v for k, v in state.items() if k in alerts or v > cutoff}

    _save_alert_state(state)

    if alerts:
        print(f"[watchdog] Done — {len(alerts)} active alert(s): {alerts}")
    else:
        print("[watchdog] Done — all systems OK")


if __name__ == "__main__":
    main()
