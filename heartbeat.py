import concurrent.futures as _cf
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Load .env so all child processes inherit DataForSEO / API credentials
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(Path.home() / ".openclaw" / ".env", override=False)
except Exception:
    pass

BASE      = Path(__file__).resolve().parent
ERROR_LOG = BASE / "error_log.jsonl"
GOLDEN    = BASE / "golden_opportunities.json"

sys.path.insert(0, str(BASE))
try:
    from config.logging_config import get_logger
    _log = get_logger("heartbeat")
except Exception:
    import logging as _logging
    _log = _logging.getLogger("heartbeat")

# ── LiteLLM contention management ─────────────────────────────────────────────
# During pipeline runs, remove Ollama from LiteLLM fallbacks so LiteLLM never
# competes with pipeline scripts for the local model.
import shutil as _shutil

_LITELLM_CONFIG = Path.home() / ".openclaw" / "litellm_config.yaml"
_LITELLM_CONFIG_BAK = _LITELLM_CONFIG.with_suffix(".yaml.bak")
_LITELLM_PLIST = Path.home() / "Library" / "LaunchAgents" / "ai.openclaw.litellm.plist"


def _restart_litellm():
    """Restart LiteLLM via launchctl."""
    try:
        subprocess.run(["launchctl", "unload", str(_LITELLM_PLIST)],
                        capture_output=True, timeout=10)
        subprocess.run(["launchctl", "load", str(_LITELLM_PLIST)],
                        capture_output=True, timeout=10)
        import time as _t; _t.sleep(3)
    except Exception as e:
        print(f"  [LiteLLM] Restart failed: {e}")


def _strip_ollama_from_litellm() -> bool:
    """Remove dwight-local from LiteLLM fallback chains during pipeline run."""
    if not _LITELLM_CONFIG.exists():
        return False
    try:
        import yaml
        _shutil.copy2(_LITELLM_CONFIG, _LITELLM_CONFIG_BAK)
        cfg = yaml.safe_load(_LITELLM_CONFIG.read_text())
        changed = False
        for fb in cfg.get("router_settings", {}).get("fallbacks", []):
            for key, chain in list(fb.items()):
                if "dwight-local" in chain:
                    fb[key] = [m for m in chain if m != "dwight-local"]
                    changed = True
        if changed:
            _LITELLM_CONFIG.write_text(yaml.dump(cfg, default_flow_style=False))
            _restart_litellm()
            print("  [LiteLLM] Removed dwight-local from fallbacks for pipeline run")
        return changed
    except Exception as e:
        print(f"  [LiteLLM] Config swap failed: {e}")
        return False


def _restore_litellm():
    """Restore original LiteLLM config after pipeline run."""
    if _LITELLM_CONFIG_BAK.exists():
        try:
            _shutil.copy2(_LITELLM_CONFIG_BAK, _LITELLM_CONFIG)
            _LITELLM_CONFIG_BAK.unlink()
            _restart_litellm()
            print("  [LiteLLM] Restored original config with dwight-local fallbacks")
        except Exception as e:
            print(f"  [LiteLLM] Config restore failed: {e}")

# Phase 2.4: Prometheus metrics integration
_PROM_AVAILABLE = False
try:
    sys.path.insert(0, str(BASE))
    from prometheus_exporter import update_metrics as _update_prom_metrics
    _PROM_AVAILABLE = True
except Exception:
    pass

import time as _time
_stage_durations: dict = {}  # stage_name -> seconds

# Sprint 3 Task 3.5 (R2-C8): entry-point attribution — log which process
# triggered us. Makes every unexpected re-entry self-explanatory.
_ppid_entry = os.getppid()
_parent_cmd_entry = "(unknown)"
try:
    _parent_cmd_entry = subprocess.check_output(
        ["ps", "-o", "command=", "-p", str(_ppid_entry)],
        text=True, timeout=2,
    ).strip() or "(unknown)"
except Exception:
    pass
_log.info(
    "[heartbeat] start pid=%s ppid=%s parent=%r",
    os.getpid(), _ppid_entry, _parent_cmd_entry,
)
print(f"  [Entry] pid={os.getpid()} ppid={_ppid_entry} parent={_parent_cmd_entry!r}")

# Sprint 2: watchdog + freshness sentinel helpers (R2-C1/C2/C3).
try:
    from pipeline_watchdog import (
        start_watchdog as _start_watchdog,
        stop_watchdog as _stop_watchdog,
        write_stale_sentinel as _write_stale_sentinel,
        clear_stale_sentinel as _clear_stale_sentinel,
        STAGE_ALIVE_SLA as _STAGE_ALIVE_SLA,
    )
    _WATCHDOG_AVAILABLE = True
except Exception:
    _WATCHDOG_AVAILABLE = False
    _STAGE_ALIVE_SLA = {}

# ── Concurrent-run guard (atomic via fcntl.flock) ────────────────────────────
import fcntl as _fcntl
import os as _os
_LOCK_FILE = Path("/tmp/openclaw_heartbeat.lock")
_MY_PID    = _os.getpid()

# Acquire an exclusive advisory lock. fcntl.flock() is atomic — no TOCTOU race.
# The lock auto-releases if the process dies (kernel cleans up the fd).
_lock_fd = open(_LOCK_FILE, "w")
try:
    _fcntl.flock(_lock_fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
    _lock_fd.write(str(_MY_PID))
    _lock_fd.flush()
except OSError:
    print(f"⚠️  Heartbeat already running (lock: {_LOCK_FILE}). Exiting to prevent duplicate run.")
    _lock_fd.close()
    sys.exit(0)

import atexit as _atexit

def _release_lock():
    try:
        _fcntl.flock(_lock_fd, _fcntl.LOCK_UN)
        _lock_fd.close()
        _LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass

_atexit.register(_release_lock)

# Prevent macOS idle sleep while pipeline is running
_CAFF_PROC = None
try:
    _CAFF_PROC = subprocess.Popen(["caffeinate", "-s", "-w", str(_MY_PID)])
    _atexit.register(lambda: _CAFF_PROC.terminate() if _CAFF_PROC else None)
except FileNotFoundError:
    pass  # non-macOS or caffeinate missing — no-op

# ── Run-slot checkpoint (per-stage marker files) ─────────────────────────────
CHECKPOINT_DIR = Path("/tmp/openclaw_checkpoints")

def _current_run_id() -> str:
    """Derive run_id from today's date (daily schedule).

    Any run on the same calendar day gets the same ID, so a crashed
    process that restarts the same day resumes correctly.
    The next day gets a fresh ID automatically.
    """
    return datetime.now().strftime("%Y-%m-%d")

def _is_stage_done(script_name: str, run_id: str) -> bool:
    """Check if a stage has already completed for this run slot."""
    marker = CHECKPOINT_DIR / f"{run_id}_{script_name}.done"
    return marker.exists()

def _mark_stage_done(script_name: str, run_id: str):
    """Mark a stage as completed for this run slot. No shared state — no race."""
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    marker = CHECKPOINT_DIR / f"{run_id}_{script_name}.done"
    marker.write_text(datetime.now().isoformat())

def _cleanup_old_checkpoints():
    """Remove marker files older than 24h."""
    if not CHECKPOINT_DIR.exists():
        return
    cutoff = datetime.now().timestamp() - 86400
    for f in CHECKPOINT_DIR.iterdir():
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
        except Exception:
            pass

# Per-stage timeouts (seconds)
STAGE_TIMEOUTS = {
    "subreddit_discovery.py": 1200,   # increased: async comment mining can be slow
    "reddit_intelligence.py": 300,
    "trends_scraper.py":      1800,
    "trends_postprocess.py":  5400,   # R2-C1: >60 min on 2780+ trends, per-200 checkpoint
    "money_flow_classifier.py": 1800, # R2-C2: 900s repeatedly timed out; bump to absorb Ollama variance
    "keyword_expander.py":    600,
    "commercial_keyword_transformer.py": 3600,  # increased: LLM transformation, safety net with think=False
    "consequence_generator.py": 1800, # 30 min — LLM per classified trend (cap 80), local-only
    "keyword_extractor.py":   21600,  # 6h — LLM batches + DataForSEO expansion
    "vetting.py":             3600,
    "validation.py":          1800,
    "angle_engine.py":        7800,   # increased: wall_limit=7200 + 600s cleanup margin
    "dashboard_builder.py":   120,
    "reflection.py":          300,
    "intel_bridge.py":        300,    # Sprint 4: inject intelligence keywords before vetting
    "run_intelligence.py":    1800,   # Sprint 4: intelligence engine + daily analyzer
}

STAGES = [
    ("0-intel", "run_intelligence.py"),  # Sprint 4: intelligence engine first (feeds intel_bridge)
    ("0a", "subreddit_discovery.py"),
    (0,    "reddit_intelligence.py"),
    (1,    "trends_scraper.py"),
    ("1b", "trends_postprocess.py"),
    ("1c", "money_flow_classifier.py"),     # Sprint 2: enrich explosive_trends.json with money-flow archetypes
    ("2a", "keyword_expander.py"),
    ("2a.5", "commercial_keyword_transformer.py"),  # Transform CPC=$0 keywords
    ("2a.7", "consequence_generator.py"),           # Sprint 4: service/info/product keywords from money-flow trends
    (2,    "keyword_extractor.py"),
    ("2c", "intel_bridge.py"),           # Sprint 4: inject missed opps + competitor keywords
    ("2b", "vetting.py"),
    (3,    "validation.py"),
    ("3a", "angle_engine.py"),        # RSOC angle scoring + selection
    (4,    "dashboard_builder.py"),
    (5,    "reflection.py"),
    (6,    "deploy_dashboard.sh"),
]

# Scripts that run concurrently with the immediately following STAGES entry
PARALLEL_WITH = {"keyword_expander.py"}

print("=" * 56)
print("  OpenClaw Heartbeat — Full Pipeline Run")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 56)
_pipeline_start = _time.time()
_log.info("Pipeline start")

# ── Post-stage assertions & funnel tracking ──────────────────────────────────
# Maps stage script → primary output file(s) to validate after each stage
_STAGE_OUTPUTS = {
    "subreddit_discovery.py":             [BASE / "subreddit_registry.json"],
    "reddit_intelligence.py":             [BASE / "reddit_intelligence.json"],
    "trends_scraper.py":                  [BASE / "latest_trends.json"],
    "trends_postprocess.py":              [BASE / "explosive_trends.json"],
    "money_flow_classifier.py":           [BASE / "explosive_trends.json"],  # enriches in-place
    "keyword_expander.py":                [BASE / "expanded_keywords.json"],
    "commercial_keyword_transformer.py":  [BASE / "transformed_keywords.json"],
    "consequence_generator.py":           [BASE / "transformed_keywords.json"],  # appends in-place
    "keyword_extractor.py":              [BASE / "commercial_keywords.json"],
    "intel_bridge.py":                   [BASE / "commercial_keywords.json"],  # appends to existing
    "vetting.py":                        [BASE / "vetted_opportunities.json"],
    "validation.py":                     [BASE / "validated_opportunities.json",
                                          BASE / "golden_opportunities.json"],
    "angle_engine.py":                   [BASE / "angle_candidates.json"],
    "dashboard_builder.py":              [BASE / "dashboard.html"],
}
_funnel_counts: dict = {}  # stage_name -> record count

def _stage_outputs_valid(script_name: str) -> bool:
    """Self-healing: re-run stage if its output files are missing or empty."""
    outputs = _STAGE_OUTPUTS.get(script_name, [])
    if not outputs:
        return True  # stages without defined outputs pass by default
    return all(p.exists() and p.stat().st_size > 0 for p in outputs)

def _assert_stage_output(script_name: str):
    """Check output files exist and are non-empty after a stage. Log funnel counts."""
    outputs = _STAGE_OUTPUTS.get(script_name, [])
    for out_path in outputs:
        if not out_path.exists():
            print(f"  ⚠️  [Assert] {script_name} → {out_path.name} NOT FOUND")
            continue
        size = out_path.stat().st_size
        if size == 0:
            print(f"  ⚠️  [Assert] {script_name} → {out_path.name} is EMPTY (0 bytes)")
            continue
        # Count records in JSON array files
        try:
            data = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                count = len(data)
                _funnel_counts[out_path.name] = count
                print(f"  📊 [Funnel] {out_path.name}: {count} records")
                if count == 0:
                    print(f"  ⚠️  [Assert] {script_name} → {out_path.name} has 0 records")
        except Exception:
            pass  # non-JSON or HTML — size check is enough

errors = []
dashboard_ok = False

def _exec_stage(stage_num, script_name) -> bool:
    """Run one pipeline stage. Returns True only if dashboard_builder.py succeeded."""
    script_path = BASE / script_name
    if not script_path.exists():
        msg = f"{script_path} not found"
        print(f"[Stage {stage_num}] ❌ Failed: {msg}")
        errors.append({"timestamp": datetime.now().isoformat(),
                        "stage": str(stage_num), "error": msg})
        return False
    timeout_secs = STAGE_TIMEOUTS.get(script_name, 1800)
    cmd = ["bash", str(script_path)] if script_name.endswith(".sh") else [sys.executable, str(script_path)]
    _log.info("Stage start", extra={"stage": script_name})
    _t0 = _time.time()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _watchdog_stop = None
    if _WATCHDOG_AVAILABLE and script_name in _STAGE_ALIVE_SLA:
        try:
            _watchdog_stop = _start_watchdog(
                script_name, proc.pid,
                error_log_append=lambda rec: errors.append(rec),
            )
        except Exception as _wd_err:
            _log.warning("Watchdog failed to start for %s: %s", script_name, _wd_err)
    try:
        stdout, stderr = proc.communicate(timeout=timeout_secs)
        rc = proc.returncode
        _stage_durations[script_name] = round(_time.time() - _t0, 1)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        if _WATCHDOG_AVAILABLE:
            try: _write_stale_sentinel(script_name, f"timeout after {timeout_secs}s")
            except Exception: pass
        msg = f"{script_name} timed out after {timeout_secs}s"
        print(f"[Stage {stage_num}] ❌ Timeout: {msg}")
        _log.error("Stage timeout: %s", msg, extra={"stage": script_name, "duration": timeout_secs})
        errors.append({"timestamp": datetime.now().isoformat(),
                        "stage": str(stage_num), "error": msg})
        return False
    finally:
        if _watchdog_stop is not None:
            try: _stop_watchdog(_watchdog_stop)
            except Exception: pass
    if rc == 0:
        _mark_stage_done(script_name, _run_id)
        if _WATCHDOG_AVAILABLE:
            try: _clear_stale_sentinel(script_name)
            except Exception: pass
        print(f"[Stage {stage_num}] ✅ Done")
        if stdout.strip():
            for line in stdout.strip().splitlines():
                print(f"           {line}")
        _assert_stage_output(script_name)
        _log.info("Stage completed", extra={"stage": script_name, "duration": _stage_durations[script_name]})
        return script_name == "dashboard_builder.py"
    else:
        stderr_out = stderr.strip() or stdout.strip() or "non-zero exit"
        first_error_line = stderr_out.splitlines()[0] if stderr_out else "unknown error"
        if _WATCHDOG_AVAILABLE:
            try: _write_stale_sentinel(script_name, first_error_line[:200])
            except Exception: pass
        print(f"[Stage {stage_num}] ❌ Failed: {first_error_line}")
        _log.error("Stage failed: %s", first_error_line, extra={"stage": script_name, "duration": _stage_durations.get(script_name, 0)})
        errors.append({"timestamp": datetime.now().isoformat(),
                        "stage": str(stage_num), "error": stderr_out})
        return False

_run_id = _current_run_id()
_started_at = datetime.now().isoformat()
_cleanup_old_checkpoints()
print(f"  [Run ID] {_run_id}")

# Remove Ollama from LiteLLM fallbacks to prevent contention during pipeline
_litellm_swapped = _strip_ollama_from_litellm()

# Warm Ollama so money_flow_classifier's first call isn't paying for model load.
# qwen3:14b cold-start can be 30-90s on M1; keep_alive=-1 pins it for the run.
try:
    _warm_req = urllib.request.Request(
        "http://localhost:11434/api/generate",
        data=json.dumps({
            "model": "qwen3:14b",
            "prompt": "ok",
            "stream": False,
            "keep_alive": -1,
            "options": {"num_ctx": 32768, "num_predict": 1},
        }).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(_warm_req, timeout=180).read()
    print("  [Ollama] Warmed qwen3:14b (keep_alive=-1)")
except Exception as _warm_e:
    print(f"  [Ollama] Warm-up failed: {_warm_e}")  # non-fatal

try:
    stages_iter = iter(STAGES)
    for stage_num, script_name in stages_iter:
        if script_name in PARALLEL_WITH:
            next_stage_num, next_script = next(stages_iter)
            skip_a = _is_stage_done(script_name, _run_id) and _stage_outputs_valid(script_name)
            skip_b = _is_stage_done(next_script, _run_id) and _stage_outputs_valid(next_script)
            if skip_a and skip_b:
                print(f"[Parallel] ⏭  {script_name} + {next_script} (checkpoint)")
            elif skip_a:
                print(f"[Stage {stage_num}] ⏭  Skipped (checkpoint)")
                if _exec_stage(next_stage_num, next_script):
                    dashboard_ok = True
            elif skip_b:
                print(f"[Stage {next_stage_num}] ⏭  Skipped (checkpoint)")
                if _exec_stage(stage_num, script_name):
                    dashboard_ok = True
            else:
                print(f"[Parallel] {script_name} + {next_script} starting concurrently…")
                with _cf.ThreadPoolExecutor(max_workers=2) as pool:
                    fut_a = pool.submit(_exec_stage, stage_num, script_name)
                    fut_b = pool.submit(_exec_stage, next_stage_num, next_script)
                    if fut_a.result():
                        dashboard_ok = True
                    if fut_b.result():
                        dashboard_ok = True
        else:
            if _is_stage_done(script_name, _run_id) and _stage_outputs_valid(script_name):
                print(f"[Stage {stage_num}] ⏭  Skipped (already done)")
                continue
            if _exec_stage(stage_num, script_name):
                dashboard_ok = True
finally:
    if _litellm_swapped:
        _restore_litellm()

# ── Log errors ───────────────────────────────────────────────────────────────
if errors:
    try:
        with ERROR_LOG.open("a") as f:
            for e in errors:
                f.write(json.dumps(e) + "\n")
    except Exception as _log_err:
        print(f"  [Error Log] Failed to write error log: {_log_err}")

# ── Data flow funnel summary ─────────────────────────────────────────────────
if _funnel_counts:
    print()
    print("=" * 56)
    print("  DATA FLOW FUNNEL")
    print("=" * 56)
    funnel_order = [
        "subreddit_registry.json", "reddit_intelligence.json",
        "latest_trends.json", "explosive_trends.json",
        "expanded_keywords.json", "transformed_keywords.json",
        "commercial_keywords.json", "vetted_opportunities.json",
        "validated_opportunities.json", "golden_opportunities.json",
        "angle_candidates.json",
    ]
    for name in funnel_order:
        if name in _funnel_counts:
            print(f"  {name:<40} {_funnel_counts[name]:>6} records")
    # Stage timing summary
    if _stage_durations:
        print()
        print("  STAGE TIMING")
        for stage, dur in sorted(_stage_durations.items(), key=lambda x: -x[1]):
            print(f"  {stage:<40} {dur:>8.1f}s")
        print(f"  {'TOTAL':<40} {sum(_stage_durations.values()):>8.1f}s")

# ── Golden summary ────────────────────────────────────────────────────────────
print()
print("=" * 56)
print("  GOLDEN OPPORTUNITIES")
print("=" * 56)

if GOLDEN.exists():
    try:
        golden_items = json.loads(GOLDEN.read_text())
        golden_only = [o for o in golden_items if o.get("tag") == "GOLDEN_OPPORTUNITY"]
        if not golden_only:
            print("  (none found)")
        else:
            golden_sorted = sorted(golden_only, key=lambda x: x.get("arbitrage_index", 0), reverse=True)
            for o in golden_sorted:
                kw  = o.get("keyword", "—")
                ai  = o.get("arbitrage_index", 0)
                cpc = o.get("cpc_usd", 0)
                geo = o.get("country", "—")
                print(f"  {kw:<30} AI={ai:.4f}  CPC=${cpc:.2f}  [{geo}]")
        _log.info("Golden count: %d", len(golden_only))
    except Exception as e:
        print(f"  ⚠️  Could not read golden_opportunities.json: {e}")
        _log.warning("Golden read failed: %s", e)
else:
    print("  golden_opportunities.json not found")
    _log.warning("golden_opportunities.json not found")

# ── Phase 0.2: Zero-GOLDEN drought alert ─────────────────────────────────────
def _check_golden_drought():
    """Alert via Telegram if 0 GOLDEN opportunities found in last 26h (daily pipeline)."""
    try:
        validated_path = BASE / "validated_opportunities.json"
        history_path   = BASE / "validation_history.jsonl"
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=26)).isoformat()
        golden_recent = 0

        # Check current validated file
        if validated_path.exists():
            validated = json.loads(validated_path.read_text(encoding='utf-8'))
            for rec in validated:
                if rec.get('tag') == 'GOLDEN_OPPORTUNITY':
                    ts = rec.get('validated_at', '')
                    if ts > cutoff:
                        golden_recent += 1

        # Also check history for recent entries
        if golden_recent == 0 and history_path.exists():
            for line in history_path.read_text(encoding='utf-8').splitlines()[-500:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if (rec.get('tag') == 'GOLDEN_OPPORTUNITY'
                            and rec.get('validated_at', '') > cutoff):
                        golden_recent += 1
                        break
                except Exception:
                    continue

        if golden_recent > 0:
            return

        # Send Telegram alert
        import os
        bot_token = os.environ.get('TELEGRAM_TOKEN', '')
        chat_id   = os.environ.get('TELEGRAM_ALERT_CHAT_ID', '') or os.environ.get('TELEGRAM_CHAT_ID', '')
        if not bot_token or not chat_id:
            print("  [Drought Alert] 0 GOLDEN in last 12h — no Telegram creds to send alert")
            return

        import requests
        msg = ('*OpenClaw Alert*\n'
               '0 GOLDEN opportunities in last 2 runs (12h)\n'
               f'Time: {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}\n'
               'Action: Check commercial\\_intent filter + CPC thresholds')
        requests.post(
            f'https://api.telegram.org/bot{bot_token}/sendMessage',
            json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'},
            timeout=10,
        )
        print("  [Drought Alert] 0 GOLDEN in last 12h — Telegram alert sent")
    except Exception as e:
        print(f"  [Drought Alert] Check failed: {e}")

_check_golden_drought()

# Phase 2.4: Update Prometheus metrics after pipeline run
if _PROM_AVAILABLE and _stage_durations:
    try:
        _update_prom_metrics()
        print(f"  [Prometheus] Metrics updated ({len(_stage_durations)} stage durations recorded)")
    except Exception as _prom_err:
        print(f"  [Prometheus] Metrics update failed: {_prom_err}")

# ── Sync to backend ─────────────────────────────────────────────────────────
# Sprint 3 (R2-C7): operator can disable backend sync by touching
# config/sync.muted — stops the 500 spam and lets Railway recover without a
# code change. State transitions (ok→failed, ok→muted, etc.) are alerted
# once via lib.alerts.
try:
    from lib.alerts import alert as _ops_alert
except Exception:
    _ops_alert = None  # soft degrade — local log still works via monitors

_SYNC_MUTE_FILE = BASE / "config" / "sync.muted"
_SYNC_STATE_FILE = Path.home() / ".openclaw" / "logs" / ".sync_last_status.json"

def _read_last_sync_status() -> str:
    try:
        return json.loads(_SYNC_STATE_FILE.read_text()).get("status", "unknown")
    except Exception:
        return "unknown"

def _write_sync_status(status: str, http_code: int | None = None) -> None:
    try:
        _SYNC_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SYNC_STATE_FILE.write_text(json.dumps({
            "status": status,
            "http_code": http_code,
            "ts": datetime.now().isoformat(),
        }))
    except Exception:
        pass

print()
print("=" * 56)
print("  BACKEND SYNC")
print("=" * 56)
_prev_sync_status = _read_last_sync_status()
if _SYNC_MUTE_FILE.exists():
    print(f"  [sync] Muted via {_SYNC_MUTE_FILE.relative_to(BASE)} — skipping")
    if _prev_sync_status != "muted" and _ops_alert is not None:
        _ops_alert("info", "railway-sync-muted",
                   "Backend sync paused via config/sync.muted",
                   detail=f"Set by operator; remove the file to re-enable.")
    _write_sync_status("muted")
else:
    try:
        _sync_script = BASE / "sync_to_backend.py"
        if _sync_script.exists():
            _sync_env = {**os.environ, "RUN_ID": _run_id, "STARTED_AT": _started_at}
            _sync_proc = subprocess.run(
                [sys.executable, str(_sync_script),
                 "--run-id", _run_id, "--started-at", _started_at],
                env=_sync_env, capture_output=True, text=True, timeout=300
            )
            for _line in _sync_proc.stdout.strip().splitlines():
                print(f"  {_line}")
            # Heuristic: if sync output mentions "upstream 5" at all, treat
            # the run as degraded. The sync script prints "upstream 500 —
            # skipping" rather than exiting non-zero.
            _had_5xx = "upstream 5" in (_sync_proc.stdout or "")
            if _sync_proc.returncode != 0:
                print(f"  ⚠️  sync_to_backend.py exited {_sync_proc.returncode}")
                if _sync_proc.stderr.strip():
                    print(f"  {_sync_proc.stderr.strip()[:200]}")
                _new_status = "failed"
            elif _had_5xx:
                _new_status = "degraded"
            else:
                _new_status = "ok"
            if _new_status != "ok" and _prev_sync_status == "ok" and _ops_alert is not None:
                _ops_alert("error", "railway-sync-degraded",
                           f"Backend sync transitioned ok → {_new_status}",
                           detail=(_sync_proc.stdout or "")[-1000:])
            _write_sync_status(_new_status)
        else:
            print("  sync_to_backend.py not found — skipping")
            _write_sync_status("missing")
    except Exception as _sync_err:
        print(f"  ⚠️  Backend sync failed: {_sync_err}")
        if _prev_sync_status == "ok" and _ops_alert is not None:
            _ops_alert("error", "railway-sync-degraded",
                       f"Backend sync raised {type(_sync_err).__name__}",
                       detail=str(_sync_err)[:500])
        _write_sync_status("failed")


def _notify_golden_opportunities():
    """Send Telegram notification if new GOLDEN opportunities were found this run."""
    try:
        if not GOLDEN.exists():
            return
        goldens = json.loads(GOLDEN.read_text())
        if not goldens:
            return

        cutoff = (datetime.now() - timedelta(hours=26)).isoformat()
        new_goldens = [
            g for g in goldens
            if g.get("tag") == "GOLDEN_OPPORTUNITY"
            and (g.get("validated_at") or "") > cutoff
        ]
        if not new_goldens:
            return

        lines = [f"🏆 {len(new_goldens)} new GOLDEN opportunities found!\n"]
        for g in new_goldens[:5]:
            kw = g.get("keyword", "?")
            country = g.get("country", "?")
            cpc = g.get("cpc_usd", 0) or 0
            vol = g.get("search_volume", 0) or 0
            score = g.get("opportunity_score", 0) or 0
            lines.append(f"• {kw} ({country}) — ${cpc:.2f} CPC, {vol:,} vol, score {score:.1f}")
        if len(new_goldens) > 5:
            lines.append(f"  ...and {len(new_goldens) - 5} more")

        msg = "\n".join(lines)

        token = os.environ.get("TELEGRAM_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            print("  ⚠️  TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set — skipping golden notification")
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps({"chat_id": chat_id, "text": msg}).encode()
        req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        print(f"  ✅ Sent golden notification: {len(new_goldens)} opportunities")
    except Exception as e:
        print(f"  ⚠️  Golden notification failed: {e}")


_notify_golden_opportunities()

# Sprint 3 Task 3.3: freshness monitors at end-of-run.
try:
    from lib.freshness_monitors import run_all as _run_freshness_monitors
    _fresh_results = _run_freshness_monitors()
    _fired = [k for k, v in _fresh_results.items() if v]
    if _fired:
        print(f"  [Freshness] alerts fired: {', '.join(_fired)}")
except Exception as _fresh_err:
    print(f"  [Freshness] monitor run failed: {_fresh_err}")

print()
status = f"{len(errors)} error(s)" if errors else "all stages OK"
print(f"✅ Heartbeat complete — {status}")
_log.info("Pipeline complete", extra={"duration": round(_time.time() - _pipeline_start, 1)})
