import concurrent.futures as _cf
import json
import subprocess
import sys
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
    _CAFF_PROC = subprocess.Popen(["caffeinate", "-i", "-w", str(_MY_PID)])
    _atexit.register(lambda: _CAFF_PROC.terminate() if _CAFF_PROC else None)
except FileNotFoundError:
    pass  # non-macOS or caffeinate missing — no-op

# ── Stage checkpoint (resume after sleep/crash) ───────────────────────────────
CHECKPOINT_FILE = Path("/tmp/openclaw_run_checkpoint.json")

def _load_checkpoint() -> set:
    """Return set of stage names completed today. Resets checkpoint if new day."""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        if data.get("run_date") == today:
            completed = set(data.get("completed", []))
            if completed:
                print(f"  [Checkpoint] Resuming — {len(completed)} stages already done: {', '.join(sorted(completed))}")
            return completed
    except Exception:
        pass
    CHECKPOINT_FILE.write_text(json.dumps({
        "run_date": today,
        "started_at": datetime.now().isoformat(),
        "completed": []
    }))
    return set()

def _mark_stage_done(script_name: str):
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        if data.get("run_date") == today:
            completed = data.get("completed", [])
            if script_name not in completed:
                completed.append(script_name)
                data["completed"] = completed
                CHECKPOINT_FILE.write_text(json.dumps(data))
    except Exception:
        pass

# Per-stage timeouts (seconds)
STAGE_TIMEOUTS = {
    "subreddit_discovery.py": 1200,   # increased: async comment mining can be slow
    "reddit_intelligence.py": 300,
    "trends_scraper.py":      1800,
    "trends_postprocess.py":  3600,   # LanceDB dedup embeds 700+ trends (~30-35 min)
    "keyword_expander.py":    600,
    "commercial_keyword_transformer.py": 3600,  # increased: LLM transformation, safety net with think=False
    "keyword_extractor.py":   21600,  # 6h — LLM batches + DataForSEO expansion
    "vetting.py":             3600,
    "validation.py":          1800,
    "angle_engine.py":        5400,   # increased: LLM title generation, safety net with think=False
    "dashboard_builder.py":   120,
    "reflection.py":          300,
}

STAGES = [
    ("0a", "subreddit_discovery.py"),
    (0,    "reddit_intelligence.py"),
    (1,    "trends_scraper.py"),
    ("1b", "trends_postprocess.py"),
    ("2a", "keyword_expander.py"),
    ("2a.5", "commercial_keyword_transformer.py"),  # Transform CPC=$0 keywords
    (2,    "keyword_extractor.py"),
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
    _t0 = _time.time()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_secs)
        rc = proc.returncode
        _stage_durations[script_name] = round(_time.time() - _t0, 1)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        msg = f"{script_name} timed out after {timeout_secs}s"
        print(f"[Stage {stage_num}] ❌ Timeout: {msg}")
        errors.append({"timestamp": datetime.now().isoformat(),
                        "stage": str(stage_num), "error": msg})
        return False
    if rc == 0:
        _mark_stage_done(script_name)
        print(f"[Stage {stage_num}] ✅ Done")
        if stdout.strip():
            for line in stdout.strip().splitlines():
                print(f"           {line}")
        return script_name == "dashboard_builder.py"
    else:
        stderr_out = stderr.strip() or stdout.strip() or "non-zero exit"
        first_error_line = stderr_out.splitlines()[0] if stderr_out else "unknown error"
        print(f"[Stage {stage_num}] ❌ Failed: {first_error_line}")
        errors.append({"timestamp": datetime.now().isoformat(),
                        "stage": str(stage_num), "error": stderr_out})
        return False

_completed_stages = _load_checkpoint()

# Remove Ollama from LiteLLM fallbacks to prevent contention during pipeline
_litellm_swapped = _strip_ollama_from_litellm()

try:
    stages_iter = iter(STAGES)
    for stage_num, script_name in stages_iter:
        if script_name in PARALLEL_WITH:
            next_stage_num, next_script = next(stages_iter)
            skip_a = script_name in _completed_stages
            skip_b = next_script in _completed_stages
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
            if script_name in _completed_stages:
                print(f"[Stage {stage_num}] ⏭  Skipped (checkpoint)")
                continue
            if _exec_stage(stage_num, script_name):
                dashboard_ok = True
finally:
    if _litellm_swapped:
        _restore_litellm()

# ── Log errors ───────────────────────────────────────────────────────────────
if errors:
    with ERROR_LOG.open("a") as f:
        for e in errors:
            f.write(json.dumps(e) + "\n")

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
    except Exception as e:
        print(f"  ⚠️  Could not read golden_opportunities.json: {e}")
else:
    print("  golden_opportunities.json not found")

# ── Phase 0.2: Zero-GOLDEN drought alert ─────────────────────────────────────
def _check_golden_drought():
    """Alert via Telegram if 0 GOLDEN opportunities found in last 12h (2 cycles)."""
    try:
        validated_path = BASE / "validated_opportunities.json"
        history_path   = BASE / "validation_history.jsonl"
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
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

print()
status = f"{len(errors)} error(s)" if errors else "all stages OK"
print(f"✅ Heartbeat complete — {status}")
