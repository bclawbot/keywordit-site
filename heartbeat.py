import concurrent.futures as _cf
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASE      = Path("/Users/newmac/.openclaw/workspace")
ERROR_LOG = BASE / "error_log.jsonl"
GOLDEN    = BASE / "golden_opportunities.json"

# Per-stage timeouts (seconds)
STAGE_TIMEOUTS = {
    "subreddit_discovery.py": 900,
    "reddit_intelligence.py": 300,
    "trends_scraper.py":      1800,
    "trends_postprocess.py":  600,
    "keyword_expander.py":    600,
    "keyword_extractor.py":   21600,  # 6h — LLM batches + DataForSEO expansion
    "vetting.py":             3600,
    "validation.py":          900,
    "dashboard_builder.py":   120,
    "reflection.py":          300,
}

STAGES = [
    ("0a", "subreddit_discovery.py"),
    (0,    "reddit_intelligence.py"),
    (1,    "trends_scraper.py"),
    ("1b", "trends_postprocess.py"),
    ("2a", "keyword_expander.py"),
    (2,    "keyword_extractor.py"),
    ("2b", "vetting.py"),
    (3,    "validation.py"),
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
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_secs)
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        msg = f"{script_name} timed out after {timeout_secs}s"
        print(f"[Stage {stage_num}] ❌ Timeout: {msg}")
        errors.append({"timestamp": datetime.now().isoformat(),
                        "stage": str(stage_num), "error": msg})
        return False
    if rc == 0:
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

stages_iter = iter(STAGES)
for stage_num, script_name in stages_iter:
    if script_name in PARALLEL_WITH:
        next_stage_num, next_script = next(stages_iter)
        print(f"[Parallel] {script_name} + {next_script} starting concurrently…")
        with _cf.ThreadPoolExecutor(max_workers=2) as pool:
            fut_a = pool.submit(_exec_stage, stage_num, script_name)
            fut_b = pool.submit(_exec_stage, next_stage_num, next_script)
            if fut_a.result():
                dashboard_ok = True
            if fut_b.result():
                dashboard_ok = True
    else:
        if _exec_stage(stage_num, script_name):
            dashboard_ok = True

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

print()
status = f"{len(errors)} error(s)" if errors else "all stages OK"
print(f"✅ Heartbeat complete — {status}")
