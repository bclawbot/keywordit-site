import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASE      = Path("/Users/newmac/.openclaw/workspace")
ERROR_LOG = BASE / "error_log.jsonl"
GOLDEN    = BASE / "golden_opportunities.json"

STAGES = [
    (1,  "trends_scraper.py"),
    ("1b", "trends_postprocess.py"),
    (2,  "vetting.py"),
    (3,  "validation.py"),
    (4,  "dashboard_builder.py"),
    (5,  "reflection.py"),
]

print("=" * 56)
print("  OpenClaw Heartbeat — Full Pipeline Run")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 56)

errors = []

for stage_num, script_name in STAGES:
    script_path = BASE / script_name
    if not script_path.exists():
        msg = f"{script_path} not found"
        print(f"[Stage {stage_num}] ❌ Failed: {msg}")
        errors.append({"timestamp": datetime.now().isoformat(), "stage": str(stage_num), "error": msg})
        continue

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print(f"[Stage {stage_num}] ✅ Done")
        if result.stdout.strip():
            for line in result.stdout.strip().splitlines():
                print(f"           {line}")
    else:
        stderr = result.stderr.strip() or result.stdout.strip() or "non-zero exit"
        first_error_line = stderr.splitlines()[0] if stderr else "unknown error"
        print(f"[Stage {stage_num}] ❌ Failed: {first_error_line}")
        errors.append({
            "timestamp": datetime.now().isoformat(),
            "stage": str(stage_num),
            "error": stderr,
        })

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
