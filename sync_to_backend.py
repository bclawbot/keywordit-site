#!/usr/bin/env python3
"""
sync_to_backend.py — Push pipeline data + run report to Railway backend.

Called by heartbeat.py after the pipeline completes (or partially completes).
Syncs: opportunities, angles, trends, and pipeline run analytics.

Usage:
    python3 sync_to_backend.py [--run-id <id>] [--data-only] [--pipeline-only]
"""

import json
import os
import platform
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import httpx
except ImportError:
    print("[SYNC] httpx not installed. Run: pip install httpx")
    sys.exit(1)

# ── Config ───────────────────────────────────────────────────────────────────

BACKEND_URL = os.getenv("BACKEND_URL", "https://keywordit-api-production.up.railway.app")
SYNC_API_KEY = os.getenv("SYNC_API_KEY", "")
WORKSPACE = Path.home() / ".openclaw" / "workspace"

# Load from .env if key not in environment
if not SYNC_API_KEY:
    env_path = WORKSPACE / "backend" / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("SYNC_API_KEY="):
                SYNC_API_KEY = line.split("=", 1)[1].strip()
                break

HEADERS = {
    "Authorization": f"Bearer {SYNC_API_KEY}",
    "Content-Type": "application/json",
}

# Pipeline stages (must match heartbeat.py)
STAGES = [
    ("0a", "subreddit_discovery.py"),
    ("0", "reddit_intelligence.py"),
    ("1", "trends_scraper.py"),
    ("1b", "trends_postprocess.py"),
    ("2a", "keyword_expander.py"),
    ("2a.5", "commercial_keyword_transformer.py"),
    ("2", "keyword_extractor.py"),
    ("2b", "vetting.py"),
    ("3", "validation.py"),
    ("3a", "angle_engine.py"),
    ("4", "dashboard_builder.py"),
    ("5", "reflection.py"),
    ("6", "deploy_dashboard.sh"),
]

STAGE_OUTPUTS = {
    "subreddit_discovery.py": "subreddit_registry.json",
    "reddit_intelligence.py": "reddit_intelligence.json",
    "trends_scraper.py": "latest_trends.json",
    "trends_postprocess.py": "explosive_trends.json",
    "keyword_expander.py": "expanded_keywords.json",
    "commercial_keyword_transformer.py": "transformed_keywords.json",
    "keyword_extractor.py": "commercial_keywords.json",
    "vetting.py": "vetted_opportunities.json",
    "validation.py": "validated_opportunities.json",
    "angle_engine.py": "angle_candidates.json",
    "dashboard_builder.py": "dashboard.html",
    "reflection.py": None,
    "deploy_dashboard.sh": None,
}


def _post(endpoint: str, payload: dict, timeout: int = 120) -> dict | None:
    """POST to backend with retry."""
    for attempt in range(3):
        try:
            resp = httpx.post(
                f"{BACKEND_URL}{endpoint}",
                json=payload,
                headers=HEADERS,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[SYNC] {endpoint} attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    return None


def sync_data(run_id: str):
    """Sync opportunity, trend, and angle data."""
    # Opportunities (validated + golden deduplicated)
    validated_path = WORKSPACE / "validated_opportunities.json"
    golden_path = WORKSPACE / "golden_opportunities.json"
    if validated_path.exists():
        validated = json.loads(validated_path.read_text())
        golden = json.loads(golden_path.read_text()) if golden_path.exists() else []
        seen = set()
        combined = []
        for opp in golden + validated:
            key = (opp.get("keyword", ""), opp.get("country", ""))
            if key not in seen:
                seen.add(key)
                combined.append(opp)
        result = _post("/api/sync/opportunities", {"records": combined, "run_id": run_id})
        print(f"[SYNC] Opportunities: {result}")

    # Trends
    trends_path = WORKSPACE / "explosive_trends.json"
    if trends_path.exists():
        trends = json.loads(trends_path.read_text())
        result = _post("/api/sync/trends", {"records": trends, "run_id": run_id})
        print(f"[SYNC] Trends: {result}")

    # Angles (batched)
    angles_path = WORKSPACE / "angle_candidates.json"
    if angles_path.exists():
        angles = json.loads(angles_path.read_text())
        BATCH = 500
        for i in range(0, len(angles), BATCH):
            batch = angles[i : i + BATCH]
            mark_stale = i == 0
            result = _post(
                "/api/sync/angles",
                {"records": batch, "run_id": run_id, "mark_stale": mark_stale},
                timeout=300,
            )
            print(f"[SYNC] Angles batch {i // BATCH + 1}: {result}")


def build_pipeline_report(run_id: str, started_at: str, errors: list) -> dict:
    """Build a pipeline run report from local artifacts."""
    checkpoints_dir = Path("/tmp/openclaw_checkpoints")

    stages_report = []
    completed = 0
    failed = 0
    skipped = 0

    for stage_num, script_name in STAGES:
        # Check if stage completed via checkpoint marker
        done_marker = None
        if checkpoints_dir.exists():
            for f in checkpoints_dir.glob(f"*_{script_name}.done"):
                done_marker = f
                break

        # Check output file
        output_file = STAGE_OUTPUTS.get(script_name)
        output_path = WORKSPACE / output_file if output_file else None
        records_out = None

        if output_path and output_path.exists() and output_path.stat().st_size > 0:
            try:
                data = json.loads(output_path.read_text())
                if isinstance(data, list):
                    records_out = len(data)
            except Exception:
                pass

        # Determine status
        stage_error = None
        for err in errors:
            if err.get("stage") == str(stage_num) or script_name in err.get("error", ""):
                stage_error = err.get("error", "Unknown error")
                break

        if stage_error:
            status = "failed"
            failed += 1
        elif done_marker:
            status = "completed"
            completed += 1
        elif output_path and output_path.exists() and output_path.stat().st_size > 0:
            status = "completed"
            completed += 1
        else:
            status = "skipped"
            skipped += 1

        # Get duration from stage timing if available
        duration = None
        mtime = None
        if done_marker and done_marker.exists():
            mtime = datetime.fromtimestamp(done_marker.stat().st_mtime).isoformat()

        stages_report.append({
            "stage_num": str(stage_num),
            "script_name": script_name,
            "status": status,
            "completed_at": mtime,
            "duration_secs": duration,
            "records_out": records_out,
            "error_message": stage_error,
            "output_file": output_file,
        })

    # Overall status
    if failed > 0:
        overall_status = "partial"
    elif completed == len(STAGES):
        overall_status = "completed"
    elif completed > 0:
        overall_status = "partial"
    else:
        overall_status = "failed"

    # Funnel counts
    funnel = {}
    for script_name, output_file in STAGE_OUTPUTS.items():
        if output_file and (WORKSPACE / output_file).exists():
            try:
                data = json.loads((WORKSPACE / output_file).read_text())
                if isinstance(data, list):
                    funnel[output_file] = len(data)
            except Exception:
                pass

    # Golden/validated counts
    golden_count = funnel.get("golden_opportunities.json", 0)
    golden_path = WORKSPACE / "golden_opportunities.json"
    if golden_path.exists():
        try:
            golden_data = json.loads(golden_path.read_text())
            golden_count = len([o for o in golden_data if o.get("tag") == "GOLDEN_OPPORTUNITY"])
        except Exception:
            pass

    error_summary = None
    if errors:
        error_summary = "; ".join(
            f"Stage {e.get('stage', '?')}: {e.get('error', '?')[:100]}" for e in errors[:5]
        )

    return {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": datetime.now().isoformat(),
        "status": overall_status,
        "total_stages": len(STAGES),
        "completed_stages": completed,
        "failed_stages": failed,
        "skipped_stages": skipped,
        "golden_count": golden_count,
        "total_validated": funnel.get("validated_opportunities.json"),
        "total_trends": funnel.get("explosive_trends.json"),
        "funnel_counts": funnel,
        "error_summary": error_summary,
        "mac_hostname": platform.node(),
        "stages": stages_report,
    }


def sync_pipeline(run_id: str, started_at: str, errors: list | None = None):
    """Sync pipeline run report to backend."""
    errors = errors or []

    # Also read errors from error_log.jsonl
    error_log = WORKSPACE / "error_log.jsonl"
    if error_log.exists():
        try:
            for line in error_log.read_text().splitlines()[-20:]:
                entry = json.loads(line)
                if entry.get("timestamp", "").startswith(run_id[:10]):
                    errors.append(entry)
        except Exception:
            pass

    report = build_pipeline_report(run_id, started_at, errors)
    result = _post("/api/pipeline/sync", report)
    print(f"[SYNC] Pipeline run: {result}")
    return report


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Sync pipeline data to backend")
    parser.add_argument("--run-id", default=datetime.now().isoformat())
    parser.add_argument("--started-at", default=datetime.now().isoformat())
    parser.add_argument("--data-only", action="store_true")
    parser.add_argument("--pipeline-only", action="store_true")
    args = parser.parse_args()

    if not SYNC_API_KEY:
        print("[SYNC] ERROR: SYNC_API_KEY not set")
        sys.exit(1)

    print(f"[SYNC] Backend: {BACKEND_URL}")
    print(f"[SYNC] Run ID: {args.run_id}")

    if not args.pipeline_only:
        sync_data(args.run_id)

    if not args.data_only:
        report = sync_pipeline(args.run_id, args.started_at)
        print(f"[SYNC] Status: {report['status']} ({report['completed_stages']}/{report['total_stages']} stages)")


if __name__ == "__main__":
    main()
