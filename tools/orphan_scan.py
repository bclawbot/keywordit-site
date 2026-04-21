#!/usr/bin/env python3
"""orphan_scan.py — which JSON artifacts have no reader? (Sprint 4 Task 4.6).

Walks the workspace root for `*.json` files, then greps the tree (excluding
.git, __pycache__, node_modules, .venv) for the filename's stem to see if
any Python source references it. A file with 0 references is an orphan.

Usage:
    python tools/orphan_scan.py [--json]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE_DIRS = {".git", "__pycache__", "node_modules", ".venv", "venv",
                ".pytest_cache", ".ruff_cache", "dist", "build"}
EXCLUDE_SUFFIXES = {".schema.json"}


def list_json_artifacts() -> list[Path]:
    out = []
    for p in sorted(ROOT.glob("*.json")):
        if any(p.name.endswith(sfx) for sfx in EXCLUDE_SUFFIXES):
            continue
        out.append(p)
    return out


def count_references(filename: str) -> int:
    """Count Python source references to `filename`."""
    pattern = re.escape(filename)
    try:
        # `grep` is fine here — orphan_scan is a one-shot CLI, not a pipeline step.
        cmd = [
            "grep", "-r", "-l", "--include=*.py",
            *(f"--exclude-dir={d}" for d in EXCLUDE_DIRS),
            pattern, str(ROOT),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode not in (0, 1):
            return -1
        lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        return len(lines)
    except Exception:
        return -1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = ap.parse_args()

    artifacts = list_json_artifacts()
    rows = []
    for p in artifacts:
        refs = count_references(p.name)
        rows.append({
            "file": p.name,
            "size_kb": round(p.stat().st_size / 1024, 1),
            "reference_count": refs,
            "is_orphan": refs == 0,
        })

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    # Tabular output
    col_w = max(len(r["file"]) for r in rows) + 2
    print(f"{'FILE':<{col_w}} {'SIZE':>8} {'REFS':>5} {'ORPHAN':>7}")
    print("-" * (col_w + 24))
    orphans = 0
    for r in sorted(rows, key=lambda x: (x["is_orphan"], x["file"])):
        mark = "YES" if r["is_orphan"] else ""
        orphans += int(r["is_orphan"])
        print(f"{r['file']:<{col_w}} {r['size_kb']:>6}KB  {r['reference_count']:>5} {mark:>7}")
    print(f"\n{orphans} orphan(s) out of {len(rows)} artifacts")
    return 0 if orphans == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
