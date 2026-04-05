#!/usr/bin/env python3
"""
One-time migration: import existing JSON opportunity/angle files into SQLite.

Usage:
    python3 scripts/migrate_json_to_sqlite.py [--db PATH] [--dry-run]

This reads:
    - validated_opportunities.json → stage="validated"
    - vetted_opportunities.json    → stage="vetted"
    - golden_opportunities.json    → stage="golden"
    - angle_candidates.json        → angle_candidates table

Original JSON files are NOT deleted.
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sqlite_store import OpportunityStore

BASE = Path(__file__).resolve().parent.parent

FILES = [
    ("validated_opportunities.json", "validated"),
    ("vetted_opportunities.json",    "vetted"),
    ("golden_opportunities.json",    "golden"),
]

ANGLE_FILE = "angle_candidates.json"


def main():
    parser = argparse.ArgumentParser(description="Migrate JSON files to SQLite")
    parser.add_argument("--db", type=str, default=None, help="SQLite DB path (default: pipeline_store.db)")
    parser.add_argument("--dry-run", action="store_true", help="Only report what would be imported")
    args = parser.parse_args()

    store = OpportunityStore(db_path=args.db)
    total = 0

    for filename, stage in FILES:
        path = BASE / filename
        if not path.exists():
            print(f"  SKIP {filename} — not found")
            continue
        size_mb = path.stat().st_size / (1024 * 1024)
        if args.dry_run:
            print(f"  WOULD import {filename} ({size_mb:.1f} MB) as stage='{stage}'")
            continue
        print(f"  Importing {filename} ({size_mb:.1f} MB) as stage='{stage}'...", end=" ", flush=True)
        t0 = time.time()
        count = store.import_from_json(path, stage=stage)
        elapsed = time.time() - t0
        print(f"{count} records in {elapsed:.1f}s")
        total += count

    # Angle candidates (large file — stream in chunks)
    angle_path = BASE / ANGLE_FILE
    if angle_path.exists():
        size_mb = angle_path.stat().st_size / (1024 * 1024)
        if args.dry_run:
            print(f"  WOULD import {ANGLE_FILE} ({size_mb:.1f} MB) to angle_candidates")
        else:
            print(f"  Importing {ANGLE_FILE} ({size_mb:.1f} MB) to angle_candidates...", end=" ", flush=True)
            t0 = time.time()
            count = store.import_angles_from_json(angle_path)
            elapsed = time.time() - t0
            print(f"{count} records in {elapsed:.1f}s")
            total += count
    else:
        print(f"  SKIP {ANGLE_FILE} — not found")

    if not args.dry_run:
        print(f"\nTotal: {total} records imported to {store.db_path}")
        print(f"  Opportunities: {store.count_opportunities()}")
        print(f"  Angles: {store.count_angles()}")


if __name__ == "__main__":
    main()
