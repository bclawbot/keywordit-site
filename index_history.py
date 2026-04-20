#!/usr/bin/env python3
"""Incremental LanceDB indexer for historical trends and opportunities.

Phase 3.2: Tracks last-indexed byte offset per file so re-runs only
process new entries. Safe to run from cron or after every heartbeat.

Usage:
    python3 index_history.py              # incremental (default)
    python3 index_history.py --full       # full re-index
"""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

try:
    from vector_store import add_trend, add_opportunity
    _VS_AVAILABLE = True
except ImportError:
    _VS_AVAILABLE = False

WORKSPACE = Path.home() / ".openclaw" / "workspace"
STATE_FILE = WORKSPACE / ".index_history_state.json"


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def index_trends(full: bool = False):
    state = _load_state() if not full else {}
    for fname in ["trends_all_history.jsonl", "explosive_trends_history.jsonl"]:
        fpath = WORKSPACE / fname
        if not fpath.exists():
            print(f"Skipping {fname} — not found")
            continue
        last_offset = state.get(fname, 0) if not full else 0
        file_size = fpath.stat().st_size
        if last_offset >= file_size:
            print(f"  {fname}: up to date (offset={last_offset})")
            continue
        count = 0
        errors = 0
        with open(fpath, encoding="utf-8") as f:
            f.seek(last_offset)
            for line in f:
                try:
                    rec = json.loads(line.strip())
                    keyword = rec.get("keyword") or rec.get("term") or rec.get("title", "")
                    country = rec.get("country") or rec.get("geo", "unknown")
                    date = rec.get("date") or rec.get("fetched_at") or rec.get("pubDate", "")
                    if keyword:
                        add_trend(keyword, country, str(date), fname, rec.get("traffic", 0))
                        count += 1
                        if count % 500 == 0:
                            print(f"  {fname}: indexed {count} entries...")
                except Exception:
                    errors += 1
                    continue
            new_offset = f.tell()
        state[fname] = new_offset
        print(f"Done: {fname} — {count} new entries indexed"
              f"{f', {errors} errors' if errors else ''}")
    _save_state(state)


def index_opportunities(full: bool = False):
    fpath = WORKSPACE / "golden_opportunities.json"
    if not fpath.exists():
        print("No golden_opportunities.json found")
        return
    with open(fpath, encoding="utf-8") as f:
        opps = json.load(f)
    if isinstance(opps, dict):
        opps = opps.get("opportunities", list(opps.values()))
    count = 0
    for opp in opps:
        keyword = opp.get("keyword", "")
        country = opp.get("country", "unknown")
        if keyword:
            add_opportunity(keyword, country, opp.get("arbitrage_index", 0),
                          opp.get("tag", ""), opp)
            count += 1
    print(f"Done: golden_opportunities — {count} entries indexed")

    # Also index validation_history.jsonl incrementally
    vhist = WORKSPACE / "validation_history.jsonl"
    if vhist.exists():
        state = _load_state()
        last_offset = state.get("validation_history.jsonl", 0) if not full else 0
        if last_offset < vhist.stat().st_size:
            vcount = 0
            with open(vhist, encoding="utf-8") as f:
                f.seek(last_offset)
                for line in f:
                    try:
                        rec = json.loads(line.strip())
                        kw = rec.get("keyword", "")
                        cc = rec.get("country", "unknown")
                        if kw and rec.get("tag") in ("GOLDEN_OPPORTUNITY", "WATCH", "EMERGING", "EMERGING_HIGH"):
                            add_opportunity(kw, cc, rec.get("arbitrage_index", 0),
                                          rec.get("tag", ""), rec)
                            vcount += 1
                    except Exception:
                        continue
                state["validation_history.jsonl"] = f.tell()
            _save_state(state)
            if vcount:
                print(f"Done: validation_history — {vcount} new entries indexed")


if __name__ == "__main__":
    if not _VS_AVAILABLE:
        print("⚠️  vector_store not available — cannot index")
        sys.exit(1)

    full_reindex = "--full" in sys.argv
    mode = "full re-index" if full_reindex else "incremental"
    print(f"LanceDB indexer ({mode})")
    print("Indexing trend history...")
    index_trends(full=full_reindex)
    print("Indexing opportunities...")
    index_opportunities(full=full_reindex)
    print("All done.")
