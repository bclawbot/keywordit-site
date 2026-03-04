#!/usr/bin/env python3
"""One-time script to index existing trend history into LanceDB."""
import json
from pathlib import Path
from vector_store import add_trend, add_opportunity

WORKSPACE = Path.home() / ".openclaw" / "workspace"

def index_trends():
    for fname in ["trends_all_history.jsonl", "explosive_trends_history.jsonl"]:
        fpath = WORKSPACE / fname
        if not fpath.exists():
            print(f"Skipping {fname} — not found")
            continue
        count = 0
        with open(fpath) as f:
            for line in f:
                try:
                    rec = json.loads(line.strip())
                    keyword = rec.get("keyword") or rec.get("term") or rec.get("title", "")
                    country = rec.get("country") or rec.get("geo", "unknown")
                    date = rec.get("date") or rec.get("fetched_at") or rec.get("pubDate", "")
                    if keyword:
                        add_trend(keyword, country, str(date), fname, rec.get("traffic", 0))
                        count += 1
                        if count % 100 == 0:
                            print(f"  {fname}: indexed {count} entries...")
                except Exception:
                    continue
        print(f"Done: {fname} — {count} entries indexed")

def index_opportunities():
    fpath = WORKSPACE / "golden_opportunities.json"
    if not fpath.exists():
        print("No golden_opportunities.json found")
        return
    with open(fpath) as f:
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

if __name__ == "__main__":
    print("Indexing trend history...")
    index_trends()
    print("Indexing opportunities...")
    index_opportunities()
    print("All done.")
