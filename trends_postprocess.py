import json
import sys
from pathlib import Path
from datetime import datetime

BASE = Path("/Users/newmac/.openclaw/workspace")
SNAP = BASE / "latest_trends.json"
EXP  = BASE / "explosive_trends.json"
EXP_LOG = BASE / "explosive_trends_history.jsonl"  # append-only
ERROR_LOG = BASE / "error_log.jsonl"

sys.path.insert(0, str(BASE))
try:
    from vector_store import is_duplicate, add_trend
    _VECTOR_STORE_AVAILABLE = True
except Exception:
    _VECTOR_STORE_AVAILABLE = False

data = json.loads(SNAP.read_text())

def score(t):
    val = t.get("traffic", "0").replace("+","").replace(",","")
    val = val.replace("K","000").replace("M","000000")
    try:
        return int(val)
    except ValueError:
        return 0

explosive = [x for x in data if score(x) >= 20000]
explosive_sorted = sorted(explosive, key=score, reverse=True)

deduped_explosive = []
skipped_semantic = 0
for rec in explosive_sorted:
    rec['explosive_score'] = score(rec)
    rec['marked_at'] = datetime.now().isoformat()
    keyword = rec.get("term", "")
    country = rec.get("geo", "unknown")
    if _VECTOR_STORE_AVAILABLE and keyword:
        try:
            if is_duplicate(keyword, country):
                skipped_semantic += 1
                with ERROR_LOG.open("a") as f:
                    f.write(json.dumps({
                        "timestamp": datetime.now().isoformat(),
                        "stage": "trends_postprocess",
                        "reason": "semantic_duplicate",
                        "keyword": keyword,
                        "country": country,
                    }) + "\n")
                continue
            add_trend(keyword, country, rec.get("fetched_at", ""), rec.get("source", ""), score(rec))
        except Exception:
            pass  # vector store errors are non-fatal
    deduped_explosive.append(rec)

if skipped_semantic:
    print(f"Skipped {skipped_semantic} semantic duplicates via LanceDB")

explosive_sorted = deduped_explosive
EXP.write_text(json.dumps(explosive_sorted, indent=2))

with EXP_LOG.open("a") as f:
    for rec in explosive_sorted:
        f.write(json.dumps(rec) + "\n")

print(f"Saved {len(explosive_sorted)} explosive trends to {EXP}")
print(f"Appended to history log: {EXP_LOG}")
