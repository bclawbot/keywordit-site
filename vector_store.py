import lancedb
import json
import requests
import warnings
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

DB_PATH = Path.home() / ".openclaw" / "vector_db"
OLLAMA_URL = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "bge-m3"

# How long to keep trend vectors (dedup window).
# Trends older than this are not considered duplicates.
DEDUP_TTL_DAYS = 14

def _get_db():
    DB_PATH.mkdir(parents=True, exist_ok=True)
    return lancedb.connect(str(DB_PATH))

def embed_text(text: str) -> list:
    resp = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text}, timeout=30)
    resp.raise_for_status()
    return resp.json()["embedding"]

def add_trend(keyword: str, country: str, date: str, source: str, score: float, raw_text: str = ""):
    db = _get_db()
    embedding = embed_text(f"{keyword} {country} {raw_text[:200]}")
    table_name = "trends"
    now_iso = datetime.now().isoformat()
    data = [{"keyword": keyword, "country": country, "date": date,
              "source": source, "score": score, "added_at": now_iso, "vector": embedding}]
    if table_name in db.list_tables():
        db.open_table(table_name).add(data)
    else:
        db.create_table(table_name, data=data, metric="cosine")

def is_duplicate(keyword: str, country: str, threshold: float = 0.85) -> bool:
    db = _get_db()
    if "trends" not in db.list_tables():
        return False
    embedding = embed_text(f"{keyword} {country}")
    # Only search within the TTL window — skip very old vectors
    cutoff = (datetime.now() - timedelta(days=DEDUP_TTL_DAYS)).isoformat()
    try:
        tbl = db.open_table("trends")
        results = (
            tbl.search(embedding)
            .where(f"added_at > '{cutoff}'", prefilter=True)
            .limit(1)
            .to_list()
        )
    except Exception:
        # Prefilter not supported on this schema (e.g. fresh table without added_at)
        results = db.open_table("trends").search(embedding).limit(1).to_list()
    if not results:
        return False
    return results[0].get("_distance", 1.0) < (1.0 - threshold)

def search_opportunities(query: str, top_k: int = 10) -> list:
    db = _get_db()
    if "opportunities" not in db.list_tables():
        return []
    embedding = embed_text(query)
    return db.open_table("opportunities").search(embedding).limit(top_k).to_list()

def add_opportunity(keyword: str, country: str, arbitrage_index: float, tag: str, raw: dict):
    db = _get_db()
    embedding = embed_text(f"{keyword} {country} {tag}")
    data = [{"keyword": keyword, "country": country,
              "arbitrage_index": arbitrage_index, "tag": tag,
              "raw_json": json.dumps(raw), "vector": embedding}]
    if "opportunities" in db.list_tables():
        db.open_table("opportunities").add(data)
    else:
        db.create_table("opportunities", data=data, metric="cosine")

def maintenance():
    """
    Compact LanceDB and remove old version files.
    Call once per pipeline run (at the end of trends_postprocess).
    Keeps the _versions directory small to avoid the 9GB bloat issue.
    """
    db = _get_db()
    for table_name in db.list_tables():
        try:
            tbl = db.open_table(table_name)
            tbl.cleanup_old_versions(older_than=timedelta(hours=1), delete_unverified=False)
        except Exception:
            pass  # non-fatal
