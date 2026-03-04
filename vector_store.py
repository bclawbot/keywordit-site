import lancedb
import json
import requests
from pathlib import Path

DB_PATH = Path.home() / ".openclaw" / "vector_db"
OLLAMA_URL = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "bge-m3"

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
    data = [{"keyword": keyword, "country": country, "date": date,
              "source": source, "score": score, "vector": embedding}]
    if table_name in db.table_names():
        db.open_table(table_name).add(data)
    else:
        db.create_table(table_name, data=data)

def is_duplicate(keyword: str, country: str, threshold: float = 0.85) -> bool:
    db = _get_db()
    if "trends" not in db.table_names():
        return False
    embedding = embed_text(f"{keyword} {country}")
    results = db.open_table("trends").search(embedding).limit(1).to_list()
    if not results:
        return False
    return results[0].get("_distance", 1.0) < (1.0 - threshold)

def search_opportunities(query: str, top_k: int = 10) -> list:
    db = _get_db()
    if "opportunities" not in db.table_names():
        return []
    embedding = embed_text(query)
    return db.open_table("opportunities").search(embedding).limit(top_k).to_list()

def add_opportunity(keyword: str, country: str, arbitrage_index: float, tag: str, raw: dict):
    db = _get_db()
    embedding = embed_text(f"{keyword} {country} {tag}")
    data = [{"keyword": keyword, "country": country,
              "arbitrage_index": arbitrage_index, "tag": tag,
              "raw_json": json.dumps(raw), "vector": embedding}]
    if "opportunities" in db.table_names():
        db.open_table("opportunities").add(data)
    else:
        db.create_table("opportunities", data=data)
