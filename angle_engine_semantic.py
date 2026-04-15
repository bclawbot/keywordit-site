"""
angle_engine_semantic.py — Semantic matching: find fb_intel ads similar to
expansion keywords using bge-m3 embeddings via Ollama + LanceDB.
"""
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
FB_DB = BASE / "dwight" / "fb_intelligence" / "data" / "fb_intelligence.db"
LANCE_DIR = BASE / "dwight" / "fb_intelligence" / "data" / "lancedb"
EXPANSION_FILE = BASE / "data" / "expansion_results.jsonl"


def embed_text(text: str) -> list:
    """Get bge-m3 embedding via Ollama."""
    try:
        import requests
    except ImportError:
        return []
    resp = requests.post("http://localhost:11434/api/embeddings", json={
        "model": "bge-m3",
        "prompt": text
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["embedding"]


def find_similar_ads(keyword: str, top_k: int = 5, threshold: float = 0.75):
    """
    Embed keyword, search LanceDB for similar ads.
    Returns list of dicts with ad_id, headline, similarity.
    """
    try:
        import lancedb
    except ImportError:
        return []

    try:
        db = lancedb.connect(str(LANCE_DIR))
        table = db.open_table("ad_embeddings")

        query_embedding = embed_text(keyword)
        if not query_embedding:
            return []
        results = table.search(query_embedding).limit(top_k).to_pandas()

        # Filter by threshold
        matches = []
        for _, row in results.iterrows():
            score = 1 - row.get("_distance", 1)  # cosine similarity
            if score >= threshold:
                matches.append({
                    "ad_id": row.get("ad_id"),
                    "headline": row.get("headline", ""),
                    "similarity": round(score, 3),
                })
        return matches
    except Exception as e:
        print(f"[semantic] Error: {e}")
        return []
