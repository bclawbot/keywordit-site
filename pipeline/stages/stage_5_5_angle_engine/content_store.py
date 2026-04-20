"""
content_store.py — LanceDB table management for angle_candidates and generated_articles.

Creates two new tables in the existing vector_db. Does NOT touch the existing
'trends' or 'opportunities' tables created by vector_store.py.

Embedding model: bge-m3 via Ollama (same as vector_store.py).
Non-fatal: all write functions log to error_log.jsonl on failure and return
a sentinel value rather than raising — the pipeline must not crash on LanceDB errors.
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

# ─── Paths ────────────────────────────────────────────────────────────────────
DB_PATH     = Path.home() / ".openclaw" / "vector_db"
ERROR_LOG   = Path.home() / ".openclaw" / "workspace" / "error_log.jsonl"
OLLAMA_URL  = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "bge-m3"

ANGLE_TABLE    = "angle_candidates"
ARTICLES_TABLE = "generated_articles"


# ─── Logging (matches validation.py _log_error pattern) ──────────────────────
def _log_error(stage: str, msg: str, extra: Optional[dict] = None) -> None:
    entry: dict = {
        "timestamp": datetime.now().isoformat(),
        "stage":     stage,
        "error":     str(msg),
    }
    if extra:
        entry.update(extra)
    try:
        with open(ERROR_LOG, "a") as fh:
            fh.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # never crash on log failure


# ─── Embedding ────────────────────────────────────────────────────────────────
def _embed(text: str) -> list:
    """Call Ollama bge-m3 embedding. Returns list[float] or [] on failure."""
    try:
        import urllib.request
        payload = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
        req = urllib.request.Request(
            OLLAMA_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            return data.get("embedding", [])
    except Exception as e:
        _log_error("content_store._embed", f"bge-m3 embedding failed: {e}")
        return []


# ─── DB Connection ────────────────────────────────────────────────────────────
def _get_db():
    """Open (or create) the LanceDB at DB_PATH. Returns connection or None."""
    try:
        import lancedb
        return lancedb.connect(str(DB_PATH))
    except Exception as e:
        _log_error("content_store._get_db", f"LanceDB connect failed: {e}")
        return None


# ─── Angle Candidates Table ───────────────────────────────────────────────────
def write_angle_candidates(records: list) -> int:
    """
    Write angle candidate records to LanceDB angle_candidates table.
    Each record is an AngleEngineOutput serialised as a dict.
    Returns count written; returns 0 on any failure (non-fatal).

    Schema columns:
        cluster_id, keyword, country, language_code, vertical,
        cpc_usd, tag, angle_type, angle_title, ad_category,
        rsoc_score, selected, discovery_signal_type,
        processed_at, vector
    """
    if not records:
        return 0

    db = _get_db()
    if db is None:
        return 0

    try:
        rows = []
        for output in records:
            kw      = output.get("keyword", "")
            country = output.get("country", "")
            for angle in output.get("selected_angles", []):
                embed_text = f"{kw} {angle.get('angle_type','')} {angle.get('angle_title','')}"
                vec = _embed(embed_text)
                row = {
                    "cluster_id":           output.get("cluster_id", ""),
                    "keyword":              kw,
                    "country":              country,
                    "language_code":        output.get("language_code", "en"),
                    "vertical":             output.get("vertical", "unknown"),
                    "cpc_usd":              float(output.get("cpc_usd", 0.0)),
                    "tag":                  output.get("tag", ""),
                    "angle_type":           angle.get("angle_type", ""),
                    "angle_title":          angle.get("angle_title", ""),
                    "ad_category":          angle.get("ad_category", ""),
                    "rsoc_score":           float(angle.get("rsoc_score", 0.0)),
                    "selected":             bool(angle.get("selected", True)),
                    "discovery_signal_type": output.get("discovery_context", {}).get("signal_type", "keyword_expansion"),
                    "processed_at":         output.get("processed_at", datetime.now().isoformat()),
                }
                if vec:
                    row["vector"] = vec
                rows.append(row)

        if not rows:
            return 0

        try:
            tbl = db.open_table(ANGLE_TABLE)
            tbl.add(rows)
        except Exception:
            # Table doesn't exist yet — create it
            import pyarrow as pa
            vec_dim = len(rows[0].get("vector", [])) or 1024
            schema = pa.schema([
                pa.field("cluster_id",            pa.string()),
                pa.field("keyword",               pa.string()),
                pa.field("country",               pa.string()),
                pa.field("language_code",         pa.string()),
                pa.field("vertical",              pa.string()),
                pa.field("cpc_usd",               pa.float32()),
                pa.field("tag",                   pa.string()),
                pa.field("angle_type",            pa.string()),
                pa.field("angle_title",           pa.string()),
                pa.field("ad_category",           pa.string()),
                pa.field("rsoc_score",            pa.float32()),
                pa.field("selected",              pa.bool_()),
                pa.field("discovery_signal_type", pa.string()),
                pa.field("processed_at",          pa.string()),
                pa.field("vector",                pa.list_(pa.float32(), vec_dim)),
            ])
            # Ensure all rows have a vector (pad with zeros if embedding failed)
            for r in rows:
                if "vector" not in r or not r["vector"]:
                    r["vector"] = [0.0] * vec_dim
            db.create_table(ANGLE_TABLE, data=rows, schema=schema)

        return len(rows)

    except Exception as e:
        _log_error("content_store.write_angle_candidates", str(e))
        return 0


# ─── Generated Articles Table ─────────────────────────────────────────────────
def write_article_record(record: dict) -> bool:
    """
    Write one generated article record to LanceDB generated_articles table.
    Returns True on success, False on failure (non-fatal).

    Schema columns:
        keyword, country, angle_type, angle_title, language_code, vertical,
        word_count, file_path, raf_compliant, quality_score,
        compliance_risk_level, generated_at, model_used,
        generation_time_secs, vector
    """
    db = _get_db()
    if db is None:
        return False

    try:
        embed_text = f"{record.get('keyword','')} {record.get('angle_title','')}"
        vec = _embed(embed_text)

        row = {
            "keyword":               record.get("keyword", ""),
            "country":               record.get("country", ""),
            "angle_type":            record.get("angle_type", ""),
            "angle_title":           record.get("angle_title", ""),
            "language_code":         record.get("language_code", "en"),
            "vertical":              record.get("vertical", "unknown"),
            "word_count":            int(record.get("word_count", 0)),
            "file_path":             record.get("file_path", ""),
            "raf_compliant":         bool(record.get("raf_compliant", False)),
            "quality_score":         float(record.get("quality_score", 0.0)),
            "compliance_risk_level": record.get("compliance_risk_level", "UNKNOWN"),
            "generated_at":          record.get("generated_at", datetime.now().isoformat()),
            "model_used":            record.get("model_used", ""),
            "generation_time_secs":  float(record.get("generation_time_secs", 0.0)),
        }
        if vec:
            row["vector"] = vec

        try:
            tbl = db.open_table(ARTICLES_TABLE)
            tbl.add([row])
        except Exception:
            import pyarrow as pa
            vec_dim = len(vec) if vec else 1024
            if not vec:
                row["vector"] = [0.0] * vec_dim
            schema = pa.schema([
                pa.field("keyword",               pa.string()),
                pa.field("country",               pa.string()),
                pa.field("angle_type",            pa.string()),
                pa.field("angle_title",           pa.string()),
                pa.field("language_code",         pa.string()),
                pa.field("vertical",              pa.string()),
                pa.field("word_count",            pa.int32()),
                pa.field("file_path",             pa.string()),
                pa.field("raf_compliant",         pa.bool_()),
                pa.field("quality_score",         pa.float32()),
                pa.field("compliance_risk_level", pa.string()),
                pa.field("generated_at",          pa.string()),
                pa.field("model_used",            pa.string()),
                pa.field("generation_time_secs",  pa.float32()),
                pa.field("vector",                pa.list_(pa.float32(), vec_dim)),
            ])
            db.create_table(ARTICLES_TABLE, data=[row], schema=schema)

        return True

    except Exception as e:
        _log_error("content_store.write_article_record", str(e),
                   {"keyword": record.get("keyword", "?")})
        return False


# ─── Read helpers (for dashboard) ────────────────────────────────────────────
def read_angle_candidates(limit: int = 1000) -> list:
    """Read recent angle candidates from LanceDB. Returns [] on failure."""
    db = _get_db()
    if db is None:
        return []
    try:
        tbl = db.open_table(ANGLE_TABLE)
        return tbl.to_pandas().tail(limit).to_dict(orient="records")
    except Exception:
        return []


def read_article_records(limit: int = 500) -> list:
    """Read recent article records from LanceDB. Returns [] on failure."""
    db = _get_db()
    if db is None:
        return []
    try:
        tbl = db.open_table(ARTICLES_TABLE)
        return tbl.to_pandas().tail(limit).to_dict(orient="records")
    except Exception:
        return []
