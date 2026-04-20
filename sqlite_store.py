"""
sqlite_store.py — SQLite-backed storage for pipeline opportunities and angle candidates.

Replaces monolithic JSON files (angle_candidates.json at 17MB, validated_opportunities.json, etc.)
with indexed SQLite tables. Provides the same read/write API to minimize migration friction.

Usage:
    from sqlite_store import OpportunityStore

    store = OpportunityStore()  # uses default DB path

    # Write (replaces json.dumps + file.write_text)
    store.upsert_opportunities(opportunities, stage="validated")

    # Read (replaces json.loads + file.read_text)
    all_validated = store.get_opportunities(stage="validated")
    golden_only = store.get_opportunities(stage="validated", tag="GOLDEN_OPPORTUNITY")

    # Angle candidates
    store.upsert_angles(angle_list)
    angles = store.get_angles(keyword="best vpn", country="US")
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path


_DEFAULT_DB = Path(__file__).resolve().parent / "pipeline_store.db"


class OpportunityStore:
    """SQLite-backed store for pipeline opportunities and angle candidates."""

    def __init__(self, db_path: Path | str | None = None):
        self.db_path = str(db_path or _DEFAULT_DB)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        return con

    def _init_db(self):
        con = self._conn()
        con.executescript("""
            CREATE TABLE IF NOT EXISTS opportunities (
                keyword       TEXT NOT NULL,
                country       TEXT NOT NULL DEFAULT 'US',
                stage         TEXT NOT NULL,
                cpc_usd       REAL,
                search_volume INTEGER,
                competition   REAL,
                arbitrage_index REAL,
                rsoc_score    REAL,
                rpc_expected  REAL,
                tag           TEXT,
                vertical      TEXT,
                source_trend  TEXT,
                metadata      TEXT DEFAULT '{}',
                created_at    TEXT DEFAULT (datetime('now')),
                updated_at    TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (keyword, country, stage)
            );
            CREATE INDEX IF NOT EXISTS idx_opp_stage ON opportunities(stage);
            CREATE INDEX IF NOT EXISTS idx_opp_tag ON opportunities(tag);
            CREATE INDEX IF NOT EXISTS idx_opp_score ON opportunities(rsoc_score);
            CREATE INDEX IF NOT EXISTS idx_opp_country ON opportunities(country);
            CREATE INDEX IF NOT EXISTS idx_opp_created ON opportunities(created_at);

            CREATE TABLE IF NOT EXISTS angle_candidates (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword       TEXT NOT NULL,
                country       TEXT NOT NULL DEFAULT 'US',
                angle_type    TEXT,
                title         TEXT,
                rsoc_score    REAL,
                ad_category   TEXT,
                vertical      TEXT,
                source_trend  TEXT,
                metadata      TEXT DEFAULT '{}',
                created_at    TEXT DEFAULT (datetime('now')),
                UNIQUE(keyword, country, angle_type, title)
            );
            CREATE INDEX IF NOT EXISTS idx_angle_keyword ON angle_candidates(keyword, country);
            CREATE INDEX IF NOT EXISTS idx_angle_score ON angle_candidates(rsoc_score);
            CREATE INDEX IF NOT EXISTS idx_angle_type ON angle_candidates(angle_type);
        """)
        con.commit()
        con.close()

    # ── Opportunities ────────────────────────────────────────────────────────

    def upsert_opportunities(self, opportunities: list[dict], stage: str):
        """Insert or update a batch of opportunities for a given stage."""
        con = self._conn()
        now = datetime.now().isoformat()
        # Separate known columns from metadata overflow
        known_cols = {
            "keyword", "country", "cpc_usd", "search_volume", "competition",
            "arbitrage_index", "rsoc_score", "rpc_expected", "tag", "vertical",
            "source_trend",
        }
        rows = []
        for opp in opportunities:
            metadata = {k: v for k, v in opp.items() if k not in known_cols}
            rows.append((
                opp.get("keyword", ""),
                opp.get("country", "US"),
                stage,
                opp.get("cpc_usd"),
                opp.get("search_volume"),
                opp.get("competition"),
                opp.get("arbitrage_index"),
                opp.get("rsoc_score"),
                opp.get("rpc_expected"),
                opp.get("tag"),
                opp.get("vertical"),
                opp.get("source_trend"),
                json.dumps(metadata, default=str),
                now,
            ))
        con.executemany("""
            INSERT INTO opportunities
                (keyword, country, stage, cpc_usd, search_volume, competition,
                 arbitrage_index, rsoc_score, rpc_expected, tag, vertical,
                 source_trend, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(keyword, country, stage) DO UPDATE SET
                cpc_usd=excluded.cpc_usd,
                search_volume=excluded.search_volume,
                competition=excluded.competition,
                arbitrage_index=excluded.arbitrage_index,
                rsoc_score=excluded.rsoc_score,
                rpc_expected=excluded.rpc_expected,
                tag=excluded.tag,
                vertical=excluded.vertical,
                source_trend=excluded.source_trend,
                metadata=excluded.metadata,
                updated_at=excluded.updated_at
        """, rows)
        con.commit()
        con.close()

    def get_opportunities(
        self,
        stage: str | None = None,
        tag: str | None = None,
        country: str | None = None,
        min_score: float | None = None,
        limit: int = 0,
    ) -> list[dict]:
        """Query opportunities with optional filters. Returns list of dicts."""
        con = self._conn()
        where = []
        params = []
        if stage:
            where.append("stage = ?")
            params.append(stage)
        if tag:
            where.append("tag = ?")
            params.append(tag)
        if country:
            where.append("country = ?")
            params.append(country)
        if min_score is not None:
            where.append("rsoc_score >= ?")
            params.append(min_score)

        sql = "SELECT * FROM opportunities"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY rsoc_score DESC"
        if limit:
            sql += f" LIMIT {limit}"

        rows = con.execute(sql, params).fetchall()
        con.close()

        results = []
        for row in rows:
            d = dict(row)
            # Merge metadata back into the dict for backward compatibility
            meta = json.loads(d.pop("metadata", "{}"))
            d.update(meta)
            results.append(d)
        return results

    def count_opportunities(self, stage: str | None = None, tag: str | None = None) -> int:
        con = self._conn()
        where = []
        params = []
        if stage:
            where.append("stage = ?")
            params.append(stage)
        if tag:
            where.append("tag = ?")
            params.append(tag)
        sql = "SELECT COUNT(*) FROM opportunities"
        if where:
            sql += " WHERE " + " AND ".join(where)
        count = con.execute(sql, params).fetchone()[0]
        con.close()
        return count

    # ── Angle candidates ─────────────────────────────────────────────────────

    def upsert_angles(self, angles: list[dict]):
        """Insert angle candidates (from angle_engine.py output)."""
        con = self._conn()
        rows = []
        for entry in angles:
            keyword = entry.get("keyword", "")
            country = entry.get("country", "US")
            vertical = entry.get("vertical")
            source_trend = entry.get("source_trend")

            for angle in entry.get("angles", []):
                for title in angle.get("titles", [""]):
                    metadata = {k: v for k, v in angle.items()
                                if k not in {"angle_type", "rsoc_score", "primary_ad_category", "titles"}}
                    rows.append((
                        keyword, country,
                        angle.get("angle_type"),
                        title,
                        angle.get("rsoc_score"),
                        angle.get("primary_ad_category"),
                        vertical,
                        source_trend,
                        json.dumps(metadata, default=str),
                    ))
        con.executemany("""
            INSERT OR IGNORE INTO angle_candidates
                (keyword, country, angle_type, title, rsoc_score,
                 ad_category, vertical, source_trend, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        con.commit()
        con.close()

    def get_angles(
        self,
        keyword: str | None = None,
        country: str | None = None,
        angle_type: str | None = None,
        min_score: float | None = None,
        limit: int = 0,
    ) -> list[dict]:
        con = self._conn()
        where = []
        params = []
        if keyword:
            where.append("keyword = ?")
            params.append(keyword)
        if country:
            where.append("country = ?")
            params.append(country)
        if angle_type:
            where.append("angle_type = ?")
            params.append(angle_type)
        if min_score is not None:
            where.append("rsoc_score >= ?")
            params.append(min_score)

        sql = "SELECT * FROM angle_candidates"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY rsoc_score DESC"
        if limit:
            sql += f" LIMIT {limit}"

        rows = con.execute(sql, params).fetchall()
        con.close()
        return [dict(row) for row in rows]

    def count_angles(self) -> int:
        con = self._conn()
        count = con.execute("SELECT COUNT(*) FROM angle_candidates").fetchone()[0]
        con.close()
        return count

    # ── Migration helpers ────────────────────────────────────────────────────

    def import_from_json(self, json_path: Path | str, stage: str) -> int:
        """Import opportunities from a JSON file. Returns count imported."""
        path = Path(json_path)
        if not path.exists():
            return 0
        data = json.loads(path.read_text())
        if not isinstance(data, list):
            return 0
        self.upsert_opportunities(data, stage=stage)
        return len(data)

    def import_angles_from_json(self, json_path: Path | str) -> int:
        """Import angle candidates from angle_candidates.json. Returns count imported."""
        path = Path(json_path)
        if not path.exists():
            return 0
        data = json.loads(path.read_text())
        if not isinstance(data, list):
            return 0
        self.upsert_angles(data)
        return len(data)

    def export_to_json(self, stage: str, output_path: Path | str, **filters) -> int:
        """Export opportunities to a JSON file for backward compatibility."""
        data = self.get_opportunities(stage=stage, **filters)
        Path(output_path).write_text(json.dumps(data, indent=2, default=str))
        return len(data)
