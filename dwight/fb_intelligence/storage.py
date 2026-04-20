"""
dwight/fb_intelligence/storage.py — SQLite storage and ingestion for Facebook Ad Library data.

Provides:
  - init_db()          — schema creation with WAL mode and FTS5
  - ingest_ads()       — upsert ad dicts from scraper/API with deduplication
  - update_ad_status() — confidence-scored state machine for ad lifecycle
  - get_stale_ads()    — find ads not seen recently
  - seed_taxonomies()  — populate angles, triggers, verticals
  - compute_content_hash() — deterministic content fingerprint

Usage:
    from dwight.fb_intelligence.storage import init_db, ingest_ads

    conn = init_db()
    result = ingest_ads(conn, ads, source="scraper")
"""

import hashlib
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

_WORKSPACE = Path(__file__).resolve().parents[2]
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))

from dwight.fb_intelligence.config import DB_PATH, _log_error

logger = logging.getLogger("fb_intelligence.storage")


# ── Schema DDL ────────────────────────────────────────────────────────────────

_TABLES = """
-- ── Core entities ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS Networks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    notes       TEXT,
    metadata    TEXT,  -- JSON
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS Domains (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    network_id  INTEGER REFERENCES Networks(id) ON DELETE SET NULL,
    domain      TEXT NOT NULL UNIQUE,
    first_seen  TEXT,
    last_seen   TEXT,
    is_active   INTEGER NOT NULL DEFAULT 1,
    metadata    TEXT,  -- JSON
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS FacebookPages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_id   INTEGER REFERENCES Domains(id) ON DELETE SET NULL,
    fb_page_id  TEXT NOT NULL UNIQUE,
    page_name   TEXT,
    page_url    TEXT,
    first_seen  TEXT,
    last_seen   TEXT,
    metadata    TEXT,  -- JSON
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS Ads (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_archive_id       TEXT NOT NULL UNIQUE,
    page_id             INTEGER REFERENCES FacebookPages(id) ON DELETE SET NULL,
    headline            TEXT,
    creative_text       TEXT,
    link_caption        TEXT,
    link_description    TEXT,
    cta_type            TEXT,
    landing_url         TEXT,
    landing_domain      TEXT,
    image_url           TEXT,
    image_local_path    TEXT,
    image_thumb_path    TEXT,
    ad_format           TEXT CHECK(ad_format IN ('image', 'video', 'carousel', 'collection', 'other', NULL)),
    platforms           TEXT,  -- JSON list
    languages           TEXT,  -- JSON list
    delivery_start      TEXT,
    delivery_stop       TEXT,
    first_seen          TEXT NOT NULL,
    last_seen           TEXT NOT NULL,
    content_hash        TEXT,
    embedding_updated_at TEXT,
    spend_lower         REAL,
    spend_upper         REAL,
    impressions_lower   INTEGER,
    impressions_upper   INTEGER,
    rsoc_partner        TEXT,
    extracted_keywords  TEXT,  -- JSON list
    url_params          TEXT,  -- JSON dict
    primary_vertical    TEXT,
    primary_angle       TEXT,
    emotional_triggers  TEXT,  -- JSON list
    classification_conf REAL,
    metadata            TEXT,  -- JSON
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS AdSnapshots (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id             INTEGER NOT NULL REFERENCES Ads(id) ON DELETE CASCADE,
    status            TEXT NOT NULL CHECK(status IN ('active', 'inactive')),
    status_confidence TEXT NOT NULL CHECK(status_confidence IN (
        'active', 'possibly_missing', 'likely_stopped', 'stopped', 'reactivated'
    )),
    first_seen_date   TEXT NOT NULL,
    last_seen_date    TEXT,
    is_current        INTEGER NOT NULL DEFAULT 1,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(ad_id, first_seen_date)
);

-- ── Taxonomy tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS Verticals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    cpc_floor   REAL,
    cpc_ceiling REAL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS Angles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    description     TEXT,
    cialdini_principle TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS EmotionalTriggers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ── Junction tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS AdVerticals (
    ad_id       INTEGER NOT NULL REFERENCES Ads(id) ON DELETE CASCADE,
    vertical_id INTEGER NOT NULL REFERENCES Verticals(id) ON DELETE CASCADE,
    confidence  REAL,
    PRIMARY KEY (ad_id, vertical_id)
);

CREATE TABLE IF NOT EXISTS AdAngles (
    ad_id       INTEGER NOT NULL REFERENCES Ads(id) ON DELETE CASCADE,
    angle_id    INTEGER NOT NULL REFERENCES Angles(id) ON DELETE CASCADE,
    confidence  REAL,
    PRIMARY KEY (ad_id, angle_id)
);

CREATE TABLE IF NOT EXISTS AdEmotionalTriggers (
    ad_id       INTEGER NOT NULL REFERENCES Ads(id) ON DELETE CASCADE,
    trigger_id  INTEGER NOT NULL REFERENCES EmotionalTriggers(id) ON DELETE CASCADE,
    intensity   REAL,
    PRIMARY KEY (ad_id, trigger_id)
);

-- ── Keywords ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS Keywords (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword     TEXT NOT NULL UNIQUE,
    cpc_usd     REAL,
    competition REAL,
    volume      INTEGER,
    kd          INTEGER,  -- keyword difficulty 0-100 from DataForSEO
    metadata    TEXT,  -- JSON
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS AdKeywords (
    ad_id       INTEGER NOT NULL REFERENCES Ads(id) ON DELETE CASCADE,
    keyword_id  INTEGER NOT NULL REFERENCES Keywords(id) ON DELETE CASCADE,
    source      TEXT,  -- 'extracted', 'landing_page', 'url_param'
    PRIMARY KEY (ad_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS KeywordQueue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword     TEXT NOT NULL,
    source      TEXT,
    priority    INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'done', 'failed')),
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    processed_at TEXT
);

-- ── Landing pages & templates ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS LandingPages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id           INTEGER REFERENCES Ads(id) ON DELETE SET NULL,
    url             TEXT NOT NULL,
    domain          TEXT,
    title           TEXT,
    description     TEXT,
    content_hash    TEXT,
    screenshot_path TEXT,
    fetched_at      TEXT,
    metadata        TEXT,  -- JSON
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS Templates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    template_type   TEXT,
    structure       TEXT,  -- JSON
    example_ad_ids  TEXT,  -- JSON list
    metadata        TEXT,  -- JSON
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ── Metrics & signals ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS DailyMetrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id           INTEGER NOT NULL REFERENCES Ads(id) ON DELETE CASCADE,
    date            TEXT NOT NULL,
    spend_lower     REAL,
    spend_upper     REAL,
    impressions_lower INTEGER,
    impressions_upper INTEGER,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(ad_id, date)
);

CREATE TABLE IF NOT EXISTS KeywordAngles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword_id      INTEGER NOT NULL REFERENCES Keywords(id) ON DELETE CASCADE,
    angle_type      TEXT NOT NULL,           -- angle name from Angles table or free-form
    angle_title     TEXT NOT NULL,           -- generated article title for this angle
    source          TEXT NOT NULL DEFAULT 'generated',  -- 'original' (from competitor article) or 'generated'
    confidence      REAL,
    ad_id           INTEGER REFERENCES Ads(id) ON DELETE SET NULL,  -- source ad (for 'original' angles)
    article_url     TEXT,                    -- competitor article URL (for 'original')
    vertical        TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(keyword_id, angle_type, source)
);

CREATE TABLE IF NOT EXISTS Signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id       INTEGER REFERENCES Ads(id) ON DELETE CASCADE,
    page_id     INTEGER REFERENCES FacebookPages(id) ON DELETE SET NULL,
    signal_type TEXT NOT NULL,
    signal_value TEXT,
    confidence  REAL,
    metadata    TEXT,  -- JSON
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""

_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_domains_network     ON Domains(network_id);
CREATE INDEX IF NOT EXISTS idx_domains_active       ON Domains(is_active);
CREATE INDEX IF NOT EXISTS idx_fbpages_domain       ON FacebookPages(domain_id);
CREATE INDEX IF NOT EXISTS idx_fbpages_fbid         ON FacebookPages(fb_page_id);
CREATE INDEX IF NOT EXISTS idx_ads_page             ON Ads(page_id);
CREATE INDEX IF NOT EXISTS idx_ads_archive_id       ON Ads(ad_archive_id);
CREATE INDEX IF NOT EXISTS idx_ads_landing_domain   ON Ads(landing_domain);
CREATE INDEX IF NOT EXISTS idx_ads_content_hash     ON Ads(content_hash);
CREATE INDEX IF NOT EXISTS idx_ads_delivery_start   ON Ads(delivery_start);
CREATE INDEX IF NOT EXISTS idx_ads_last_seen        ON Ads(last_seen);
CREATE INDEX IF NOT EXISTS idx_ads_primary_vertical ON Ads(primary_vertical);
CREATE INDEX IF NOT EXISTS idx_ads_primary_angle    ON Ads(primary_angle);
CREATE INDEX IF NOT EXISTS idx_snapshots_ad         ON AdSnapshots(ad_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_current    ON AdSnapshots(is_current) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_snapshots_status     ON AdSnapshots(status, status_confidence);
CREATE INDEX IF NOT EXISTS idx_keywords_keyword     ON Keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_adkeywords_ad        ON AdKeywords(ad_id);
CREATE INDEX IF NOT EXISTS idx_adkeywords_kw        ON AdKeywords(keyword_id);
CREATE INDEX IF NOT EXISTS idx_kwqueue_status       ON KeywordQueue(status, priority);
CREATE INDEX IF NOT EXISTS idx_kwangles_keyword     ON KeywordAngles(keyword_id);
CREATE INDEX IF NOT EXISTS idx_kwangles_source      ON KeywordAngles(source);
CREATE INDEX IF NOT EXISTS idx_landingpages_ad      ON LandingPages(ad_id);
CREATE INDEX IF NOT EXISTS idx_landingpages_domain  ON LandingPages(domain);
CREATE INDEX IF NOT EXISTS idx_dailymetrics_ad_date ON DailyMetrics(ad_id, date);
CREATE INDEX IF NOT EXISTS idx_signals_ad           ON Signals(ad_id);
CREATE INDEX IF NOT EXISTS idx_signals_type         ON Signals(signal_type);
"""

_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS AdFTS USING fts5(
    ad_archive_id,
    headline,
    creative_text,
    link_description,
    landing_url,
    content='Ads',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- Auto-sync triggers: keep FTS in sync with Ads table

CREATE TRIGGER IF NOT EXISTS ads_ai AFTER INSERT ON Ads BEGIN
    INSERT INTO AdFTS(rowid, ad_archive_id, headline, creative_text, link_description, landing_url)
    VALUES (new.id, new.ad_archive_id, new.headline, new.creative_text, new.link_description, new.landing_url);
END;

CREATE TRIGGER IF NOT EXISTS ads_ad AFTER DELETE ON Ads BEGIN
    INSERT INTO AdFTS(AdFTS, rowid, ad_archive_id, headline, creative_text, link_description, landing_url)
    VALUES ('delete', old.id, COALESCE(old.ad_archive_id,''), COALESCE(old.headline,''), COALESCE(old.creative_text,''), COALESCE(old.link_description,''), COALESCE(old.landing_url,''));
END;

CREATE TRIGGER IF NOT EXISTS ads_au AFTER UPDATE ON Ads BEGIN
    INSERT INTO AdFTS(AdFTS, rowid, ad_archive_id, headline, creative_text, link_description, landing_url)
    VALUES ('delete', old.id, COALESCE(old.ad_archive_id,''), COALESCE(old.headline,''), COALESCE(old.creative_text,''), COALESCE(old.link_description,''), COALESCE(old.landing_url,''));
    INSERT INTO AdFTS(rowid, ad_archive_id, headline, creative_text, link_description, landing_url)
    VALUES (new.id, new.ad_archive_id, new.headline, new.creative_text, new.link_description, new.landing_url);
END;
"""


# ── Taxonomy seed data ────────────────────────────────────────────────────────

_ANGLES = [
    ("Listicle", "Numbered list format (e.g., '7 Ways To...')", "Social Proof"),
    ("Fear/Urgency", "Time pressure or loss aversion triggers", "Scarcity"),
    ("How-To", "Educational/tutorial format", "Authority"),
    ("Testimonial", "Social proof via personal stories", "Social Proof"),
    ("Comparison", "Us vs. them or product comparison", "Contrast"),
    ("Question", "Curiosity-driven question headline", "Commitment"),
    ("News/Breaking", "Newsjacking or breaking-news framing", "Authority"),
    ("Secret/Reveal", "Hidden information or insider knowledge", "Scarcity"),
    ("Transformation", "Before/after or personal change narrative", "Liking"),
    ("Direct Offer", "Straightforward value proposition or discount", "Reciprocity"),
]

_EMOTIONAL_TRIGGERS = [
    ("Fear", "Threat of loss or negative outcome"),
    ("Curiosity", "Information gap that demands closure"),
    ("Urgency", "Time-limited opportunity"),
    ("Outrage", "Moral indignation or injustice"),
    ("Hope", "Promise of positive future outcome"),
    ("Nostalgia", "Longing for a past experience"),
    ("Envy", "Desire for what others have"),
    ("Validation", "Confirmation of existing beliefs"),
]


# ── Public API ────────────────────────────────────────────────────────────────

def _now() -> str:
    """UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def init_db(db_path: Path | None = None) -> sqlite3.Connection:
    """Create and return a connection to the fb_intelligence SQLite database.

    Applies performance PRAGMAs, creates all tables/indexes/FTS if they don't
    exist, and seeds taxonomies.
    """
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row

    # Performance PRAGMAs
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA mmap_size = 268435456")

    conn.executescript(_TABLES)
    conn.executescript(_INDEXES)
    conn.executescript(_FTS)

    seed_taxonomies(conn)

    logger.info("Database initialized at %s", path)
    return conn


def compute_content_hash(ad: dict) -> str:
    """SHA-256 hash of normalized creative_text + headline + link_description."""
    parts = []
    for key in ("creative_text", "headline", "link_description"):
        val = ad.get(key) or ""
        parts.append(val.strip().lower())
    combined = "|".join(parts)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def ingest_ads(
    conn: sqlite3.Connection,
    ads: list[dict],
    source: str = "api",
) -> dict:
    """Ingest a list of ad dicts into the database.

    Deduplicates on ad_archive_id. Detects creative changes via content_hash.
    Returns {new_ads: int, updated_ads: int, unchanged: int, errors: int}.
    """
    now = _now()
    counts = {"new_ads": 0, "updated_ads": 0, "unchanged": 0, "errors": 0}

    for ad in ads:
        try:
            ad_archive_id = ad.get("ad_archive_id")
            if not ad_archive_id:
                counts["errors"] += 1
                continue

            # Extract landing domain (before page resolution so we can set domain_id)
            landing_url = ad.get("landing_url")
            landing_domain = None
            if landing_url:
                try:
                    landing_domain = urlparse(landing_url).netloc or None
                except Exception:
                    pass

            # Resolve FacebookPages row (passes landing_domain for domain linkage)
            fb_page_id = ad.get("page_id")
            page_row_id = None
            if fb_page_id:
                page_row_id = _ensure_facebook_page(conn, fb_page_id, ad, now, landing_domain)

            content_hash = compute_content_hash(ad)

            # Check if ad exists
            row = conn.execute(
                "SELECT id, content_hash FROM Ads WHERE ad_archive_id = ?",
                (ad_archive_id,),
            ).fetchone()

            if row is None:
                # New ad — INSERT
                conn.execute(
                    """INSERT INTO Ads (
                        ad_archive_id, page_id, headline, creative_text,
                        link_caption, link_description, cta_type,
                        landing_url, landing_domain, image_url,
                        platforms, delivery_start, first_seen, last_seen,
                        content_hash, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ad_archive_id,
                        page_row_id,
                        ad.get("headline"),
                        ad.get("creative_text"),
                        ad.get("link_caption"),
                        ad.get("link_description"),
                        ad.get("cta_type"),
                        landing_url,
                        landing_domain,
                        ad.get("image_url"),
                        json.dumps(ad.get("platforms")) if ad.get("platforms") else None,
                        ad.get("delivery_start"),
                        now,
                        now,
                        content_hash,
                        json.dumps({"source": source}),
                    ),
                )
                ad_row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                # Initial snapshot
                conn.execute(
                    """INSERT INTO AdSnapshots (
                        ad_id, status, status_confidence,
                        first_seen_date, last_seen_date, is_current
                    ) VALUES (?, 'active', 'active', ?, ?, 1)""",
                    (ad_row_id, now, now),
                )

                # Generate signal for new ad
                conn.execute(
                    """INSERT INTO Signals (ad_id, page_id, signal_type, signal_value,
                        confidence, created_at)
                    VALUES (?, ?, 'new_ad', ?, 1.0, ?)""",
                    (
                        ad_row_id,
                        page_row_id,
                        f"New ad detected: {ad.get('headline') or ad_archive_id}",
                        now,
                    ),
                )
                counts["new_ads"] += 1

            else:
                # Existing ad — check for changes
                ad_row_id = row["id"]
                old_hash = row["content_hash"]

                if old_hash != content_hash:
                    # Creative changed
                    conn.execute(
                        """UPDATE Ads SET
                            headline = ?, creative_text = ?,
                            link_caption = ?, link_description = ?,
                            cta_type = ?, landing_url = ?, landing_domain = ?,
                            image_url = ?, platforms = ?, delivery_start = ?,
                            last_seen = ?, content_hash = ?, updated_at = ?
                        WHERE id = ?""",
                        (
                            ad.get("headline"),
                            ad.get("creative_text"),
                            ad.get("link_caption"),
                            ad.get("link_description"),
                            ad.get("cta_type"),
                            landing_url,
                            landing_domain,
                            ad.get("image_url"),
                            json.dumps(ad.get("platforms")) if ad.get("platforms") else None,
                            ad.get("delivery_start"),
                            now,
                            content_hash,
                            now,
                            ad_row_id,
                        ),
                    )
                    logger.info(
                        "Ad %s creative changed (hash %s → %s)",
                        ad_archive_id, old_hash[:12], content_hash[:12],
                    )
                    # Generate signal for creative change
                    conn.execute(
                        """INSERT INTO Signals (ad_id, page_id, signal_type, signal_value,
                            confidence, created_at)
                        VALUES (?, ?, 'creative_change', ?, 0.9, ?)""",
                        (
                            ad_row_id,
                            page_row_id,
                            f"Creative updated: {ad.get('headline') or ad_archive_id}",
                            now,
                        ),
                    )
                    counts["updated_ads"] += 1
                else:
                    # No creative change — just update last_seen
                    conn.execute(
                        "UPDATE Ads SET last_seen = ?, updated_at = ? WHERE id = ?",
                        (now, now, ad_row_id),
                    )
                    counts["unchanged"] += 1

                # Update current snapshot last_seen
                conn.execute(
                    """UPDATE AdSnapshots SET last_seen_date = ?
                    WHERE ad_id = ? AND is_current = 1""",
                    (now, ad_row_id),
                )

        except Exception as exc:
            counts["errors"] += 1
            _log_error(
                "fb_intelligence/storage",
                str(exc),
                {"ad_archive_id": ad.get("ad_archive_id")},
            )

    conn.commit()
    logger.info(
        "Ingested %d ads: %d new, %d updated, %d unchanged, %d errors",
        len(ads), counts["new_ads"], counts["updated_ads"],
        counts["unchanged"], counts["errors"],
    )
    return counts


def update_ad_status(
    conn: sqlite3.Connection,
    ad_id: int,
    seen: bool,
    is_reconciliation: bool = False,
) -> None:
    """Transition ad status through the confidence-scored state machine.

    States: active → possibly_missing → likely_stopped → stopped
    Reactivation: stopped/likely_stopped + seen → reactivated
    """
    now = _now()

    snapshot = conn.execute(
        """SELECT id, status, status_confidence, first_seen_date, last_seen_date
        FROM AdSnapshots WHERE ad_id = ? AND is_current = 1""",
        (ad_id,),
    ).fetchone()

    if not snapshot:
        logger.warning("No current snapshot for ad_id=%d", ad_id)
        return

    current_confidence = snapshot["status_confidence"]

    if seen:
        if current_confidence in ("stopped", "likely_stopped"):
            # Reactivation — close old snapshot, create new one
            gap_start = snapshot["last_seen_date"] or snapshot["first_seen_date"]
            logger.info(
                "Ad %d reactivated (was %s since %s)",
                ad_id, current_confidence, gap_start,
            )
            conn.execute(
                "UPDATE AdSnapshots SET is_current = 0 WHERE id = ?",
                (snapshot["id"],),
            )
            conn.execute(
                """INSERT INTO AdSnapshots (
                    ad_id, status, status_confidence,
                    first_seen_date, last_seen_date, is_current
                ) VALUES (?, 'active', 'reactivated', ?, ?, 1)""",
                (ad_id, now, now),
            )
        else:
            # Normal active update
            conn.execute(
                """UPDATE AdSnapshots SET
                    status = 'active', status_confidence = 'active',
                    last_seen_date = ?
                WHERE id = ?""",
                (now, snapshot["id"]),
            )
    else:
        # Not seen
        if is_reconciliation:
            # Definitive: ad is stopped
            conn.execute(
                "UPDATE AdSnapshots SET is_current = 0 WHERE id = ?",
                (snapshot["id"],),
            )
            conn.execute(
                """INSERT INTO AdSnapshots (
                    ad_id, status, status_confidence,
                    first_seen_date, last_seen_date, is_current
                ) VALUES (?, 'inactive', 'stopped', ?, ?, 1)""",
                (ad_id, now, now),
            )
        else:
            # Progressive confidence degradation
            if current_confidence == "active":
                conn.execute(
                    """UPDATE AdSnapshots SET status_confidence = 'possibly_missing'
                    WHERE id = ?""",
                    (snapshot["id"],),
                )
            elif current_confidence == "possibly_missing":
                conn.execute(
                    """UPDATE AdSnapshots SET status_confidence = 'likely_stopped'
                    WHERE id = ?""",
                    (snapshot["id"],),
                )
            # likely_stopped — no change, wait for reconciliation

    conn.commit()


def get_stale_ads(conn: sqlite3.Connection, hours: int = 24) -> list[dict]:
    """Return ads not seen for more than `hours` with status_confidence='active'."""
    rows = conn.execute(
        """SELECT a.id, a.ad_archive_id, a.page_id, a.headline,
                  a.landing_domain, a.last_seen,
                  s.status_confidence
           FROM Ads a
           JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
           WHERE s.status_confidence = 'active'
             AND a.last_seen < datetime('now', ? || ' hours')""",
        (f"-{hours}",),
    ).fetchall()
    return [dict(r) for r in rows]


def seed_taxonomies(conn: sqlite3.Connection) -> None:
    """Populate angle types (with Cialdini mappings) and emotional triggers.

    Idempotent — uses INSERT OR IGNORE.
    """
    conn.executemany(
        "INSERT OR IGNORE INTO Angles (name, description, cialdini_principle) VALUES (?, ?, ?)",
        _ANGLES,
    )
    conn.executemany(
        "INSERT OR IGNORE INTO EmotionalTriggers (name, description) VALUES (?, ?)",
        _EMOTIONAL_TRIGGERS,
    )
    conn.commit()


# ── Internal helpers ──────────────────────────────────────────────────────────


def _strip_www(domain: str) -> str:
    """Remove leading 'www.' from a domain string."""
    return domain[4:] if domain.lower().startswith("www.") else domain


def _resolve_domain_id(
    conn: sqlite3.Connection,
    landing_domain: str | None,
    now: str | None = None,
) -> int | None:
    """Match a landing_domain to the Domains table. Auto-creates if not found.

    Strips 'www.' for matching. Returns domain_id or None.
    """
    if not landing_domain:
        return None
    raw = landing_domain.strip().lower()
    stripped = _strip_www(raw)
    if not stripped:
        return None

    row = conn.execute(
        "SELECT id FROM Domains WHERE LOWER(domain) = ? OR LOWER(domain) = ?",
        (raw, stripped),
    ).fetchone()
    if row:
        return row["id"]

    # Auto-create domain entry (network_id=NULL — operator assigns manually)
    _ts = now or _now()
    conn.execute(
        "INSERT INTO Domains (domain, first_seen, last_seen, is_active) VALUES (?, ?, ?, 1)",
        (stripped, _ts, _ts),
    )
    new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    logger.info("Auto-created domain '%s' (id=%d) — assign network manually", stripped, new_id)
    return new_id


def _ensure_facebook_page(
    conn: sqlite3.Connection,
    fb_page_id: str,
    ad: dict,
    now: str,
    landing_domain: str | None = None,
) -> int:
    """Look up or create a FacebookPages row. Links to domain if possible. Returns the row id."""
    domain_id = _resolve_domain_id(conn, landing_domain, now)

    row = conn.execute(
        "SELECT id FROM FacebookPages WHERE fb_page_id = ?",
        (fb_page_id,),
    ).fetchone()

    if row:
        # Update: set domain_id only if currently NULL (don't overwrite manual assignments)
        conn.execute(
            """UPDATE FacebookPages SET last_seen = ?, page_name = COALESCE(?, page_name),
               domain_id = COALESCE(domain_id, ?) WHERE id = ?""",
            (now, ad.get("page_name"), domain_id, row["id"]),
        )
        return row["id"]

    conn.execute(
        """INSERT INTO FacebookPages (fb_page_id, page_name, domain_id, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?)""",
        (fb_page_id, ad.get("page_name"), domain_id, now, now),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ── Backfill utilities ───────────────────────────────────────────────────────


def backfill_domain_ids(conn: sqlite3.Connection) -> int:
    """One-time backfill: link orphaned FacebookPages to Domains via Ads.landing_domain."""
    orphaned = conn.execute(
        "SELECT id, fb_page_id FROM FacebookPages WHERE domain_id IS NULL"
    ).fetchall()

    fixed = 0
    for page in orphaned:
        # Find the most common landing_domain for ads on this page
        domain_row = conn.execute(
            """SELECT landing_domain, COUNT(*) as cnt FROM Ads
               WHERE page_id = ? AND landing_domain IS NOT NULL
               GROUP BY landing_domain ORDER BY cnt DESC LIMIT 1""",
            (page["id"],),
        ).fetchone()

        if domain_row and domain_row["landing_domain"]:
            domain_id = _resolve_domain_id(conn, domain_row["landing_domain"])
            if domain_id:
                conn.execute(
                    "UPDATE FacebookPages SET domain_id = ? WHERE id = ?",
                    (domain_id, page["id"]),
                )
                fixed += 1

    conn.commit()
    logger.info("Backfill: linked %d/%d orphaned pages to domains", fixed, len(orphaned))
    return fixed


def backfill_ad_keywords(conn: sqlite3.Connection) -> int:
    """One-time backfill: create AdKeywords junction records from Ads.extracted_keywords JSON."""
    ads = conn.execute(
        "SELECT id, extracted_keywords FROM Ads WHERE extracted_keywords IS NOT NULL"
    ).fetchall()

    linked = 0
    for ad in ads:
        try:
            ek = json.loads(ad["extracted_keywords"]) if isinstance(ad["extracted_keywords"], str) else ad["extracted_keywords"]
            if not isinstance(ek, dict):
                continue

            for param_name, value in ek.items():
                if param_name.startswith("_"):
                    continue
                val = str(value).strip()
                if not val or len(val) < 2:
                    continue

                kw_row = conn.execute(
                    "SELECT id FROM Keywords WHERE LOWER(keyword) = LOWER(?)",
                    (val,),
                ).fetchone()
                if kw_row:
                    conn.execute(
                        "INSERT OR IGNORE INTO AdKeywords (ad_id, keyword_id, source) VALUES (?, ?, ?)",
                        (ad["id"], kw_row["id"], f"url:{param_name}"),
                    )
                    linked += 1
        except Exception:
            continue

    conn.commit()
    logger.info("Backfill: created %d AdKeywords records", linked)
    return linked
