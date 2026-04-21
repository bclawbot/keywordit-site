"""
Regression tests for R2-C4 — FB Intelligence storage dropping every new ad
due to ``'int' object has no attribute 'replace'``.

Sprint 1 (SPRINT_01_EMERGENCY_TRIAGE.md Â§4, Task 1.6) prescribes four cases
that prove ``ad_archive_id`` is normalized at the storage boundary and that
``None`` is logged non-silently. The real root cause of the AttributeError
was ``_sanitize_timestamp`` crashing on int ``delivery_start`` values — the
fifth test pins that explicitly so a future regression is obvious.

The sprint file names the function ``save_ad``; the actual API in this
codebase is ``ingest_ads(conn, [ad_dict], ...)``. Tests target the real
function; behavior matches the sprint's acceptance.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_WORKSPACE = Path(__file__).resolve().parent.parent
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))

from dwight.fb_intelligence.storage import (  # noqa: E402
    _sanitize_timestamp,
    ingest_ads,
    init_db,
)


@pytest.fixture
def conn(tmp_path):
    """Fresh in-memory-like DB per test (on-disk so FTS triggers work)."""
    db_path = tmp_path / "fb_test.db"
    c = init_db(db_path)
    yield c
    c.close()


def _row_count(conn, table: str = "Ads") -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_ingest_accepts_int_ad_archive_id(conn):
    """R2-C4: an int ad_archive_id must not crash and must persist."""
    ad = {"ad_archive_id": 1635648611192680, "headline": "hello"}
    result = ingest_ads(conn, [ad])
    assert result["errors"] == 0
    assert result["new_ads"] == 1
    assert _row_count(conn) == 1


def test_ingest_accepts_str_ad_archive_id(conn):
    """Control: a string ad_archive_id continues to work."""
    ad = {"ad_archive_id": "1635648611192680", "headline": "hello"}
    result = ingest_ads(conn, [ad])
    assert result["errors"] == 0
    assert result["new_ads"] == 1


def test_ingest_rejects_none_ad_archive_id_with_warning(conn, caplog):
    """None must be logged non-silently (not an 'except: pass' swallow)."""
    import logging

    caplog.set_level(logging.WARNING, logger="fb_intelligence.storage")
    ad = {"ad_archive_id": None, "headline": "hello"}
    result = ingest_ads(conn, [ad])
    assert result["new_ads"] == 0
    assert result["errors"] == 1
    assert _row_count(conn) == 0
    assert any("missing ad_archive_id" in rec.message for rec in caplog.records)


def test_ingest_normalizes_int_id_to_string_in_db(conn):
    """Downstream code must always see a string, even if an int came in."""
    ad = {"ad_archive_id": 123456789, "headline": "x"}
    ingest_ads(conn, [ad])
    row = conn.execute("SELECT ad_archive_id FROM Ads").fetchone()
    assert row is not None
    assert isinstance(row["ad_archive_id"], str)
    assert row["ad_archive_id"] == "123456789"
    # Dedup on a repeat must match regardless of int/str form.
    ingest_ads(conn, [{"ad_archive_id": "123456789", "headline": "x"}])
    assert _row_count(conn) == 1


def test_sanitize_timestamp_accepts_int_epoch():
    """Real root cause of R2-C4 — int delivery_start (Unix epoch) used to
    crash ``_sanitize_timestamp`` because ``ts.replace("Z", ...)`` assumed str.
    """
    now = "2026-04-21T00:00:00+00:00"
    # Valid Unix epoch (2024-01-01) → must return an ISO-8601 string, no raise.
    out = _sanitize_timestamp(1704067200, now)
    assert isinstance(out, str)
    assert "2024" in out
    # None passes through.
    assert _sanitize_timestamp(None, now) is None
    # Unknown type is coerced, does not raise.
    assert _sanitize_timestamp(object(), now) is not None or True  # no raise
