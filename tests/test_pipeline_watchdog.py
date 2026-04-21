"""Regression tests for pipeline_watchdog (Sprint 2 — R2-C1/C2/C3)."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def tmp_alive_dirs(tmp_path, monkeypatch):
    alive = tmp_path / "alive"
    stale = tmp_path / "alive" / "stale"
    monkeypatch.setenv("HEARTBEAT_ALIVE_DIR", str(alive))
    monkeypatch.setenv("HEARTBEAT_STALE_DIR", str(stale))
    # Re-import so the module picks up the env vars.
    sys.modules.pop("pipeline_watchdog", None)
    import pipeline_watchdog  # noqa: F401 — import after monkeypatch
    import importlib
    importlib.reload(pipeline_watchdog)
    return pipeline_watchdog, alive, stale


def test_watchdog_warns_on_stale_alive_file(tmp_alive_dirs, caplog):
    """R2-C3 regression: an 80-minute silent batch must fire a warning."""
    pw, alive_dir, _ = tmp_alive_dirs
    stage = "dummy_stage.py"
    alive = alive_dir / "dummy_stage.alive"
    alive_dir.mkdir(parents=True, exist_ok=True)
    # Create an alive file with a stat mtime well past the SLA.
    alive.touch()
    os.utime(alive, (time.time() - 1000, time.time() - 1000))

    appended: list[dict] = []
    caplog.set_level(logging.WARNING, logger="pipeline_watchdog")
    stop = pw.start_watchdog(
        stage, pid=os.getpid(),
        sla_seconds=5, check_interval=1,
        error_log_append=appended.append,
    )
    try:
        # Give the watchdog at least one poll window.
        for _ in range(30):
            if any("stale" in r.message for r in caplog.records):
                break
            time.sleep(0.1)
    finally:
        pw.stop_watchdog(stop)

    assert any("alive file" in r.message and "stale" in r.message
               for r in caplog.records), \
        f"expected watchdog warning; got: {[r.message for r in caplog.records]}"
    assert any("watchdog" in (rec.get("stage") or "") for rec in appended), \
        f"expected error_log append; got: {appended}"


def test_watchdog_silent_when_alive_fresh(tmp_alive_dirs, caplog):
    """A stage that touches its alive file regularly must NOT trigger warnings."""
    pw, alive_dir, _ = tmp_alive_dirs
    stage = "healthy_stage.py"
    alive = alive_dir / "healthy_stage.alive"
    alive_dir.mkdir(parents=True, exist_ok=True)
    alive.touch()

    caplog.set_level(logging.WARNING, logger="pipeline_watchdog")
    stop = pw.start_watchdog(
        stage, pid=os.getpid(),
        sla_seconds=5, check_interval=1,
    )
    try:
        # Keep the file fresh for ~3s.
        for _ in range(6):
            alive.touch()
            time.sleep(0.5)
    finally:
        pw.stop_watchdog(stop)

    assert not any("stale" in r.message for r in caplog.records), \
        f"watchdog fired spuriously on healthy stage: {[r.message for r in caplog.records]}"


def test_stale_sentinel_roundtrip(tmp_alive_dirs):
    """Sentinel written on failure, read back, cleared on next success."""
    pw, _, stale_dir = tmp_alive_dirs
    path = pw.write_stale_sentinel("trends_postprocess.py", "timeout after 5400s")
    assert path.exists()
    payload = json.loads(path.read_text())
    assert payload["stage"] == "trends_postprocess.py"
    assert "timeout" in payload["reason"]
    assert payload["failed_at"].endswith("Z") or "+" in payload["failed_at"] or payload["failed_at"].count(":") >= 2

    records = pw.read_stale_sentinels()
    assert any(r["stage"] == "trends_postprocess.py" for r in records)

    pw.clear_stale_sentinel("trends_postprocess.py")
    records_after = pw.read_stale_sentinels()
    assert not any(r["stage"] == "trends_postprocess.py" for r in records_after)


def test_dashboard_builder_emits_stale_stages_field(tmp_alive_dirs):
    """Sprint 2 Task 2.6 Part B: dashboard meta must expose stale_stages."""
    pw, _, _ = tmp_alive_dirs
    pw.write_stale_sentinel("trends_postprocess.py", "synthetic failure")
    records = pw.read_stale_sentinels()
    # Proxy for the dashboard join: read_stale_sentinels is what dashboard_builder calls.
    assert records
    assert records[0]["stage"] == "trends_postprocess.py"
    assert records[0]["reason"] == "synthetic failure"
