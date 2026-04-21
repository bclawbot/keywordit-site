"""Regression tests for lib/alerts.py + lib/freshness_monitors.py (Sprint 3 — R2-C7)."""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def fresh_alerts(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENCLAW_LOG_DIR", str(tmp_path))
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_ALERT_CHAT_ID", raising=False)
    sys.modules.pop("lib.alerts", None)
    sys.modules.pop("lib", None)
    from lib import alerts  # noqa: E402
    importlib.reload(alerts)
    return alerts, tmp_path


def test_alert_appends_local_even_without_telegram(fresh_alerts):
    alerts_mod, log_dir = fresh_alerts
    delivered = alerts_mod.alert("info", "test-code-1", "hi there")
    assert delivered is False  # no telegram creds configured
    lines = (log_dir / "alerts.jsonl").read_text().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["code"] == "test-code-1"
    assert rec["delivered"] is False
    assert rec["rate_limited"] is False


def test_alert_rate_limits_within_window(fresh_alerts, monkeypatch):
    alerts_mod, log_dir = fresh_alerts
    # Force the rate-file to show a very recent fire for this code.
    from datetime import datetime, timezone
    rate_file = log_dir / ".alerts_last_fired.json"
    rate_file.write_text(json.dumps(
        {"test-code-2": datetime.now(timezone.utc).isoformat()}
    ))
    alerts_mod.alert("warn", "test-code-2", "second call")
    rec = json.loads((log_dir / "alerts.jsonl").read_text().splitlines()[-1])
    assert rec["rate_limited"] is True
    assert rec["delivered"] is False


def test_alert_not_rate_limited_when_window_expired(fresh_alerts, monkeypatch):
    alerts_mod, log_dir = fresh_alerts
    from datetime import datetime, timezone, timedelta
    # Fire an hour ago with a 10-minute window → should be considered fresh.
    rate_file = log_dir / ".alerts_last_fired.json"
    stale = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    rate_file.write_text(json.dumps({"test-code-3": stale}))
    alerts_mod.alert("info", "test-code-3", "third", dedupe_window_minutes=10)
    rec = json.loads((log_dir / "alerts.jsonl").read_text().splitlines()[-1])
    assert rec["rate_limited"] is False


@pytest.fixture
def fresh_monitors(tmp_path, monkeypatch):
    # Reset alerts module so monitors pick up the test log dir.
    monkeypatch.setenv("OPENCLAW_LOG_DIR", str(tmp_path))
    sys.modules.pop("lib.alerts", None)
    sys.modules.pop("lib.freshness_monitors", None)
    sys.modules.pop("lib", None)
    from lib import freshness_monitors as fm
    importlib.reload(fm)
    return fm, tmp_path


def test_fb_intel_freshness_alerts_when_stale(fresh_monitors, monkeypatch):
    fm, log_dir = fresh_monitors
    log = log_dir / "fb.log"
    log.write_text(
        "blah blah\n"
        "2026-04-21 21:00 | INFO  | freshness_monitor\n"
        '      "Freshness: last data is 200h old"\n',
        encoding="utf-8",
    )
    fired = fm.check_fb_intel_freshness(log_path=log, max_age_hours=6.0)
    assert fired is True
    rec = json.loads((log_dir / "alerts.jsonl").read_text().splitlines()[-1])
    assert rec["code"] == "fb-intel-stale"


def test_fb_intel_freshness_silent_when_fresh(fresh_monitors):
    fm, log_dir = fresh_monitors
    log = log_dir / "fb.log"
    log.write_text(
        '      "Freshness: last data is 2h old"\n', encoding="utf-8",
    )
    fired = fm.check_fb_intel_freshness(log_path=log, max_age_hours=6.0)
    assert fired is False
    assert not (log_dir / "alerts.jsonl").exists()


def test_artifact_freshness_missing_file(fresh_monitors):
    fm, log_dir = fresh_monitors
    fired = fm.check_artifact_freshness(
        log_dir / "nonexistent.json",
        code="test-missing", label="nonexistent", max_age_hours=12,
    )
    assert fired is True
    rec = json.loads((log_dir / "alerts.jsonl").read_text().splitlines()[-1])
    assert rec["code"] == "test-missing"


def test_artifact_freshness_old_file(fresh_monitors, tmp_path):
    fm, log_dir = fresh_monitors
    artifact = tmp_path / "old.json"
    artifact.write_text("[]")
    # Backdate the file 24h.
    past = time.time() - 24 * 3600
    os.utime(artifact, (past, past))
    fired = fm.check_artifact_freshness(
        artifact, code="test-old", label="old artifact", max_age_hours=12,
    )
    assert fired is True


def test_artifact_freshness_recent_file(fresh_monitors, tmp_path):
    fm, log_dir = fresh_monitors
    artifact = tmp_path / "fresh.json"
    artifact.write_text("[]")
    fired = fm.check_artifact_freshness(
        artifact, code="test-fresh", label="fresh artifact", max_age_hours=12,
    )
    assert fired is False
