"""Sprint 6 Task 6.5 — daily digest rendering tests."""

from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def isolated_workspace(tmp_path, monkeypatch):
    """Quarantined workspace + log dir + reset Sprint 3/5 modules."""
    ws = tmp_path / "ws"
    logs = tmp_path / "logs"
    ws.mkdir()
    logs.mkdir()
    monkeypatch.setenv("OPENCLAW_LOG_DIR", str(logs))
    for m in ("lib", "lib.alerts", "lib.dashboard_enrich", "lib.daily_digest"):
        sys.modules.pop(m, None)
    from lib import daily_digest
    importlib.reload(daily_digest)
    return daily_digest, ws, logs


def test_digest_renders_with_empty_workspace(isolated_workspace):
    dd, ws, logs = isolated_workspace
    text = dd.render_digest(workspace=ws, log_dir=logs,
                            now=datetime(2026, 4, 22, tzinfo=timezone.utc))
    assert "Pipeline digest — 2026-04-22" in text
    assert "trends:" in text
    assert "golden:" in text
    assert "Alerts in last 24h" in text


def test_digest_counts_tags_and_countries(isolated_workspace):
    dd, ws, logs = isolated_workspace
    (ws / "validated_opportunities.json").write_text(json.dumps([
        {"keyword": "a", "country": "US", "tag": "GOLDEN_OPPORTUNITY"},
        {"keyword": "b", "country": "US", "tag": "EMERGING"},
        {"keyword": "c", "country": "AU", "tag": "GOLDEN_OPPORTUNITY"},
    ]))
    (ws / "golden_opportunities.json").write_text(json.dumps([
        {"keyword": "a", "country": "US", "tag": "GOLDEN_OPPORTUNITY"},
        {"keyword": "c", "country": "AU", "tag": "GOLDEN_OPPORTUNITY"},
    ]))
    text = dd.render_digest(workspace=ws, log_dir=logs)
    assert "GOLDEN_OPPORTUNITY=2" in text
    assert "EMERGING=1" in text
    assert "US=1" in text and "AU=1" in text


def test_digest_reads_sync_status(isolated_workspace):
    dd, ws, logs = isolated_workspace
    (logs / ".sync_last_status.json").write_text(json.dumps({
        "status": "muted", "ts": "2026-04-21T03:44:00",
    }))
    text = dd.render_digest(workspace=ws, log_dir=logs)
    assert "railway:   muted" in text


def test_digest_new_golden_delta_from_prev_run(isolated_workspace):
    dd, ws, logs = isolated_workspace
    # Sprint-4 sidecar doubles as "prev run finish" per Sprint 5 helper.
    (ws / "golden_opportunities.json.schema.json").write_text(json.dumps({
        "schema_version": "1.0",
        "generated_at": "2026-04-20T20:00:00+00:00",
    }))
    (ws / "golden_opportunities.json").write_text(json.dumps([
        {"tag": "GOLDEN_OPPORTUNITY", "validated_at": "2026-04-21T09:00:00+00:00"},
        {"tag": "GOLDEN_OPPORTUNITY", "validated_at": "2026-04-20T10:00:00+00:00"},
        {"tag": "EMERGING", "validated_at": "2026-04-21T09:00:00+00:00"},
    ]))
    text = dd.render_digest(workspace=ws, log_dir=logs)
    assert "New GOLDEN since last run: 1" in text


def test_digest_counts_non_rate_limited_alerts(isolated_workspace):
    dd, ws, logs = isolated_workspace
    now = datetime.now(timezone.utc).isoformat()
    with (logs / "alerts.jsonl").open("w") as f:
        f.write(json.dumps({"ts": now, "rate_limited": False}) + "\n")
        f.write(json.dumps({"ts": now, "rate_limited": True}) + "\n")
        f.write(json.dumps({"ts": now, "rate_limited": False}) + "\n")
    text = dd.render_digest(workspace=ws, log_dir=logs)
    assert "Alerts in last 24h: 2" in text


def test_send_digest_rate_limits_after_one_day(isolated_workspace, monkeypatch):
    """R2-C8 rule: digest is sent at most once per 24h via the 1440-min window."""
    dd, ws, logs = isolated_workspace
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    # No Telegram creds → delivered=False for both; but the second call must
    # still append a record with rate_limited=True.
    from datetime import timezone as _tz, datetime as _dt
    rate_file = logs / ".alerts_last_fired.json"
    rate_file.write_text(json.dumps({"daily-digest": _dt.now(_tz.utc).isoformat()}))
    dd.send_digest(workspace=ws)
    last = json.loads((logs / "alerts.jsonl").read_text().splitlines()[-1])
    assert last["code"] == "daily-digest"
    assert last["rate_limited"] is True
