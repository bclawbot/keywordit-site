"""Sprint 6 Task 6.5 — Daily operator digest.

Once-per-day summary delivered through `lib.alerts.alert(code="daily-digest")`.
Pure formatting logic; data gathered from the JSON artifacts and alert log.

Used from `heartbeat.py` end-of-run. The alert helper's rate limiting keeps
the digest to once per 24h even if heartbeat fires multiple times a day.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

_log = logging.getLogger("daily_digest")


def _mtime_hours(path: Path) -> float | None:
    if not path.exists():
        return None
    from time import time
    return (time() - path.stat().st_mtime) / 3600.0


def _count_tags(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, int] = {}
    for row in rows if isinstance(rows, list) else []:
        tag = row.get("tag") or "UNKNOWN"
        out[tag] = out.get(tag, 0) + 1
    return out


def _count_by_country(path: Path, *, tag: str | None = None) -> dict[str, int]:
    if not path.exists():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, int] = {}
    for row in rows if isinstance(rows, list) else []:
        if tag and row.get("tag") != tag:
            continue
        co = (row.get("country") or "??").upper()
        out[co] = out.get(co, 0) + 1
    return out


def _golden_delta(workspace: Path) -> int:
    """New GOLDEN since prev-run cutoff (from Sprint 5 enrich helper).

    Returns the count; 0 if we can't determine it.
    """
    try:
        from .dashboard_enrich import load_prev_run_finish
    except Exception:
        return 0
    prev = load_prev_run_finish(workspace)
    if not prev:
        return 0
    golden = workspace / "golden_opportunities.json"
    if not golden.exists():
        return 0
    try:
        rows = json.loads(golden.read_text(encoding="utf-8"))
    except Exception:
        return 0
    count = 0
    for row in rows if isinstance(rows, list) else []:
        if row.get("tag") != "GOLDEN_OPPORTUNITY":
            continue
        ts = row.get("validated_at") or ""
        if ts and ts > prev:
            count += 1
    return count


def _alerts_today(log_dir: Path) -> int:
    """Count non-rate-limited alerts dispatched in the last 24h."""
    path = log_dir / "alerts.jsonl"
    if not path.exists():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    count = 0
    try:
        for line in path.read_text(encoding="utf-8").splitlines()[-500:]:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            ts = rec.get("ts", "")
            try:
                when = datetime.fromisoformat(ts)
                if when.tzinfo is None:
                    when = when.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if when < cutoff:
                continue
            if rec.get("rate_limited"):
                continue
            count += 1
    except Exception:
        return 0
    return count


def _sync_status(log_dir: Path) -> str:
    path = log_dir / ".sync_last_status.json"
    if not path.exists():
        return "unknown"
    try:
        return json.loads(path.read_text()).get("status", "unknown")
    except Exception:
        return "unknown"


def _fb_freshness_hours(log_dir: Path) -> float | None:
    import re
    log = log_dir / "fb_intelligence.log"
    if not log.exists():
        return None
    try:
        text = log.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None
    match = None
    for line in reversed(text.splitlines()[-5000:]):
        m = re.search(r"last data is (\d+(?:\.\d+)?)\s*h", line, re.I)
        if m:
            match = m
            break
    return float(match.group(1)) if match else None


def render_digest(
    workspace: Path | None = None,
    *,
    log_dir: Path | None = None,
    now: datetime | None = None,
) -> str:
    workspace = Path(workspace) if workspace else Path(__file__).resolve().parent.parent
    log_dir = Path(log_dir) if log_dir else Path.home() / ".openclaw" / "logs"
    now = now or datetime.now(timezone.utc)

    tag_counts = _count_tags(workspace / "validated_opportunities.json")
    golden_country = _count_by_country(workspace / "golden_opportunities.json",
                                        tag="GOLDEN_OPPORTUNITY")
    country_summary = ", ".join(
        f"{c}={n}" for c, n in sorted(golden_country.items(), key=lambda x: -x[1])[:5]
    ) or "(none)"

    trends_age = _mtime_hours(workspace / "explosive_trends.json")
    golden_age = _mtime_hours(workspace / "golden_opportunities.json")
    fb_age = _fb_freshness_hours(log_dir)
    sync = _sync_status(log_dir)
    alerts_fired = _alerts_today(log_dir)
    new_goldens = _golden_delta(workspace)

    def _age(h: float | None) -> str:
        if h is None:
            return "unknown"
        return f"{h:.1f}h"

    lines = [
        f"📊 Pipeline digest — {now.strftime('%Y-%m-%d')}",
        "",
        f"Freshness:",
        f"  trends:    {_age(trends_age)}",
        f"  golden:    {_age(golden_age)}",
        f"  fb_intel:  {_age(fb_age)}",
        f"  railway:   {sync}",
        "",
        f"Tiers: "
        + ", ".join(f"{k}={v}" for k, v in sorted(tag_counts.items(), key=lambda x: -x[1])),
        f"Countries (GOLDEN top 5): {country_summary}",
        f"New GOLDEN since last run: {new_goldens}",
        "",
        f"Alerts in last 24h: {alerts_fired}",
    ]
    return "\n".join(lines)


def send_digest(workspace: Path | None = None) -> bool:
    """Render + dispatch via lib.alerts. Returns whether Telegram delivery succeeded."""
    try:
        from .alerts import alert
    except Exception as e:
        _log.warning("daily_digest: alert import failed: %s", e)
        return False
    text = render_digest(workspace)
    # 1440m window = one delivery per calendar day.
    return alert("info", "daily-digest",
                 summary="Daily pipeline digest",
                 detail=text,
                 dedupe_window_minutes=1440)
