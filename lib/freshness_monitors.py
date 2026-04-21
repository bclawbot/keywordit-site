"""End-of-run freshness checks (Sprint 3 — R2-C7).

Each monitor is independent and idempotent. Called from heartbeat.py after
the stage loop finishes. Each monitor surfaces problems through lib.alerts;
rate limiting and local logging are handled there.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

from .alerts import alert

_log = logging.getLogger("freshness_monitors")

BASE = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = Path.home() / ".openclaw" / "logs"

_FB_FRESH_RE = re.compile(r"last data is (\d+(?:\.\d+)?)\s*h(?:ours?)?\s+old",
                          re.IGNORECASE)


def _mtime_age_hours(path: Path) -> float | None:
    if not path.exists():
        return None
    return (time.time() - path.stat().st_mtime) / 3600.0


def check_fb_intel_freshness(
    log_path: Path | None = None,
    max_age_hours: float = 6.0,
    tail_lines: int = 5000,
) -> bool:
    """Scan the tail of fb_intelligence.log for the 'last data is Nh old' line.

    Fires ``fb-intel-stale`` if > threshold. Returns True if alert fired.
    """
    log_path = log_path or (DEFAULT_LOG_DIR / "fb_intelligence.log")
    if not log_path.exists():
        return False
    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        _log.warning("fb-intel-stale: could not read %s: %s", log_path, e)
        return False
    for line in reversed(lines[-tail_lines:]):
        m = _FB_FRESH_RE.search(line)
        if not m:
            continue
        hours = float(m.group(1))
        if hours > max_age_hours:
            alert(
                "error", "fb-intel-stale",
                f"FB Intelligence data is {hours:.0f}h old "
                f"(threshold {max_age_hours:.0f}h)",
                detail=f"Source line: {line.strip()[:200]}",
            )
            return True
        return False
    return False


def check_artifact_freshness(
    artifact: Path,
    code: str,
    label: str,
    max_age_hours: float,
) -> bool:
    """Alert if ``artifact`` is either missing or older than ``max_age_hours``."""
    age = _mtime_age_hours(artifact)
    if age is None:
        alert(
            "warn", code,
            f"{label} missing at {artifact.name}",
            detail=str(artifact),
        )
        return True
    if age > max_age_hours:
        alert(
            "warn", code,
            f"{label} is {age:.1f}h old (threshold {max_age_hours:.0f}h)",
            detail=str(artifact),
        )
        return True
    return False


def check_explosive_trends_freshness(max_age_hours: float = 12.0) -> bool:
    return check_artifact_freshness(
        BASE / "explosive_trends.json",
        code="trends-stale",
        label="explosive_trends.json",
        max_age_hours=max_age_hours,
    )


def check_golden_opportunities_freshness(max_age_hours: float = 12.0) -> bool:
    return check_artifact_freshness(
        BASE / "golden_opportunities.json",
        code="golden-stale",
        label="golden_opportunities.json",
        max_age_hours=max_age_hours,
    )


def check_validation_hard_gate_rejections(
    log_path: Path | None = None,
    runs_to_consider: int = 2,
) -> bool:
    """If the same keyword has been rejected by validation_hard_gate in the
    last ``runs_to_consider`` days, fire an info-level nag.
    """
    log_path = log_path or (BASE / "error_log.jsonl")
    if not log_path.exists():
        return False
    import json
    cutoff = datetime.now() - timedelta(days=runs_to_consider)
    tally: dict[tuple[str, str], int] = {}
    try:
        for line in log_path.read_text(encoding="utf-8").splitlines()[-5000:]:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue
            stage = entry.get("stage", "")
            if "validation_hard_gate" not in stage and "navigational_intent" not in (entry.get("error") or ""):
                continue
            ts = entry.get("timestamp", "")
            try:
                when = datetime.fromisoformat(ts.replace("Z", ""))
            except Exception:
                continue
            if when < cutoff:
                continue
            keyword = str(entry.get("keyword") or "").strip().lower()
            reason = str(entry.get("reason") or entry.get("error") or "").strip().lower()[:40]
            if keyword and reason:
                tally[(keyword, reason)] = tally.get((keyword, reason), 0) + 1
    except Exception as e:
        _log.warning("validation-gate check failed: %s", e)
        return False
    repeat_offenders = [(kw, reason, n) for (kw, reason), n in tally.items()
                         if n >= runs_to_consider]
    if not repeat_offenders:
        return False
    top = ", ".join(f"{kw!r}({n}x)" for kw, _, n in repeat_offenders[:5])
    alert(
        "info", "validation-gate-nag",
        f"{len(repeat_offenders)} keywords repeatedly rejected by validation gate",
        detail=f"Top: {top}. Either allowlist or tighten the rule.",
    )
    return True


def run_all(*, max_fb_hours: float = 6.0, max_artifact_hours: float = 12.0) -> dict:
    """Run every monitor and return a dict of `code -> fired`."""
    return {
        "fb-intel-stale":        check_fb_intel_freshness(max_age_hours=max_fb_hours),
        "trends-stale":          check_explosive_trends_freshness(max_age_hours=max_artifact_hours),
        "golden-stale":          check_golden_opportunities_freshness(max_age_hours=max_artifact_hours),
        "validation-gate-nag":   check_validation_hard_gate_rejections(),
    }
