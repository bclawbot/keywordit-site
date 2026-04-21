"""Single outbound alert pipe (Sprint 3 — R2-C7/C8).

Every pipeline freshness/backend monitor calls `alert(...)` rather than
opening its own Telegram client. The helper:

* Rate-limits per `code` so a multi-day Railway outage stays out of the inbox.
* Always appends to `~/.openclaw/logs/alerts.jsonl` regardless of Telegram
  success, so the operator has an audit trail even if Telegram itself is down.
* Degrades quietly when TELEGRAM_TOKEN / TELEGRAM_CHAT_ID are missing: local
  file append still happens, network send is skipped.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

try:
    import urllib.request
    import urllib.error
except Exception:  # pragma: no cover
    urllib = None  # type: ignore

_log = logging.getLogger("alerts")

LOG_DIR = Path(os.environ.get(
    "OPENCLAW_LOG_DIR", str(Path.home() / ".openclaw" / "logs"))
)
ALERT_LOG = LOG_DIR / "alerts.jsonl"
RATE_FILE = LOG_DIR / ".alerts_last_fired.json"

Severity = Literal["info", "warn", "error"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read_rate_state() -> dict:
    try:
        return json.loads(RATE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _write_rate_state(state: dict) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        RATE_FILE.write_text(json.dumps(state), encoding="utf-8")
    except Exception:
        pass


def _within_rate_window(code: str, window_minutes: int) -> bool:
    state = _read_rate_state()
    last = state.get(code)
    if not last:
        return False
    try:
        last_dt = datetime.fromisoformat(last)
    except Exception:
        return False
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    age = (_now() - last_dt).total_seconds()
    return age < window_minutes * 60


def _mark_fired(code: str) -> None:
    state = _read_rate_state()
    state[code] = _now().isoformat()
    _write_rate_state(state)


def _append_local(record: dict) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with ALERT_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        _log.warning("alerts: failed to append local log: %s", e)


def _send_telegram(text: str, timeout: float = 10.0) -> bool:
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat_id = (os.environ.get("TELEGRAM_ALERT_CHAT_ID", "").strip()
               or os.environ.get("TELEGRAM_CHAT_ID", "").strip())
    if not token or not chat_id:
        return False
    if urllib is None:
        return False
    try:
        data = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data, headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=timeout).read()
        return True
    except Exception as e:
        _log.warning("alerts: Telegram send failed: %s", e)
        return False


def alert(
    severity: Severity,
    code: str,
    summary: str,
    detail: str | None = None,
    dedupe_window_minutes: int = 60,
) -> bool:
    """Send an operator alert, rate-limited per `code`.

    Returns True if a Telegram message was actually dispatched; False means
    either rate-limited or network send skipped. The local jsonl entry is
    always appended regardless.
    """
    rate_limited = _within_rate_window(code, dedupe_window_minutes)
    record = {
        "ts": _now().isoformat(),
        "severity": severity,
        "code": code,
        "summary": summary,
        "detail": detail or "",
        "rate_limited": rate_limited,
        "delivered": False,
    }
    text = f"[{severity.upper()} {code}] {summary}"
    if detail:
        text = f"{text}\n\n{detail}"
    if not rate_limited:
        delivered = _send_telegram(text)
        record["delivered"] = delivered
        if delivered:
            _mark_fired(code)
    _append_local(record)
    return record["delivered"]
