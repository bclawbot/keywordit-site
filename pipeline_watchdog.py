"""Pipeline watchdog + freshness-sentinel helpers (Sprint 2 — R2-C1/C2/C3).

Two orthogonal concerns:

1. **Watchdog** — every long-running stage writes an `<stage>.alive` side-channel
   file when it makes progress. A thread on the parent process inspects the
   file's mtime every 30 s; if the age exceeds the stage's SLA, a WARNING is
   logged to `heartbeat.log` and appended to `error_log.jsonl`. Never kills;
   the kill decision stays with the subprocess timeout.

2. **Freshness sentinel** — when a stage times out or exits non-zero, the
   parent writes `./logs/stale/<stage>.stale` containing a small JSON blob
   `{stage, failed_at, reason}`. The dashboard builder reads them so a stale
   upstream artifact is visible to the operator instead of silently accepted.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

_log = logging.getLogger("pipeline_watchdog")

ALIVE_DIR = Path(os.environ.get(
    "HEARTBEAT_ALIVE_DIR", str(Path.home() / ".openclaw" / "logs"))
)
STALE_DIR = Path(os.environ.get(
    "HEARTBEAT_STALE_DIR", str(Path.home() / ".openclaw" / "logs" / "stale"))
)

STAGE_ALIVE_SLA = {
    "keyword_extractor.py":              600,   # single LLM batch may take ~420s; +headroom
    "trends_postprocess.py":             300,   # LanceDB batches slower than LLM
    "money_flow_classifier.py":          180,   # local Ollama, mostly regex
    "consequence_generator.py":          180,
    "commercial_keyword_transformer.py": 300,
}

DEFAULT_SLA = 120
WATCHDOG_CHECK_INTERVAL = 30   # seconds between mtime polls


def _alive_path(stage_name: str) -> Path:
    name = stage_name.replace(".py", "").replace(".sh", "")
    return ALIVE_DIR / f"{name}.alive"


def _stale_path(stage_name: str) -> Path:
    return STALE_DIR / f"{stage_name}.stale"


def start_watchdog(
    stage_name: str,
    pid: int,
    *,
    sla_seconds: int | None = None,
    check_interval: int = WATCHDOG_CHECK_INTERVAL,
    error_log_append=None,
) -> threading.Event:
    """Start a background thread that warns when <stage>.alive goes stale.

    Returns a `stop_event` — `stop_event.set()` tells the thread to exit at
    the next poll boundary. Non-fatal; the subprocess keeps running.
    """
    sla = sla_seconds if sla_seconds is not None else STAGE_ALIVE_SLA.get(
        stage_name, DEFAULT_SLA
    )
    alive = _alive_path(stage_name)
    stop_event = threading.Event()

    def _run():
        warned_at: float | None = None
        while not stop_event.wait(check_interval):
            try:
                if not alive.exists():
                    continue
                age = time.time() - alive.stat().st_mtime
                if age <= sla:
                    warned_at = None
                    continue
                # Re-warn at most every 5 minutes to avoid log spam.
                if warned_at is not None and (time.time() - warned_at) < 300:
                    continue
                warned_at = time.time()
                msg = (
                    f"[watchdog] {stage_name} alive file {age:.0f}s stale "
                    f"(sla={sla}s, pid={pid})"
                )
                _log.warning(msg)
                if error_log_append is not None:
                    try:
                        error_log_append({
                            "timestamp": datetime.now().isoformat(),
                            "stage": f"watchdog/{stage_name}",
                            "error": msg,
                        })
                    except Exception:
                        pass
            except Exception:
                pass  # watchdog is non-fatal by design

    thread = threading.Thread(target=_run, daemon=True, name=f"watchdog-{stage_name}")
    thread.start()
    stop_event._thread = thread  # type: ignore[attr-defined]
    return stop_event


def stop_watchdog(stop_event: threading.Event, join_timeout: float = 2.0) -> None:
    stop_event.set()
    thread = getattr(stop_event, "_thread", None)
    if thread is not None:
        thread.join(timeout=join_timeout)


def write_stale_sentinel(stage_name: str, reason: str) -> Path:
    """Record that a stage finished in a bad state so the dashboard knows."""
    STALE_DIR.mkdir(parents=True, exist_ok=True)
    path = _stale_path(stage_name)
    payload = {
        "stage": stage_name,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def clear_stale_sentinel(stage_name: str) -> None:
    try:
        _stale_path(stage_name).unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass


def read_stale_sentinels() -> list[dict]:
    """Return all stale-stage records; used by dashboard_builder."""
    if not STALE_DIR.exists():
        return []
    out: list[dict] = []
    for p in sorted(STALE_DIR.glob("*.stale")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    return out
