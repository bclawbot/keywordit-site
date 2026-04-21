"""Sprint 5 — dashboard enrichment helpers (product-audit §5.2/5.3/5.4/5.5).

This module is pure-function and imported by `dashboard_builder.py` at the
top-level. Keeping the joins out of `dashboard_builder.py` itself lets us
unit-test them without spinning up the whole dashboard pipeline — and keeps
our Sprint 5 diff reviewable.

Functions:
    annotate_new_this_run(rows, prev_run_finish)
    build_reddit_mentions_index(reddit_rows)
    build_angle_index(angle_candidates)
    enrich_rows(rows, angle_index, reddit_mentions_index, fb_intel_keywords)
    load_prev_run_finish(workspace_base)
    load_dashboard_flags(workspace_base)
    country_chip_counts(rows, pinned=PINNED_COUNTRY_CHIPS)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

_log = logging.getLogger("dashboard_enrich")

PINNED_COUNTRY_CHIPS: tuple[str, ...] = ("US", "AU", "GB", "CA", "DE")

# ── feature flags ────────────────────────────────────────────────────────────

DEFAULT_FLAGS: dict[str, bool] = {
    "since_last_run_filter": True,
    "country_chips":         True,
    "angle_preview_column":  True,
    "has_fb_intel_badge":    True,
    "stale_banner":          True,
    "golden_only_default":   True,
}


def load_dashboard_flags(workspace_base: Path) -> dict[str, bool]:
    """Read config/dashboard_flags.json with DEFAULT_FLAGS fallback."""
    path = Path(workspace_base) / "config" / "dashboard_flags.json"
    if not path.exists():
        return dict(DEFAULT_FLAGS)
    try:
        user = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(user, dict):
            return dict(DEFAULT_FLAGS)
        merged = dict(DEFAULT_FLAGS)
        for k, v in user.items():
            if k in DEFAULT_FLAGS and isinstance(v, bool):
                merged[k] = v
        return merged
    except Exception as e:
        _log.warning("dashboard flags read failed: %s", e)
        return dict(DEFAULT_FLAGS)


# ── previous-run cutoff (product-audit §5.4: "since last run") ─────────────

def load_prev_run_finish(workspace_base: Path) -> str | None:
    """Return ISO timestamp of the previous run's completion, or None.

    Strategy: prefer `heartbeat_state.json` if it exists; fall back to the
    schema sidecar `golden_opportunities.json.schema.json` (Sprint 4) which
    carries `generated_at` — treat *that* as last-run-finish.
    """
    base = Path(workspace_base)
    state = base / "heartbeat_state.json"
    if state.exists():
        try:
            payload = json.loads(state.read_text(encoding="utf-8"))
            ts = payload.get("prev_run_finish") or payload.get("last_run_at")
            if ts:
                return str(ts)
        except Exception:
            pass
    sidecar = base / "golden_opportunities.json.schema.json"
    if sidecar.exists():
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            return payload.get("generated_at")
        except Exception:
            pass
    return None


# ── per-row joins ────────────────────────────────────────────────────────────

def build_angle_index(angle_candidates: Sequence[dict]) -> dict[str, dict]:
    """Map `keyword|country` -> angle cluster (already indexed form)."""
    out: dict[str, dict] = {}
    for cluster in angle_candidates or []:
        kw = str(cluster.get("keyword", "")).lower().strip()
        co = str(cluster.get("country", "")).upper()
        if not kw:
            continue
        out[f"{kw}|{co}"] = cluster
    return out


def _extract_top_angle(cluster: dict) -> str | None:
    """Pick a human-readable 'top angle' from an angle cluster."""
    if not cluster:
        return None
    # Prefer an explicit top_angle field if the pipeline already picked one.
    for key in ("top_angle", "selected_angle", "best_angle"):
        val = cluster.get(key)
        if val:
            return str(val)[:200]
    # Otherwise: first angle in the cluster's candidates list.
    for key in ("angles", "candidates", "rows"):
        items = cluster.get(key)
        if isinstance(items, list) and items:
            first = items[0]
            if isinstance(first, dict):
                for sub in ("angle", "title", "headline", "hook"):
                    if first.get(sub):
                        return str(first[sub])[:200]
            elif isinstance(first, str):
                return first[:200]
    return None


def build_reddit_mentions_index(reddit_rows: Sequence[dict]) -> dict[str, dict]:
    """Map `keyword` (lowercase) -> {title, url, subreddit} from reddit intel.

    We keep only rows tagged `keyword_mention` (Sprint 4 enum) and index by
    lowercased keyword. First hit wins — enough for a single tooltip.
    """
    out: dict[str, dict] = {}
    for post in reddit_rows or []:
        cats = post.get("categories") or []
        if "keyword_mention" not in cats:
            continue
        keywords: Iterable[str] = post.get("keyword_mentions") or post.get("keywords") or []
        for kw in keywords:
            k = str(kw).lower().strip()
            if not k or k in out:
                continue
            out[k] = {
                "title":     str(post.get("title", ""))[:280],
                "url":       str(post.get("url", "")),
                "subreddit": str(post.get("subreddit", "")),
            }
    return out


def annotate_new_this_run(rows: list[dict], prev_run_finish: str | None) -> int:
    """Set row['is_new_this_run']. Returns count of new rows."""
    if not prev_run_finish:
        for row in rows:
            row["is_new_this_run"] = False
        return 0
    new_count = 0
    for row in rows:
        ts = row.get("validated_at") or row.get("vetted_at") or ""
        is_new = bool(ts and ts > prev_run_finish)
        row["is_new_this_run"] = is_new
        if is_new:
            new_count += 1
    return new_count


def enrich_rows(
    rows: list[dict],
    *,
    angle_index: dict[str, dict],
    reddit_mentions: dict[str, dict],
    fb_intel_keywords: set[str] | None = None,
) -> None:
    """Add angle_preview, reddit_mention, has_fb_intel per row (in-place)."""
    fb_intel_keywords = fb_intel_keywords or set()
    for row in rows:
        kw = str(row.get("keyword", "")).lower().strip()
        co = str(row.get("country", "")).upper()
        cluster = angle_index.get(f"{kw}|{co}") or angle_index.get(f"{kw}|") or {}
        row["angle_preview"] = _extract_top_angle(cluster)
        mention = reddit_mentions.get(kw)
        row["reddit_mention"] = mention  # dict or None
        row["has_fb_intel"] = kw in fb_intel_keywords if fb_intel_keywords else False


def country_chip_counts(
    rows: Sequence[dict],
    *,
    pinned: Sequence[str] = PINNED_COUNTRY_CHIPS,
    golden_only: bool = False,
) -> list[dict]:
    """Return `[{country, count}]` in pinned order for the chip row.

    Counts pin-country rows (not the long-tail dropdown). If `golden_only`
    is True, only tier-GOLDEN rows count — so the chip tracks "what you can
    actually act on today."
    """
    tally: dict[str, int] = {c: 0 for c in pinned}
    for row in rows:
        co = str(row.get("country", "")).upper()
        if co not in tally:
            continue
        if golden_only and row.get("tag") != "GOLDEN_OPPORTUNITY":
            continue
        tally[co] += 1
    return [{"country": c, "count": tally[c]} for c in pinned]


def summarize_enrichment(rows: Sequence[dict]) -> dict:
    """Counters for the dashboard banner and telemetry."""
    new_count = sum(1 for r in rows if r.get("is_new_this_run"))
    with_angle = sum(1 for r in rows if r.get("angle_preview"))
    with_reddit = sum(1 for r in rows if r.get("reddit_mention"))
    with_fb = sum(1 for r in rows if r.get("has_fb_intel"))
    return {
        "rows":         len(rows),
        "new_this_run": new_count,
        "with_angle":   with_angle,
        "with_reddit":  with_reddit,
        "with_fb":      with_fb,
        "computed_at":  datetime.now(timezone.utc).isoformat(),
    }
