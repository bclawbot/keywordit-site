"""Schema sidecar + enum-taxonomy validators (Sprint 4 — data model drift guard).

The pipeline writes ~12 JSON artifacts. Wrapping each payload in a versioned
object would break every downstream reader (including `dashboard_builder.py`
and the Railway sync endpoints). Instead, every writer emits a sibling
`xxx.schema.json` sidecar with `{schema_version, generated_at, source_stage}`.

Enum validators for known-enumerable fields (angle_type, reddit category)
live here too — call `validate_and_normalize_angle_type()` or
`validate_reddit_category()` at write time. Drift raises `TaxonomyError`,
which `lib.alerts.alert()` picks up and broadcasts.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

_log = logging.getLogger("schema_version")


class TaxonomyError(ValueError):
    """Raised when a row uses a value outside the allowed enum set."""


# ── schema sidecar ────────────────────────────────────────────────────────────

def write_schema_sidecar(
    artifact_path: Path,
    version: str,
    source_stage: str,
    *,
    extra: dict | None = None,
) -> Path:
    """Write `<artifact>.schema.json` next to `artifact_path`.

    Atomic write via tmp+rename so a crash never leaves a half-written sidecar.
    """
    artifact_path = Path(artifact_path)
    sidecar = artifact_path.with_suffix(artifact_path.suffix + ".schema.json")
    payload: dict = {
        "schema_version": version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_stage": source_stage,
        "artifact": artifact_path.name,
    }
    if extra:
        payload.update(extra)
    tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, sidecar)
    return sidecar


def read_schema_sidecar(artifact_path: Path) -> dict | None:
    sidecar = Path(artifact_path).with_suffix(Path(artifact_path).suffix + ".schema.json")
    if not sidecar.exists():
        return None
    try:
        return json.loads(sidecar.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── angle_candidates.json — snake_case is the canonical form ─────────────────

ANGLE_TYPES_CURRENT: set[str] = {
    "comparison", "alternative", "problem_agitation", "loss_aversion",
    "timing_pressure", "social_proof", "authority", "curiosity",
    "how_to", "list", "faq", "case_study", "trust", "urgency",
    "pain_point", "benefit", "feature", "objection",
}

# Legacy Title-Case spelling that we auto-repair on write.
ANGLE_TYPES_LEGACY_MAP: dict[str, str] = {
    "Comparison":        "comparison",
    "Alternative":       "alternative",
    "Problem Agitation": "problem_agitation",
    "Loss Aversion":     "loss_aversion",
    "Timing Pressure":   "timing_pressure",
    "Social Proof":      "social_proof",
    "Authority":         "authority",
    "Curiosity":         "curiosity",
    "How-To":            "how_to",
    "How To":            "how_to",
    "List":              "list",
    "FAQ":               "faq",
    "Case Study":        "case_study",
    "Trust":             "trust",
    "Urgency":           "urgency",
    "Pain Point":        "pain_point",
    "Benefit":           "benefit",
    "Feature":           "feature",
    "Objection":         "objection",
}

_STRICT = os.environ.get("STRICT_SCHEMA", "1").strip().lower() not in ("0", "false", "no")


def validate_and_normalize_angle_type(
    raw: str,
    *,
    source_stage: str = "angle_engine",
    alert_on_drift: bool = True,
) -> str:
    """Normalize Title-Case angle types to snake_case; raise on unknown.

    Returns the canonical snake_case form. Title-Case inputs are auto-repaired
    with a deprecation log, so the writer silently converges the bifurcation.
    Unknown values raise `TaxonomyError`, which the `alert()` helper picks
    up (see lib.alerts) so the operator is notified in Telegram.
    """
    if raw is None:
        raise TaxonomyError("angle_type is None")
    key = str(raw).strip()
    if key in ANGLE_TYPES_CURRENT:
        return key
    if key in ANGLE_TYPES_LEGACY_MAP:
        normalized = ANGLE_TYPES_LEGACY_MAP[key]
        _log.info("angle_type deprecated Title-Case %r -> %r", key, normalized)
        return normalized
    # Try a best-effort snake_case coerce for unseen Title-Case values.
    low = key.lower().replace("-", "_").replace(" ", "_")
    if low in ANGLE_TYPES_CURRENT:
        return low
    msg = f"unknown angle_type={key!r} (source_stage={source_stage})"
    if alert_on_drift:
        try:
            from .alerts import alert as _alert
            _alert("warn", "taxonomy-drift",
                   f"Unknown angle_type {key!r}",
                   detail=f"stage={source_stage}")
        except Exception:
            pass
    if _STRICT:
        raise TaxonomyError(msg)
    _log.warning("angle_type drift (non-strict): %s", msg)
    return low


# ── reddit_intelligence.json categories ─────────────────────────────────────

REDDIT_CATEGORIES: set[str] = {
    "feed_intel", "decay_signal", "noise", "keyword_mention",
    "vertical_signal",
}


def validate_reddit_category(
    raw: str,
    *,
    source_stage: str = "reddit_intelligence",
    alert_on_drift: bool = True,
) -> str:
    if raw is None:
        raise TaxonomyError("reddit category is None")
    key = str(raw).strip()
    if key in REDDIT_CATEGORIES:
        return key
    msg = f"unknown reddit category={key!r} (source_stage={source_stage})"
    if alert_on_drift:
        try:
            from .alerts import alert as _alert
            _alert("warn", "taxonomy-drift",
                   f"Unknown reddit category {key!r}",
                   detail=f"stage={source_stage}")
        except Exception:
            pass
    if _STRICT:
        raise TaxonomyError(msg)
    _log.warning("reddit category drift (non-strict): %s", msg)
    return key


def filter_known_categories(values: Iterable[str], source_stage: str = "reddit_intelligence") -> list[str]:
    """Return only the categories we recognise; alerts once per unknown."""
    out = []
    for v in values or []:
        try:
            out.append(validate_reddit_category(v, source_stage=source_stage))
        except TaxonomyError:
            continue
    return out


# ── end-of-run sidecar sweep ────────────────────────────────────────────────

# Keyed by artifact filename (as produced in the workspace root). Each entry
# carries the schema_version and the stage that produces it — when a sidecar
# is written we record both so `orphan_scan` / dashboard can tell at a glance
# who owns a file.
ARTIFACT_SCHEMAS: dict[str, tuple[str, str]] = {
    "latest_trends.json":             ("1.0", "trends_scraper"),
    "explosive_trends.json":          ("1.1", "trends_postprocess"),
    "expanded_keywords.json":         ("1.0", "keyword_expander"),
    "transformed_keywords.json":      ("1.0", "commercial_keyword_transformer"),
    "commercial_keywords.json":       ("1.0", "keyword_extractor"),
    "vetted_opportunities.json":      ("1.0", "vetting"),
    "validated_opportunities.json":   ("1.0", "validation"),
    "golden_opportunities.json":      ("1.0", "validation"),
    "angle_candidates.json":          ("1.0", "angle_engine"),
    "reddit_intelligence.json":       ("1.1", "reddit_intelligence"),
    "missed_opportunities.json":      ("1.0", "intel_bridge"),
    "subreddit_registry.json":        ("1.0", "subreddit_discovery"),
}


def write_all_sidecars(workspace: Path) -> dict[str, Path]:
    """Sweep the workspace and write a sidecar for every known artifact.

    Returns a `{filename: sidecar_path}` map of the sidecars actually written.
    Missing artifacts are skipped silently — a stage may legitimately not have
    produced output in a partial run.
    """
    written: dict[str, Path] = {}
    for name, (version, stage) in ARTIFACT_SCHEMAS.items():
        path = workspace / name
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            sidecar = write_schema_sidecar(path, version, stage)
            written[name] = sidecar
        except Exception as e:
            _log.warning("sidecar write failed for %s: %s", name, e)
    return written
