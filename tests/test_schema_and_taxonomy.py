"""Sprint 4 — schema sidecar + taxonomy enum tests (R2-related data drift)."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def fresh_lib(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENCLAW_LOG_DIR", str(tmp_path))
    # Ensure strict mode is on.
    monkeypatch.setenv("STRICT_SCHEMA", "1")
    for m in ("lib.schema_version", "lib.alerts", "lib"):
        sys.modules.pop(m, None)
    from lib import schema_version
    importlib.reload(schema_version)
    return schema_version, tmp_path


def test_schema_sidecar_written_for_artifact(fresh_lib, tmp_path):
    sv, _ = fresh_lib
    artifact = tmp_path / "golden_opportunities.json"
    artifact.write_text("[]")
    sidecar = sv.write_schema_sidecar(artifact, "1.0", "validation",
                                       extra={"record_count": 0})
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text())
    assert payload["schema_version"] == "1.0"
    assert payload["source_stage"] == "validation"
    assert payload["record_count"] == 0


def test_sweep_writes_sidecars_for_known_artifacts(fresh_lib, tmp_path):
    sv, _ = fresh_lib
    (tmp_path / "golden_opportunities.json").write_text("[]")
    (tmp_path / "explosive_trends.json").write_text("[]")
    (tmp_path / "unknown_file.json").write_text("[]")  # not in ARTIFACT_SCHEMAS
    written = sv.write_all_sidecars(tmp_path)
    assert "golden_opportunities.json" in written
    assert "explosive_trends.json" in written
    assert "unknown_file.json" not in written
    # Empty/missing files are skipped.
    assert not (tmp_path / "unknown_file.json.schema.json").exists()


def test_angle_type_title_case_autonormalizes(fresh_lib):
    sv, _ = fresh_lib
    assert sv.validate_and_normalize_angle_type("Comparison") == "comparison"
    assert sv.validate_and_normalize_angle_type("How-To") == "how_to"
    assert sv.validate_and_normalize_angle_type("Pain Point") == "pain_point"


def test_angle_type_rejects_unknown(fresh_lib):
    sv, _ = fresh_lib
    with pytest.raises(sv.TaxonomyError):
        sv.validate_and_normalize_angle_type("bogus_type")


def test_angle_type_rejects_none(fresh_lib):
    sv, _ = fresh_lib
    with pytest.raises(sv.TaxonomyError):
        sv.validate_and_normalize_angle_type(None)


def test_angle_type_unknown_triggers_alert(fresh_lib, tmp_path):
    sv, log_dir = fresh_lib
    with pytest.raises(sv.TaxonomyError):
        sv.validate_and_normalize_angle_type("bogus_type",
                                             source_stage="unit_test")
    alert_log = log_dir / "alerts.jsonl"
    assert alert_log.exists(), "expected taxonomy-drift to append an alert"
    rec = json.loads(alert_log.read_text().splitlines()[-1])
    assert rec["code"] == "taxonomy-drift"


def test_reddit_category_accepts_keyword_mention(fresh_lib):
    """R2 drift: keyword_mention added silently on Apr 21; must be in allowed set."""
    sv, _ = fresh_lib
    assert sv.validate_reddit_category("keyword_mention") == "keyword_mention"


def test_reddit_category_rejects_unknown(fresh_lib):
    sv, _ = fresh_lib
    with pytest.raises(sv.TaxonomyError):
        sv.validate_reddit_category("mystery_bucket")


def test_reddit_filter_drops_unknown_but_keeps_known(fresh_lib):
    sv, _ = fresh_lib
    result = sv.filter_known_categories(
        ["keyword_mention", "mystery_bucket", "feed_intel"],
        source_stage="unit_test",
    )
    assert "keyword_mention" in result
    assert "feed_intel" in result
    assert "mystery_bucket" not in result


def test_non_strict_schema_downgrades_to_warning(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENCLAW_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("STRICT_SCHEMA", "0")
    for m in ("lib.schema_version", "lib.alerts", "lib"):
        sys.modules.pop(m, None)
    from lib import schema_version as sv
    importlib.reload(sv)
    # Should NOT raise in non-strict mode.
    sv.validate_and_normalize_angle_type("unknown_type")
    sv.validate_reddit_category("unknown_cat")
