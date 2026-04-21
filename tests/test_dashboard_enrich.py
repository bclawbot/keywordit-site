"""Sprint 5 — dashboard enrichment module tests (product-audit §5.1/5.2/5.3/5.7)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from lib.dashboard_enrich import (  # noqa: E402
    DEFAULT_FLAGS,
    PINNED_COUNTRY_CHIPS,
    annotate_new_this_run,
    build_angle_index,
    build_reddit_mentions_index,
    country_chip_counts,
    enrich_rows,
    load_dashboard_flags,
    load_prev_run_finish,
    summarize_enrichment,
)


def test_pinned_countries_order_is_stable():
    """Operator muscle memory depends on this being a fixed order."""
    assert PINNED_COUNTRY_CHIPS == ("US", "AU", "GB", "CA", "DE")


def test_country_chip_counts_golden_only(tmp_path):
    rows = [
        {"country": "US", "tag": "GOLDEN_OPPORTUNITY"},
        {"country": "US", "tag": "EMERGING"},
        {"country": "AU", "tag": "GOLDEN_OPPORTUNITY"},
        {"country": "AU", "tag": "GOLDEN_OPPORTUNITY"},
        {"country": "BR", "tag": "GOLDEN_OPPORTUNITY"},  # non-pinned
    ]
    counts = country_chip_counts(rows, golden_only=True)
    by_country = {c["country"]: c["count"] for c in counts}
    assert by_country == {"US": 1, "AU": 2, "GB": 0, "CA": 0, "DE": 0}
    # Non-golden-only includes EMERGING
    counts_all = country_chip_counts(rows, golden_only=False)
    by_all = {c["country"]: c["count"] for c in counts_all}
    assert by_all["US"] == 2


def test_annotate_new_this_run_marks_rows_after_cutoff():
    rows = [
        {"keyword": "a", "validated_at": "2026-04-21T12:00:00"},
        {"keyword": "b", "validated_at": "2026-04-20T12:00:00"},
        {"keyword": "c"},  # no validated_at
    ]
    new = annotate_new_this_run(rows, prev_run_finish="2026-04-21T00:00:00")
    assert new == 1
    assert rows[0]["is_new_this_run"] is True
    assert rows[1]["is_new_this_run"] is False
    assert rows[2]["is_new_this_run"] is False


def test_annotate_new_this_run_returns_zero_without_cutoff():
    rows = [{"keyword": "x", "validated_at": "2026-04-21T12:00:00"}]
    new = annotate_new_this_run(rows, prev_run_finish=None)
    assert new == 0
    assert rows[0]["is_new_this_run"] is False


def test_build_angle_index_keys_are_keyword_country():
    clusters = [
        {"keyword": "Car Insurance", "country": "us", "top_angle": "Compare quotes"},
        {"keyword": "debt consolidation", "country": "GB", "top_angle": "Low rates"},
    ]
    idx = build_angle_index(clusters)
    assert "car insurance|US" in idx
    assert "debt consolidation|GB" in idx
    assert idx["car insurance|US"]["top_angle"] == "Compare quotes"


def test_build_reddit_mentions_filters_to_keyword_mention():
    posts = [
        {"title": "ignored", "categories": ["noise"], "keywords": ["foo"]},
        {"title": "relevant", "categories": ["keyword_mention"],
         "keywords": ["Solar Panels"], "subreddit": "r/solar",
         "url": "https://reddit.com/x"},
    ]
    idx = build_reddit_mentions_index(posts)
    assert "solar panels" in idx
    assert "foo" not in idx
    assert idx["solar panels"]["subreddit"] == "r/solar"


def test_enrich_rows_sets_angle_preview_and_reddit_and_fb():
    angle_index = {
        "car insurance|US": {"top_angle": "Compare quotes in 60s"},
    }
    reddit = {"car insurance": {"title": "r/personalfinance post",
                                 "url": "https://...", "subreddit": "r/personalfinance"}}
    fb_set = {"car insurance", "debt consolidation"}
    rows = [
        {"keyword": "Car Insurance", "country": "us"},
        {"keyword": "unrelated", "country": "US"},
    ]
    enrich_rows(rows, angle_index=angle_index,
                reddit_mentions=reddit, fb_intel_keywords=fb_set)
    assert rows[0]["angle_preview"] == "Compare quotes in 60s"
    assert rows[0]["reddit_mention"]["subreddit"] == "r/personalfinance"
    assert rows[0]["has_fb_intel"] is True
    assert rows[1]["angle_preview"] is None
    assert rows[1]["reddit_mention"] is None
    assert rows[1]["has_fb_intel"] is False


def test_load_dashboard_flags_defaults_when_missing(tmp_path):
    flags = load_dashboard_flags(tmp_path)
    assert flags == DEFAULT_FLAGS


def test_load_dashboard_flags_merges_user_overrides(tmp_path):
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "dashboard_flags.json").write_text(json.dumps({
        "country_chips": False,
        "ignored_unknown_flag": True,
    }))
    flags = load_dashboard_flags(tmp_path)
    assert flags["country_chips"] is False
    # Default still on for flags the user didn't touch.
    assert flags["golden_only_default"] is True
    # Unknown keys are dropped.
    assert "ignored_unknown_flag" not in flags


def test_load_prev_run_finish_prefers_heartbeat_state(tmp_path):
    (tmp_path / "heartbeat_state.json").write_text(json.dumps({
        "prev_run_finish": "2026-04-21T03:44:00+00:00",
    }))
    assert load_prev_run_finish(tmp_path) == "2026-04-21T03:44:00+00:00"


def test_load_prev_run_finish_falls_back_to_sidecar(tmp_path):
    (tmp_path / "golden_opportunities.json.schema.json").write_text(json.dumps({
        "generated_at": "2026-04-21T03:44:00+00:00",
        "schema_version": "1.0",
    }))
    assert load_prev_run_finish(tmp_path) == "2026-04-21T03:44:00+00:00"


def test_load_prev_run_finish_returns_none_when_neither(tmp_path):
    assert load_prev_run_finish(tmp_path) is None


def test_summarize_enrichment_counts():
    rows = [
        {"is_new_this_run": True, "angle_preview": "x",
         "reddit_mention": {"title": "y"}, "has_fb_intel": True},
        {"is_new_this_run": False, "angle_preview": None,
         "reddit_mention": None, "has_fb_intel": False},
    ]
    summary = summarize_enrichment(rows)
    assert summary["rows"] == 2
    assert summary["new_this_run"] == 1
    assert summary["with_angle"] == 1
    assert summary["with_reddit"] == 1
    assert summary["with_fb"] == 1
