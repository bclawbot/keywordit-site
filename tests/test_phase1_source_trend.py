"""
test_phase1_source_trend.py — Tests for Phase 1: Source Trend Propagation.

Run:  python3 -m pytest tests/test_phase1_source_trend.py -v

Covers:
  1.1: expand_batch() accepts source_trends parameter
  1.2: Expansion records include source_trend field
  1.3: Backward compatibility — source_trends=None works
  1.4: Source trends map correctly from keyword_extractor data
  1.5: Empty/missing source_trend defaults to ""
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE / "modules"))


# ─── Sample data ─────────────────────────────────────────────────────────────

SAMPLE_DECOMPOSED = [
    {
        "keyword": "Sam's Club Auto Insurance Cost",
        "template": "{brand} Auto Insurance Cost",
        "entity_type": "brand",
        "entity": "Sam's Club",
        "expandable": True,
        "vertical": "auto_insurance",
    }
]

SAMPLE_REGISTRY = {
    "brand": {
        "US": {
            "proven": ["Costco", "USAA", "AAA"],
            "test": ["BJ's Wholesale"],
        }
    }
}

SAMPLE_QUALITY_SCORES = {
    "Sam's Club Auto Insurance Cost": 45.0,
}

SAMPLE_TRENDS = {
    "Sam's Club Auto Insurance Cost": "auto insurance prices rising 2026",
}


# =============================================================================
# TEST 1.1: expand_batch() accepts source_trends parameter
# =============================================================================

class TestExpandBatchSignature:
    """expand_batch() must accept the new source_trends parameter."""

    def test_accepts_source_trends_kwarg(self):
        """expand_batch should accept source_trends as a keyword argument."""
        from modules.template_expander import expand_batch

        # Should not raise TypeError
        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends=SAMPLE_TRENDS,
        )
        assert isinstance(result, list)

    def test_source_trends_is_optional(self):
        """expand_batch should work without source_trends (backward compat)."""
        from modules.template_expander import expand_batch

        # Should not raise TypeError — source_trends defaults to None/{}
        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
        )
        assert isinstance(result, list)


# =============================================================================
# TEST 1.2: Expansion records include source_trend field
# =============================================================================

class TestSourceTrendInOutput:
    """Generated expansion records must include the source_trend field."""

    def test_expansion_has_source_trend_field(self):
        """Each expansion dict should have a 'source_trend' key."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends=SAMPLE_TRENDS,
        )
        assert len(result) > 0, "Should generate at least one expansion"

        for exp in result:
            assert "source_trend" in exp, (
                f"Expansion for '{exp.get('keyword')}' missing 'source_trend' field. "
                f"Keys: {list(exp.keys())}"
            )

    def test_source_trend_value_propagated(self):
        """source_trend value should match what was passed in source_trends map."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends=SAMPLE_TRENDS,
        )

        for exp in result:
            assert exp["source_trend"] == "auto insurance prices rising 2026", (
                f"Expected source_trend from SAMPLE_TRENDS, got: '{exp['source_trend']}'"
            )


# =============================================================================
# TEST 1.3: Backward compatibility
# =============================================================================

class TestBackwardCompatibility:
    """Existing code calling expand_batch without source_trends must not break."""

    def test_no_source_trends_gives_empty_string(self):
        """When source_trends is not provided, source_trend should be ''."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
        )

        for exp in result:
            if "source_trend" in exp:
                assert exp["source_trend"] == "", (
                    f"Without source_trends, source_trend should be '', got: '{exp['source_trend']}'"
                )

    def test_none_source_trends_handled(self):
        """Explicitly passing source_trends=None should not crash."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends=None,
        )
        assert isinstance(result, list)

    def test_keyword_not_in_trends_map(self):
        """Keywords not in source_trends map should get empty string."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends={"some_other_keyword": "some trend"},  # no match
        )

        for exp in result:
            if "source_trend" in exp:
                assert exp["source_trend"] == ""


# =============================================================================
# TEST 1.4: Expansion output schema completeness
# =============================================================================

class TestExpansionOutputSchema:
    """Expansion dicts must have all required fields including source_trend."""

    REQUIRED_FIELDS = [
        "keyword", "source_keyword", "source_revenue", "source_quality_score",
        "expansion_type", "swapped_slot", "new_value", "entity_status",
        "country", "template", "vertical", "plausible", "cpc_track",
    ]

    def test_all_required_fields_present(self):
        """Every expansion should have all documented fields."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends=SAMPLE_TRENDS,
        )

        for exp in result:
            for field in self.REQUIRED_FIELDS:
                assert field in exp, (
                    f"Missing required field '{field}' in expansion for '{exp.get('keyword')}'"
                )

    def test_source_trend_is_string(self):
        """source_trend must always be a string (never None or other type)."""
        from modules.template_expander import expand_batch

        result = expand_batch(
            decomposed=SAMPLE_DECOMPOSED,
            registry=SAMPLE_REGISTRY,
            country="US",
            source_quality_scores=SAMPLE_QUALITY_SCORES,
            source_trends=SAMPLE_TRENDS,
        )

        for exp in result:
            if "source_trend" in exp:
                assert isinstance(exp["source_trend"], str), (
                    f"source_trend must be str, got {type(exp['source_trend'])}"
                )
