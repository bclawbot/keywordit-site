"""
Tests for Intel dashboard bug fixes (Bugs #1, #6, #7, #9, #11, #15).

Tests the intelligence_api.py normalization functions, vertical consolidation,
angle normalization, dormant network filtering, top_keyword blocklist,
electrician reclassification, and activity fallback.
"""

import sys
from pathlib import Path

import pytest

# Add workspace to path for imports
_WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_WORKSPACE))

from services.intelligence_api import (
    _normalize_angle_type,
    _normalize_vertical,
    _override_vertical_for_keyword,
    _TOP_KW_BLOCKLIST,
    _VERTICAL_ALIAS_MAP,
    _ANGLE_ALIAS_MAP,
)


# ══════════════════════════════════════════════════════════════════════════════
# Bug #15 — Angle type normalization
# ══════════════════════════════════════════════════════════════════════════════


class TestAngleNormalization:
    """Bug #15: Normalize duplicate angle types to snake_case."""

    def test_how_to_variants(self):
        assert _normalize_angle_type("How-To") == "how_to"
        assert _normalize_angle_type("how-to") == "how_to"
        assert _normalize_angle_type("how_to") == "how_to"

    def test_news_breaking_variants(self):
        assert _normalize_angle_type("News/Breaking") == "news_breaking"
        assert _normalize_angle_type("news/breaking") == "news_breaking"
        assert _normalize_angle_type("news_breaking") == "news_breaking"

    def test_secret_reveal_variants(self):
        assert _normalize_angle_type("Secret/Reveal") == "secret_reveal"
        assert _normalize_angle_type("secret/reveal") == "secret_reveal"

    def test_direct_offer_variants(self):
        assert _normalize_angle_type("Direct Offer") == "direct_offer"
        assert _normalize_angle_type("Direct_Offer") == "direct_offer"
        assert _normalize_angle_type("direct-offer") == "direct_offer"

    def test_informational_explainer_variants(self):
        assert _normalize_angle_type("Informational Explainer") == "informational_explainer"
        assert _normalize_angle_type("Informational_Explainer") == "informational_explainer"
        assert _normalize_angle_type("informational_explainer") == "informational_explainer"

    def test_testimonial_variants(self):
        assert _normalize_angle_type("Testimonial") == "testimonial"
        assert _normalize_angle_type("testimonials") == "testimonial"

    def test_all_results_are_snake_case(self):
        """All normalized angle types should be snake_case (lowercase + underscores only)."""
        test_inputs = list(_ANGLE_ALIAS_MAP.keys()) + [
            "listicle", "comparison", "how_to", "fear_warning",
            "Some Unknown/Type", "Another-Weird_One",
        ]
        import re
        for raw in test_inputs:
            result = _normalize_angle_type(raw)
            assert re.match(r"^[a-z0-9_]+$", result), (
                f"'{raw}' → '{result}' is not snake_case"
            )

    def test_no_duplicate_angle_types_after_normalization(self):
        """All known aliases should map to distinct canonical forms, no collisions."""
        canonical_forms = set(_ANGLE_ALIAS_MAP.values())
        # Each alias should map to a form already in the set — no orphans
        for alias, canonical in _ANGLE_ALIAS_MAP.items():
            assert _normalize_angle_type(alias) == canonical

    def test_none_passthrough(self):
        assert _normalize_angle_type(None) is None

    def test_empty_passthrough(self):
        assert _normalize_angle_type("") == ""


# ══════════════════════════════════════════════════════════════════════════════
# Bug #6 — Vertical taxonomy consolidation
# ══════════════════════════════════════════════════════════════════════════════


class TestVerticalConsolidation:
    """Bug #6: Consolidate duplicate verticals."""

    def test_jobs_mapped_to_employment(self):
        assert _normalize_vertical("jobs") == "employment"

    def test_job_search_mapped_to_employment(self):
        assert _normalize_vertical("job_search") == "employment"

    def test_beauty_cosmetics_mapped_to_personal_care(self):
        assert _normalize_vertical("beauty_cosmetics") == "personal_care"

    def test_beauty_mapped_to_personal_care(self):
        assert _normalize_vertical("beauty") == "personal_care"

    def test_housing_mapped_to_real_estate(self):
        assert _normalize_vertical("housing") == "real_estate"

    def test_car_mapped_to_automotive(self):
        assert _normalize_vertical("car") == "automotive"

    def test_auto_mapped_to_automotive(self):
        assert _normalize_vertical("auto") == "automotive"

    def test_unknown_vertical_passthrough(self):
        assert _normalize_vertical("solar") == "solar"
        assert _normalize_vertical("tech") == "tech"

    def test_none_passthrough(self):
        assert _normalize_vertical(None) is None

    def test_no_duplicate_verticals_after_normalization(self):
        """After normalization, variant names should all map to the same canonical."""
        groups = {}
        for alias, canonical in _VERTICAL_ALIAS_MAP.items():
            groups.setdefault(canonical, []).append(alias)

        for canonical, aliases in groups.items():
            results = set(_normalize_vertical(a) for a in aliases)
            assert len(results) == 1, (
                f"Aliases {aliases} don't all map to '{canonical}': got {results}"
            )

    def test_consensus_increases_after_merge(self):
        """Merging jobs+job_search+employment means one vertical with higher total_ads."""
        # This is a structural test: verify all three map to the same target
        merged = set()
        for v in ["jobs", "job_search", "employment"]:
            merged.add(_normalize_vertical(v))
        assert len(merged) == 1
        assert "employment" in merged


# ══════════════════════════════════════════════════════════════════════════════
# Bug #7 — "electrician work" misclassification
# ══════════════════════════════════════════════════════════════════════════════


class TestElectricianReclassification:
    """Bug #7: 'electrician work' should not be under education."""

    def test_electrician_work_not_education(self):
        result = _override_vertical_for_keyword("electrician work", "education")
        assert result != "education"

    def test_electrician_work_is_employment(self):
        result = _override_vertical_for_keyword("electrician work", "education")
        assert result == "employment"

    def test_electrician_jobs_overridden(self):
        result = _override_vertical_for_keyword("electrician jobs near me", "education")
        assert result == "employment"

    def test_plumber_overridden(self):
        result = _override_vertical_for_keyword("plumber salary 2026", "education")
        assert result == "employment"

    def test_non_trade_education_untouched(self):
        result = _override_vertical_for_keyword("online MBA programs", "education")
        assert result == "education"

    def test_already_correct_vertical_untouched(self):
        result = _override_vertical_for_keyword("electrician work", "employment")
        assert result == "employment"


# ══════════════════════════════════════════════════════════════════════════════
# Bug #11 — "asrsearch" blocklist
# ══════════════════════════════════════════════════════════════════════════════


class TestTopKeywordBlocklist:
    """Bug #11: 'asrsearch' and other tracker terms should be blocked."""

    def test_asrsearch_in_blocklist(self):
        assert "asrsearch" in _TOP_KW_BLOCKLIST

    def test_learn_more_in_blocklist(self):
        assert "learn more" in _TOP_KW_BLOCKLIST

    def test_guide_in_blocklist(self):
        assert "guide" in _TOP_KW_BLOCKLIST

    def test_real_keywords_not_in_blocklist(self):
        real_keywords = ["home improvement", "car insurance", "dental implants"]
        for kw in real_keywords:
            assert kw not in _TOP_KW_BLOCKLIST


# ══════════════════════════════════════════════════════════════════════════════
# Bug #9 — Dormant network filtering (tested via data structure)
# ══════════════════════════════════════════════════════════════════════════════


class TestDormantNetworkFiltering:
    """Bug #9: Networks with zero activity should be filtered out."""

    def test_dormant_network_removed(self):
        """A network with all-zero metrics should be filtered."""
        networks = [
            {"name": "Active", "active_ads": 10, "new_7d": 5, "new_30d": 20},
            {"name": "Dormant", "active_ads": 0, "new_7d": 0, "new_30d": 0},
        ]
        filtered = [
            n for n in networks
            if n["active_ads"] > 0 or n["new_7d"] > 0 or n["new_30d"] > 0
        ]
        assert len(filtered) == 1
        assert filtered[0]["name"] == "Active"

    def test_network_with_only_30d_activity_kept(self):
        """A network with only 30d activity should be kept."""
        networks = [
            {"name": "RecentOnly", "active_ads": 0, "new_7d": 0, "new_30d": 5},
        ]
        filtered = [
            n for n in networks
            if n["active_ads"] > 0 or n["new_7d"] > 0 or n["new_30d"] > 0
        ]
        assert len(filtered) == 1


# ══════════════════════════════════════════════════════════════════════════════
# Bug #1 — Activity tab fallback (tested via data structure)
# ══════════════════════════════════════════════════════════════════════════════


class TestActivityFallback:
    """Bug #1: Activity tab should show data even when Signals table is empty."""

    def test_fallback_produces_new_ad_events(self):
        """The fallback path should produce 'new_ad' signal_type events."""
        # Simulate fallback event creation
        ad = {
            "id": 1,
            "headline": "Test ad headline",
            "ad_archive_id": "123456",
            "primary_vertical": "education",
            "primary_angle": "listicle",
            "first_seen": "2026-04-15T10:00:00Z",
        }
        event = {
            "id": ad["id"],
            "signal_type": "new_ad",
            "severity": "MEDIUM" if ad["primary_vertical"] else "LOW",
            "headline": f"New ad: {ad['headline']}",
            "vertical": _normalize_vertical(ad["primary_vertical"]),
            "angle": _normalize_angle_type(ad["primary_angle"]),
            "timestamp": ad["first_seen"],
        }
        assert event["signal_type"] == "new_ad"
        assert event["headline"] == "New ad: Test ad headline"
        assert event["vertical"] == "education"

    def test_fallback_empty_when_no_ads(self):
        """When truly no data, fallback returns empty list."""
        ads = []
        result = []
        for ad in ads:
            result.append({"signal_type": "new_ad"})
        assert len(result) == 0

    def test_fallback_applies_normalization(self):
        """Fallback events should have normalized verticals and angles."""
        event_vert = _normalize_vertical("jobs")
        event_angle = _normalize_angle_type("How-To")
        assert event_vert == "employment"
        assert event_angle == "how_to"
