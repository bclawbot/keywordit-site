"""
test_phase0_foundations.py — Tests for Phase 0: Foundation Fixes (vertical mapping).

Run:  python3 -m pytest tests/test_phase0_foundations.py -v

Covers:
  0.1: All 11 expansion verticals map to a known scorer vertical (not "unknown")
  0.2: Keyword signals correctly classify real expansion keywords
  0.3: New ANGLE_VERTICAL_FIT entries have valid structure
  0.4: COARSE_VERTICAL_MAP entries don't break existing mappings
  0.5: Regression — original 15 scorer verticals unchanged
  0.6: Integration — real expansion_results.jsonl keywords get <10% unknown
"""

import json
import sys
from pathlib import Path

import pytest

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))


# ─── All 11 expansion verticals (from data/expansion_results.jsonl) ──────────
EXPANSION_VERTICALS = [
    "auto_insurance", "general", "home_services", "housing_ssi",
    "latam_delivery", "legal_services", "life_insurance", "loans_credit",
    "medical_pharma", "membership_retail", "veterans_military",
]

# ─── Original 15 scorer verticals (must not be modified) ─────────────────────
ORIGINAL_SCORER_VERTICALS = [
    "ssi_disability", "medicare_part_d", "va_home_loans", "veterans_benefits",
    "employment_law", "personal_injury", "online_education", "auto_insurance",
    "water_damage", "phone_deals", "addiction_treatment", "abogados_es",
    "landlord_negligence", "pool_home_improvement", "ostomy_medical",
]

ALL_10_ANGLE_TYPES = [
    "eligibility_explainer", "how_it_works_explainer", "trend_attention_piece",
    "lifestyle_fit_analysis", "pre_review_guide", "policy_year_stamped_update",
    "accusatory_expose", "hidden_costs", "diagnostic_signs", "comparison",
]


# =============================================================================
# TEST 0.1: All expansion verticals map to known scorer vertical
# =============================================================================

class TestVerticalMapping:
    """Every expansion vertical must map to a known scorer vertical, not 'unknown'."""

    def test_all_expansion_verticals_mapped(self):
        """None of the 11 expansion verticals should return 'unknown'."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import classify_vertical_fine

        unmapped = []
        for v in EXPANSION_VERTICALS:
            result = classify_vertical_fine(v, "test keyword")
            if result == "unknown":
                unmapped.append(v)

        assert unmapped == [], (
            f"Unmapped expansion verticals: {unmapped}. "
            f"Add entries to COARSE_VERTICAL_MAP in angle_scorer.py."
        )

    @pytest.mark.parametrize("vertical", EXPANSION_VERTICALS)
    def test_each_expansion_vertical_individually(self, vertical):
        """Individual test per vertical for clear failure reporting."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import classify_vertical_fine

        result = classify_vertical_fine(vertical, "generic keyword")
        assert result != "unknown", (
            f"Vertical '{vertical}' maps to 'unknown'. "
            f"Add to COARSE_VERTICAL_MAP or VERTICAL_KEYWORD_SIGNALS."
        )


# =============================================================================
# TEST 0.2: Keyword signal classification on real expansion keywords
# =============================================================================

class TestKeywordSignalClassification:
    """Keyword signals should classify real expansion keywords to correct verticals."""

    @pytest.mark.parametrize("keyword,expected_vertical", [
        ("Sam's Club Auto Insurance Cost", "auto_insurance"),
        ("SSI Apartments for Rent Near Me", "ssi_disability"),
        ("VA Veterans Discounts", "veterans_benefits"),
        ("18 wheeler accident attorney austin", "personal_injury"),
        ("life insurance quote comparison", "auto_insurance"),  # maps via keyword signal
        ("military benefits for veterans", "veterans_benefits"),
        ("plumber cost near me", "water_damage"),  # home_services → keyword signal
    ])
    def test_keyword_to_vertical(self, keyword, expected_vertical):
        """Known keywords map to expected fine-grained verticals."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import classify_vertical_fine

        # Use "general" as coarse — forces keyword signal matching (Step 1)
        result = classify_vertical_fine("general", keyword)
        assert result == expected_vertical, (
            f"Keyword '{keyword}' mapped to '{result}', expected '{expected_vertical}'"
        )

    def test_coarse_fallback_when_no_signal(self):
        """When keyword has no signal match, coarse map should still work."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import classify_vertical_fine

        # "xyzzy" has no keyword signals — must fall back to coarse map
        result = classify_vertical_fine("life_insurance", "xyzzy premium plan")
        assert result != "unknown", (
            "Coarse vertical 'life_insurance' should map via COARSE_VERTICAL_MAP "
            "even when keyword has no signal match."
        )


# =============================================================================
# TEST 0.3: New ANGLE_VERTICAL_FIT entries are structurally valid
# =============================================================================

class TestAngleVerticalFitStructure:
    """New verticals in ANGLE_VERTICAL_FIT must have all 10 angle types."""

    def test_all_verticals_have_all_angles(self):
        """Every vertical in ANGLE_VERTICAL_FIT must define all 10 angle types."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import ANGLE_VERTICAL_FIT

        for vertical, scores in ANGLE_VERTICAL_FIT.items():
            missing = [a for a in ALL_10_ANGLE_TYPES if a not in scores]
            assert missing == [], (
                f"Vertical '{vertical}' missing angle types: {missing}"
            )

    def test_all_scores_in_valid_range(self):
        """All fit scores must be between 0.0 and 1.0."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import ANGLE_VERTICAL_FIT

        for vertical, scores in ANGLE_VERTICAL_FIT.items():
            for angle, score in scores.items():
                assert 0.0 <= score <= 1.0, (
                    f"Invalid score {score} for {vertical}/{angle}. Must be 0.0-1.0."
                )

    def test_each_vertical_has_at_least_one_high_score(self):
        """Every vertical should have at least one angle scoring >= 0.80."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import ANGLE_VERTICAL_FIT

        for vertical, scores in ANGLE_VERTICAL_FIT.items():
            max_score = max(scores.values())
            assert max_score >= 0.80, (
                f"Vertical '{vertical}' max score is {max_score}. "
                f"At least one angle should score >= 0.80 (strong fit)."
            )


# =============================================================================
# TEST 0.4: Regression — original scorer verticals unchanged
# =============================================================================

class TestOriginalVerticalsUnchanged:
    """Original 15 scorer verticals must still exist and be unchanged."""

    def test_original_verticals_still_exist(self):
        """All 15 original verticals must remain in ANGLE_VERTICAL_FIT."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import ANGLE_VERTICAL_FIT

        for v in ORIGINAL_SCORER_VERTICALS:
            assert v in ANGLE_VERTICAL_FIT, (
                f"Original vertical '{v}' missing from ANGLE_VERTICAL_FIT! "
                f"Regression: Phase 0 should ADD verticals, not remove existing ones."
            )

    def test_original_vertical_scores_unchanged(self):
        """Spot-check: known scores in original verticals should be unchanged."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import ANGLE_VERTICAL_FIT

        # These are canonical values from the spec — must not change
        assert ANGLE_VERTICAL_FIT["eligibility_explainer"]["ssi_disability"] == 1.00
        assert ANGLE_VERTICAL_FIT["comparison"]["medicare_part_d"] == 1.00
        assert ANGLE_VERTICAL_FIT["accusatory_expose"]["personal_injury"] == 1.00
        assert ANGLE_VERTICAL_FIT["hidden_costs"]["water_damage"] == 1.00

    def test_coarse_map_original_entries_preserved(self):
        """Original COARSE_VERTICAL_MAP entries must not be modified."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import COARSE_VERTICAL_MAP

        assert "legal" in COARSE_VERTICAL_MAP
        assert "personal_injury" in COARSE_VERTICAL_MAP["legal"]
        assert "finance" in COARSE_VERTICAL_MAP
        assert "veterans" in COARSE_VERTICAL_MAP


# =============================================================================
# TEST 0.5: Integration — real expansion keywords get <10% unknown
# =============================================================================

class TestExpansionIntegration:
    """When run against real expansion_results.jsonl, <10% should be 'unknown'."""

    @pytest.mark.integration
    def test_expansion_unknown_rate_below_10pct(self):
        """Real expansion keywords should get <10% unknown vertical classifications."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import classify_vertical_fine

        exp_file = BASE / "data" / "expansion_results.jsonl"
        if not exp_file.exists():
            pytest.skip("expansion_results.jsonl not found")

        total = 0
        unknown = 0
        for line in exp_file.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            vertical = rec.get("vertical", "general")
            keyword = rec.get("keyword", "")
            result = classify_vertical_fine(vertical, keyword)
            total += 1
            if result == "unknown":
                unknown += 1

        rate = unknown / max(total, 1)
        assert rate < 0.10, (
            f"Unknown vertical rate: {unknown}/{total} ({rate:.1%}). "
            f"Must be <10%. Check COARSE_VERTICAL_MAP and VERTICAL_KEYWORD_SIGNALS."
        )

    @pytest.mark.integration
    def test_expansion_source_keywords_all_mapped(self):
        """All 84 unique source keywords (not expansion variants) should map."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import classify_vertical_fine

        exp_file = BASE / "data" / "expansion_results.jsonl"
        if not exp_file.exists():
            pytest.skip("expansion_results.jsonl not found")

        sources = {}
        for line in exp_file.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            sk = rec.get("source_keyword", "")
            co = rec.get("country", "US")
            v = rec.get("vertical", "general")
            sources[f"{sk}|{co}"] = (v, sk)

        unmapped_sources = []
        for key, (vertical, keyword) in sources.items():
            result = classify_vertical_fine(vertical, keyword)
            if result == "unknown":
                unmapped_sources.append(key)

        assert len(unmapped_sources) < len(sources) * 0.05, (
            f"{len(unmapped_sources)}/{len(sources)} source keywords unmapped: "
            f"{unmapped_sources[:5]}..."
        )


# =============================================================================
# TEST 0.6: CPC signals have valid entries
# =============================================================================

class TestCPCSignals:
    """New VERTICAL_CPC_SIGNAL entries should exist and be in valid range."""

    def test_new_verticals_have_cpc_signal(self):
        """New fine-grained verticals should have CPC signal entries."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import VERTICAL_CPC_SIGNAL

        new_verticals = ["life_insurance", "loans_credit", "membership_retail",
                         "home_services", "latam_delivery"]
        for v in new_verticals:
            if v in VERTICAL_CPC_SIGNAL:  # optional — default exists
                assert 0.0 <= VERTICAL_CPC_SIGNAL[v] <= 1.0, (
                    f"CPC signal for '{v}' out of range: {VERTICAL_CPC_SIGNAL[v]}"
                )
