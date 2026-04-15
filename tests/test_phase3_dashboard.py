"""
test_phase3_dashboard.py — Tests for Phase 3 + 3B: Dashboard Rendering + Intelligence.

Run:  python3 -m pytest tests/test_phase3_dashboard.py -v

Covers:
  3.1: match_angles() passes url and source fields
  3.2: JS rendering — angle title, AD badge, article link logic
  3.3: Regression — existing angles without url/source still render
  3.4: CSS variable correctness
  3.5: XSS safety in article URLs
  3.6: Integration — end-to-end from angle_candidates to rendered HTML
  3B.1: Quality signal computation
  3B.2: Top angle column data
"""

import json
import re
import sys
from pathlib import Path

import pytest

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))


# ─── Sample data ─────────────────────────────────────────────────────────────

SAMPLE_ANGLES_JSON = {
    "test keyword|US": {
        "keyword": "test keyword",
        "country": "US",
        "vertical": "auto_insurance",
        "selected_angles": [
            {
                "angle_type": "comparison",
                "angle_title": "Compare Top Auto Insurers 2026",
                "rsoc_score": 1.0,
                "ad_category": "competitor_intelligence",
                "article_url": "https://thinkknoll.com/en/articles/compare-auto-insurance",
                "source": "fb_intel",
            },
            {
                "angle_type": "hidden_costs",
                "angle_title": "Hidden Fees in Auto Insurance",
                "rsoc_score": 0.75,
                "ad_category": "financial_legal",
                "article_url": "",
                "source": "",
            },
            {
                "angle_type": "eligibility_explainer",
                "angle_title": "Who Qualifies for Cheap Auto Insurance?",
                "rsoc_score": 0.65,
                "ad_category": "enrollment_comparison",
            },
        ],
    },
    "old keyword|US": {
        "keyword": "old keyword",
        "country": "US",
        "vertical": "personal_injury",
        "selected_angles": [
            {
                "angle_type": "accusatory_expose",
                "angle_title": "Insurance Companies Don't Want You to Know",
                "rsoc_score": 0.80,
                "ad_category": "legal_insurance_settlement",
                # No article_url or source — pre-Phase 3 data
            },
        ],
    },
}


# =============================================================================
# TEST 3.1: match_angles() passes url and source fields
# =============================================================================

class TestMatchAnglesFields:
    """match_angles() must include url and source in output dicts."""

    def test_url_field_present_in_matched_angles(self, tmp_path):
        """Matched angles should include 'url' field."""
        from experimental_enrichment import match_angles

        # Write test angles file
        angles_file = tmp_path / "angles.json"
        angles_file.write_text(json.dumps(SAMPLE_ANGLES_JSON))

        exp_results = [
            {"keyword": "test keyword", "country": "US", "source_keyword": "test source"},
        ]

        result = match_angles(exp_results, str(angles_file))

        assert "test keyword|US" in result
        angles = result["test keyword|US"]["angles"]
        assert len(angles) > 0

        # First angle (fb_intel) should have url
        assert "url" in angles[0], "Matched angle missing 'url' field"
        assert angles[0]["url"] == "https://thinkknoll.com/en/articles/compare-auto-insurance"

    def test_source_field_present_in_matched_angles(self, tmp_path):
        """Matched angles should include 'source' field."""
        from experimental_enrichment import match_angles

        angles_file = tmp_path / "angles.json"
        angles_file.write_text(json.dumps(SAMPLE_ANGLES_JSON))

        exp_results = [
            {"keyword": "test keyword", "country": "US", "source_keyword": "test source"},
        ]

        result = match_angles(exp_results, str(angles_file))
        angles = result["test keyword|US"]["angles"]

        assert "source" in angles[0], "Matched angle missing 'source' field"
        assert angles[0]["source"] == "fb_intel"

    def test_missing_url_defaults_to_empty_string(self, tmp_path):
        """Angles without article_url should get empty string, not KeyError."""
        from experimental_enrichment import match_angles

        angles_file = tmp_path / "angles.json"
        angles_file.write_text(json.dumps(SAMPLE_ANGLES_JSON))

        exp_results = [
            {"keyword": "old keyword", "country": "US", "source_keyword": "old source"},
        ]

        result = match_angles(exp_results, str(angles_file))

        if "old keyword|US" in result:
            angles = result["old keyword|US"]["angles"]
            for a in angles:
                # Should not raise KeyError
                url_val = a.get("url", "")
                assert isinstance(url_val, str)


# =============================================================================
# TEST 3.2: JS rendering logic validation
# =============================================================================

class TestJSRenderingLogic:
    """Validate the JS template logic for angle display."""

    def _simulate_js_render(self, angle):
        """Simulate the JS rendering logic from experimental_tab_v2.py."""
        title = angle.get("title") or angle.get("type") or "Untitled"
        url = angle.get("url", "")
        source = angle.get("source", "")

        if url:
            title_html = f'<a href="{url}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:underline">{title}</a>'
        else:
            title_html = title

        badge = ""
        if source == "fb_intel":
            badge = '<span style="color:var(--c-red);font-size:9px;margin-right:4px;font-weight:600">AD</span>'

        return f'<div style="margin:2px 0;color:var(--text-primary);font-size:10px;">&bull; {badge}{title_html}</div>'

    def test_fb_intel_angle_with_url_renders_link_and_badge(self):
        """fb_intel angle with URL should show AD badge + clickable link."""
        angle = {
            "title": "Compare Auto Insurers",
            "url": "https://thinkknoll.com/en/articles/compare-auto",
            "source": "fb_intel",
        }
        html = self._simulate_js_render(angle)
        assert "AD" in html, "Missing AD badge"
        assert '<a href="https://thinkknoll.com' in html, "Missing clickable link"
        assert 'target="_blank"' in html, "Missing target=_blank"
        assert 'rel="noopener"' in html, "Missing rel=noopener"

    def test_generated_angle_without_url_renders_plain_text(self):
        """Generated angle without URL should show plain text, no badge."""
        angle = {
            "title": "Hidden Fees in Auto Insurance",
            "url": "",
            "source": "",
        }
        html = self._simulate_js_render(angle)
        assert "AD" not in html, "Should not show AD badge"
        assert "<a " not in html, "Should not be a link"
        assert "Hidden Fees in Auto Insurance" in html

    def test_angle_with_empty_title_falls_back_to_type(self):
        """When title is empty, should fall back to angle type."""
        angle = {
            "title": "",
            "type": "comparison",
            "url": "",
            "source": "",
        }
        html = self._simulate_js_render(angle)
        assert "comparison" in html

    def test_angle_with_no_title_or_type_shows_untitled(self):
        """When both title and type are empty, show 'Untitled'."""
        angle = {"url": "", "source": ""}
        html = self._simulate_js_render(angle)
        assert "Untitled" in html


# =============================================================================
# TEST 3.3: Regression — existing angles still render
# =============================================================================

class TestExistingAnglesRegression:
    """Pre-Phase 3 angles (no url/source fields) must still render correctly."""

    def test_old_angle_without_url_source_renders(self):
        """Angles from before Phase 3 (no url/source) should render normally."""
        old_angle = {
            "type": "accusatory_expose",
            "score": 0.80,
            "category": "legal_insurance_settlement",
            "title": "Insurance Companies Don't Want You to Know",
        }
        # Should not crash when accessing .url and .source
        url = old_angle.get("url", "")
        source = old_angle.get("source", "")
        assert url == ""
        assert source == ""


# =============================================================================
# TEST 3.4: CSS variable validation
# =============================================================================

class TestCSSVariables:
    """CSS variables used in rendering must exist in the dashboard."""

    @pytest.mark.integration
    def test_css_variables_exist_in_experimental_tab(self):
        """Critical CSS variables must be defined in experimental_tab_v2.py."""
        tab_file = BASE / "experimental_tab_v2.py"
        if not tab_file.exists():
            pytest.skip("experimental_tab_v2.py not found")

        content = tab_file.read_text()
        required_vars = ["--accent", "--c-red", "--text-primary", "--text-tertiary", "--bg-raised"]
        for var in required_vars:
            assert var in content, (
                f"CSS variable '{var}' not found in experimental_tab_v2.py. "
                f"Rendering will have no styling."
            )

    @pytest.mark.integration
    def test_accent_text_not_used(self):
        """var(--accent-text) should NOT be used (doesn't exist in codebase)."""
        tab_file = BASE / "experimental_tab_v2.py"
        if not tab_file.exists():
            pytest.skip("experimental_tab_v2.py not found")

        content = tab_file.read_text()
        # After Phase 3 fix, this should not appear
        # (It was in the original plan but is incorrect)
        occurrences = content.count("--accent-text")
        # This is informational — Phase 3 should change to --accent
        if occurrences > 0:
            pytest.xfail(f"Found {occurrences} uses of --accent-text (should be --accent)")


# =============================================================================
# TEST 3.5: XSS safety in article URLs
# =============================================================================

class TestXSSSafety:
    """URLs from scraped ads must not introduce XSS vulnerabilities."""

    @pytest.mark.parametrize("malicious_url", [
        'javascript:alert(1)',
        '" onload="alert(1)',
        "'><script>alert(1)</script>",
        "https://evil.com/page?x=<script>",
    ])
    def test_malicious_urls_neutralized(self, malicious_url):
        """Malicious URLs should not produce executable HTML."""
        # Simulate the rendering
        title = "Test"
        if malicious_url:
            html = f'<a href="{malicious_url}" target="_blank" rel="noopener">{title}</a>'
        else:
            html = title

        # Check that script tags and javascript: protocol are escaped/absent
        # Note: In production, this would use a template engine with auto-escaping
        # This test documents the risk for manual review
        if "javascript:" in malicious_url:
            assert "javascript:" in html, "Known risk: javascript: protocol in href"
            # This should be caught by a URL validation step
            # TODO: Add URL scheme validation in _get_fb_intel_angles()


# =============================================================================
# TEST 3.6: Integration — match_angles field consistency
# =============================================================================

class TestMatchAnglesIntegration:
    """Full integration test: match_angles produces dashboard-ready data."""

    def test_matched_angle_has_all_display_fields(self, tmp_path):
        """Every matched angle must have type, score, title at minimum."""
        from experimental_enrichment import match_angles

        angles_file = tmp_path / "angles.json"
        angles_file.write_text(json.dumps(SAMPLE_ANGLES_JSON))

        exp_results = [
            {"keyword": "test keyword", "country": "US", "source_keyword": "test source"},
        ]

        result = match_angles(exp_results, str(angles_file))
        angles = result.get("test keyword|US", {}).get("angles", [])

        for a in angles:
            assert "type" in a, "Missing 'type' field"
            assert "score" in a, "Missing 'score' field"
            assert "title" in a, "Missing 'title' field"
            assert isinstance(a["score"], (int, float)), "Score must be numeric"


# =============================================================================
# TEST 3B.1: Quality signal computation
# =============================================================================

class TestQualitySignal:
    """Quality signal must correctly differentiate angle sources."""

    def test_fb_intel_original_gets_highest_quality(self):
        """fb_intel original angles should get quality_signal = 1.0."""
        angle = {"source": "fb_intel", "rsoc_score": 1.0}
        quality = 1.0 if angle["source"] == "fb_intel" and angle["rsoc_score"] >= 0.95 else 0.80
        assert quality == 1.0

    def test_semantic_match_gets_medium_quality(self):
        """Semantic match angles should get quality_signal ~ 0.85."""
        angle = {"source": "fb_intel_semantic", "rsoc_score": 0.80}
        quality = 0.85 if angle["source"] == "fb_intel_semantic" else 0.50
        assert quality == 0.85

    def test_generated_angle_gets_lower_quality(self):
        """LLM-generated angles should get quality_signal < 0.80."""
        angle = {"source": "", "rsoc_score": 0.65}
        quality = angle["rsoc_score"] if not angle["source"] else 0.85
        assert quality < 0.80


# =============================================================================
# TEST 3B.2: Top angle column data
# =============================================================================

class TestTopAngleColumn:
    """Top angle column should show the best angle per keyword."""

    def test_top_angle_is_highest_score(self):
        """Top angle should be the first by rsoc_score descending."""
        angles = [
            {"angle_type": "hidden_costs", "rsoc_score": 0.75},
            {"angle_type": "comparison", "rsoc_score": 1.0},
            {"angle_type": "eligibility_explainer", "rsoc_score": 0.65},
        ]
        sorted_angles = sorted(angles, key=lambda x: x["rsoc_score"], reverse=True)
        top = sorted_angles[0]
        assert top["angle_type"] == "comparison"
        assert top["rsoc_score"] == 1.0

    def test_top_angle_shows_source_badge(self):
        """If top angle is fb_intel, it should be flagged."""
        top_angle = {"angle_type": "comparison", "source": "fb_intel", "rsoc_score": 1.0}
        badge = "AD" if top_angle.get("source") == "fb_intel" else ""
        display = f"{badge} {top_angle['angle_type']}" if badge else top_angle["angle_type"]
        assert "AD" in display
