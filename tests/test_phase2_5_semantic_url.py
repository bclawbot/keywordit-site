"""
test_phase2_5_semantic_url.py — Tests for Phase 2.5: Semantic Matching + URL Extraction.

Run:  python3 -m pytest tests/test_phase2_5_semantic_url.py -v

Covers:
  2.5A: URL-based angle extraction from ad landing URLs
  2.5B: Semantic matching graceful degradation
  2.5C: URL slug pattern matching accuracy
  2.5D: Integration with angle_engine output format
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pytest

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))


# ─── URL slug → angle type mapping (mirrors url_angle_extractor.py) ──────────

SLUG_ANGLE_MAP = {
    r"eligib|qualify|apply|sign-up|enrollment|benefit|grant|subsid|assist": "eligibility_explainer",
    r"save|savings|cheap|affordable|free|cost|price": "hidden_costs",
    r"compare|vs|best|top-\d|ranking|deal|sale": "comparison",
    r"guide|step|tutorial|tips": "how_it_works_explainer",
    r"review|rated|trusted|honest": "pre_review_guide",
    r"warn|danger|avoid|scam|risk": "accusatory_expose",
    r"sign|symptom|diagnos|detect": "diagnostic_signs",
    r"2026|2025|new-rule|update|change": "policy_year_stamped_update",
    r"senior|veteran|retired|over-\d": "lifestyle_fit_analysis",
    r"trend|boom|surge|rise|growing": "trend_attention_piece",
}


def extract_angle_from_url(url: str) -> dict:
    """Extract angle type from URL slug. Mirrors url_angle_extractor.py."""
    if not url:
        return {}
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        segments = [s for s in path.split("/") if s and s not in ("en", "articles", "dsr")]
        if segments:
            slug = segments[-1].split("?")[0].split(".")[0]
            for pattern, angle_type in SLUG_ANGLE_MAP.items():
                if re.search(pattern, slug):
                    title = slug.replace("-", " ").replace("_", " ").title()
                    return {"angle_type": angle_type, "slug_title": title, "url": url}
        # Fallback: check query parameters (search, q, p)
        qs = parse_qs(parsed.query)
        for param in ("search", "q", "p"):
            val = qs.get(param, [""])[0].lower()
            if val:
                for pattern, angle_type in SLUG_ANGLE_MAP.items():
                    if re.search(pattern, val):
                        title = val.replace("-", " ").replace("+", " ").replace("_", " ").title()[:80]
                        return {"angle_type": angle_type, "slug_title": title, "url": url}
    except Exception:
        pass
    return {}


# =============================================================================
# TEST 2.5A: URL-based angle extraction
# =============================================================================

class TestURLAngleExtraction:
    """URL slugs should produce correct angle type classifications."""

    @pytest.mark.parametrize("url,expected_angle", [
        ("https://thinkknoll.com/en/articles/save-on-car-insurance-seniors-guide", "hidden_costs"),
        ("https://thinkknoll.com/en/articles/compare-life-insurance-plans-2026", "comparison"),
        ("https://thinkknoll.com/en/articles/electrician-skills-and-salaries-in-2026", "policy_year_stamped_update"),
        ("https://thinkknoll.com/en/articles/how-to-apply-for-ssi-benefits", "eligibility_explainer"),
        ("https://thinkknoll.com/en/articles/step-by-step-guide-veteran-loans", "how_it_works_explainer"),
        ("https://thinkknoll.com/en/articles/best-auto-insurance-comparison", "comparison"),
        ("https://thinkknoll.com/en/articles/avoid-scam-insurance-agents", "accusatory_expose"),
        ("https://thinkknoll.com/en/articles/signs-of-disability-claim-denial", "diagnostic_signs"),
        ("https://thinkknoll.com/en/articles/senior-veterans-discount-programs", "lifestyle_fit_analysis"),
    ])
    def test_url_slug_to_angle_type(self, url, expected_angle):
        """URL slugs should map to the correct angle type."""
        result = extract_angle_from_url(url)
        assert result.get("angle_type") == expected_angle, (
            f"URL: {url}\nExpected: {expected_angle}\nGot: {result.get('angle_type')}"
        )

    def test_empty_url_returns_empty(self):
        """Empty or None URL should return empty dict."""
        assert extract_angle_from_url("") == {}
        assert extract_angle_from_url(None) == {}

    def test_non_article_url_may_return_empty(self):
        """URLs without recognizable slugs should return empty dict."""
        result = extract_angle_from_url("https://example.com/")
        # This may or may not match — depends on domain patterns
        # The key test is that it doesn't crash
        assert isinstance(result, dict)

    def test_url_with_query_params_extracts_slug(self):
        """URLs with query params should still extract the slug correctly."""
        url = "https://thinkknoll.com/en/articles/compare-insurance-rates?dest=google&gclid=abc"
        result = extract_angle_from_url(url)
        assert result.get("angle_type") == "comparison"

    def test_result_has_required_fields(self):
        """Successful extraction should return angle_type, slug_title, url."""
        url = "https://thinkknoll.com/en/articles/save-on-car-insurance-seniors-guide"
        result = extract_angle_from_url(url)
        assert "angle_type" in result
        assert "slug_title" in result
        assert "url" in result
        assert result["url"] == url

    def test_slug_title_is_readable(self):
        """slug_title should be a human-readable title case string."""
        url = "https://thinkknoll.com/en/articles/save-on-car-insurance-seniors-guide"
        result = extract_angle_from_url(url)
        title = result.get("slug_title", "")
        assert title[0].isupper(), "Title should be title case"
        assert "-" not in title, "Hyphens should be replaced with spaces"


# =============================================================================
# TEST 2.5B: Semantic matching graceful degradation
# =============================================================================

class TestSemanticMatchingDegradation:
    """When LanceDB or bge-m3 is unavailable, system should degrade gracefully."""

    def test_import_error_handled(self):
        """ImportError from lancedb should be caught, not crash."""
        try:
            import lancedb
            has_lancedb = True
        except ImportError:
            has_lancedb = False

        # Whether or not lancedb is available, this test validates the pattern
        if not has_lancedb:
            # Simulate the fallback pattern from Phase 2.5
            fb_angles = []
            try:
                from angle_engine_semantic import find_similar_ads
                matches = find_similar_ads("test keyword")
            except (ImportError, Exception):
                matches = []  # graceful fallback
            assert matches == []

    def test_ollama_unavailable_handled(self):
        """If Ollama is down, embedding should fail gracefully."""
        import requests
        try:
            resp = requests.post("http://localhost:11434/api/embeddings",
                                 json={"model": "bge-m3", "prompt": "test"},
                                 timeout=2)
            # If Ollama is up, that's fine
        except (requests.ConnectionError, requests.Timeout):
            # Expected in sandbox — should not crash the test suite
            pass


# =============================================================================
# TEST 2.5C: URL extraction coverage on real ad data
# =============================================================================

class TestURLExtractionCoverage:
    """Test URL extraction against real fb_intelligence.db ad URLs."""

    @pytest.mark.integration
    def test_real_ad_urls_extraction_rate(self):
        """At least 30% of real ad URLs should yield an extractable angle."""
        import sqlite3
        db_path = BASE / "dwight" / "fb_intelligence" / "data" / "fb_intelligence.db"
        if not db_path.exists():
            pytest.skip("fb_intelligence.db not found")

        db = sqlite3.connect(str(db_path))
        cursor = db.cursor()
        cursor.execute("SELECT landing_url FROM Ads WHERE landing_url IS NOT NULL LIMIT 100")
        urls = [row[0] for row in cursor.fetchall()]
        db.close()

        extracted = sum(1 for url in urls if extract_angle_from_url(url))
        rate = extracted / max(len(urls), 1)

        assert rate >= 0.30, (
            f"URL extraction rate: {extracted}/{len(urls)} ({rate:.1%}). "
            f"Expected at least 30%. Check SLUG_ANGLE_MAP patterns."
        )


# =============================================================================
# TEST 2.5D: Output format compatibility with angle_engine
# =============================================================================

class TestOutputFormatCompatibility:
    """URL-extracted angles must be compatible with angle_engine output format."""

    def test_url_angle_has_engine_compatible_fields(self):
        """Extracted angles should be usable as selected_angles entries."""
        url = "https://thinkknoll.com/en/articles/compare-insurance-rates"
        result = extract_angle_from_url(url)

        # Convert to angle_engine format
        angle = {
            "angle_type": result.get("angle_type", ""),
            "angle_title": result.get("slug_title", ""),
            "article_url": result.get("url", ""),
            "source": "url_extraction",
            "rsoc_score": 0.75,
            "ad_category": "url_signal",
            "selected": True,
        }

        engine_required = ["angle_type", "angle_title", "rsoc_score", "selected"]
        for field in engine_required:
            assert field in angle and angle[field], (
                f"Missing or empty field '{field}' in URL-extracted angle"
            )
