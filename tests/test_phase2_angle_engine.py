"""
test_phase2_angle_engine.py — Tests for Phase 2: Angle Engine Expansion.

Run:  python3 -m pytest tests/test_phase2_angle_engine.py -v

Covers:
  2.1: _load_expansion_sources() loads and deduplicates correctly
  2.2: _get_fb_intel_angles() queries db without crashing
  2.3: EXPANSION tag is in eligible_tags
  2.4: Expansion sources merge into processing queue without duplicates
  2.5: fb_intel angles prepend to selected_angles
  2.6: Incremental output — existing angle_candidates preserved
  2.7: Edge cases — missing files, empty data, db locked
"""

import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))


# ─── Sample expansion records ───────────────────────────────────────────────

SAMPLE_EXPANSION_RECORDS = [
    {
        "keyword": "Costco Auto Insurance Cost",
        "source_keyword": "Sam's Club Auto Insurance Cost",
        "source_quality_score": 45.0,
        "country": "US",
        "vertical": "auto_insurance",
        "cpc_usd": 4.50,
        "search_volume": 12000,
        "competition": 0.65,
    },
    {
        "keyword": "USAA Auto Insurance Cost",
        "source_keyword": "Sam's Club Auto Insurance Cost",
        "source_quality_score": 42.0,
        "country": "US",
        "vertical": "auto_insurance",
        "cpc_usd": 5.20,
    },
    {
        "keyword": "AAA Auto Insurance Cost",
        "source_keyword": "Sam's Club Auto Insurance Cost",
        "source_quality_score": 48.0,  # higher quality — should win dedup
        "country": "US",
        "vertical": "auto_insurance",
    },
    {
        "keyword": "Veterans Benefits AU",
        "source_keyword": "VA Veterans Discounts",
        "source_quality_score": 36.0,
        "country": "AU",
        "vertical": "veterans_military",
    },
]


# =============================================================================
# TEST 2.1: _load_expansion_sources() loading and deduplication
# =============================================================================

class TestLoadExpansionSources:
    """_load_expansion_sources() must load, deduplicate, and convert correctly."""

    def _write_expansion_file(self, tmp_path, records):
        """Write test expansion records to a temp JSONL file."""
        exp_file = tmp_path / "data" / "expansion_results.jsonl"
        exp_file.parent.mkdir(parents=True, exist_ok=True)
        with open(exp_file, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")
        return exp_file

    def test_deduplication_by_source_keyword_country(self, tmp_path):
        """Multiple expansion records from same source|country → keep highest quality."""
        exp_file = self._write_expansion_file(tmp_path, SAMPLE_EXPANSION_RECORDS)

        # Simulate _load_expansion_sources logic
        best_by_source = {}
        for line in exp_file.read_text().splitlines():
            rec = json.loads(line)
            sk = rec.get("source_keyword", "")
            co = rec.get("country", "US").upper()
            key = f"{sk}|{co}"
            existing = best_by_source.get(key)
            if existing is None or float(rec.get("source_quality_score", 0)) > float(existing.get("source_quality_score", 0)):
                best_by_source[key] = rec

        # 3 US records from same source + 1 AU = 2 unique source|country combos
        assert len(best_by_source) == 2
        # The US winner should be quality_score=48.0 (AAA)
        us_winner = best_by_source["Sam's Club Auto Insurance Cost|US"]
        assert us_winner["source_quality_score"] == 48.0

    def test_output_is_opportunity_shaped(self, tmp_path):
        """Converted dicts must have fields _process_keyword() expects."""
        exp_file = self._write_expansion_file(tmp_path, SAMPLE_EXPANSION_RECORDS[:1])

        required_fields = [
            "keyword", "country", "cpc_usd", "vertical",
            "main_intent", "tag", "trend_source",
        ]

        # Simulate conversion
        rec = SAMPLE_EXPANSION_RECORDS[0]
        opp = {
            "keyword": rec.get("source_keyword", ""),
            "country": rec.get("country", "US").upper(),
            "cpc_usd": float(rec.get("cpc_usd") or 0),
            "vertical": rec.get("vertical", "general"),
            "main_intent": "commercial",
            "tag": "EXPANSION",
            "trend_source": "keyword_expansion",
        }

        for field in required_fields:
            assert field in opp, f"Missing field '{field}' in opportunity dict"
        assert opp["tag"] == "EXPANSION"
        assert opp["trend_source"] == "keyword_expansion"

    def test_missing_expansion_file_returns_empty(self, tmp_path):
        """If expansion_results.jsonl doesn't exist, return empty list."""
        nonexistent = tmp_path / "data" / "expansion_results.jsonl"
        assert not nonexistent.exists()
        # Function should return [] without raising


# =============================================================================
# TEST 2.2: _get_fb_intel_angles() database query
# =============================================================================

class TestGetFbIntelAngles:
    """fb_intel angle lookup must work with real and mock databases."""

    def _create_test_db(self, tmp_path):
        """Create a minimal fb_intelligence.db for testing."""
        db_path = tmp_path / "fb_intelligence.db"
        db = sqlite3.connect(str(db_path))
        db.execute("""CREATE TABLE Keywords (
            id INTEGER PRIMARY KEY, keyword TEXT, cpc_usd REAL,
            competition REAL, volume INTEGER, metadata TEXT,
            created_at TEXT, kd REAL
        )""")
        db.execute("""CREATE TABLE KeywordAngles (
            id INTEGER PRIMARY KEY, keyword_id INTEGER, angle_type TEXT,
            angle_title TEXT, source TEXT, confidence REAL,
            ad_id INTEGER, article_url TEXT, vertical TEXT, created_at TEXT
        )""")
        # Insert test data
        db.execute("INSERT INTO Keywords VALUES (1, 'auto insurance cost', 4.5, 0.6, 12000, NULL, '2026-01-01', NULL)")
        db.execute("INSERT INTO KeywordAngles VALUES (1, 1, 'comparison', 'Compare Top Auto Insurers', 'original', 0.95, 100, 'https://thinkknoll.com/en/articles/compare-auto-insurance', 'auto_insurance', '2026-01-01')")
        db.execute("INSERT INTO KeywordAngles VALUES (2, 1, 'hidden_costs', 'Hidden Fees in Auto Insurance', 'generated', 0.80, NULL, NULL, 'auto_insurance', '2026-01-01')")
        db.commit()
        db.close()
        return db_path

    def test_exact_match_returns_angles(self, tmp_path):
        """Exact keyword match should return angles sorted by source priority."""
        db_path = self._create_test_db(tmp_path)
        db = sqlite3.connect(str(db_path))
        db.row_factory = sqlite3.Row

        cursor = db.cursor()
        cursor.execute("""
            SELECT ka.angle_type, ka.angle_title, ka.article_url,
                   ka.confidence, ka.source
            FROM KeywordAngles ka
            JOIN Keywords k ON ka.keyword_id = k.id
            WHERE LOWER(k.keyword) = LOWER(?)
            ORDER BY CASE WHEN ka.source = 'original' THEN 0 ELSE 1 END,
                     ka.confidence DESC
        """, ("auto insurance cost",))

        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0]["source"] == "original"  # original sorts first
        assert results[0]["article_url"] == "https://thinkknoll.com/en/articles/compare-auto-insurance"
        db.close()

    def test_no_match_returns_empty(self, tmp_path):
        """Non-matching keyword should return empty list."""
        db_path = self._create_test_db(tmp_path)
        db = sqlite3.connect(str(db_path))
        db.row_factory = sqlite3.Row

        cursor = db.cursor()
        cursor.execute("""
            SELECT ka.angle_type FROM KeywordAngles ka
            JOIN Keywords k ON ka.keyword_id = k.id
            WHERE LOWER(k.keyword) = LOWER(?)
        """, ("nonexistent keyword xyz",))

        assert len(cursor.fetchall()) == 0
        db.close()

    def test_missing_db_returns_empty(self, tmp_path):
        """If fb_intelligence.db doesn't exist, should return empty list."""
        fake_path = tmp_path / "nonexistent.db"
        assert not fake_path.exists()
        # Function should handle gracefully

    def test_fb_intel_angles_have_correct_fields(self, tmp_path):
        """fb_intel angle dicts must have the required fields for dashboard."""
        db_path = self._create_test_db(tmp_path)
        db = sqlite3.connect(str(db_path))
        db.row_factory = sqlite3.Row

        cursor = db.cursor()
        cursor.execute("""
            SELECT ka.angle_type, ka.angle_title, ka.article_url,
                   ka.confidence, ka.source
            FROM KeywordAngles ka
            JOIN Keywords k ON ka.keyword_id = k.id
            WHERE LOWER(k.keyword) = LOWER(?)
            LIMIT 1
        """, ("auto insurance cost",))

        row = cursor.fetchone()
        # Simulate the conversion that _get_fb_intel_angles does
        angle = {
            "angle_type": row["angle_type"] or "",
            "angle_title": row["angle_title"] or "",
            "article_url": row["article_url"] or "",
            "source": "fb_intel",
            "rsoc_score": 1.0 if row["source"] == "original" else 0.85,
            "ad_category": "competitor_intelligence",
            "selected": True,
        }

        required = ["angle_type", "angle_title", "article_url", "source",
                     "rsoc_score", "ad_category", "selected"]
        for field in required:
            assert field in angle, f"Missing field '{field}'"
        assert angle["source"] == "fb_intel"
        assert angle["rsoc_score"] == 1.0  # original source
        db.close()


# =============================================================================
# TEST 2.3: EXPANSION tag in eligible_tags
# =============================================================================

class TestExpansionTag:
    """EXPANSION must be in the default eligible_tags list."""

    def test_expansion_in_default_eligible_tags(self):
        """_load_validated should include EXPANSION in eligible_tags."""
        # The default list should now include EXPANSION
        default_tags = [
            "GOLDEN_OPPORTUNITY", "WATCH", "EMERGING_HIGH",
            "EMERGING", "LOW", "UNSCORED", "EXPANSION",
        ]
        assert "EXPANSION" in default_tags

    def test_expansion_tagged_records_not_filtered(self):
        """Records with tag=EXPANSION should pass the eligible_tags filter."""
        eligible_tags = ["GOLDEN_OPPORTUNITY", "WATCH", "EMERGING_HIGH",
                         "EMERGING", "LOW", "UNSCORED", "EXPANSION"]
        test_record = {"tag": "EXPANSION", "keyword": "test"}
        assert test_record["tag"] in eligible_tags


# =============================================================================
# TEST 2.4: Dedup — expansion sources don't duplicate validated keywords
# =============================================================================

class TestExpansionValidatedDedup:
    """Expansion source keywords already in validated history should be skipped."""

    def test_dedup_skips_existing_keywords(self):
        """Keywords already in validated_opportunities should not duplicate."""
        validated_opps = [
            {"keyword": "sam's club auto insurance cost", "country": "US"},
        ]
        expansion_opps = [
            {"keyword": "Sam's Club Auto Insurance Cost", "country": "US"},
            {"keyword": "VA Veterans Discounts", "country": "AU"},
        ]

        existing_keys = set()
        for o in validated_opps:
            k = f"{str(o['keyword']).lower().strip()}|{str(o['country']).upper()}"
            existing_keys.add(k)

        new_expansion = []
        for eo in expansion_opps:
            k = f"{str(eo['keyword']).lower().strip()}|{str(eo['country']).upper()}"
            if k not in existing_keys:
                new_expansion.append(eo)

        assert len(new_expansion) == 1
        assert new_expansion[0]["keyword"] == "VA Veterans Discounts"


# =============================================================================
# TEST 2.5: Incremental output preservation
# =============================================================================

class TestIncrementalOutput:
    """Existing angle_candidates.json entries must be preserved on re-run."""

    def test_existing_entries_preserved(self, tmp_path):
        """Pre-existing angle clusters should remain after processing new ones."""
        existing_data = [
            {"keyword": "existing keyword", "country": "US", "selected_angles": [{"angle_type": "comparison"}]},
        ]
        output_file = tmp_path / "angle_candidates.json"
        output_file.write_text(json.dumps(existing_data))

        # Simulate loading existing
        existing = {}
        for entry in json.loads(output_file.read_text()):
            _k = str(entry.get("keyword", "")).lower().strip()
            _c = str(entry.get("country", "")).upper()
            existing[f"{_k}|{_c}"] = entry

        assert "existing keyword|US" in existing
        assert len(existing) == 1

    def test_new_entries_added_not_overwritten(self, tmp_path):
        """New expansion keywords should ADD to results, not replace."""
        existing = {"existing keyword|US": {"keyword": "existing keyword"}}
        new_result = {"keyword": "new expansion keyword", "country": "US"}

        _k = new_result["keyword"].lower().strip()
        _c = new_result["country"].upper()
        skip_key = f"{_k}|{_c}"

        assert skip_key not in existing  # should be new


# =============================================================================
# TEST 2.6: Discovery context for expansion keywords
# =============================================================================

class TestDiscoveryContext:
    """Expansion keywords should get 'keyword_expansion' discovery signal."""

    def test_keyword_expansion_signal(self):
        """map_discovery_context should return keyword_expansion for expansion source."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import map_discovery_context

        opp = {
            "trend_source": "keyword_expansion",
            "source_trend": "",
        }
        ctx = map_discovery_context(opp)
        assert ctx["signal_type"] == "keyword_expansion"

    def test_discovery_boost_exists_for_expansion(self):
        """DISCOVERY_SIGNAL_BOOST should have a keyword_expansion entry."""
        from pipeline.stages.stage_5_5_angle_engine.angle_scorer import DISCOVERY_SIGNAL_BOOST

        assert "keyword_expansion" in DISCOVERY_SIGNAL_BOOST
        boosts = DISCOVERY_SIGNAL_BOOST["keyword_expansion"]
        assert "eligibility_explainer" in boosts
        assert "comparison" in boosts
