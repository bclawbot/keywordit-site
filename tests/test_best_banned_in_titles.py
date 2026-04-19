"""Compliance: the standalone word "best" must not appear in generated titles
or in the static templates that feed angle generation."""
import re
from pathlib import Path


def test_best_not_in_template_json():
    tpl = Path(__file__).resolve().parent.parent / "data" / "angle_templates.json"
    text = tpl.read_text(encoding="utf-8")
    assert not re.search(r"\bbest\b", text, re.IGNORECASE), \
        "data/angle_templates.json still contains the standalone word 'best'"


def test_best_blocked_by_validator():
    from pipeline.stages.stage_5_5_angle_engine.title_generator import _validate_llm_title

    title, valid = _validate_llm_title("Best Car Insurance in 2026")
    assert not valid and title is None, "'Best ...' title should fail validation"

    title, valid = _validate_llm_title("Car Insurance Options in 2026")
    assert valid and title is not None, "Clean title should pass validation"

    # Whole-word — words that merely contain the substring 'best' must still pass
    title, valid = _validate_llm_title("How to Bestow Trust in Advisors in 2026")
    assert valid and title is not None, "'Bestow' must not be false-positived"


def test_fb_intel_best_angles_dropped():
    """fb_intel competitor angles are injected with pre-baked titles from real
    FB ad copy. STEP 9 §6 rule applies to them too — drop any that contain the
    standalone word 'best'."""
    from angle_engine import _filter_best_from_fb_angles

    input_angles = [
        {"angle_title": "How to Get the Best Car Insurance", "angle_type": "How-To"},
        {"angle_title": "Car Insurance Options for 2026",    "angle_type": "Comparison"},
        {"angle_title": "Top Rated Policies — Best Coverage Guide", "angle_type": "How-To"},
        {"angle_title": "Bestow a Legacy with Term Life",   "angle_type": "Comparison"},
    ]
    kept = _filter_best_from_fb_angles(input_angles)
    kept_titles = [a["angle_title"] for a in kept]

    assert "Car Insurance Options for 2026" in kept_titles
    assert "Bestow a Legacy with Term Life" in kept_titles, \
        "'Bestow' is a different word and must survive the whole-word filter"
    assert "How to Get the Best Car Insurance" not in kept_titles
    assert "Top Rated Policies — Best Coverage Guide" not in kept_titles
    assert len(kept) == 2
