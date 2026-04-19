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
