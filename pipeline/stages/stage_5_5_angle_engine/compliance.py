"""
compliance.py — RAF compliance scanning for generated articles.

Scans article text for patterns documented as RAF violation triggers.
Sources: TheOptimizer.io, Coinis, Clickflare, Google AdSense Help Center.

Risk levels:
    CRITICAL — blocks publication unconditionally (no config override)
    HIGH     — blocks publication (configurable via block_high in angle_engine.yaml)
    MEDIUM   — logs warning, allows publication
    LOW      — no action

See spec Section 7.2 for documented violation patterns.
"""
import re
from dataclasses import dataclass, field


@dataclass
class ComplianceReport:
    passed: bool
    violations: list = field(default_factory=list)   # CRITICAL + HIGH items
    yellow_flags: list = field(default_factory=list)  # MEDIUM items
    risk_level: str = "LOW"  # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"


# ─── CRITICAL patterns (documented RAF egregious violations) ─────────────────
CRITICAL_PATTERNS = [
    (r"sponsored results? below",
     "Exposes RSOC unit as sponsored in article body"),
    (r"click (the )?(ad|ads|sponsored)",
     "Directs users to click ads"),
    (r"(continue reading|read more) to (see|find|get)",
     "Possible fake CTA gate"),
    (r"you (will|are going to) receive",
     "Outcome guarantee language"),
    (r"guaranteed (approval|eligibility|compensation)",
     "Guarantee language"),
    (r"auto.?redirect",
     "Auto-redirect pattern detected"),
    (r"(overlay|interstitial|popup).*(click|close)",
     "Forced click-path pattern"),
]

# ─── HIGH-risk patterns ───────────────────────────────────────────────────────
HIGH_RISK_PATTERNS = [
    (r"you may (be entitled|qualify) (to|for) compensation",
     "Financial outcome implication"),
    (r"you (could|might) (win|recover) \$",
     "Dollar recovery implication"),
    (r"(best|top|#1|number one) (attorney|lawyer|plan|provider)",
     "Unsubstantiated ranking claim"),
    (r"(studies show|research proves|experts confirm) that",
     "Potentially fabricated authority claim"),
    (r"you have a (case|claim)",
     "Direct legal determination — not permitted"),
    (r"you (are|were) wrongfully",
     "Direct legal determination — not permitted"),
    (r"(file|start) (a|your) (claim|lawsuit|case) (today|now)",
     "Direct legal call to action"),
]

# ─── MEDIUM-risk patterns (yellow flags) ────────────────────────────────────
MEDIUM_RISK_PATTERNS = [
    (r"\b(buy|purchase|order|shop) (now|today|here)\b",
     "Direct sales language"),
    (r"\blimited time\b",
     "Artificial urgency"),
    (r"\bact now\b",
     "Artificial urgency"),
    (r"\bcall now\b",
     "Direct response language"),
    (r"\bfree consultation\b",
     "Possible lead-gen CTA"),
    (r"\bno obligation\b",
     "Lead-gen framing"),
    (r"\bcompare quotes\b",
     "Price-comparison CTA"),
]


def compliance_scan(article_text: str) -> ComplianceReport:
    """
    Scans article text for RAF compliance violations.
    Returns ComplianceReport with violation details and risk_level.

    Articles with CRITICAL or HIGH violations must NOT be published.
    This is enforced in content_generator.py; there is no config override
    for CRITICAL violations.
    """
    violations  = []
    yellow_flags = []
    max_risk    = "LOW"

    for pattern, reason in CRITICAL_PATTERNS:
        if re.search(pattern, article_text, re.IGNORECASE):
            violations.append(f"CRITICAL: {reason} (pattern: {pattern})")
            max_risk = "CRITICAL"

    for pattern, reason in HIGH_RISK_PATTERNS:
        if re.search(pattern, article_text, re.IGNORECASE):
            violations.append(f"HIGH: {reason} (pattern: {pattern})")
            if max_risk not in ("CRITICAL",):
                max_risk = "HIGH"

    for pattern, reason in MEDIUM_RISK_PATTERNS:
        if re.search(pattern, article_text, re.IGNORECASE):
            yellow_flags.append(f"MEDIUM: {reason} (pattern: {pattern})")
            if max_risk == "LOW":
                max_risk = "MEDIUM"

    passed = max_risk not in ("CRITICAL", "HIGH")

    return ComplianceReport(
        passed=passed,
        violations=violations,
        yellow_flags=yellow_flags,
        risk_level=max_risk,
    )
