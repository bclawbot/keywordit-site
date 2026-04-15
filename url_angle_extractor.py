"""
url_angle_extractor.py — Extract RSOC angle signals from ad landing URL slugs.

Zero LLM cost. Uses only stdlib.
"""
import re
from urllib.parse import urlparse, parse_qs

# URL slug patterns → angle type mapping
# Order matters: first match wins. More specific patterns come first.
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
    """
    Extract angle type and descriptive title from URL slug or query params.
    Returns {angle_type, slug_title, url} or empty dict.
    """
    if not url:
        return {}

    try:
        parsed = urlparse(url)
        path = parsed.path.lower()

        # Extract slug from path (last meaningful segment)
        segments = [s for s in path.split("/") if s and s not in ("en", "articles", "dsr")]
        if segments:
            slug = segments[-1]
            # Clean slug: remove query params, file extensions
            slug = slug.split("?")[0].split(".")[0]

            # Match against angle patterns
            for pattern, angle_type in SLUG_ANGLE_MAP.items():
                if re.search(pattern, slug):
                    # Convert slug to readable title
                    title = slug.replace("-", " ").replace("_", " ").title()
                    return {
                        "angle_type": angle_type,
                        "slug_title": title,
                        "url": url,
                    }

        # Fallback: check query parameters (search, q, p)
        qs = parse_qs(parsed.query)
        for param in ("search", "q", "p"):
            val = qs.get(param, [""])[0].lower()
            if val:
                for pattern, angle_type in SLUG_ANGLE_MAP.items():
                    if re.search(pattern, val):
                        title = val.replace("-", " ").replace("+", " ").replace("_", " ").title()[:80]
                        return {
                            "angle_type": angle_type,
                            "slug_title": title,
                            "url": url,
                        }
    except Exception:
        pass

    return {}
