"""
normalize.py — Shared keyword normalization for dedup and API queries.

Used by validation.py (dedup comparison) and keyword_extractor.py (DataForSEO prep).
"""

import re
from datetime import datetime

_RE_YEAR = re.compile(r'\s+\b20\d{2}\b\s*$')
_RE_ARTICLE = re.compile(r'^(the|a|an)\s+', re.IGNORECASE)
_RE_FILLER = re.compile(
    r'\s+(online|near me|for free|review|reviews|today|now|'
    r'here|guide|tutorial|explained|reddit|quora)\s*$',
    re.IGNORECASE,
)
_RE_PUNCT = re.compile(r'[^\w\s-]')
_RE_WHITESPACE = re.compile(r'\s+')


def normalize_keyword(
    text: str,
    *,
    strip_year: bool = True,
    strip_articles: bool = True,
    strip_filler: bool = False,
    strip_punctuation: bool = True,
    max_words: int = 0,
) -> str:
    """Normalize a keyword string for comparison or API submission.

    Args:
        text: Raw keyword string.
        strip_year: Remove trailing 4-digit year (e.g., "best vpn 2026" -> "best vpn").
        strip_articles: Remove leading articles (the, a, an).
        strip_filler: Remove trailing filler words (online, near me, review, etc.).
        strip_punctuation: Remove non-word characters (except hyphens).
        max_words: Truncate to this many words (0 = no limit).
    """
    kw = text.lower().strip()
    if strip_year:
        kw = _RE_YEAR.sub('', kw)
    if strip_articles:
        kw = _RE_ARTICLE.sub('', kw)
    if strip_filler:
        kw = _RE_FILLER.sub('', kw)
    if strip_punctuation:
        kw = _RE_PUNCT.sub(' ', kw)
    kw = _RE_WHITESPACE.sub(' ', kw).strip()
    if max_words > 0:
        words = kw.split()
        kw = ' '.join(words[:max_words])
    return kw
