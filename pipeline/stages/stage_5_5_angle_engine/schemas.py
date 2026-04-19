"""
schemas.py — Dataclass definitions for the Angle Engine pipeline.
No external dependencies — pure stdlib only.
"""
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime


@dataclass
class DiscoveryContext:
    """
    Captures WHY and HOW this keyword entered the pipeline.
    Derived from existing source/source_trend/commercial_category fields.
    Drives angle selection via DISCOVERY_SIGNAL_BOOST in angle_scorer.py.

    signal_type values:
        "google_trends"        — keyword spiked on Google Trends RSS
        "reddit_discussion"    — keyword came from Reddit hot posts
        "news_event"           — keyword came from Google News or Bing News RSS
        "commercial_intent"    — keyword came from Bing autosuggest (real user commercial queries)
        "commercial_transform" — keyword was transformed from a non-commercial term
        "keyword_expansion"    — keyword came from DataForSEO expansion
    """
    signal_type: str   # one of the 5 values above
    signal_text: str   # human-readable description for prompt injection


@dataclass
class RSocKeyword:
    """
    Normalized view of a validated_opportunities.json entry.
    Built by load_eligible_keywords() in angle_engine.py.
    """
    keyword: str
    country: str
    language_code: str          # ISO 639-1, defaults to "en"
    tag: str                    # GOLDEN_OPPORTUNITY | WATCH | EMERGING_HIGH
    vertical: str               # coarse vertical from vetting.py; refined in angle_scorer.py
    cpc_usd: float
    cpc_high_usd: float
    search_volume: int
    competition: float          # 0.0–1.0; often 0 from DFS Labs (known gap)
    rsoc_score: float
    main_intent: Optional[str]  # "commercial" | "informational" | "transactional" | None
    commercial_category: Optional[str]
    source_trend: Optional[str] # raw trend text that led to this keyword
    trend_source: Optional[str] # "google_trends_rss" | "reddit_hot" | etc.
    discovery_context: DiscoveryContext


@dataclass
class AngleCandidate:
    """One editorial angle scored for one keyword."""
    keyword: str
    country: str
    language_code: str
    angle_type: str         # one of the 10 canonical spec angle types
    angle_title: str        # generated H1 title
    ad_category: str        # primary ad category — enforces diversity rule
    rsoc_score: float       # 0.0–1.0, from angle_rsoc_score()
    discovery_boosted: bool # True if DiscoveryContext added a boost
    selected: bool          # True = chosen for article generation


@dataclass
class AngleEngineOutput:
    """All candidates for one keyword, with top N selected."""
    cluster_id: str
    keyword: str
    country: str
    language_code: str
    vertical: str
    cpc_usd: float
    tag: str
    candidates: List[AngleCandidate]        # all scored candidates
    selected_angles: List[AngleCandidate]   # top N (min 5) by diversity rule
    discovery_context: DiscoveryContext
    processed_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ArticleRecord:
    """One generated and validated article."""
    keyword: str
    country: str
    angle_type: str
    angle_title: str
    language_code: str
    vertical: str
    word_count: int
    file_path: str              # relative to workspace root
    raf_compliant: bool
    quality_score: float        # 0.0–1.0 from validate_rsoc_article()
    compliance_risk_level: str  # LOW | MEDIUM | HIGH | CRITICAL
    generated_at: str
    model_used: str
    generation_time_secs: float
