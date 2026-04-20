"""
intelligence_api.py — Flask blueprint for fb_intelligence dashboard API.

Serves data from fb_intelligence.db for the Intelligence page on keywordit.xyz.
7 endpoints (A-G) covering networks, activity, keywords, verticals, matrix,
AI analysis, and system health.
"""

import json
import math
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests as http_requests
from flask import Blueprint, jsonify, request

_WORKSPACE = Path(__file__).resolve().parent.parent
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))

from dwight.fb_intelligence.config import DATA_DIR, DB_PATH
from dwight.fb_intelligence.metrics import (
    calculate_durability,
    classify_durability,
    creative_velocity,
    normalize_durability,
    normalize_velocity,
    rsoc_opportunity_score,
)

intel_bp = Blueprint("intelligence", __name__, url_prefix="/api/intel")

import re as _re

# ── Angle type normalization ─────────────────────────────────────────────────
# Canonical snake_case forms for duplicated angle types.
_ANGLE_ALIAS_MAP: dict[str, str] = {
    "how-to": "how_to",
    "How-To": "how_to",
    "how to": "how_to",
    "News/Breaking": "news_breaking",
    "news/breaking": "news_breaking",
    "Secret/Reveal": "secret_reveal",
    "secret/reveal": "secret_reveal",
    "Fear/Urgency": "fear_urgency",
    "fear/urgency": "fear_urgency",
    "Direct Offer": "direct_offer",
    "Direct_Offer": "direct_offer",
    "direct-offer": "direct_offer",
    "Informational Explainer": "informational_explainer",
    "Informational_Explainer": "informational_explainer",
    "Comparison": "comparison",
    "Guide": "guide",
    "Listicle": "listicle",
    "Question": "question",
    "Testimonial": "testimonial",
    "testimonials": "testimonial",
    "Transformation": "transformation",
    "Shopping Guide": "shopping_guide",
}


def _normalize_angle_type(raw: str) -> str:
    """Normalize angle type to consistent snake_case."""
    if not raw:
        return raw
    mapped = _ANGLE_ALIAS_MAP.get(raw)
    if mapped:
        return mapped
    # Generic normalization: lowercase, replace non-alphanum with underscore
    normalized = _re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return normalized


# ── Vertical taxonomy consolidation ──────────────────────────────────────────
# Maps duplicate/variant verticals to canonical names.
_VERTICAL_ALIAS_MAP: dict[str, str] = {
    "jobs": "employment",
    "job_search": "employment",
    "career": "employment",
    "careers": "employment",
    "freelance": "employment",
    "freelance_work": "employment",
    "remote_work": "employment",
    "work_from_home": "employment",
    "beauty": "personal_care",
    "beauty_cosmetics": "personal_care",
    "beauty_skincare": "personal_care",
    "beauty_treatments": "personal_care",
    "health_and_beauty": "personal_care",
    "housing": "real_estate",
    "housing_assistance": "real_estate",
    "auto": "automotive",
    "car": "automotive",
    "car_parts": "automotive",
    "automotive_parts": "automotive",
}


def _normalize_vertical(raw: str | None) -> str | None:
    """Map variant vertical names to canonical form."""
    if not raw:
        return raw
    return _VERTICAL_ALIAS_MAP.get(raw, raw)


# Keyword-based vertical overrides: when a keyword matches a pattern,
# force-reclassify to the correct vertical regardless of LLM output.
_KEYWORD_VERTICAL_OVERRIDES: list[tuple[str, str]] = [
    ("electrician", "employment"),
    ("plumber", "employment"),
    ("hvac tech", "employment"),
    ("welder", "employment"),
    ("carpenter", "employment"),
]


def _override_vertical_for_keyword(keyword: str, current_vertical: str) -> str:
    """Check if a keyword should be reclassified based on pattern rules."""
    kw_lower = keyword.lower()
    for pattern, target_vertical in _KEYWORD_VERTICAL_OVERRIDES:
        if pattern in kw_lower and current_vertical != target_vertical:
            return target_vertical
    return current_vertical


# ── DB helper ─────────────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA cache_size = -32000")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Caches ────────────────────────────────────────────────────────────────────

_verticals_cache: dict = {"data": None, "ts": 0}
_VERTICALS_TTL = 300  # 5 min

# Keywords that are tracker params, CTAs, or generic terms — not real search keywords.
# Excluded from top_keyword derivation in verticals.
_TOP_KW_BLOCKLIST = {
    "asrsearch", "asr search", "learn more", "guide", "click here",
    "sign up", "apply now", "get started", "search", "find out",
}

_ANALYSIS_CACHE_PATH = DATA_DIR / "analysis_cache.json"
_ANALYSIS_TTL = 43200  # 12 hours


# ══════════════════════════════════════════════════════════════════════════════
# A. NETWORK WAR ROOM
# ══════════════════════════════════════════════════════════════════════════════

@intel_bp.route("/networks")
def api_networks():
    conn = _get_conn()
    try:
        cursor = conn.cursor()

        networks = cursor.execute("""
            SELECT
                n.id, n.name,
                COUNT(DISTINCT d.id) AS domain_count,
                COUNT(DISTINCT CASE WHEN s.status = 'active' AND s.is_current = 1
                    THEN a.id END) AS active_ads,
                COUNT(DISTINCT CASE WHEN a.first_seen >= datetime('now', '-1 day')
                    THEN a.id END) AS new_today,
                COUNT(DISTINCT CASE WHEN a.first_seen >= datetime('now', '-7 days')
                    THEN a.id END) AS new_7d,
                COUNT(DISTINCT CASE WHEN a.first_seen >= datetime('now', '-30 days')
                    THEN a.id END) AS new_30d,
                MAX(a.last_seen) AS last_scrape
            FROM Networks n
            LEFT JOIN Domains d ON d.network_id = n.id
            LEFT JOIN FacebookPages fp ON fp.domain_id = d.id
            LEFT JOIN Ads a ON a.page_id = fp.id
            LEFT JOIN AdSnapshots s ON s.ad_id = a.id
            GROUP BY n.id, n.name
            ORDER BY active_ads DESC
        """).fetchall()

        result = []
        for net in networks:
            net_id = net["id"]

            # Avg durability for active ads in this network
            active_ad_ids = cursor.execute("""
                SELECT a.id FROM Ads a
                JOIN FacebookPages fp ON a.page_id = fp.id
                JOIN Domains d ON fp.domain_id = d.id
                JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
                WHERE d.network_id = ? AND s.status = 'active'
            """, (net_id,)).fetchall()

            durations = [calculate_durability(r["id"], cursor) for r in active_ad_ids]
            avg_dur = sum(durations) / len(durations) if durations else 0

            # Velocity: sum of creative_velocity across pages
            pages = cursor.execute("""
                SELECT DISTINCT fp.id FROM FacebookPages fp
                JOIN Domains d ON fp.domain_id = d.id
                WHERE d.network_id = ?
            """, (net_id,)).fetchall()
            total_vel = sum(creative_velocity(p["id"], cursor) for p in pages)

            # Top 3 verticals
            top_verts = cursor.execute("""
                SELECT a.primary_vertical, COUNT(*) AS cnt FROM Ads a
                JOIN FacebookPages fp ON a.page_id = fp.id
                JOIN Domains d ON fp.domain_id = d.id
                WHERE d.network_id = ? AND a.primary_vertical IS NOT NULL
                GROUP BY a.primary_vertical
                ORDER BY cnt DESC LIMIT 3
            """, (net_id,)).fetchall()

            # Domain breakdown for expansion
            domains = cursor.execute("""
                SELECT d.id, d.domain,
                    COUNT(DISTINCT fp.id) AS pages,
                    COUNT(DISTINCT a.id) AS ads
                FROM Domains d
                LEFT JOIN FacebookPages fp ON fp.domain_id = d.id
                LEFT JOIN Ads a ON a.page_id = fp.id
                WHERE d.network_id = ?
                GROUP BY d.id, d.domain
            """, (net_id,)).fetchall()

            result.append({
                "id": net_id,
                "name": net["name"],
                "domain_count": net["domain_count"],
                "active_ads": net["active_ads"],
                "new_today": net["new_today"],
                "new_7d": net["new_7d"],
                "new_30d": net["new_30d"],
                "avg_durability": round(avg_dur, 1),
                "durability_class": classify_durability(int(avg_dur)),
                "velocity_7d": total_vel,
                "top_verticals": list(dict.fromkeys(
                    _normalize_vertical(v["primary_vertical"]) or v["primary_vertical"]
                    for v in top_verts
                )),
                "last_scrape": net["last_scrape"],
                "domains": [
                    {"domain": d["domain"], "pages": d["pages"], "ads": d["ads"]}
                    for d in domains
                ],
            })

        # Filter out dormant networks (zero activity across all time periods)
        result = [
            n for n in result
            if n["active_ads"] > 0 or n["new_7d"] > 0 or n["new_30d"] > 0
        ]

        return jsonify({"networks": result, "timestamp": _now_iso()})
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# B. COMPETITOR ACTIVITY FEED
# ══════════════════════════════════════════════════════════════════════════════

_SEVERITY_MAP = {
    "durability_milestone": "HIGH",
    "new_network_vertical": "HIGH",
    "mass_launch": "HIGH",
    "velocity_burst": "HIGH",
    "scraper_health": "HIGH",
    "consensus_crossing": "MEDIUM",
    "geo_expansion": "MEDIUM",
}


@intel_bp.route("/activity")
def api_activity():
    period = request.args.get("period", "7", type=str)
    page = request.args.get("page", 1, type=int)
    limit = 20
    offset = (page - 1) * limit

    period_map = {"1": "-1 day", "7": "-7 days", "30": "-30 days"}
    time_filter = period_map.get(period, "-7 days")

    conn = _get_conn()
    try:
        # Check if Signals table has data
        signal_count = conn.execute(
            "SELECT COUNT(*) FROM Signals WHERE created_at >= datetime('now', ?)",
            (time_filter,),
        ).fetchone()[0]

        if signal_count > 0:
            # ── Primary path: Signals-based activity ──
            items = conn.execute(f"""
                SELECT
                    s.id, s.signal_type, s.signal_value, s.confidence,
                    s.created_at,
                    a.ad_archive_id, a.headline, a.image_url, a.image_local_path,
                    a.primary_vertical, a.primary_angle,
                    fp.page_name,
                    d.domain,
                    n.name AS network_name
                FROM Signals s
                LEFT JOIN Ads a ON s.ad_id = a.id
                LEFT JOIN FacebookPages fp ON COALESCE(a.page_id, s.page_id) = fp.id
                LEFT JOIN Domains d ON fp.domain_id = d.id
                LEFT JOIN Networks n ON d.network_id = n.id
                WHERE s.created_at >= datetime('now', ?)
                ORDER BY s.created_at DESC
                LIMIT ? OFFSET ?
            """, (time_filter, limit + 1, offset)).fetchall()

            has_more = len(items) > limit
            items = items[:limit]

            result = []
            for row in items:
                signal_type = row["signal_type"] or ""
                severity = "LOW"
                if ":" in (row["signal_value"] or ""):
                    sev_part = row["signal_value"].split(":")[0]
                    if sev_part in ("HIGH", "MEDIUM", "P0"):
                        severity = sev_part
                else:
                    for key, sev in _SEVERITY_MAP.items():
                        if key in signal_type:
                            severity = sev
                            break

                result.append({
                    "id": row["id"],
                    "signal_type": signal_type,
                    "severity": severity,
                    "ad_archive_id": row["ad_archive_id"],
                    "headline": row["headline"],
                    "image_url": row["image_url"],
                    "has_local_image": bool(row["image_local_path"]),
                    "vertical": _normalize_vertical(row["primary_vertical"]),
                    "angle": _normalize_angle_type(row["primary_angle"]) if row["primary_angle"] else None,
                    "page_name": row["page_name"],
                    "domain": row["domain"],
                    "network": row["network_name"],
                    "timestamp": row["created_at"],
                })
        else:
            # ── Fallback: derive activity from AdSnapshots/Ads ──
            # New ads detected in this period
            items = conn.execute(f"""
                SELECT
                    a.id, 'new_ad' AS signal_type,
                    a.ad_archive_id, a.headline, a.image_url, a.image_local_path,
                    a.primary_vertical, a.primary_angle,
                    a.first_seen AS created_at,
                    fp.page_name,
                    d.domain,
                    n.name AS network_name
                FROM Ads a
                LEFT JOIN FacebookPages fp ON a.page_id = fp.id
                LEFT JOIN Domains d ON fp.domain_id = d.id
                LEFT JOIN Networks n ON d.network_id = n.id
                WHERE a.first_seen >= datetime('now', ?)
                ORDER BY a.first_seen DESC
                LIMIT ? OFFSET ?
            """, (time_filter, limit + 1, offset)).fetchall()

            has_more = len(items) > limit
            items = items[:limit]

            result = []
            for row in items:
                vert = _normalize_vertical(row["primary_vertical"])
                angle = _normalize_angle_type(row["primary_angle"]) if row["primary_angle"] else None

                # Severity: ads with known verticals are more notable
                severity = "MEDIUM" if vert else "LOW"

                headline_text = row["headline"] or row["ad_archive_id"] or "New ad"
                result.append({
                    "id": row["id"],
                    "signal_type": "new_ad",
                    "severity": severity,
                    "headline": f"New ad: {headline_text}",
                    "ad_archive_id": row["ad_archive_id"],
                    "image_url": row["image_url"],
                    "has_local_image": bool(row["image_local_path"]),
                    "vertical": vert,
                    "angle": angle,
                    "page_name": row["page_name"],
                    "domain": row["domain"],
                    "network": row["network_name"],
                    "timestamp": row["created_at"],
                })

        return jsonify({"items": result, "has_more": has_more})
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# C. KEYWORD INTELLIGENCE TABLE
# ══════════════════════════════════════════════════════════════════════════════

_KW_SORT_WHITELIST = {
    "keyword", "network_count", "max_durability", "cpc_usd", "volume",
    "competition", "kd", "rsoc_score", "first_seen", "top_angle_type",
}


@intel_bp.route("/keywords")
def api_keywords():
    vertical = request.args.get("vertical", "")
    sort_col = request.args.get("sort", "network_count")
    sort_dir = request.args.get("dir", "desc").upper()
    page = request.args.get("page", 1, type=int)
    limit = 50
    offset = (page - 1) * limit

    if sort_col not in _KW_SORT_WHITELIST:
        sort_col = "network_count"
    if sort_dir not in ("ASC", "DESC"):
        sort_dir = "DESC"

    angle_type = request.args.get("angle_type", "")
    has_original = request.args.get("has_original", "")

    conn = _get_conn()
    try:
        # Build WHERE clause
        where_parts = ["1=1"]
        params: list = []
        if vertical:
            where_parts.append("a.primary_vertical = ?")
            params.append(vertical)

        date_from = request.args.get("date_from", "")
        date_to = request.args.get("date_to", "")
        if date_from:
            where_parts.append("k.created_at >= ?")
            params.append(date_from)
        if date_to:
            where_parts.append("k.created_at <= ? || 'T23:59:59Z'")
            params.append(date_to)

        if angle_type:
            where_parts.append(
                "k.id IN (SELECT keyword_id FROM KeywordAngles WHERE angle_type = ?)"
            )
            params.append(angle_type)
        if has_original:
            where_parts.append(
                "k.id IN (SELECT keyword_id FROM KeywordAngles WHERE source = 'original')"
            )

        where_sql = " AND ".join(where_parts)

        # Main query — split into two phases for performance:
        # Phase 1: keyword listing WITHOUT AdSnapshots (0.07s vs 14s with)
        _sort_col = sort_col if sort_col != "max_durability" else "network_count"
        rows = conn.execute(f"""
            SELECT
                k.id, k.keyword, k.cpc_usd, k.volume, k.competition, k.kd,
                k.metadata, k.created_at AS first_seen,
                COUNT(DISTINCT n.id) AS network_count,
                GROUP_CONCAT(DISTINCT a.primary_vertical) AS verticals
            FROM Keywords k
            LEFT JOIN AdKeywords ak ON ak.keyword_id = k.id
            LEFT JOIN Ads a ON ak.ad_id = a.id
            LEFT JOIN FacebookPages fp ON a.page_id = fp.id
            LEFT JOIN Domains d ON fp.domain_id = d.id
            LEFT JOIN Networks n ON d.network_id = n.id
            WHERE {where_sql}
            GROUP BY k.id, k.keyword
            ORDER BY {_sort_col} {sort_dir}
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()

        # Phase 2: fetch durability only for the returned keyword IDs
        durability_map: dict = {}
        if rows:
            _kw_ids = [r["id"] for r in rows]
            _ph = ",".join("?" * len(_kw_ids))
            for dr in conn.execute(f"""
                SELECT ak.keyword_id,
                    MAX(julianday(COALESCE(snap.last_seen_date, snap.first_seen_date))
                        - julianday(snap.first_seen_date)) AS max_durability
                FROM AdKeywords ak
                JOIN AdSnapshots snap ON snap.ad_id = ak.ad_id AND snap.is_current = 1
                WHERE ak.keyword_id IN ({_ph})
                GROUP BY ak.keyword_id
            """, _kw_ids).fetchall():
                durability_map[dr["keyword_id"]] = dr["max_durability"] or 0

        # Count total
        total_row = conn.execute(f"""
            SELECT COUNT(DISTINCT k.id) FROM Keywords k
            LEFT JOIN AdKeywords ak ON ak.keyword_id = k.id
            LEFT JOIN Ads a ON ak.ad_id = a.id
            WHERE {where_sql}
        """, params).fetchone()
        total = total_row[0] if total_row else 0

        # Distinct verticals for dropdown
        vert_rows = conn.execute(
            "SELECT DISTINCT primary_vertical FROM Ads WHERE primary_vertical IS NOT NULL ORDER BY primary_vertical"
        ).fetchall()

        # Batch-fetch angles for all returned keywords (avoid N+1 queries)
        kw_ids = [r["id"] for r in rows]
        angles_by_kw: dict = {}
        if kw_ids:
            placeholders = ",".join("?" * len(kw_ids))
            angle_rows = conn.execute(f"""
                SELECT keyword_id, angle_type, angle_title, source, confidence, vertical, article_url
                FROM KeywordAngles WHERE keyword_id IN ({placeholders})
                ORDER BY keyword_id, CASE WHEN source = 'original' THEN 0 ELSE 1 END, confidence DESC
            """, kw_ids).fetchall()
            for a in angle_rows:
                angles_by_kw.setdefault(a["keyword_id"], []).append({
                    "type": _normalize_angle_type(a["angle_type"]),
                    "title": a["angle_title"],
                    "source": a["source"],
                    "confidence": a["confidence"],
                    "vertical": _normalize_vertical(a["vertical"]),
                    "url": a["article_url"],
                })

        keywords = []
        for r in rows:
            net_count = r["network_count"] or 0
            max_dur = durability_map.get(r["id"], 0)
            cpc = r["cpc_usd"]
            vol = r["volume"]
            comp = r["competition"]

            # Compute rsoc_opportunity_score if we have enough data
            rsoc_score = None
            if cpc is not None and vol is not None:
                sv_norm = min(math.log10(max(vol, 1)) / 6.0, 1.0)
                cpc_norm = min(cpc / 10.0, 1.0)
                kd_norm = float(comp) if comp is not None else 0.5
                dur_norm = normalize_durability(int(max_dur))
                rsoc_score = round(rsoc_opportunity_score(
                    sv_norm, cpc_norm, kd_norm, 0.5, 0.5, dur_norm
                ), 3)

            highlight = net_count >= 3 and max_dur >= 14
            angles = angles_by_kw.get(r["id"], [])

            keywords.append({
                "id": r["id"],
                "keyword": r["keyword"],
                "network_count": net_count,
                "max_durability": round(max_dur, 1),
                "durability_class": classify_durability(int(max_dur)),
                "verticals": sorted(set(
                    _override_vertical_for_keyword(
                        r["keyword"], _normalize_vertical(v) or v
                    )
                    for v in r["verticals"].split(",")
                )) if r["verticals"] else [],
                "cpc_usd": round(cpc, 2) if cpc is not None else None,
                "volume": vol,
                "competition": round(comp, 3) if comp is not None else None,
                "kd": r["kd"],
                "rsoc_score": rsoc_score,
                "validation_status": "validated" if cpc is not None else "pending",
                "first_seen": r["first_seen"],
                "highlight": highlight,
                "angles": angles,
            })

        # Distinct angle types for dropdown
        angle_type_rows = conn.execute(
            "SELECT DISTINCT angle_type FROM KeywordAngles WHERE angle_type IS NOT NULL ORDER BY angle_type"
        ).fetchall()

        return jsonify({
            "keywords": keywords,
            "total": total,
            "verticals": sorted(set(
                _normalize_vertical(v["primary_vertical"]) for v in vert_rows
            )),
            "angle_types": sorted(set(
                _normalize_angle_type(a["angle_type"]) for a in angle_type_rows
            )),
        })
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# C2. GENERATE ANGLES FOR A KEYWORD (on-demand via LLM)
# ══════════════════════════════════════════════════════════════════════════════

@intel_bp.route("/generate-angles", methods=["POST"])
def api_generate_angles():
    """Generate content angles for a keyword using Ollama, save to DB."""
    data = request.json or {}
    keyword_id = data.get("keyword_id")
    keyword_text = data.get("keyword", "")
    vertical = data.get("vertical", "general")

    if not keyword_id or not keyword_text:
        return jsonify({"error": "keyword_id and keyword required"}), 400

    # Pre-check: is Ollama responsive?
    try:
        http_requests.get("http://localhost:11434/api/ps", timeout=3)
    except Exception:
        return jsonify({"error": "Ollama is busy — angles will be generated in the next pipeline run"}), 503

    prompt = f"""Generate 5 unique content angles for the keyword "{keyword_text}" in the "{vertical}" vertical.

Each angle has type (one of: listicle, how-to, comparison, cost_savings, informational_explainer, review, quiz) and title (compelling article title, 50-80 chars).

Return ONLY a JSON array, no explanation:
[{{"type": "listicle", "title": "..."}}, ...]"""

    # Try Ollama (chat API to properly disable thinking mode)
    import re as _re
    angles = []
    raw = ""
    try:
        resp = http_requests.post(
            "http://localhost:11434/api/chat",
            json={
                "model": "qwen3:14b",
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "think": False,
                "options": {"temperature": 0.7, "num_predict": 512, "num_ctx": 32768},
                "keep_alive": -1,
            },
            timeout=120,
        )
        if resp.status_code == 200:
            raw = resp.json().get("message", {}).get("content", "")
            match = _re.search(r"\[.*\]", raw, _re.DOTALL)
            if match:
                angles = json.loads(match.group(0))
        else:
            print(f"[intel] Ollama returned {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[intel] Angle generation error: {e}")
        return jsonify({"error": f"LLM call failed: {e}"}), 500

    if not angles:
        print(f"[intel] No angles parsed from response: {raw[:300]}")
        return jsonify({"error": "LLM returned no valid angles — try again"}), 500

    # Save to DB
    conn = _get_conn()
    saved = []
    try:
        for a in angles[:5]:
            a_type = a.get("type", "unknown")
            a_title = a.get("title", "")
            if not a_title:
                continue
            conn.execute(
                """INSERT OR IGNORE INTO KeywordAngles
                   (keyword_id, angle_type, angle_title, source, confidence, vertical)
                   VALUES (?, ?, ?, 'generated', 0.7, ?)""",
                (keyword_id, a_type, a_title, vertical),
            )
            saved.append({
                "type": a_type,
                "title": a_title,
                "source": "generated",
                "confidence": 0.7,
                "vertical": vertical,
                "url": None,
            })
        conn.commit()
    finally:
        conn.close()

    return jsonify({"angles": saved, "keyword_id": keyword_id})


# ══════════════════════════════════════════════════════════════════════════════
# D. VERTICAL LEADERBOARD
# ══════════════════════════════════════════════════════════════════════════════

@intel_bp.route("/verticals")
def api_verticals():
    now = time.time()
    if _verticals_cache["data"] and (now - _verticals_cache["ts"]) < _VERTICALS_TTL:
        return jsonify(_verticals_cache["data"])

    conn = _get_conn()
    try:
        cursor = conn.cursor()

        rows = cursor.execute("""
            SELECT
                a.primary_vertical AS vertical,
                COUNT(DISTINCT n.id) AS network_count,
                COUNT(DISTINCT a.id) AS total_ads,
                AVG(
                    julianday(COALESCE(s.last_seen_date, s.first_seen_date))
                    - julianday(s.first_seen_date)
                ) AS avg_durability
            FROM Ads a
            JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
            LEFT JOIN FacebookPages fp ON a.page_id = fp.id
            LEFT JOIN Domains d ON fp.domain_id = d.id
            LEFT JOIN Networks n ON d.network_id = n.id
            WHERE a.primary_vertical IS NOT NULL AND s.status = 'active'
            GROUP BY a.primary_vertical
            ORDER BY network_count DESC
        """).fetchall()

        # Build raw verticals, then merge aliases into canonical names
        raw_verticals: dict = {}  # canonical_name → merged row
        for r in rows:
            vert_raw = r["vertical"]
            vert = _normalize_vertical(vert_raw) or vert_raw
            avg_dur = r["avg_durability"] or 0

            if vert in raw_verticals:
                # Merge into existing canonical entry
                existing = raw_verticals[vert]
                existing["_raw_names"].append(vert_raw)
                existing["total_ads"] += r["total_ads"]
                # Take max network_count across aliases
                existing["network_count"] = max(
                    existing["network_count"], r["network_count"]
                )
                # Weighted avg durability
                old_total = existing.get("_dur_weight", 0)
                new_total = old_total + r["total_ads"]
                if new_total > 0:
                    existing["avg_durability"] = (
                        existing["avg_durability"] * old_total + avg_dur * r["total_ads"]
                    ) / new_total
                existing["_dur_weight"] = new_total
            else:
                raw_verticals[vert] = {
                    "vertical": vert,
                    "network_count": r["network_count"],
                    "total_ads": r["total_ads"],
                    "avg_durability": avg_dur,
                    "_raw_names": [vert_raw],
                    "_dur_weight": r["total_ads"],
                }

        verticals = []
        _bl_ph = ",".join("?" * len(_TOP_KW_BLOCKLIST))
        for vert, data in raw_verticals.items():
            avg_dur = data["avg_durability"]

            # Top keyword — query across all raw variant names
            raw_names = data["_raw_names"]
            _vn_ph = ",".join("?" * len(raw_names))
            top_kw_row = cursor.execute(f"""
                SELECT k.keyword, COUNT(*) AS cnt
                FROM Keywords k
                JOIN AdKeywords ak ON ak.keyword_id = k.id
                JOIN Ads a ON ak.ad_id = a.id
                WHERE a.primary_vertical IN ({_vn_ph})
                  AND LOWER(k.keyword) NOT IN ({_bl_ph})
                  AND LENGTH(k.keyword) > 3
                GROUP BY k.keyword
                ORDER BY cnt DESC LIMIT 1
            """, (*raw_names, *_TOP_KW_BLOCKLIST)).fetchone()

            # Velocity: sum across all raw variant names
            vel_row = cursor.execute(f"""
                SELECT COUNT(*) FROM Ads
                WHERE primary_vertical IN ({_vn_ph})
                  AND first_seen >= datetime('now', '-7 days')
            """, raw_names).fetchone()

            verticals.append({
                "vertical": vert,
                "network_count": data["network_count"],
                "total_ads": data["total_ads"],
                "avg_durability": round(avg_dur, 1),
                "durability_class": classify_durability(int(avg_dur)),
                "top_keyword": top_kw_row["keyword"] if top_kw_row else None,
                "velocity_7d": vel_row[0] if vel_row else 0,
            })

        # Sort by a simple consensus proxy (network_count * log(1+total_ads))
        verticals.sort(
            key=lambda v: v["network_count"] * math.log2(1 + v["total_ads"]),
            reverse=True,
        )

        # Add rank
        for i, v in enumerate(verticals):
            v["rank"] = i + 1

        result = {"verticals": verticals, "timestamp": _now_iso()}
        _verticals_cache["data"] = result
        _verticals_cache["ts"] = now
        return jsonify(result)
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# E. ANGLE × VERTICAL MATRIX
# ══════════════════════════════════════════════════════════════════════════════

@intel_bp.route("/matrix")
def api_matrix():
    conn = _get_conn()
    try:
        rows = conn.execute("""
            SELECT
                a.primary_vertical AS vertical,
                a.primary_angle AS angle,
                COUNT(DISTINCT n.id) AS network_count,
                COUNT(DISTINCT a.id) AS ad_count,
                MAX(
                    julianday(COALESCE(s.last_seen_date, s.first_seen_date))
                    - julianday(s.first_seen_date)
                ) AS max_durability
            FROM Ads a
            JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
            LEFT JOIN FacebookPages fp ON a.page_id = fp.id
            LEFT JOIN Domains d ON fp.domain_id = d.id
            LEFT JOIN Networks n ON d.network_id = n.id
            WHERE a.primary_vertical IS NOT NULL
              AND a.primary_angle IS NOT NULL
              AND s.status = 'active'
            GROUP BY a.primary_vertical, a.primary_angle
        """).fetchall()

        # Collect distinct verticals and angles
        vert_set: set[str] = set()
        angle_set: set[str] = set()
        cells: dict = {}

        for r in rows:
            v = _normalize_vertical(r["vertical"]) or r["vertical"]
            ang = _normalize_angle_type(r["angle"]) if r["angle"] else r["angle"]
            vert_set.add(v)
            angle_set.add(ang)

            nc = r["network_count"] or 0
            md = r["max_durability"] or 0

            if nc >= 5 and md >= 30:
                classification = "saturated"
            elif nc >= 3:
                classification = "competitive"
            elif nc >= 1:
                classification = "emerging"
            else:
                classification = "gap"

            key = f"{v}|{ang}"
            if key in cells:
                # Merge duplicate canonical cells
                existing = cells[key]
                existing["ad_count"] += r["ad_count"]
                existing["network_count"] = max(existing["network_count"], nc)
                existing["max_durability"] = max(existing["max_durability"], round(md, 1))
            else:
                cells[key] = {
                    "ad_count": r["ad_count"],
                    "network_count": nc,
                    "max_durability": round(md, 1),
                    "classification": classification,
                }

        return jsonify({
            "verticals": sorted(vert_set),
            "angles": sorted(angle_set),
            "cells": cells,
        })
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# F. AI TREND ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

@intel_bp.route("/analysis")
def api_analysis():
    force = request.args.get("force", "false").lower() == "true"

    # Check cache
    if not force and _ANALYSIS_CACHE_PATH.exists():
        try:
            cache = json.loads(_ANALYSIS_CACHE_PATH.read_text())
            cache_ts = cache.get("generated_at", "")
            if cache_ts:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                    cache_ts.replace("Z", "+00:00")
                )).total_seconds()
                if age < _ANALYSIS_TTL:
                    cache["cached"] = True
                    return jsonify(cache)
        except Exception:
            pass

    conn = _get_conn()
    try:
        cursor = conn.cursor()

        # Gather data for prompt
        total_ads = cursor.execute("SELECT COUNT(*) FROM Ads").fetchone()[0]
        active_ads = cursor.execute(
            "SELECT COUNT(*) FROM AdSnapshots WHERE is_current = 1 AND status = 'active'"
        ).fetchone()[0]
        new_24h = cursor.execute(
            "SELECT COUNT(*) FROM Ads WHERE first_seen >= datetime('now', '-1 day')"
        ).fetchone()[0]
        new_7d = cursor.execute(
            "SELECT COUNT(*) FROM Ads WHERE first_seen >= datetime('now', '-7 days')"
        ).fetchone()[0]

        # Top verticals
        top_verts = cursor.execute("""
            SELECT primary_vertical, COUNT(*) AS cnt FROM Ads
            WHERE primary_vertical IS NOT NULL
            GROUP BY primary_vertical ORDER BY cnt DESC LIMIT 10
        """).fetchall()

        # Recent signals
        signals = cursor.execute("""
            SELECT signal_type, signal_value, created_at FROM Signals
            WHERE created_at >= datetime('now', '-7 days')
            ORDER BY created_at DESC LIMIT 20
        """).fetchall()

        # Top durable ads
        durable = cursor.execute("""
            SELECT a.headline, a.primary_vertical, a.primary_angle,
                   a.first_seen, a.last_seen
            FROM Ads a
            JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
            WHERE s.status = 'active'
            ORDER BY a.first_seen ASC LIMIT 5
        """).fetchall()

        # Build prompt
        data_summary = f"""Total ads tracked: {total_ads}
Active ads: {active_ads}
New ads (24h): {new_24h}
New ads (7d): {new_7d}

Top verticals: {', '.join(f"{v['primary_vertical']} ({v['cnt']})" for v in top_verts)}

Recent signals (7d): {len(signals)} events
{chr(10).join(f"- {s['signal_type']}: {s['signal_value']}" for s in signals[:10])}

Longest-running active ads:
{chr(10).join(f"- {d['headline'] or 'N/A'} | {d['primary_vertical']} | since {d['first_seen']}" for d in durable)}"""

        prompt = f"""Analyze the following Facebook Ad Library intelligence data and provide insights.

DATA:
{data_summary}

Respond with a JSON object containing exactly 5 keys:
- "today_moves": What changed in the last 24 hours
- "weekly_patterns": Acceleration or deceleration trends this week
- "monthly_trends": Rotation, seasonality, or structural changes
- "predicted_opportunities": What verticals/angles to prioritize next
- "anomalies": Anything unusual or noteworthy

Each value should be a string of 2-3 sentences. Be specific and actionable.
Respond ONLY with the JSON object."""

        # Call Ollama
        analysis = _call_llm_for_analysis(prompt)

        if analysis:
            result = {
                "insights": analysis,
                "generated_at": _now_iso(),
                "cached": False,
            }
            try:
                _ANALYSIS_CACHE_PATH.write_text(json.dumps(result, indent=2))
            except Exception:
                pass
            return jsonify(result)
        else:
            return jsonify({
                "error": "LLM analysis unavailable — Ollama may be down",
                "insights": None,
                "cached": False,
            })
    finally:
        conn.close()


def _call_llm_for_analysis(prompt: str) -> dict | None:
    """Call Ollama qwen3:14b, fallback to OpenRouter."""
    # Try Ollama first
    try:
        resp = http_requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "qwen3:14b",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 1024, "num_ctx": 8192},
            },
            timeout=120,
        )
        if resp.status_code == 200:
            raw = resp.json().get("response", "")
            return _parse_analysis_json(raw)
    except Exception:
        pass

    # Fallback: OpenRouter
    api_key = _WORKSPACE / ".." / ".env"
    import os
    or_key = os.environ.get("OPENROUTER_API_KEY", "")
    if or_key:
        try:
            resp = http_requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {or_key}"},
                json={
                    "model": "deepseek/deepseek-v3.2",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 1024,
                },
                timeout=60,
            )
            if resp.status_code == 200:
                raw = resp.json()["choices"][0]["message"]["content"]
                return _parse_analysis_json(raw)
        except Exception:
            pass

    return None


def _parse_analysis_json(raw: str) -> dict | None:
    """Parse LLM response into the 5-insight dict."""
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(raw[start:end])
    except Exception:
        pass
    # Return a fallback structure with raw text
    if raw.strip():
        return {
            "today_moves": raw[:200],
            "weekly_patterns": "Analysis parsing failed — see raw response above.",
            "monthly_trends": "",
            "predicted_opportunities": "",
            "anomalies": "",
        }
    return None


# ══════════════════════════════════════════════════════════════════════════════
# G. SYSTEM HEALTH
# ══════════════════════════════════════════════════════════════════════════════

@intel_bp.route("/health")
def api_health():
    conn = _get_conn()
    try:
        last_scrape = conn.execute("SELECT MAX(last_seen) FROM Ads").fetchone()[0]
        total_ads = conn.execute("SELECT COUNT(*) FROM Ads").fetchone()[0]
        active_ads = conn.execute(
            "SELECT COUNT(*) FROM AdSnapshots WHERE is_current = 1 AND status = 'active'"
        ).fetchone()[0]
        unclassified = conn.execute(
            "SELECT COUNT(*) FROM Ads WHERE classification_conf IS NULL"
        ).fetchone()[0]
        pending_kw = conn.execute(
            "SELECT COUNT(*) FROM KeywordQueue WHERE status = 'pending'"
        ).fetchone()[0]

        # Scraper status
        status = "no_data"
        hours_stale = None
        if last_scrape:
            try:
                last_ts = datetime.fromisoformat(last_scrape.replace("Z", "+00:00"))
                hours_stale = (
                    datetime.now(timezone.utc) - last_ts
                ).total_seconds() / 3600
                if hours_stale < 24:
                    status = "healthy"
                elif hours_stale < 48:
                    status = "degraded"
                else:
                    status = "down"
            except Exception:
                status = "unknown"

        return jsonify({
            "last_scrape": last_scrape,
            "hours_since_scrape": round(hours_stale, 1) if hours_stale else None,
            "total_ads": total_ads,
            "active_ads": active_ads,
            "unclassified": unclassified,
            "pending_keywords": pending_kw,
            "scraper_status": status,
            "timestamp": _now_iso(),
        })
    finally:
        conn.close()
