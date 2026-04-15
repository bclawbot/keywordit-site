"""
angle_engine.py — Stage 3a: RSOC Angle Engine

Reads:  validated_opportunities.json  (Stage 3 output)
        commercial_keywords.json       (source_trend recovery join)
Writes: angle_candidates.json          (Stage 3b input)
        LanceDB angle_candidates table

Filters to tags: GOLDEN_OPPORTUNITY, WATCH, EMERGING_HIGH.
For each keyword, scores all 10 angle types and selects the top 5
using an ad-category diversity rule.

Run directly: python3 angle_engine.py
"""
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path

import yaml  # PyYAML — already used by litellm in this environment

BASE      = Path(__file__).resolve().parent
INPUT     = BASE / "validated_opportunities.json"
HISTORY   = BASE / "validation_history.jsonl"    # full accumulated history
CK_INPUT  = BASE / "commercial_keywords.json"    # fallback join for source_trend
OUTPUT    = BASE / "angle_candidates.json"
ERROR_LOG = BASE / "error_log.jsonl"
CONFIG    = BASE / "config" / "angle_engine.yaml"

from pipeline.stages.stage_5_5_angle_engine.angle_scorer import (
    map_discovery_context,
    classify_vertical_fine,
)
from pipeline.stages.stage_5_5_angle_engine.angle_selector import select_angles
from pipeline.stages.stage_5_5_angle_engine.title_generator import (
    generate_title,
    generate_titles_batch,
    _detect_lang_from_script,
)
from pipeline.stages.stage_5_5_angle_engine.content_store import write_angle_candidates

# Country → language code (mirrors validation.py _DFS_LANG)
_DFS_LANG = {
    "US": "en", "GB": "en", "AU": "en", "CA": "en", "NZ": "en",
    "IE": "en", "ZA": "en", "SG": "en", "PH": "en", "MY": "en",
    "NG": "en", "KE": "en", "IN": "en",
    "DE": "de", "AT": "de", "CH": "de",
    "FR": "fr", "BE": "nl", "NL": "nl",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "IT": "it", "PT": "pt", "BR": "pt",
    "JP": "ja", "KR": "ko", "HK": "zh", "TW": "zh",
    "PL": "pl", "CZ": "cs", "RO": "ro", "HU": "hu", "GR": "el",
    "SE": "sv", "DK": "da", "FI": "fi",
    "TH": "th", "ID": "id", "VN": "vi",
    "TR": "tr", "UA": "uk", "IL": "he",
    "EG": "ar", "SA": "ar",
}


# ─── Logging ──────────────────────────────────────────────────────────────────
def _log_error(stage: str, msg: str, extra: dict = None) -> None:
    entry = {"timestamp": datetime.now().isoformat(), "stage": stage, "error": str(msg)}
    if extra:
        entry.update(extra)
    try:
        with open(ERROR_LOG, "a") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ─── Config ───────────────────────────────────────────────────────────────────
def _load_config() -> dict:
    if CONFIG.exists():
        with open(CONFIG) as fh:
            return yaml.safe_load(fh) or {}
    return {}


# ─── Source-trend recovery join ───────────────────────────────────────────────
def _build_ck_index() -> dict:
    """
    Build a lookup dict from commercial_keywords.json keyed on
    (keyword.lower(), country.upper()).
    Provides source_trend and language_code when vetting.py dropped them.
    """
    idx = {}
    if not CK_INPUT.exists():
        return idx
    try:
        with open(CK_INPUT) as fh:
            data = json.load(fh)
        for item in (data if isinstance(data, list) else []):
            key = (str(item.get("keyword", "")).lower(),
                   str(item.get("country", "")).upper())
            idx[key] = item
    except Exception as e:
        _log_error("3a.ck_index", f"commercial_keywords.json join failed: {e}")
    return idx


# ─── Expansion source loading ────────────────────────────────────────────────
EXPANSION_FILE = BASE / "data" / "expansion_results.jsonl"

def _load_expansion_sources() -> list:
    """
    Load expansion_results.jsonl and build opportunity-shaped dicts
    that _process_keyword() can consume. Deduplicates by source_keyword|country,
    keeping the record with the highest source_quality_score.
    """
    if not EXPANSION_FILE.exists():
        print("[angle_engine] No expansion_results.jsonl found — skipping expansion sources")
        return []

    # Deduplicate: keep best quality record per source_keyword|country
    best_by_source: dict = {}
    total = 0
    for line in EXPANSION_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            total += 1
            sk = rec.get("source_keyword", "")
            co = rec.get("country", "US").upper()
            key = f"{sk}|{co}"
            existing = best_by_source.get(key)
            if existing is None or float(rec.get("source_quality_score", 0)) > float(existing.get("source_quality_score", 0)):
                best_by_source[key] = rec
        except Exception:
            continue

    # Convert to opportunity-shaped dicts for _process_keyword()
    opps = []
    for key, rec in best_by_source.items():
        opp = {
            "keyword":          rec.get("source_keyword", ""),
            "country":          rec.get("country", "US").upper(),
            "cpc_usd":          float(rec.get("cpc_usd") or rec.get("inherited_cpc") or 0),
            "search_volume":    int(rec.get("search_volume") or 0),
            "competition":      float(rec.get("competition") or 0.5),
            "vertical":         rec.get("vertical", "general"),
            "main_intent":      "commercial",   # expansion keywords are commercial by design
            "tag":              "EXPANSION",
            "rsoc_score":       float(rec.get("opportunity_score") or 0),
            "source_trend":     rec.get("source_trend", ""),
            "trend_source":     "keyword_expansion",
            "language_code":    "",              # let angle_engine resolve from country
        }
        opps.append(opp)

    print(f"[angle_engine] Loaded {len(opps)} expansion source keywords "
          f"(from {total} total expansion records)")
    return opps


# ─── FB Intelligence angle lookup ────────────────────────────────────────────
FB_INTEL_DB = BASE / "dwight" / "fb_intelligence" / "data" / "fb_intelligence.db"

def _get_fb_intel_angles(keyword: str, country: str, vertical: str) -> list:
    """
    Query fb_intelligence.db for competitor angles matching this keyword.
    Returns list of angle dicts sorted by confidence descending.

    Strategy:
      1. Exact keyword match via Keywords → KeywordAngles join
      2. Falls back to empty list (no loose matching — wait for Phase 4 semantic)
    """
    if not FB_INTEL_DB.exists():
        return []

    import sqlite3
    angles = []
    try:
        db = sqlite3.connect(str(FB_INTEL_DB), timeout=5)
        db.row_factory = sqlite3.Row
        cursor = db.cursor()

        # Exact keyword match → KeywordAngles with source='original' first
        cursor.execute("""
            SELECT ka.angle_type, ka.angle_title, ka.article_url,
                   ka.confidence, ka.source, k.keyword
            FROM KeywordAngles ka
            JOIN Keywords k ON ka.keyword_id = k.id
            WHERE LOWER(k.keyword) = LOWER(?)
            ORDER BY
                CASE WHEN ka.source = 'original' THEN 0 ELSE 1 END,
                ka.confidence DESC
            LIMIT 5
        """, (keyword,))

        for row in cursor.fetchall():
            angles.append({
                "angle_type":   row["angle_type"] or "",
                "angle_title":  row["angle_title"] or "",
                "article_url":  row["article_url"] or "",
                "confidence":   float(row["confidence"] or 0),
                "source":       "fb_intel",
                "rsoc_score":   1.0 if row["source"] == "original" else 0.85,
                "ad_category":  "competitor_intelligence",
                "selected":     True,
            })

        db.close()
    except Exception as e:
        _log_error("3a.fb_intel", f"fb_intelligence.db query failed: {e}",
                   {"keyword": keyword})

    return angles


# ─── Input loading ────────────────────────────────────────────────────────────
def _load_all_history() -> list:
    """
    Load full deduplicated keyword history from validation_history.jsonl,
    merging current validated_opportunities.json on top (current run wins).
    Returns all unique keyword+country pairs across all pipeline runs.
    """
    history_map: dict = {}
    if HISTORY.exists():
        for line in HISTORY.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                key = f"{rec.get('keyword','').lower().strip()}|{rec.get('country','')}"
                existing = history_map.get(key)
                if existing is None or rec.get("validated_at", "") >= existing.get("validated_at", ""):
                    history_map[key] = rec
            except Exception:
                continue
    # Merge current run on top
    if INPUT.exists():
        try:
            for rec in json.load(open(INPUT)):
                key = f"{rec.get('keyword','').lower().strip()}|{rec.get('country','')}"
                history_map[key] = rec
        except Exception:
            pass
    return list(history_map.values())


def _load_validated(cfg: dict) -> list:
    opps = _load_all_history()
    eligible_tags = cfg.get("angle_engine", {}).get(
        "eligible_tags", ["GOLDEN_OPPORTUNITY", "WATCH", "EMERGING_HIGH",
                          "EMERGING", "LOW", "UNSCORED", "EXPANSION"]
    )
    if not opps:
        _log_error("3a.load", "No keywords found in history or validated_opportunities.json")
        return []
    filtered = [o for o in opps if o.get("tag") in eligible_tags]
    print(f"[angle_engine] {len(opps)} total opportunities (all history), "
          f"{len(filtered)} tag-eligible ({', '.join(eligible_tags)})")
    if not filtered:
        cpc_fallback = [o for o in opps if float(o.get("cpc_usd") or 0) > 0]
        if cpc_fallback:
            print(f"[angle_engine] WARN: No tag-eligible keywords. "
                  f"CPC fallback: {len(cpc_fallback)} keywords with cpc_usd > 0")
            return cpc_fallback
        print("[angle_engine] WARN: No eligible keywords and no CPC fallback — 0 results")
    return filtered


# ─── Discovery context ────────────────────────────────────────────────────────
def _build_discovery_context(opp: dict, ck_idx: dict) -> dict:
    """
    Build discovery context dict from opp fields.
    Falls back to commercial_keywords.json join for missing source fields.
    """
    key = (str(opp.get("keyword", "")).lower(),
           str(opp.get("country", "")).upper())
    ck  = ck_idx.get(key, {})

    # Merge: opp fields take priority, then ck fallback
    merged = dict(ck)
    merged.update({k: v for k, v in opp.items() if v is not None})

    return map_discovery_context(merged)


# ─── Per-keyword processing ───────────────────────────────────────────────────
def _process_keyword(opp: dict, ck_idx: dict, cfg: dict) -> dict:
    """
    Score all 10 angles for one keyword and select top 5.
    Returns an AngleEngineOutput-compatible dict.
    """
    engine_cfg = cfg.get("angle_engine", {})
    top_n      = max(int(engine_cfg.get("default_top_n", 5)), 5)
    min_score  = float(engine_cfg.get("min_rsoc_score", 0.45))
    diversity  = bool(engine_cfg.get("diversity_rule", True))

    keyword     = opp.get("keyword", "")
    country     = opp.get("country", "US")
    language    = (opp.get("language_code") or "").strip()

    # Resolve unknown/missing language codes
    if not language or language in ("?", "en"):
        # Try country→language mapping first
        country_lang = _DFS_LANG.get((country or "").upper())
        if country_lang and country_lang != "en":
            language = country_lang
        elif keyword and language in ("", "?"):
            # Script detection fallback for truly unknown languages
            detected = _detect_lang_from_script(keyword)
            if detected:
                language = detected
    if not language:
        language = "en"
    cpc         = float(opp.get("cpc_usd") or 0.0)
    cpc_high    = float(opp.get("cpc_high_usd") or cpc)
    volume      = int(opp.get("search_volume") or 0)
    competition = float(opp.get("competition") or 0.5)
    coarse_vert = opp.get("vertical") or "general"
    intent      = opp.get("main_intent") or "informational"
    tag         = opp.get("tag", "")
    rsoc_score  = float(opp.get("rsoc_score") or 0.0)
    source_trend = opp.get("source_trend") or ""
    year        = datetime.now().year

    discovery_ctx = _build_discovery_context(opp, ck_idx)
    fine_vertical = classify_vertical_fine(coarse_vert, keyword)

    # Score + select all angles
    all_candidates = select_angles(
        keyword=keyword,
        coarse_vertical=coarse_vert,
        language=language,
        cpc_usd=cpc if cpc > 0 else cpc_high,
        intent_classification=intent,
        competitor_saturation=competition,
        top_n=top_n,
        diversity_rule=diversity,
        discovery_context=discovery_ctx,
    )

    # Filter below min_rsoc_score threshold (keep selected flags as-is)
    # Don't drop selected angles even if below threshold — diversity matters more
    selected = [c for c in all_candidates if c.get("selected")]

    # Build titles for all selected angles (LLM batch when source_trend exists)
    selected_types = [c["angle_type"] for c in selected]
    titles = generate_titles_batch(
        keyword=keyword,
        angle_types=selected_types,
        language_code=language,
        country=country,
        year=year,
        source_trend=source_trend,
        vertical=fine_vertical,
    )
    for c in selected:
        c["angle_title"] = titles.get(c["angle_type"], keyword)

    output = {
        "cluster_id":       str(uuid.uuid4()),
        "keyword":          keyword,
        "country":          country,
        "language_code":    language,
        "vertical":         fine_vertical,
        "coarse_vertical":  coarse_vert,
        "cpc_usd":          cpc,
        "cpc_high_usd":     cpc_high,
        "search_volume":    volume,
        "tag":              tag,
        "rsoc_score":       rsoc_score,
        "source_trend":     source_trend,
        "all_candidates":   all_candidates,
        "selected_angles":  selected,
        "discovery_context":discovery_ctx,
        "processed_at":     datetime.now().isoformat(),
    }

    # Enrich with fb_intel competitor angles
    fb_angles = _get_fb_intel_angles(keyword, country, fine_vertical)
    if not fb_angles:
        # Fallback 1: semantic matching via LanceDB embeddings
        try:
            from angle_engine_semantic import find_similar_ads
            sem_matches = find_similar_ads(keyword, top_k=2, threshold=0.75)
            for match in sem_matches:
                fb_angles.append({
                    "angle_type":  "competitor_similar",
                    "angle_title": match.get("headline", keyword),
                    "article_url": "",
                    "source":      "fb_intel_semantic",
                    "rsoc_score":  0.80 * match.get("similarity", 0.75),
                    "ad_category": "competitor_intelligence",
                    "selected":    True,
                })
        except (ImportError, Exception):
            pass
    if fb_angles:
        # Prepend fb_intel angles (they have rsoc_score=1.0 for originals)
        output["selected_angles"] = fb_angles + output["selected_angles"]
        output["fb_intel_count"] = len(fb_angles)

    # Fallback 2: URL-based angle extraction from ad URLs in fb_intel results
    try:
        from url_angle_extractor import extract_angle_from_url
        for angle in output["selected_angles"]:
            url = angle.get("article_url", "")
            if url and not angle.get("angle_type"):
                extracted = extract_angle_from_url(url)
                if extracted:
                    angle["angle_type"] = extracted["angle_type"]
                    if not angle.get("angle_title"):
                        angle["angle_title"] = extracted["slug_title"]
    except (ImportError, Exception):
        pass

    return output


# ─── Main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    print(f"[angle_engine] Stage 3a started at {datetime.now().isoformat()}")
    cfg    = _load_config()
    opps   = _load_validated(cfg)
    ck_idx = _build_ck_index()

    # Load expansion source keywords and merge
    expansion_opps = _load_expansion_sources()
    if expansion_opps:
        # Deduplicate: expansion sources that already exist in validated history are skipped
        existing_keys = set()
        for o in opps:
            k = f"{str(o.get('keyword','')).lower().strip()}|{str(o.get('country','')).upper()}"
            existing_keys.add(k)
        new_expansion = []
        for eo in expansion_opps:
            k = f"{str(eo.get('keyword','')).lower().strip()}|{str(eo.get('country','')).upper()}"
            if k not in existing_keys:
                new_expansion.append(eo)
                existing_keys.add(k)
        opps.extend(new_expansion)
        print(f"[angle_engine] Merged {len(new_expansion)} new expansion sources "
              f"({len(expansion_opps) - len(new_expansion)} already in history)")

    if not opps:
        print("[angle_engine] No eligible opportunities — writing empty output")
        OUTPUT.write_text("[]", encoding="utf-8")
        return 0

    # Load existing angles to skip already-processed keywords (incremental)
    existing: dict = {}
    if OUTPUT.exists():
        try:
            for entry in json.load(open(OUTPUT)):
                _k = str(entry.get("keyword", "")).lower().strip()
                _c = str(entry.get("country", "")).upper()
                existing[f"{_k}|{_c}"] = entry
        except Exception:
            pass

    results = list(existing.values())
    new_count = 0
    errors  = 0

    # Processing cap: avoid timeout by limiting new keywords per run.
    # Prioritize by CPC (highest first) to process most valuable keywords first.
    engine_cfg = cfg.get("angle_engine", {})
    PROCESS_CAP = int(engine_cfg.get("max_new_per_run", 500))

    pending = []
    for opp in opps:
        kw = opp.get("keyword", "?")
        co = str(opp.get("country", "")).upper()
        skip_key = f"{kw.lower().strip()}|{co}"
        if skip_key not in existing:
            pending.append((opp, skip_key))

    # Sort pending by CPC descending — process highest-value keywords first
    pending.sort(key=lambda x: float(x[0].get("cpc_usd") or 0), reverse=True)
    if len(pending) > PROCESS_CAP:
        print(f"[angle_engine] {len(pending)} new keywords pending, "
              f"capping to {PROCESS_CAP} per run (highest CPC first)")
        pending = pending[:PROCESS_CAP]

    import time as _time
    _start_ts = _time.monotonic()
    WALL_LIMIT = float(engine_cfg.get("wall_limit_seconds", 4800))  # stop before 5400s heartbeat timeout

    for opp, skip_key in pending:
        # Early exit if approaching wall-clock limit
        elapsed = _time.monotonic() - _start_ts
        if elapsed > WALL_LIMIT:
            print(f"[angle_engine] Wall-clock limit reached ({elapsed:.0f}s / {WALL_LIMIT:.0f}s). "
                  f"Stopping — {new_count} processed this run, {len(pending) - new_count} deferred.")
            break

        kw = opp.get("keyword", "?")
        kw_start = _time.monotonic()

        # Skip LLM title generation for low-CPC keywords (use templates instead)
        _orig_trend = opp.get("source_trend")
        if float(opp.get("cpc_usd") or 0) < 0.50:
            opp["source_trend"] = ""

        try:
            out = _process_keyword(opp, ck_idx, cfg)
            kw_elapsed = _time.monotonic() - kw_start
            results.append(out)
            existing[skip_key] = out
            new_count += 1
            n_sel = len(out["selected_angles"])
            best  = out["selected_angles"][0] if out["selected_angles"] else {}
            timing = f"  {kw_elapsed:.1f}s" + (" [SLOW]" if kw_elapsed > 60 else "")
            print(f"  [{kw}] vertical={out['vertical']}  "
                  f"angles={n_sel}  best={best.get('angle_type','')}  "
                  f"score={best.get('rsoc_score', 0):.3f}{timing}")
        except Exception as e:
            kw_elapsed = _time.monotonic() - kw_start
            errors += 1
            _log_error("3a.process", str(e), {"keyword": kw, "elapsed_s": round(kw_elapsed, 1)})
            print(f"  [{kw}] ERROR ({kw_elapsed:.1f}s): {e}")
        finally:
            opp["source_trend"] = _orig_trend

    total_elapsed = _time.monotonic() - _start_ts
    avg = total_elapsed / max(new_count, 1)
    print(f"[angle_engine] {new_count} new / {len(existing) - new_count} cached | "
          f"{total_elapsed:.0f}s total, {avg:.1f}s avg/keyword")
    if avg > 30:
        print(f"[angle_engine] WARNING: avg {avg:.1f}s/keyword is abnormally slow")

    # Write JSON output
    try:
        with open(OUTPUT, "w", encoding="utf-8") as fh:
            json.dump(results, fh, ensure_ascii=False, indent=2)
        print(f"[angle_engine] Wrote {len(results)} keyword clusters → {OUTPUT.name}")
    except Exception as e:
        _log_error("3a.write", str(e))
        print(f"[angle_engine] ERROR writing output: {e}")
        return 1

    # Write to LanceDB — only new entries (non-fatal)
    new_results = results[-new_count:] if new_count > 0 else []
    try:
        written = write_angle_candidates(new_results) if new_results else 0
        print(f"[angle_engine] LanceDB: wrote {written} new angle candidate rows")
    except Exception as e:
        _log_error("3a.lancedb", str(e))
        print(f"[angle_engine] WARN: LanceDB write failed (non-fatal): {e}")

    total_angles = sum(len(r.get("selected_angles", [])) for r in results)
    print(f"[angle_engine] Done. {len(results)} keywords × avg "
          f"{total_angles/max(len(results),1):.1f} angles = {total_angles} total. "
          f"Errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
