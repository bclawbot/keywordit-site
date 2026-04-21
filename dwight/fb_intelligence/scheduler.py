"""
dwight/fb_intelligence/scheduler.py — Pipeline orchestrator for fb_intelligence.

Coordinates scraping, ingestion, analysis, metrics, and alerting.
Provides a CLI for running individual stages or the full nightly pipeline.

Usage:
    python -m dwight.fb_intelligence.scheduler daily_pull
    python -m dwight.fb_intelligence.scheduler full
    python -m dwight.fb_intelligence.scheduler --list
"""

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_WORKSPACE = Path(__file__).resolve().parents[2]
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))

from dwight.fb_intelligence.alerts import AlertManager
from dwight.fb_intelligence.analyzer import IntelligenceAnalyzer
from dwight.fb_intelligence.classifier import AdClassifier
from dwight.fb_intelligence.config import DATA_DIR, SEARCHAPI_KEY, _log_error
from dwight.fb_intelligence.embeddings import EmbeddingManager
from dwight.fb_intelligence.landing_page import LandingPageCrawler
from dwight.fb_intelligence.metrics import materialize_daily_metrics
from dwight.fb_intelligence.scraper import AdLibraryScraper
from dwight.fb_intelligence.keyword_enricher import KeywordEnricher
from dwight.fb_intelligence.lifecycle_aggregator import AdLifecycleAggregator
from dwight.fb_intelligence.storage import (
    get_stale_ads,
    ingest_ads,
    init_db,
    update_ad_status,
)

logger = logging.getLogger("fb_intelligence.scheduler")

# ── Cron schedule ─────────────────────────────────────────────────────────────

CRON_SCHEDULE = {
    "daily_pull": "0 6,18 * * *",
    "discovery": "0 14 * * 1,4",
    "reconciliation": "0 2 * * *",
    "intelligence": "0 3 * * *",
    "crawl_landing_pages": "30 3 * * *",
    "digest": "0 9 * * *",
    "report": "0 17 * * 5",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class PipelineScheduler:
    """Orchestrates all fb_intelligence pipeline stages."""

    def __init__(self) -> None:
        self.conn = init_db()
        self.scraper = AdLibraryScraper()
        self.classifier = AdClassifier()
        self.embedding_manager = EmbeddingManager()
        self.analyzer = IntelligenceAnalyzer(
            conn=self.conn,
            classifier=self.classifier,
            embedding_manager=self.embedding_manager,
        )
        self.alert_manager = AlertManager(self.conn)
        self.landing_crawler = LandingPageCrawler()

    # ── Stage: Daily pull ─────────────────────────────────────────────────────

    async def run_daily_pull(self) -> dict:
        """Scrape all tracked pages for new/updated ads.

        Primary path: Playwright scraper ($0).
        Fallback: SearchAPI.io if SEARCHAPI_KEY is set.
        """
        t0 = time.monotonic()
        logger.info("Starting daily pull")

        # Get tracked pages
        pages = self.conn.execute(
            "SELECT id, fb_page_id, page_name FROM FacebookPages"
        ).fetchall()

        if not pages:
            logger.info("No tracked pages — skipping daily pull")
            return {"pages": 0, "ads": 0, "elapsed": 0}

        all_ads: list[dict] = []

        try:
            await self.scraper.setup()
            for i, page in enumerate(pages):
                logger.info(
                    "Pulling %d/%d: %s (%s)",
                    i + 1, len(pages), page[2] or page[1], page[1],
                )
                try:
                    ads = await self.scraper.scrape_page_ads(page[1])
                    all_ads.extend(ads)
                except Exception as exc:
                    logger.warning("Scraper failed for %s: %s", page[1], exc)

                    # Fallback to API if key is set
                    if SEARCHAPI_KEY:
                        try:
                            from dwight.fb_intelligence.api_client import SearchAPIClient
                            api = SearchAPIClient()
                            ads = await api.fetch_ads_for_page(page[1])
                            all_ads.extend(ads)
                            logger.info("API fallback: %d ads for %s", len(ads), page[1])
                        except Exception as api_exc:
                            logger.warning("API fallback also failed: %s", api_exc)
        finally:
            await self.scraper.close()

        # Ingest collected ads
        result = {"pages": len(pages), "ads": 0, "elapsed": 0}
        if all_ads:
            run_id = datetime.now(timezone.utc).strftime("pull_%Y%m%d_%H%M%S")
            counts = ingest_ads(self.conn, all_ads, source="scraper", scrape_run_id=run_id)
            result["ads"] = counts["new_ads"] + counts["updated_ads"]
            result.update(counts)

            # Post-scrape enrichment: lifecycle → keyword tracking
            try:
                enricher = KeywordEnricher(self.conn)
                enricher.enrich_all()
            except Exception as exc:
                logger.warning("Post-scrape enrichment failed: %s", exc)

        result["elapsed"] = round(time.monotonic() - t0, 1)
        logger.info("Daily pull complete: %s", result)
        return result

    # ── Stage: Discovery sweep ────────────────────────────────────────────────

    async def run_discovery_sweep(self) -> dict:
        """Keyword sweep for all tracked domains to find new advertisers."""
        t0 = time.monotonic()
        logger.info("Starting discovery sweep")

        domains = self.conn.execute(
            "SELECT domain FROM Domains WHERE is_active = 1"
        ).fetchall()
        domain_list = [d[0] for d in domains]

        if not domain_list:
            logger.info("No tracked domains — skipping discovery")
            return {"domains": 0, "ads": 0, "elapsed": 0}

        result = {"domains": len(domain_list), "ads": 0, "elapsed": 0}

        try:
            await self.scraper.setup()
            ads = await self.scraper.keyword_discovery_sweep(domain_list)
            if ads:
                run_id = datetime.now(timezone.utc).strftime("disc_%Y%m%d_%H%M%S")
                counts = ingest_ads(self.conn, ads, source="discovery", scrape_run_id=run_id)
                result["ads"] = counts["new_ads"]
                result.update(counts)
        finally:
            await self.scraper.close()

        result["elapsed"] = round(time.monotonic() - t0, 1)
        logger.info("Discovery sweep complete: %s", result)
        return result

    # ── Stage: Reconciliation ─────────────────────────────────────────────────

    async def run_reconciliation(self) -> dict:
        """Check all active ads — mark unseen ones via state machine."""
        t0 = time.monotonic()
        logger.info("Starting reconciliation")

        # Get all pages with active ads
        pages = self.conn.execute(
            """SELECT DISTINCT fp.fb_page_id
            FROM FacebookPages fp
            JOIN Ads a ON a.page_id = fp.id
            JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
            WHERE s.status = 'active'"""
        ).fetchall()
        page_ids = [p[0] for p in pages]

        if not page_ids:
            return {"pages": 0, "stopped": 0, "elapsed": 0}

        result = {"pages": len(page_ids), "stopped": 0, "elapsed": 0}

        try:
            await self.scraper.setup()
            recon_data = await self.scraper.reconciliation_sweep(page_ids)
        finally:
            await self.scraper.close()

        # For each tracked page, check which ads were NOT seen
        for fb_page_id, seen_ads in recon_data.items():
            seen_archive_ids = {a.get("ad_archive_id") for a in seen_ads if a.get("ad_archive_id")}

            # Get all active ads for this page
            active_ads = self.conn.execute(
                """SELECT a.id, a.ad_archive_id
                FROM Ads a
                JOIN FacebookPages fp ON a.page_id = fp.id
                JOIN AdSnapshots s ON s.ad_id = a.id AND s.is_current = 1
                WHERE fp.fb_page_id = ? AND s.status = 'active'""",
                (fb_page_id,),
            ).fetchall()

            for ad in active_ads:
                if ad[1] not in seen_archive_ids:
                    update_ad_status(
                        self.conn, ad[0], seen=False, is_reconciliation=True
                    )
                    result["stopped"] += 1
                else:
                    update_ad_status(self.conn, ad[0], seen=True)

            # Ingest any new ads found during reconciliation
            if seen_ads:
                ingest_ads(self.conn, seen_ads, source="reconciliation")

        result["elapsed"] = round(time.monotonic() - t0, 1)
        logger.info("Reconciliation complete: %s", result)
        return result

    # ── Stage: Intelligence pipeline ──────────────────────────────────────────

    async def run_intelligence_pipeline(self) -> dict:
        """Classify → extract keywords → sync embeddings → materialize metrics."""
        t0 = time.monotonic()
        logger.info("Starting intelligence pipeline")

        # Step 1: Classify and extract
        analysis = await self.analyzer.analyze_new_ads()

        # Step 2: Sync embeddings
        self.embedding_manager.setup()
        embedded = await self.analyzer.sync_embeddings()

        # Step 3: Materialize daily metrics
        materialize_daily_metrics(self.conn, _today())

        result = {
            **analysis,
            "embedded": embedded,
            "elapsed": round(time.monotonic() - t0, 1),
        }
        logger.info("Intelligence pipeline complete: %s", result)
        return result

    # ── Stage: Enrich keywords with CPC/volume via DataForSEO ──────────────

    async def run_enrich_keywords(self, limit: int = 2000) -> dict:
        """Enrich pending KeywordQueue entries with CPC/volume from DataForSEO.

        Cost-efficient: dedup → cache check → budget gate → Labs batch API.
        Uses cpc_cache.py for caching and budget management.
        Labs keyword_overview: $0.01/batch + $0.0001/keyword ≈ $0.18 for 1,500 kw.
        """
        import json as _json
        # Load .env BEFORE importing validation (DFS creds read at import time)
        try:
            from dotenv import load_dotenv
            load_dotenv(Path.home() / ".openclaw" / ".env", override=False)
        except Exception:
            pass
        t0 = time.monotonic()
        logger.info("Starting keyword enrichment")

        # ── Pre-step: Extract keywords from ad URLs (tiered by reliability) ────
        # Tier 1: q/query/keyword params (actual search query) — priority 100
        # Tier 2: adtitle param (article headline, cleaned) — priority 80
        # Tier 3: _slug (URL path, SEO-optimized) — priority 70
        import re as _re
        ad_rows = self.conn.execute(
            "SELECT extracted_keywords FROM Ads WHERE extracted_keywords IS NOT NULL"
        ).fetchall()
        new_kw_count = 0
        now_str = datetime.now(timezone.utc).isoformat()

        def _clean_headline(s: str) -> str:
            """Strip CTA, question prefixes, year suffixes from a headline."""
            s = _re.sub(r'\s+(Learn|Read|Find Out|Click|See)\s+(More|Here)\s*$', '', s, flags=_re.IGNORECASE)
            s = _re.sub(r'^(A\s+)?(Guide|Complete Guide)\s+To\s+', '', s, flags=_re.IGNORECASE)
            s = _re.sub(r'^These\s+Are\s+The\s+Top\s+\d+\s+', '', s, flags=_re.IGNORECASE)
            s = _re.sub(r'\s+\d{4}\s*$', '', s)
            return s.rstrip('?!.,').strip()

        def _clean_slug(slug: str) -> str:
            """Convert URL slug to readable keyword."""
            s = slug.replace('-', ' ').replace('_', ' ')
            s = _re.sub(r'\s+\d{4}\s*$', '', s)  # trailing year
            s = _re.sub(r'\b(en|es|de|fr|pt|ja)\b$', '', s).strip()  # trailing lang code
            return s

        for row in ad_rows:
            try:
                ek = _json.loads(row[0]) if isinstance(row[0], str) else row[0]

                # Tier 1: q/query/keyword params (highest value — actual search query)
                for param in ("q", "query", "keyword", "kw", "searchTerm", "forceKeyA"):
                    val = (ek.get(param) or "").strip()
                    if val and 3 <= len(val) <= 80 and not any(c in val for c in "{}[]|\\<>"):
                        self.conn.execute(
                            "INSERT OR IGNORE INTO Keywords (keyword, cpc_usd, competition, volume, created_at) "
                            "VALUES (?, 0, 0, 0, ?)", (val, now_str))
                        exists = self.conn.execute(
                            "SELECT 1 FROM KeywordQueue WHERE keyword = ? AND source LIKE 'url_%'", (val,)
                        ).fetchone()
                        if not exists:
                            self.conn.execute(
                                "INSERT INTO KeywordQueue (keyword, source, priority, status, created_at) "
                                "VALUES (?, 'url_search_query', 100, 'pending', ?)", (val, now_str))
                            new_kw_count += 1

                # Tier 2: adtitle (cleaned headline)
                adtitle = (ek.get("adtitle") or "").strip()
                if adtitle and len(adtitle) >= 3:
                    cleaned = _clean_headline(adtitle)
                    if cleaned and 3 <= len(cleaned) <= 60:
                        self.conn.execute(
                            "INSERT OR IGNORE INTO Keywords (keyword, cpc_usd, competition, volume, created_at) "
                            "VALUES (?, 0, 0, 0, ?)", (cleaned, now_str))
                        exists = self.conn.execute(
                            "SELECT 1 FROM KeywordQueue WHERE keyword = ? AND source LIKE 'url_%'", (cleaned,)
                        ).fetchone()
                        if not exists:
                            self.conn.execute(
                                "INSERT INTO KeywordQueue (keyword, source, priority, status, created_at) "
                                "VALUES (?, 'url_adtitle', 80, 'pending', ?)", (cleaned, now_str))
                            new_kw_count += 1

                # Tier 3: URL slug
                slug = (ek.get("_slug") or "").strip()
                if slug and len(slug) >= 3:
                    cleaned_slug = _clean_slug(slug)
                    if cleaned_slug and 3 <= len(cleaned_slug) <= 60:
                        self.conn.execute(
                            "INSERT OR IGNORE INTO Keywords (keyword, cpc_usd, competition, volume, created_at) "
                            "VALUES (?, 0, 0, 0, ?)", (cleaned_slug, now_str))
                        exists = self.conn.execute(
                            "SELECT 1 FROM KeywordQueue WHERE keyword = ? AND source LIKE 'url_%'", (cleaned_slug,)
                        ).fetchone()
                        if not exists:
                            self.conn.execute(
                                "INSERT INTO KeywordQueue (keyword, source, priority, status, created_at) "
                                "VALUES (?, 'url_slug', 70, 'pending', ?)", (cleaned_slug, now_str))
                            new_kw_count += 1
            except Exception:
                continue
        if new_kw_count:
            self.conn.commit()
            logger.info("Extracted %d keywords from ad URLs (q/adtitle/slug)", new_kw_count)

        # Retry previously failed keywords (Bug #10: reduce unscored count)
        retried = self.conn.execute(
            """UPDATE KeywordQueue SET status = 'pending'
               WHERE status = 'failed'
                 AND created_at >= datetime('now', '-30 days')"""
        ).rowcount
        if retried:
            self.conn.commit()
            logger.info("Retried %d previously failed keywords", retried)

        # Pull pending keywords
        pending = self.conn.execute(
            """SELECT kq.id, kq.keyword, kq.priority
               FROM KeywordQueue kq
               WHERE kq.status = 'pending'
               ORDER BY kq.priority DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()

        if not pending:
            logger.info("No pending keywords to enrich")
            return {"pending": 0, "enriched": 0, "cached": 0, "elapsed": 0}

        logger.info("Enriching %d pending keywords", len(pending))

        # Build keyword dicts (cpc_cache expects list of dicts with 'keyword'+'country')
        # and a map from (kw_lower, country) → list of kq_ids for DB updates
        kq_map: dict = {}  # (kw_lower, country) → [kq_ids]
        kw_dicts: list = []
        seen_keys: set = set()
        for row in pending:
            kw = row[1].strip()
            if not kw:
                continue
            key = (kw.lower(), "US")
            kq_map.setdefault(key, []).append(row[0])
            if key not in seen_keys:
                seen_keys.add(key)
                kw_dicts.append({"keyword": kw, "country": "US"})

        logger.info("Deduplicated: %d unique keywords from %d queue entries",
                     len(kw_dicts), len(pending))

        # Import helpers
        try:
            from cpc_cache import batch_cache_lookup, cache_write_back
            pass  # _fetch_dataforseo_labs_batch replaced by dfs_client below
        except ImportError as e:
            logger.error("Cannot import enrichment deps: %s", e)
            return {"pending": len(pending), "enriched": 0, "error": str(e), "elapsed": 0}

        # Step 1: Cache lookup (free — no API calls)
        cached_results, cache_misses = batch_cache_lookup(kw_dicts, ttl_hours=168)
        logger.info("Cache: %d hits, %d misses", len(cached_results), len(cache_misses))

        # Write cached results to Keywords table + mark queue done
        cached_count = self._write_enrichment_results(cached_results, kq_map)

        # Step 2: Budget check ($0.05 per task/batch of up to 700 for search_volume/live)
        to_lookup = []
        if cache_misses:
            try:
                from cpc_cache import _conn as _cpc_conn
                with _cpc_conn() as _con:
                    _row = _con.execute(
                        "SELECT usd_spent_today FROM api_usage WHERE date = date('now')"
                    ).fetchone()
                spent = float(_row["usd_spent_today"]) if _row else 0.0
                remaining_budget = 4.0 - spent
                # search_volume/live: $0.05 per task (one POST = one task, up to 700 kw)
                num_batches = -(-len(cache_misses) // 700)  # ceil division by batch size
                estimated_cost = num_batches * 0.05
                if estimated_cost > remaining_budget:
                    # Each batch of up to 700 costs $0.05; trim to affordable batches
                    affordable_batches = max(0, int(remaining_budget / 0.05))
                    affordable = affordable_batches * 700
                    logger.info("Budget: $%.2f remaining, can afford %d/%d keywords (%d batches)",
                                remaining_budget, affordable, len(cache_misses), affordable_batches)
                    cache_misses = cache_misses[:affordable]
                else:
                    logger.info("Budget: $%.2f remaining, cost $%.2f — all clear",
                                remaining_budget, estimated_cost)
            except Exception:
                pass
            # Filter: DFS rejects keywords with special chars, non-ASCII, >80 chars, >8 words.
            # R2-C5: a single Unicode/emoji keyword ("Levi's", "🏡") fails the ENTIRE batch of
            # up to 700 — so one bad char in one keyword silently zeroes out the run.
            _BAD_CHARS = set("!@#$%^&*(){}[]|\\;:,.<>?/")
            def _accepts_for_dfs(kw: str) -> bool:
                if not kw.strip() or len(kw) > 80 or len(kw.split()) > 8:
                    return False
                if any(c in _BAD_CHARS for c in kw):
                    return False
                # DFS rejects non-ASCII (curly quotes, emoji, diacritics via GA search_volume)
                try:
                    kw.encode("ascii")
                except UnicodeEncodeError:
                    return False
                return True
            to_lookup = [m for m in cache_misses if _accepts_for_dfs(m.get("keyword", ""))]
            skipped = len(cache_misses) - len(to_lookup)
            if skipped:
                logger.info("Filtered %d keywords (too long, special chars, or non-ASCII)", skipped)

        # Step 3: DFS search_volume/live via unified client (returns real CPC)
        api_count = 0
        if to_lookup:
            try:
                from modules.dfs_client import search_volume_batch, DFS_READY as _dfs_ok
            except ImportError as e:
                logger.error("Cannot import dfs_client: %s", e)
                _dfs_ok = False

            if not _dfs_ok:
                logger.error("DataForSEO not configured — cannot enrich keywords")
            else:
                # Group by country
                by_country: dict = {}
                for m in to_lookup:
                    country = m.get("country", "US")
                    by_country.setdefault(country, []).append(m["keyword"])

                cache_format = search_volume_batch(by_country)
                api_count = len(cache_format)

                # Write results to Keywords table
                if cache_format:
                    self._write_enrichment_results(cache_format, kq_map)
                    logger.info("Enriched %d keywords with real CPC data", len(cache_format))

                    # Write to cache
                    try:
                        cache_write_back(cache_format)
                    except Exception as ce:
                        logger.warning("Cache write-back failed: %s", ce)

                # Fetch KD via keyword_overview (also provides intent/SERP data)
                try:
                    from modules.dfs_client import keyword_overview_batch as _kw_overview
                    from cpc_cache import labs_cache_lookup_batch, labs_cache_write_batch
                    # Check labs cache first (heartbeat pipeline may have already enriched these)
                    labs_pairs = list(cache_format.keys())
                    cached_enrich, uncached = labs_cache_lookup_batch(labs_pairs, ttl_hours=168)
                    # Only call API for uncached keywords
                    if uncached:
                        by_c: dict = {}
                        for kw, c in uncached:
                            by_c.setdefault(c, []).append(kw)
                        fresh = _kw_overview(by_c)
                        if fresh:
                            labs_cache_write_batch(fresh)
                            cached_enrich.update(fresh)
                    kd_count = 0
                    for (kw, country), enrich in cached_enrich.items():
                        kd_val = enrich.get("kd")
                        if kd_val is not None:
                            self.conn.execute(
                                "UPDATE Keywords SET kd = ? WHERE LOWER(keyword) = ?",
                                (int(kd_val), kw.lower()),
                            )
                            kd_count += 1
                    if kd_count:
                        logger.info("KD enrichment: %d/%d keywords got KD (%d from cache)",
                                    kd_count, len(labs_pairs), len(cached_enrich) - len(uncached if uncached else []))
                except Exception as kd_exc:
                    logger.warning("KD enrichment failed: %s", kd_exc)

        self.conn.commit()

        result = {
            "pending": len(pending),
            "unique": len(kw_dicts),
            "cached": cached_count,
            "api_looked_up": api_count,
            "enriched": cached_count + api_count,
            "elapsed": round(time.monotonic() - t0, 1),
        }
        # R2-C5: surface the reason the stage produced zero enrichments so a
        # silent-run state cannot masquerade as healthy in the log tail.
        if result["enriched"] == 0 and result["pending"] > 0:
            if not to_lookup and cache_misses:
                reason = "all_keywords_filtered_pre_dfs"
            elif api_count == 0 and to_lookup:
                reason = "dfs_batches_rejected_or_auth_failed"
            elif not cache_misses and cached_count == 0:
                reason = "cache_empty_and_no_misses"
            else:
                reason = "unknown"
            logger.error("[enrich_keywords] degraded: reason=%s %s", reason, result)
        else:
            logger.info("Enrichment complete: %s", result)
        return result

    def _write_enrichment_results(self, results: dict, kq_map: dict) -> int:
        """Write CPC/volume data to Keywords table and mark KeywordQueue done.

        Args:
            results: {(kw_lower, country): {cpc_usd, search_volume, competition}}
            kq_map: {(kw_lower, country): [kq_ids]}

        Returns: count of keywords updated.
        """
        count = 0
        now = datetime.now(timezone.utc).isoformat()
        for (kw, country), metrics in results.items():
            cpc = float(metrics.get("cpc_usd", 0) or 0)
            vol = int(metrics.get("search_volume", 0) or metrics.get("volume", 0) or 0)
            comp = float(metrics.get("competition", 0) or 0)
            kd_val = metrics.get("kd")

            if kd_val is not None:
                self.conn.execute(
                    "UPDATE Keywords SET cpc_usd = ?, competition = ?, volume = ?, kd = ? WHERE LOWER(keyword) = ?",
                    (cpc, comp, vol, int(kd_val), kw.lower()),
                )
            else:
                self.conn.execute(
                    "UPDATE Keywords SET cpc_usd = ?, competition = ?, volume = ? WHERE LOWER(keyword) = ?",
                    (cpc, comp, vol, kw.lower()),
                )
            for kq_id in kq_map.get((kw.lower(), country.upper()), []):
                self.conn.execute(
                    "UPDATE KeywordQueue SET status = 'done', processed_at = ? WHERE id = ?",
                    (now, kq_id),
                )
            count += 1
        return count

    # ── Stage: Article analysis (original angle + generated alternatives) ────

    async def run_article_analysis(self) -> dict:
        """For each adtitle keyword, classify the competitor's original angle
        from ad creative, then generate 4-5 alternative angles via LLM.

        Stores results in KeywordAngles table.
        """
        import json as _json
        t0 = time.monotonic()
        logger.info("Starting article analysis")

        # Get adtitle keywords that don't have angles yet
        existing_kw_ids = set(
            r[0] for r in self.conn.execute(
                "SELECT DISTINCT keyword_id FROM KeywordAngles"
            ).fetchall()
        )

        # Build keyword → ad mapping from extracted_keywords JSON
        kw_ad_map: dict = {}  # keyword_text → {keyword_id, ads: [{headline, creative, url, ad_id, vertical}]}
        ad_rows = self.conn.execute(
            """SELECT a.id, a.extracted_keywords, a.headline, a.creative_text,
                      a.landing_url, a.primary_vertical, a.primary_angle
               FROM Ads a WHERE a.extracted_keywords IS NOT NULL"""
        ).fetchall()

        for row in ad_rows:
            try:
                ek = _json.loads(row[1]) if isinstance(row[1], str) else row[1]
                adtitle = (ek.get("adtitle") or "").strip()
                if not adtitle or len(adtitle) < 3:
                    continue

                # Find keyword_id
                kw_row = self.conn.execute(
                    "SELECT id FROM Keywords WHERE keyword = ?", (adtitle,)
                ).fetchone()
                if not kw_row or kw_row[0] in existing_kw_ids:
                    continue

                kw_ad_map.setdefault(adtitle, {
                    "keyword_id": kw_row[0],
                    "ads": [],
                })
                kw_ad_map[adtitle]["ads"].append({
                    "ad_id": row[0],
                    "headline": row[2],
                    "creative": row[3],
                    "url": row[4],
                    "vertical": row[5],
                    "angle": row[6],
                })
            except Exception:
                continue

        # Process in batches of 3 keywords per LLM call
        from llm_client import call as _llm_call

        if not kw_ad_map:
            logger.info("Pass 1: no keywords with ads need analysis — skipping to Pass 2")
        else:
            logger.info("Pass 1: analyzing %d keywords with ads", len(kw_ad_map))
        import asyncio

        ANGLE_TYPES = [
            "Listicle", "Fear/Urgency", "How-To", "Testimonial", "Comparison",
            "Question", "News/Breaking", "Secret/Reveal", "Transformation", "Direct Offer",
        ]

        total_angles = 0
        items = list(kw_ad_map.items())
        batch_size = 3

        for batch_start in range(0, len(items), batch_size):
            batch = items[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = -(-len(items) // batch_size)

            # Build batch prompt
            prompt_parts = []
            for idx, (kw, data) in enumerate(batch, 1):
                ad = data["ads"][0]  # use first ad as representative
                prompt_parts.append(
                    f"Keyword {idx}: \"{kw}\"\n"
                    f"  Competitor headline: {ad.get('headline') or 'N/A'}\n"
                    f"  Competitor copy: {(ad.get('creative') or 'N/A')[:200]}\n"
                    f"  Vertical: {ad.get('vertical') or 'general'}\n"
                    f"  Competitor angle: {ad.get('angle') or 'unknown'}"
                )

            system = (
                "You are an RSOC article angle strategist. For each keyword below:\n"
                "1. Identify the competitor's current angle from their headline/copy\n"
                "2. Generate 5 alternative article angles that could compete for the same keyword\n\n"
                f"Available angle types: {', '.join(ANGLE_TYPES)}\n\n"
                "Return a JSON array with one object per keyword:\n"
                '[{"keyword": "...", "original_angle": {"type": "...", "title": "..."}, '
                '"alternatives": [{"type": "...", "title": "..."}, ...]}]'
            )

            user = "Analyze these keywords:\n\n" + "\n\n".join(prompt_parts)

            try:
                raw = await asyncio.to_thread(
                    _llm_call,
                    [{"role": "system", "content": system}, {"role": "user", "content": user}],
                    max_tokens=400 * len(batch),
                    temperature=0.4,
                    timeout="normal",
                    stage="fb_intelligence/article_analysis",
                )

                # Parse response
                import re
                start = raw.find("[")
                end = raw.rfind("]") + 1
                if start >= 0 and end > start:
                    results = _json.loads(raw[start:end])
                else:
                    results = []

                for idx, (kw, data) in enumerate(batch):
                    if idx >= len(results):
                        break
                    r = results[idx]
                    kw_id = data["keyword_id"]
                    ad = data["ads"][0]
                    vertical = ad.get("vertical") or "general"

                    # Store original angle
                    orig = r.get("original_angle", {})
                    if orig.get("title"):
                        self.conn.execute(
                            """INSERT OR REPLACE INTO KeywordAngles
                               (keyword_id, angle_type, angle_title, source, confidence, ad_id, article_url, vertical)
                               VALUES (?, ?, ?, 'original', ?, ?, ?, ?)""",
                            (kw_id, orig.get("type", "unknown"), orig["title"],
                             0.9, ad.get("ad_id"), ad.get("url"), vertical),
                        )
                        total_angles += 1

                    # Store alternatives
                    for alt in (r.get("alternatives") or [])[:5]:
                        if alt.get("title"):
                            self.conn.execute(
                                """INSERT OR REPLACE INTO KeywordAngles
                                   (keyword_id, angle_type, angle_title, source, confidence, vertical)
                                   VALUES (?, ?, ?, 'generated', ?, ?)""",
                                (kw_id, alt.get("type", "unknown"), alt["title"],
                                 0.7, vertical),
                            )
                            total_angles += 1

                logger.info(
                    "Batch %d/%d: %d keywords → %d angles (%.1fs)",
                    batch_num, total_batches, len(batch), total_angles,
                    time.monotonic() - t0,
                )

            except Exception as exc:
                logger.error("Article analysis batch failed: %s", exc)
                _log_error("fb_intelligence/article_analysis", str(exc))

        self.conn.commit()
        pass1_count = len(kw_ad_map)

        # ── Pass 2: Batch-generate angles for keywords WITHOUT ads ────────
        # Re-check which keywords now have angles (after Pass 1)
        has_angles = set(
            r[0] for r in self.conn.execute(
                "SELECT DISTINCT keyword_id FROM KeywordAngles"
            ).fetchall()
        )

        # Get keywords without angles, prioritise by CPC
        placeholders = ",".join("?" * len(has_angles)) if has_angles else "0"
        no_angle_kws = self.conn.execute(f"""
            SELECT k.id, k.keyword,
                   COALESCE(
                       (SELECT GROUP_CONCAT(DISTINCT a.primary_vertical)
                        FROM AdKeywords ak JOIN Ads a ON ak.ad_id = a.id
                        WHERE ak.keyword_id = k.id AND a.primary_vertical IS NOT NULL),
                       'general'
                   ) AS vertical
            FROM Keywords k
            WHERE k.id NOT IN ({placeholders})
            ORDER BY COALESCE(k.cpc_usd, 0) DESC
            LIMIT 200
        """, list(has_angles) if has_angles else []).fetchall()

        if no_angle_kws:
            logger.info("Pass 2: generating angles for %d keywords without ads", len(no_angle_kws))
            pass2_batch_size = 5
            pass2_angles = 0

            for batch_start in range(0, len(no_angle_kws), pass2_batch_size):
                batch = no_angle_kws[batch_start:batch_start + pass2_batch_size]
                batch_num = batch_start // pass2_batch_size + 1
                total_batches = -(-len(no_angle_kws) // pass2_batch_size)

                prompt_parts = []
                for idx, row in enumerate(batch, 1):
                    vert = (row[2] or "general").split(",")[0]
                    prompt_parts.append(f'Keyword {idx}: "{row[1]}" (vertical: {vert})')

                system = (
                    "You are an RSOC article angle strategist. For each keyword below, "
                    "generate 5 unique article angles that would attract search traffic.\n\n"
                    f"Available angle types: {', '.join(ANGLE_TYPES)}\n\n"
                    "Return a JSON array with one object per keyword:\n"
                    '[{"keyword": "...", "angles": [{"type": "...", "title": "..."}, ...]}]'
                )
                user = "Generate angles for:\n\n" + "\n\n".join(prompt_parts)

                try:
                    raw = await asyncio.to_thread(
                        _llm_call,
                        [{"role": "system", "content": system}, {"role": "user", "content": user}],
                        max_tokens=400 * len(batch),
                        temperature=0.4,
                        timeout="normal",
                        stage="fb_intelligence/article_analysis_pass2",
                    )

                    start = raw.find("[")
                    end = raw.rfind("]") + 1
                    results = _json.loads(raw[start:end]) if start >= 0 and end > start else []

                    for idx, row in enumerate(batch):
                        if idx >= len(results):
                            break
                        kw_id = row[0]
                        vert = (row[2] or "general").split(",")[0]
                        for alt in (results[idx].get("angles") or [])[:5]:
                            if alt.get("title"):
                                self.conn.execute(
                                    """INSERT OR IGNORE INTO KeywordAngles
                                       (keyword_id, angle_type, angle_title, source, confidence, vertical)
                                       VALUES (?, ?, ?, 'generated', 0.7, ?)""",
                                    (kw_id, alt.get("type", "unknown"), alt["title"], vert),
                                )
                                pass2_angles += 1

                    logger.info(
                        "Pass 2 batch %d/%d: %d keywords → %d angles (%.1fs)",
                        batch_num, total_batches, len(batch), pass2_angles,
                        time.monotonic() - t0,
                    )
                except Exception as exc:
                    logger.error("Pass 2 batch %d failed: %s", batch_num, exc)
                    _log_error("fb_intelligence/article_analysis_pass2", str(exc))

            self.conn.commit()
            total_angles += pass2_angles
            logger.info("Pass 2 complete: %d angles for %d keywords", pass2_angles, len(no_angle_kws))

        result = {
            "keywords_with_ads": pass1_count,
            "keywords_without_ads": len(no_angle_kws) if no_angle_kws else 0,
            "angles": total_angles,
            "elapsed": round(time.monotonic() - t0, 1),
        }
        logger.info("Article analysis complete: %s", result)
        return result

    # ── Stage: Alert check ────────────────────────────────────────────────────

    async def run_alert_check(self) -> dict:
        """Run threshold checks and send triggered alerts."""
        return await self.alert_manager.check_and_alert()

    # ── Stage: Daily digest ───────────────────────────────────────────────────

    async def run_daily_digest(self) -> bool:
        """Send the daily digest via Telegram."""
        return await self.alert_manager.send_daily_digest()

    # ── Stage: Weekly report ──────────────────────────────────────────────────

    async def run_weekly_report(self) -> bool:
        """Send the weekly report via Telegram."""
        return await self.alert_manager.send_weekly_report()

    # ── Stage: Health checks ──────────────────────────────────────────────────

    async def run_health_checks(self) -> dict:
        """Run all health checks and alert on failures.

        Checks: job completion, volume vs 7d avg, null rates,
        data freshness, error rate, proxy health.
        """
        cursor = self.conn.cursor()
        issues: list[str] = []

        # 1. Volume check: ads per domain vs 7-day average
        domains = cursor.execute(
            "SELECT id, domain FROM Domains WHERE is_active = 1"
        ).fetchall()

        failing_domains = 0
        for domain in domains:
            recent = cursor.execute(
                """SELECT COUNT(*) FROM Ads a
                JOIN FacebookPages fp ON a.page_id = fp.id
                WHERE fp.domain_id = ?
                  AND a.last_seen >= datetime('now', '-1 day')""",
                (domain[0],),
            ).fetchone()[0]

            avg_row = cursor.execute(
                """SELECT AVG(cnt) FROM (
                    SELECT COUNT(*) AS cnt FROM Ads a
                    JOIN FacebookPages fp ON a.page_id = fp.id
                    WHERE fp.domain_id = ?
                      AND a.last_seen >= datetime('now', '-7 days')
                    GROUP BY date(a.last_seen)
                )""",
                (domain[0],),
            ).fetchone()
            avg_daily = float(avg_row[0]) if avg_row and avg_row[0] else 0

            if recent == 0 and avg_daily > 0:
                issues.append(f"Volume: {domain[1]} — 0 ads (avg {avg_daily:.0f}/day)")
                failing_domains += 1

        # 2. Data freshness
        latest = cursor.execute(
            "SELECT MAX(last_seen) FROM Ads"
        ).fetchone()
        if latest and latest[0]:
            try:
                last_ts = datetime.fromisoformat(latest[0].replace("Z", "+00:00"))
                hours_stale = (
                    datetime.now(timezone.utc) - last_ts
                ).total_seconds() / 3600
                if hours_stale > 26:
                    issues.append(f"Freshness: last data is {hours_stale:.0f}h old")
            except Exception:
                pass

        # 3. Null rate check
        total = cursor.execute("SELECT COUNT(*) FROM Ads").fetchone()[0]
        if total > 0:
            null_class = cursor.execute(
                "SELECT COUNT(*) FROM Ads WHERE classification_conf IS NULL"
            ).fetchone()[0]
            null_rate = null_class / total
            if null_rate > 0.5:
                issues.append(
                    f"Data quality: {null_rate:.0%} ads unclassified"
                )

        # 4. Error rate (alerts in last 24h)
        error_count = cursor.execute(
            """SELECT COUNT(*) FROM Signals
            WHERE signal_type = 'alert'
              AND signal_value LIKE 'P0%'
              AND created_at >= datetime('now', '-1 day')"""
        ).fetchone()[0]
        if error_count > 3:
            issues.append(f"Error rate: {error_count} P0 alerts in 24h")

        # P0: ALL domains failing
        all_failing = failing_domains == len(domains) and len(domains) > 0
        if all_failing:
            msg = (
                "🔴 <b>P0: ALL DOMAINS FAILING</b>\n"
                "No data collected from any tracked domain in 24h.\n"
                "Possible causes: proxy down, Facebook blocking, scraper crash."
            )
            await self.alert_manager.send_telegram(msg)

        result = {
            "issues": len(issues),
            "all_failing": all_failing,
            "details": issues,
        }

        if issues and not all_failing:
            msg = (
                "⚠️ <b>Health Check Issues</b>\n"
                + "\n".join(f"  • {i}" for i in issues)
            )
            await self.alert_manager.send_telegram(msg)

        logger.info("Health checks: %d issues", len(issues))
        return result

    # ── Stage: Landing page crawling ────────────────────────────────────────────

    async def run_crawl_landing_pages(self) -> dict:
        """Crawl landing pages for ads not yet analyzed. Detects RSOC partners,
        extracts keywords, computes template hashes and monetization density."""
        import json as _json
        from dwight.fb_intelligence.landing_page import (
            detect_rsoc_partner,
            extract_landing_page_keywords,
            compute_template_hash,
            monetization_density,
        )

        t0 = time.monotonic()
        counts = {"crawled": 0, "partners_detected": 0, "keywords_found": 0, "errors": 0}

        # Ads with landing URLs that don't have a LandingPages row yet
        pending = self.conn.execute(
            """SELECT a.id, a.landing_url, a.landing_domain
               FROM Ads a
               WHERE a.landing_url IS NOT NULL
                 AND a.id NOT IN (SELECT ad_id FROM LandingPages WHERE ad_id IS NOT NULL)
               ORDER BY a.last_seen DESC
               LIMIT 50""",
        ).fetchall()

        if not pending:
            logger.info("No pending landing pages to crawl")
            return {**counts, "elapsed": 0.0}

        logger.info("Crawling %d landing pages", len(pending))

        try:
            await self.landing_crawler.setup()

            for ad_row in pending:
                ad_id = ad_row["id"]
                url = ad_row["landing_url"]
                domain = ad_row["landing_domain"]

                try:
                    result = await self.landing_crawler.crawl(url)
                    html = result.get("html", "")
                    if not html:
                        counts["errors"] += 1
                        continue

                    partner_info = detect_rsoc_partner(html, result["final_url"])
                    lp_keywords = extract_landing_page_keywords(
                        html, result["final_url"], partner_info.get("partner", "unknown"),
                    )
                    template_hash = compute_template_hash(html)
                    mon_density = monetization_density(html)

                    # Extract title and description from HTML
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.title.string.strip() if soup.title and soup.title.string else None
                    desc_tag = soup.find("meta", attrs={"name": "description"})
                    description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else None

                    metadata = _json.dumps({
                        "rsoc_partner": partner_info.get("partner"),
                        "rsoc_confidence": partner_info.get("confidence"),
                        "rsoc_signals": partner_info.get("signals"),
                        "landing_keywords": lp_keywords,
                        "template_hash": template_hash,
                        "monetization_density": mon_density,
                        "final_url": result["final_url"],
                        "status_code": result["status_code"],
                    })

                    self.conn.execute(
                        """INSERT OR IGNORE INTO LandingPages
                           (ad_id, url, domain, title, description, content_hash, fetched_at, metadata, html_content)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (ad_id, url, domain, title, description, template_hash, _now(), metadata, html),
                    )

                    # Update ad's rsoc_partner if not already set
                    partner = partner_info.get("partner")
                    if partner and partner != "unknown":
                        self.conn.execute(
                            "UPDATE Ads SET rsoc_partner = ? WHERE id = ? AND (rsoc_partner IS NULL OR rsoc_partner = 'unknown')",
                            (partner, ad_id),
                        )
                        counts["partners_detected"] += 1

                    counts["keywords_found"] += len(lp_keywords)
                    counts["crawled"] += 1

                except Exception as exc:
                    counts["errors"] += 1
                    _log_error("fb_intelligence/landing_crawl", str(exc), {"ad_id": ad_id})

            self.conn.commit()

        finally:
            await self.landing_crawler.close()

        counts["elapsed"] = round(time.monotonic() - t0, 1)
        logger.info(
            "Landing pages: %d crawled, %d partners, %d keywords, %d errors in %.1fs",
            counts["crawled"], counts["partners_detected"],
            counts["keywords_found"], counts["errors"], counts["elapsed"],
        )
        return counts

    # ── Full nightly sequence ─────────────────────────────────────────────────

    async def run_full(self) -> dict:
        """Run the complete nightly pipeline sequence."""
        logger.info("Starting full pipeline run")
        t0 = time.monotonic()
        results = {}

        stages = [
            ("daily_pull", self.run_daily_pull),
            ("reconciliation", self.run_reconciliation),
            ("intelligence", self.run_intelligence_pipeline),
            ("crawl_landing_pages", self.run_crawl_landing_pages),
            ("enrich_keywords", self.run_enrich_keywords),
            ("article_analysis", self.run_article_analysis),
            ("alert_check", self.run_alert_check),
            ("health", self.run_health_checks),
        ]

        for name, fn in stages:
            try:
                logger.info("=== Stage: %s ===", name)
                results[name] = await fn()
            except Exception as exc:
                logger.error("Stage %s failed: %s", name, exc)
                _log_error(
                    "fb_intelligence/scheduler",
                    f"Stage {name} failed: {exc}",
                )
                results[name] = {"error": str(exc)}

        results["total_elapsed"] = round(time.monotonic() - t0, 1)
        logger.info("Full pipeline complete in %.1fs", results["total_elapsed"])
        return results

    # ── Schedule info ─────────────────────────────────────────────────────────

    @staticmethod
    def get_cron_schedule() -> dict:
        """Return the cron schedule for all pipeline stages."""
        return CRON_SCHEDULE


# ── CLI entry point ───────────────────────────────────────────────────────────

COMMANDS = {
    "daily_pull": "run_daily_pull",
    "discovery": "run_discovery_sweep",
    "reconciliation": "run_reconciliation",
    "intelligence": "run_intelligence_pipeline",
    "enrich_keywords": "run_enrich_keywords",
    "article_analysis": "run_article_analysis",
    "digest": "run_daily_digest",
    "report": "run_weekly_report",
    "crawl_landing_pages": "run_crawl_landing_pages",
    "health": "run_health_checks",
    "alert_check": "run_alert_check",
    "full": "run_full",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="fb_intelligence pipeline scheduler",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=list(COMMANDS.keys()),
        help="Pipeline stage to run",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List all stages and their cron schedules",
    )

    args = parser.parse_args()

    if args.list or not args.command:
        print("fb_intelligence pipeline stages:\n")
        schedule = PipelineScheduler.get_cron_schedule()
        for cmd, method in COMMANDS.items():
            cron = schedule.get(cmd, "manual")
            print(f"  {cmd:<20s} cron: {cron}")
        if not args.command:
            parser.print_usage()
        return

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
        level=logging.INFO,
    )

    scheduler = PipelineScheduler()
    method_name = COMMANDS[args.command]
    method = getattr(scheduler, method_name)

    result = asyncio.run(method())
    if isinstance(result, dict):
        import json
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


if __name__ == "__main__":
    main()
