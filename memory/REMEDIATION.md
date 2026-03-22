# OpenClaw Remediation Tracker

> Known bugs, non-functional features, and optimization roadmap.
> Read when fixing bugs or working on integration tasks.
> Last updated: 2026-03-17

---

## Known Bugs (Fix Before New Features)

| # | Bug | File | Fix | Status |
|---|-----|------|-----|--------|
| 1 | `is_duplicate()` L2 distance but threshold assumed cosine — dedup missed most duplicates. Also: `score` column had wrong type `string` (should be `float`). | `vector_store.py` | L2→cosine threshold conversion using `sqrt(2*(1-T))`. Schema migration: drops old trends table on first run, recreates with float score. | **FIXED 2026-03-17** |
| 2 | False positive detection compared raw terms vs LLM-pivoted keywords — never matched | `reflection.py` | `source_trend` field added to `validated_keywords` set | **FIXED 2026-03-16** |
| 3 | `score()` crashes if traffic value is numeric instead of string | `trends_postprocess.py:22` | `str()` cast already present in code | **FIXED (pre-existing)** |
| 4 | `dwight_agent.py` backtick sanitization uses wrong `.replace()` logic | smolagents branch | Use regex like `telegram_bot.py:155` | OPEN |
| 5 | 126 LiteLLM 401 errors in error_log.jsonl | `keyword_extractor.py` | `sk-dwight-local` already matches `master_key` in litellm_config.yaml — stale errors from old config | **FIXED (pre-existing)** |
| 6 | Country tier defs inconsistent across files | 3 files | Both `validation.py` and `dashboard_builder.py` already import `get_country_tier` from `country_config.py` | **FIXED (pre-existing)** |
| 7 | Telegram token in plaintext in MEMORY.md line 2 | `MEMORY.md` | Token redacted to `[REDACTED]` in workspace/MEMORY.md. **Operator must regenerate token via @BotFather** — old token still active and exposed in conversation history. | PARTIAL |
| 8 | 4,471 semantic_duplicate entries flooding error_log.jsonl | `trends_postprocess.py` | Already writes to `dedup_log.jsonl` — not error_log | **FIXED (pre-existing)** |
| 9 | MEMORY.md is 2,788+ lines / growing unbounded | `reflection.py` | reflection.py rewired to `memory/logs/YYYY-MM-DD.md` | **FIXED 2026-03-16** |
| 10 | **Golden rate = 0** — deferred keywords (cpc_usd=0.0, search_volume=0) matched the pre-fetch skip condition, bypassing the cache/API lookup entirely → all 975 deferred keywords went UNSCORED | `validation.py:598` | Pre-fetch skip now checks `metrics_source not in ("deferred","none_configured","")` before skipping | **FIXED 2026-03-17** |

---

## Non-Functional Features

### 1. Google Ads Keyword Planner API
**Status**: CODE COMPLETE — CREDENTIALS MISSING
**Priority**: HIGH
**Impact**: Highest-quality CPC data, free API, enables Stage 2a

**Root cause**: 5 env vars empty, Google Ads account not approved, OAuth2 refresh token not generated.

**Fix steps**:
1. Create Google Ads account (free, needs credit card)
2. Apply for API access at `https://ads.google.com/aw/apicenter` (1-3 days)
3. Create OAuth2 Desktop Client in Google Cloud Console
4. Generate refresh token: `cd scripts && python3 google_ads_setup.py`
5. Add to `~/.openclaw/.env`: `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET`, `GOOGLE_ADS_REFRESH_TOKEN`, `GOOGLE_ADS_DEVELOPER_TOKEN`, `GOOGLE_ADS_CUSTOMER_ID`
6. Verify: `python3 -c "from validation import GADS_READY; print(GADS_READY)"` → True

**Notes**: OAuth refresh token expires every 6 months. Google deprecates API versions quarterly.

### 2. LanceDB Vector Store — Opportunity Indexing
**Status**: PARTIALLY WIRED
**Priority**: HIGH

**What works**: `trends_postprocess.py` calls `is_duplicate()` and `add_trend()` ✅
**What's broken**: `validation.py` does NOT call `add_opportunity()` ❌. Historical data not backfilled.

**Fix steps**:
1. Backfill: `python3 index_history.py` (~2hrs, Ollama bottleneck)
2. Wire `validation.py` — after scoring loop, call `add_opportunity()` for GOLDEN + WATCH tags
3. Wire `telegram_bot.py` — add `/find` command using `search_opportunities()`

### 3. smolagents CodeAgent
**Status**: PROTOTYPE ON BRANCH — NOT MERGED
**Priority**: MEDIUM

**Root cause**: Code on `smolagents-migration` branch, untested end-to-end. Bot still uses fragile backtick-based exec parsing.

**Fix steps**:
1. `git checkout smolagents-migration && python3 dwight_agent.py` (test)
2. Fix backtick bug (#4 above) first
3. Merge to main, replace LLM loop in `telegram_bot.py` with DwightAgent

### 4. SearXNG Self-Hosted SERP
**Status**: NOT STARTED
**Priority**: LOW (DuckDuckGo works but CAPTCHA-prone at ~50 req/hr)

**Fix steps**:
1. `docker run -d --name searxng -p 8888:8080 -v ~/.openclaw/searxng:/etc/searxng searxng/searxng`
2. Configure engines (google, bing, brave) in settings.yml
3. Replace DuckDuckGo in `vetting.py` with `http://localhost:8888/search?q={kw}&format=json`
4. Add launchd service for auto-start

### 5. Reddit Scanner Pipeline Integration
**Status**: MODULE EXISTS — NOT WIRED TO HEARTBEAT
**Priority**: LOW (Reddit already scraped in `trends_scraper.py`)

**Fix**: Rename `reddit_scanner.py` → `reddit_trends.py`, add as Stage 1a in heartbeat.py, modify output to match trends JSON schema, merge into `trends_postprocess.py` input.

### 6. SQLite Migration (JSON → DB)
**Status**: PARTIAL (oracle.db = cache only)
**Priority**: MEDIUM

**Fix**: Create `opportunities` table in oracle.db with UNIQUE(keyword, country). Modify `validation.py` to INSERT OR REPLACE alongside JSON write. Modify `dashboard_builder.py` to read from SQLite.

### 7. Dynamic Skill Injection
**Status**: DISABLED (22 skills = 25,250 tokens, exceeded Ollama context)
**Priority**: MEDIUM

**Fix**: Build semantic skill selector using vector_store embeddings — select top-3 per message. Alternative: after smolagents migration, convert skills to `@tool` functions.

### 8. MEMORY.md Rotation / Replacement
**Status**: BROKEN (2,788+ lines, duplicate blocks, plaintext token)
**Priority**: HIGH

**Fix**: The new `memory/` system replaces this. `reflection.py` should be modified to:
- Write to `memory/logs/YYYY-MM-DD.md` instead of appending to MEMORY.md
- Keep only summary stats (golden rate, signal weights) — not 21K false positive listings
- Old MEMORY.md can be archived and deleted after token regeneration (Bug #7)

---

## Optimization Roadmap

### IMMEDIATE — Bug Fixes
- [x] ~~Fix `vector_store.py` — switch to cosine distance metric (Bug #1)~~ → L2-corrected threshold + schema migration (2026-03-17)
- [x] ~~Fix `reflection.py` — compare against `source_trend` not `keyword` (Bug #2)~~ → done (2026-03-16)
- [x] ~~Fix `trends_postprocess.py` — add `str()` cast in `score()` (Bug #3)~~ → was already present
- [x] ~~Separate semantic_duplicate logs from error_log.jsonl (Bug #8)~~ → was already using dedup_log.jsonl
- [x] ~~Unify country tier definitions (Bug #6)~~ → was already unified
- [x] ~~Fix golden rate = 0 (Bug #10)~~ → validation.py pre-fetch skip fixed (2026-03-17)
- [ ] **Regenerate Telegram token via @BotFather** (Bug #7) — operator action required. Old token exposed in conversation history and MEMORY.md backups.

### SHORT-TERM — Dual-Track Strategy + Memory
- [x] ~~Modify `reflection.py` to use new memory system (write to `memory/logs/`)~~ → done (2026-03-16)
- [ ] Add EMERGING tag to `validation.py` (if not already done)
- [ ] Create/maintain `vertical_cpc_reference.json`
- [ ] Wire `reddit_intelligence.py` properly into heartbeat
- [ ] Add ad longevity tracking to `validation.py`
- [ ] Add Track A evergreen seed list
- [ ] Update dashboard with EMERGING tab
- [ ] Wire LanceDB `add_opportunity()` in validation.py

### MEDIUM-TERM — Refinement
- [ ] Complete Google Ads OAuth setup
- [ ] Self-host SearXNG to replace DuckDuckGo
- [ ] Auto-update `vertical_cpc_reference.json` from validation_history.jsonl
- [ ] Add Telegram alerts for compliance_alert + feed_intel from reddit_intelligence
- [ ] Finalize smolagents migration

### LONG-TERM — Architecture
- [ ] Migrate JSON storage to SQLite
- [ ] Add traffic acquisition cost estimation (feed RPM − native CPM = true profit)
- [ ] Embed session logs into LanceDB for semantic search (when logs > 50 files)
