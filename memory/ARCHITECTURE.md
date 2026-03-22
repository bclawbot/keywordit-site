# OpenClaw Architecture Reference

> Read when working on specific pipeline stages, file paths, or strategy questions.

---

## All Key File Paths

```
# ── Config ────────────────────────────────────────────────────────────────
~/.openclaw/openclaw.json                          # Main config
~/.openclaw/.env                                    # API keys — do not read/print
~/.openclaw/litellm_config.yaml                    # LiteLLM proxy config

# ── Bot ───────────────────────────────────────────────────────────────────
~/.openclaw/workspace/telegram_bot.py              # Bot — main entry point
~/.openclaw/workspace/SOUL.md                      # System prompt (execution rules)

# ── Pipeline stages ───────────────────────────────────────────────────────
~/.openclaw/workspace/heartbeat.py                 # Full pipeline runner (9 stages)
~/.openclaw/workspace/reddit_intelligence.py       # Stage 0: RSOC subreddit intel
~/.openclaw/workspace/trends_scraper.py            # Stage 1: async scraper (GT+Bing+Reddit+GNews)
~/.openclaw/workspace/trends_postprocess.py        # Stage 1b: explosive filter + LanceDB dedup
~/.openclaw/workspace/keyword_expander.py          # Stage 2a: Google Ads free keyword expansion
~/.openclaw/workspace/keyword_extractor.py         # Stage 2: LLM pivot + DataForSEO batch CPC
~/.openclaw/workspace/vetting.py                   # Stage 2b: commercial SERP signal check
~/.openclaw/workspace/validation.py                # Stage 3: scoring + persistence + EMERGING
~/.openclaw/workspace/dashboard_builder.py         # Stage 4: builds dashboard.html
~/.openclaw/workspace/reflection.py                # Stage 5: updates MEMORY.md

# ── Supporting modules ────────────────────────────────────────────────────
~/.openclaw/workspace/country_config.py            # Per-country CPC/volume thresholds and tiers
~/.openclaw/workspace/cpc_cache.py                 # SQLite cache, dedup, budget gate helpers
~/.openclaw/workspace/vector_store.py              # LanceDB embed/dedup/search
~/.openclaw/workspace/trend_forecast.py            # NeuralProphet persistence forecasts
~/.openclaw/workspace/index_history.py             # One-time LanceDB backfill from history
~/.openclaw/workspace/reddit_scanner.py            # Legacy Reddit signal inspector
~/.openclaw/workspace/preload_models.sh            # Boot-time model warmup

# ── Pipeline data files ───────────────────────────────────────────────────
~/.openclaw/workspace/latest_trends.json           # Stage 1 output snapshot
~/.openclaw/workspace/explosive_trends.json        # Stage 1b output
~/.openclaw/workspace/expanded_keywords.json       # Stage 2a output
~/.openclaw/workspace/commercial_keywords.json     # Stage 2 output
~/.openclaw/workspace/vetted_opportunities.json    # Stage 2b output
~/.openclaw/workspace/validated_opportunities.json # Stage 3 output (all scored)
~/.openclaw/workspace/golden_opportunities.json    # Stage 3 output (GOLDEN + WATCH only)
~/.openclaw/workspace/dashboard.html               # Stage 4 output
~/.openclaw/workspace/dashboard_template.html      # Dashboard template (1804 lines)
~/.openclaw/workspace/cpc_cache.db                 # SQLite CPC cache
~/.openclaw/workspace/error_log.jsonl              # Stage errors
~/.openclaw/workspace/reddit_intelligence.json     # Reddit intel snapshot
~/.openclaw/workspace/reddit_intel_history.jsonl    # Reddit intel append-only log
~/.openclaw/workspace/vertical_cpc_reference.json  # Vertical CPC ceilings (powers EMERGING)

# ── Append-only history (PERMANENT — do not delete) ──────────────────────
~/.openclaw/workspace/trends_all_history.jsonl
~/.openclaw/workspace/explosive_trends_history.jsonl
~/.openclaw/workspace/validation_history.jsonl
~/.openclaw/workspace/vetted_history.jsonl

# ── Identity & memory ────────────────────────────────────────────────────
~/.openclaw/SOUL.md                                # Bot persona (loaded by telegram_bot.py)
~/.openclaw/IDENTITY.md                            # Persona definition (bot framework)
~/.openclaw/MEMORY.md                              # Legacy agent memory (DEPRECATED — see below)
~/.openclaw/HEARTBEAT.md                           # Automation schedule
~/.openclaw/vector_db/                             # LanceDB vector store directory
/Users/newmac/.openclaw/oracle.db                  # SQLite DB (oracle)

# ── Services ──────────────────────────────────────────────────────────────
~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist
~/Library/LaunchAgents/ai.openclaw.gateway.plist
~/Library/LaunchAgents/ai.openclaw.litellm.plist
~/Library/LaunchAgents/ai.openclaw.preload-models.plist
~/.openclaw/logs/gateway.log
~/.openclaw/logs/gateway.err.log
```

---

## Pipeline Stage Detail

### Stage 0: Reddit Intelligence (`reddit_intelligence.py`)

Non-fatal stage — errors don't halt pipeline.

- 9 intelligence subreddits across 3 priority tiers
  - Tier 1 (score≥5): r/SearchArbitrage, r/PPC, r/adops
  - Tier 2 (score≥10): r/Domains, r/SEO, r/bigseo
  - Tier 3 (score≥20): r/Affiliatemarketing, r/marketing, r/digital_marketing, r/FacebookAds
- 7 pattern categories: compliance_alert, feed_intel, cpc_data, vertical_signal, keyword_mention, decay_signal, platform_shift
- Extracts dollar amounts (CPC/RPM/RPC/EPC) and vertical mentions
- Output: `reddit_intelligence.json`, `reddit_intel_history.jsonl`
- **Status**: Runs standalone, NOT wired into heartbeat pipeline stage flow

### Stage 1: Trends Scraping (`trends_scraper.py`)

- 4 sources: Google Trends RSS, Google News RSS, Bing News RSS, Reddit JSON
- 49 countries, `asyncio.Semaphore(10)`, 300+ concurrent requests
- Reddit: `hot` + `rising` feeds, score≥50 for country subs, score≥5 for intel subs
- Reddit traffic bucketing: ≥10K→"500K+", ≥2K→"100K+", ≥500→"50K+", else→"20K+"
- Dedup by `(term, geo)` tuple, keeps highest traffic
- User-Agent: `"OpenClaw/1.0 (trend research bot)"`
- Output: `latest_trends.json` (snapshot), `trends_all_history.jsonl` (append-only)

### Stage 1b: Trends Postprocessing (`trends_postprocess.py`)

- Filters for "explosive" trends (score ≥ 20,000)
- Semantic dedup via `vector_store.is_duplicate()` — cosine similarity ≥ 0.85
- Indexes into LanceDB via `vector_store.add_trend()` — bge-m3 1024-dim embeddings
- Output: `explosive_trends.json`, `explosive_trends_history.jsonl`

### Stage 2a: Keyword Expansion (`keyword_expander.py`)

- Google Ads Keyword Planner API (`generateKeywordIdeas`)
- OAuth2 Bearer token — requires 5 env vars (see REMEDIATION.md)
- 3-bucket classification: A (high confidence, pass directly), B (needs DataForSEO validation), C (discard)
- **Status**: Code complete, credentials missing

### Stage 2: Keyword Extraction (`keyword_extractor.py`)

- LLM pivot: LiteLLM proxy → `dwight-primary` model → ~800-line system prompt
- Batches of 10 trends → JSON array of `{keyword, country, confidence, commercial_category}`
- 3-layer cost optimization: in-memory dedup → SQLite cache (72h TTL) → budget gate (75/day)
- DataForSEO batch: submit → poll every 30s → retrieve (5min timeout, ~$0.002/keyword)
- Deferred queue: overflow keywords → `deferred_keyword_lookups` table → recovered next run
- Output: `commercial_keywords.json`

### Stage 2b: SERP Vetting (`vetting.py`)

- DuckDuckGo HTML scraper (ThreadPoolExecutor, max_workers=5) — CAPTCHA-prone at ~50 req/hr
- Brave Search (optional, needs `BRAVE_API_KEY`)
- Commercial signal detection: 50+ commercial domains, 20+ URL patterns, 15+ title patterns
- Threshold: ≥3 commercial signals → passes
- Vertical classification: regex word-boundary matching (12 categories)
- Recent fix: `\b` boundaries prevent "spain"→"pain" false matches
- 0.5s delay between keywords, Semaphore(5)
- Output: `vetted_opportunities.json`, `vetted_history.jsonl`

### Stage 3: Validation & Scoring (`validation.py`)

- Metrics priority: Google Ads KP (if GADS_READY) → DataForSEO (if DFS_READY) → SQLite cache
- Scoring: `arbitrage_index = (cpc × efficiency) / (1 + competition)`, `weighted_score = ai × log10(volume + 1)`
- Tags: GOLDEN_OPPORTUNITY (≥8.0), WATCH (≥5.0), EMERGING (≥3.0), LOW (≥1.0), UNSCORED
- Deferred recovery: loads previous run's overflow keywords
- LanceDB `add_opportunity()` exists but is NOT wired
- Output: `validated_opportunities.json`, `golden_opportunities.json`, `validation_history.jsonl`

### Stage 4: Dashboard (`dashboard_builder.py`)

- Reads validated JSON + validation_history.jsonl
- Deduplicates by (keyword, country), enriches fields, computes metadata
- Injects into template via `__DATA__` / `__META__` string replacement
- Output: self-contained `dashboard.html`

### Stage 5: Reflection (`reflection.py`)

- Analyzes false positives (trends that never became opportunities)
- Calculates golden rate per country, signal weights
- Appends to `MEMORY.md` — currently grows unbounded (see REMEDIATION.md)

---

## Data Flow

```
trends_scraper → latest_trends.json
     → trends_postprocess → explosive_trends.json (LanceDB indexed)
          → keyword_expander → expanded_keywords.json (if Google Ads ready)
               → keyword_extractor → commercial_keywords.json (LLM + DataForSEO)
                    → vetting → vetted_opportunities.json (SERP validated)
                         → validation → validated_opportunities.json + golden_opportunities.json
                              → dashboard_builder → dashboard.html
                              → reflection → MEMORY.md
```

---

## Search Arbitrage Strategy Detail

### Track A Verticals ("Follow the Money")

| Vertical | Why It Pays | Typical T1 CPC |
|----------|-------------|----------------|
| Healthcare / Medicare | Patient acquisition worth thousands | $3–$15 |
| Legal / Mass Torts | Settlement values justify extreme bids | $5–$50+ |
| Financial Services | Actuarial precision on allowable CPA | $2–$12 |
| Insurance (auto, home, health) | Recurring LTV | $3–$20 |
| Home Improvement / Solar | High-ticket services ($5K+) | $2–$8 |
| SaaS / Software Comparison | Subscription LTV | $2–$10 |
| Automotive | High-value transactions | $1.50–$6 |
| Education / Certification | Tuition justifies bids | $1–$5 |
| Real Estate | Commission-based, high ticket | $1.50–$6 |

### Track B: Emerging Detection Logic

When DataForSEO returns CPC $0, check the vertical CPC ceiling via `vertical_cpc_reference.json`. If the keyword belongs to a vertical where adjacent terms average $1.50+ CPC → tag **EMERGING**.

Examples:
- "ozempic alternatives 2026" → $0 CPC but health vertical averages $5+ → EMERGING
- "new-fintech-product review" → $0 CPC but finance vertical averages $3+ → EMERGING

### Reddit Intelligence Subreddits

| Subreddit | Intel Value |
|-----------|-------------|
| r/PPC | Traffic costs, ROAS benchmarks (110-115%), campaign scaling |
| r/adops | eCPM manipulation, IVT detection, header bidding |
| r/SearchArbitrage | Pure RSOC tactics, feed comparison (Tonic vs System1) |
| r/Domains | AFD→RSOC migration, GiantPanda/Sedo benchmarks |
| r/SEO + r/bigseo | High-intent query discovery, organic trend forecasting |
| r/Affiliatemarketing | CPA vs RSOC margin comparison |

Key intel: ad longevity >2 weeks = scaled profitable operation. ROAS 110-115% is industry benchmark. Keyword decay occurs when end-advertisers adjust bids on syndicated traffic.

---

## Exec Command Pattern (Telegram Bot)

Regex for command detection in LLM responses:
```
exec\s*[→>]\s*(.+?)(?:\s*\|\s*(\d+))?(?=\n\s*\n|\nexec\s*[→>]|\Z)
```
- `exec → <command>` — run once
- `exec → <cmd> | 600` — run every 600 seconds
- Always use `$(cmd)` not `` `cmd` ``

---

## Stale Files (Safe to Delete or Move)

| File | Why |
|------|-----|
| `web_search.py` | Unused, wrong API. Real search is in `vetting.py` |
| `bing_search.py` | Unused stub. Bing scraping is in `trends_scraper.py` |
| `AGENTS.md` | Obsolete — references removed components |
| `CLAUDE(old).md` | Superseded |
| `__pycache__/Models switch.md` | Planning doc in wrong directory |
| `mailtm_signup.py`, `proton_signup.py` | Not pipeline — move to `~/.openclaw/tools/` |

---

## Verified Source Links

| Topic | Link |
|-------|------|
| Qwen3-coder:30b | [ollama.com/library/qwen3-coder:30b](https://ollama.com/library/qwen3-coder:30b) |
| bge-m3 | [ollama.com/library/bge-m3](https://ollama.com/library/bge-m3) |
| Ollama env vars | [docs.ollama.com/faq](https://docs.ollama.com/faq) |
| smolagents | [huggingface.co/docs/smolagents](https://huggingface.co/docs/smolagents/en/index) |
| LiteLLM proxy | [docs.litellm.ai/docs/proxy/quick_start](https://docs.litellm.ai/docs/proxy/quick_start) |
| LanceDB | [docs.lancedb.com/quickstart](https://docs.lancedb.com/quickstart) |
| NeuralProphet | [neuralprophet.com](https://neuralprophet.com/) |
| SearXNG | [searxng.org](https://searxng.org/) |
| DataForSEO | [docs.dataforseo.com/v3/keywords_data](https://docs.dataforseo.com/v3/keywords_data/) |
| Google Ads API | [developers.google.com/google-ads/api](https://developers.google.com/google-ads/api/docs/get-started/introduction) |
| DeepSeek V3.2 | [openrouter.ai/deepseek/deepseek-v3.2/pricing](https://openrouter.ai/deepseek/deepseek-v3.2/pricing) |
