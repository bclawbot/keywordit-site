# OpenClaw Project Status

> Current state. Updated weekly by operator.
> Last updated: 2026-03-16

---

## System Health

- **Pipeline**: Heartbeat runs every 6h — all 9 stages execute
- **LiteLLM Proxy**: Running on localhost:4000, model `dwight-primary`
- **Ollama**: localhost:11434, loaded: qwen3-coder:30b (primary), bge-m3 (embeddings)
- **LanceDB**: Operational for trends dedup. Opportunities table NOT wired.
- **SQLite (oracle.db / cpc_cache.db)**: CPC cache + API usage + deferred queue functional
- **Dashboard**: Static HTML, regenerated each heartbeat cycle
- **Telegram Bot**: Running, SOUL.md loaded (~3500 tokens), skills injection disabled (context limits)
- **Gateway**: Node.js on 18789, Telegram DISABLED (intentional)

## What's Working

- All 4 trend sources scraping (Google Trends, Google News, Bing, Reddit) across 49 countries
- Explosive filtering + LanceDB semantic dedup (trends table)
- LLM keyword extraction via LiteLLM → Ollama/OpenRouter
- DataForSEO CPC validation with 3-layer cache + budget gate (75/day)
- DuckDuckGo SERP vetting + vertical classification (word boundary fix applied)
- Scoring + tagging (GOLDEN/WATCH/EMERGING/LOW)
- Dashboard generation with full feature set
- Telegram bot: exec command pattern, scheduled tasks, multi-fallback LLM routing
- Heartbeat → Telegram notifications with keyword diffs

## What's Broken or Blocked

- **CRITICAL: Telegram token exposed in MEMORY.md line 2** — must regenerate via @BotFather
- **Google Ads API**: Credentials not set up — Stage 2a skipped entirely
- **LanceDB opportunities**: `validation.py` never calls `add_opportunity()`
- **vector_store.py Bug #1**: Uses L2 distance but threshold assumes cosine — dedup misses most duplicates
- **reflection.py Bug #2**: False positive detection compares wrong fields — never matches
- **MEMORY.md**: 2,788+ lines of duplicated reflection blocks, growing unbounded
- **error_log.jsonl**: Flooded with 4,471 semantic_duplicate entries + 126 LiteLLM 401s
- **smolagents**: Prototype on branch, not merged, backtick bug
- **DuckDuckGo CAPTCHA**: Rate-limited at ~50 req/hr, no proxy rotation

## Active Work

- **Memory system overhaul**: Migrating from flat CLAUDE.md + broken MEMORY.md to 3-layer architecture (CLAUDE.md → memory/*.md → memory/logs/)
- **Strategic reorientation**: Dual-track system (Track A: evergreen high-CPC, Track B: emerging keyword detection)
- **Dashboard aesthetic**: "Intelligence Terminal" minimal dark style

## Pipeline Performance (from latest reflections)

- False positive rate: Very high (~21K+ FP trends across 49 countries per reflection cycle)
- Golden rate: Near zero for most countries. Only AU (0.13), US (0.17), FR (1.00 but n=1) show any
- Signal weights: Most countries at 0.00 — indicates Bug #2 (wrong field comparison) is suppressing golden detection
- Top FP countries: US (976), GB (817), BR (720), IN (696), FR (681)

## Open Questions

- When to pursue Google Ads API setup? (needs account + 1-3 day approval)
- Is the near-zero golden rate a Bug #2 artifact or a genuine pipeline issue?
- Should SearXNG replace DuckDuckGo now or wait?
- Reddit intelligence: wire into heartbeat as Stage 1a, or keep standalone?
- When to tackle the smolagents merge?

## Environment

- Machine: MacBook Pro M1 Max, 32GB, macOS Darwin 24.5.0 ARM64
- Timezone: UTC+2
- Python: 3.14 (system)
- Docker: Available
- LaunchAgents: dwight-bot, gateway, litellm, preload-models
