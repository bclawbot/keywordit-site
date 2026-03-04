# Dwight/OpenClaw — Claude Code Project Context

> Place this file at `~/.openclaw/workspace/CLAUDE.md`
> Claude Code reads it automatically at every session start.

---

## What This Project Is

You are working on **Dwight** — a self-hosted AI agent stack on a MacBook Pro M1 Max (32GB, macOS Darwin 24.5.0 ARM64). The operator is **Bclawa** (UTC+2).

Dwight's mission: **media buying arbitrage research** — automatically discovering trending keywords with high advertiser CPC and low content saturation across 20+ countries, then delivering actionable opportunities to Telegram.

---

## Architecture — Know This Before Touching Anything

| Component | Details |
|-----------|---------|
| **Dwight bot** | `~/.openclaw/workspace/telegram_bot.py` — Python, sole user interface |
| **OpenClaw Gateway** | Node.js on port 18789. Telegram is **intentionally disabled** — do NOT re-enable it. |
| **Ollama** | Local LLM server at `http://localhost:11434` |
| **Pipeline** | Python scripts in `~/.openclaw/workspace/` — 6 stages |
| **Config** | `~/.openclaw/openclaw.json` |
| **Workspace** | `~/.openclaw/workspace/` |
| **Logs** | `~/.openclaw/logs/` |

---

## Absolute Rules — Never Break These

1. **Gateway model MUST match bot model.** `openclaw.json` primary model and `telegram_bot.py` Ollama model must always be the same string. Mismatch causes Ollama to swap models and adds 60s delays to every request.

2. **Never re-enable Telegram in the OpenClaw gateway.** It causes 409 conflicts with Dwight polling the same token.

3. **Never use backtick command substitution.** All shell commands must use `$(cmd)` syntax, never `` `cmd` ``. `telegram_bot.py` line 151 already sanitizes this, but do not generate backtick syntax in any new code.

4. **Never delete these files:**
   - `~/.openclaw/workspace/trends_all_history.jsonl`
   - `~/.openclaw/workspace/explosive_trends_history.jsonl`

5. **Do not run `heartbeat.py` unless explicitly asked.** It triggers the full 6-stage pipeline and Telegram notifications.

6. **Max 5 exec→LLM loops per message** in the bot. Do not change this limit without explicit approval.

7. **Never touch `~/.openclaw/.env`** — it contains TELEGRAM_TOKEN and OPENROUTER_API_KEY.

---

## Current Model Configuration

| Role | Model | Where Set |
|------|-------|-----------|
| **Primary (local)** | `qwen3:8b` via Ollama | `openclaw.json` + `telegram_bot.py` |
| **Fallback 1 (cloud)** | `google/gemma-3-27b-it:free` via OpenRouter | `telegram_bot.py` |
| **Fallback 2 (cloud)** | `stepfun/step-3.5-flash:free` via OpenRouter | `telegram_bot.py` |

**Planned upgrade** (not yet applied):
- Primary → `qwen3:14b` (0.971 F1 tool calling, confirmed Docker benchmark)
- Fallback 1 → `deepseek/deepseek-v3.2` ($0.28/M tokens, paid, reliable)

---

## All Key File Paths

```
~/.openclaw/openclaw.json                          # Main config
~/.openclaw/.env                                    # API keys — do not read/print
~/.openclaw/workspace/telegram_bot.py              # Bot — main entry point
~/.openclaw/workspace/SOUL.md                      # System prompt (execution rules)
~/.openclaw/workspace/heartbeat.py                 # Full pipeline runner
~/.openclaw/workspace/trends_scraper.py            # Stage 1
~/.openclaw/workspace/trends_postprocess.py        # Stage 2
~/.openclaw/workspace/vetting.py                   # Stage 3
~/.openclaw/workspace/validation.py                # Stage 4
~/.openclaw/workspace/dashboard_builder.py         # Stage 5
~/.openclaw/workspace/reflection.py                # Stage 6
~/.openclaw/workspace/golden_opportunities.json    # Top-scored opportunities
~/.openclaw/workspace/validated_opportunities.json # All scored opportunities
~/.openclaw/workspace/error_log.jsonl              # Stage errors
~/.openclaw/workspace/trends_all_history.jsonl     # PERMANENT — do not delete
~/.openclaw/workspace/explosive_trends_history.jsonl # PERMANENT — do not delete
~/.openclaw/SOUL.md                                # Identity config (legacy path)
~/.openclaw/IDENTITY.md                            # Persona definition
~/.openclaw/MEMORY.md                              # Agent long-term memory
~/.openclaw/HEARTBEAT.md                           # Automation schedule
~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist    # Bot launchd service
~/Library/LaunchAgents/ai.openclaw.gateway.plist       # Gateway launchd service
~/.openclaw/logs/gateway.log                       # Gateway logs
~/.openclaw/logs/gateway.err.log                   # Gateway errors
/Users/newmac/.openclaw/oracle.db                  # SQLite DB (oracle skill)
```

---

## Service Management Commands

```bash
# Restart Dwight bot
rm -f /tmp/dwight_bot.lock
launchctl unload ~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist
launchctl load ~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist

# Restart OpenClaw gateway
launchctl unload ~/Library/LaunchAgents/ai.openclaw.gateway.plist
launchctl load ~/Library/LaunchAgents/ai.openclaw.gateway.plist

# Check both services are running
launchctl list | grep openclaw
pgrep -fl telegram_bot.py   # must show exactly 1 process

# Check and pre-warm Ollama
ollama ps
curl -s -X POST http://localhost:11434/api/generate \
  -d '{"model":"qwen3:8b","prompt":"hi","stream":false,"options":{"num_predict":1}}'
```

---

## The 6-Stage Research Pipeline

Run via: `python3 ~/.openclaw/workspace/heartbeat.py`

| Stage | Script | What it does |
|-------|--------|-------------|
| 1 | `trends_scraper.py` | Scrapes Google Trends RSS (49 countries) |
| 2 | `trends_postprocess.py` | Filters explosive trends (≥20k traffic) |
| 3 | `vetting.py` | DuckDuckGo SERP vetting for long-form content gaps (Brave 429s non-fatal) |
| 4 | `validation.py` | Keyword metrics + arbitrage scoring |
| 5 | `dashboard_builder.py` | Builds `dashboard.html` |
| 6 | `reflection.py` | Updates MEMORY.md with false positives |

**Heartbeat schedule**: Full pipeline every 6h, stuck-check every 30min, daily brief at 8:00, weekly digest Sunday 10:00.

---

## Exec Command Pattern (How Dwight Executes Shell Commands)

The bot detects this regex in LLM responses and runs the command:
```
exec\s*[→>]\s*(.+?)(?:\s*\|\s*(\d+))?(?=\n\s*\n|\nexec\s*[→>]|\Z)
```
- `exec → <command>` — run once
- `exec → <cmd> | 600` — run every 600 seconds

When writing code that generates responses or prompts for the bot, always use this pattern. Always use `$(cmd)` not `` `cmd` ``.

---

## Installed Skills (22 total)

Key skills relevant to coding tasks:
- `oracle` — SQLite DB at `/Users/newmac/.openclaw/oracle.db`
- `skill-writer` — creates/audits SKILL.md files
- `skill-vetter` — security vetting before installing skills
- `model-usage` — reports Ollama model status and gateway logs
- `self-improving` — tiered memory, auto-promotes patterns after 3x usage
- `pipeline-runner` — runs heartbeat.py, monitors stages
- `media-researcher` — analyzes golden_opportunities.json
- `free-ride` — auto-configures top free OpenRouter models
- `mcporter` — MCP server management

---

## Optimization Roadmap (Pending Implementation)

Tasks are ordered by priority. Check them off as completed.

### IMMEDIATE — No code required

- [ ] **Set Ollama env vars** in `~/.zshrc` and gateway plist:
  ```bash
  export OLLAMA_KEEP_ALIVE=-1
  export OLLAMA_MAX_LOADED_MODELS=3
  export OLLAMA_FLASH_ATTENTION=1
  export OLLAMA_KV_CACHE_TYPE=q8_0
  ```
  Source: [docs.ollama.com/faq](https://docs.ollama.com/faq)

- [ ] **Pull new models**:
  ```bash
  ollama pull qwen3:14b
  ollama pull bge-m3
  ```

### SHORT-TERM — Config changes

- [ ] **Switch primary model** — change `qwen3:8b` → `qwen3:14b` in BOTH:
  1. `~/.openclaw/openclaw.json` (gateway primary)
  2. `~/.openclaw/workspace/telegram_bot.py` (bot Ollama model)
  Then restart both services.

- [ ] **Update cloud fallback** — change `google/gemma-3-27b-it:free` → `deepseek/deepseek-v3.2` in `telegram_bot.py`. Keep `stepfun/step-3.5-flash:free` as last resort.

- [ ] **Add boot preload** — create `~/.openclaw/workspace/preload_models.sh` and launchd plist `ai.openclaw.preload-models.plist` to warm both models at login.

### MEDIUM-TERM — New components

- [ ] **Install LanceDB** (`pip install lancedb`) and create `vector_store.py`:
  - `embed_text(text)` → calls bge-m3 at `http://localhost:11434/api/embeddings`
  - `add_trend(keyword, country, date, source, score, raw_text)` → stores with metadata
  - `is_duplicate(keyword, country, threshold=0.85)` → returns bool
  - `search_opportunities(query, top_k=10)` → semantic search over past results
  - DB path: `~/.openclaw/vector_db/`

- [ ] **Integrate deduplication** — modify `trends_postprocess.py` to call `is_duplicate()` before passing trends to vetting. Log skipped items to `error_log.jsonl` with reason `"semantic_duplicate"`.

- [ ] **Install LiteLLM Proxy** (`pip install 'litellm[proxy]'`):
  - Config at `~/.openclaw/litellm_config.yaml`
  - Primary: `ollama/qwen3:14b` → `http://localhost:11434`
  - Fallback: `openrouter/deepseek/deepseek-v3.2`
  - Port: 4000
  - Launchd plist: `ai.openclaw.litellm.plist`
  - Source: [docs.litellm.ai/docs/proxy/quick_start](https://docs.litellm.ai/docs/proxy/quick_start)

- [ ] **Self-host SearXNG** to replace DuckDuckGo in `vetting.py`:
  - Source: [searxng.org](https://searxng.org/) / [github.com/searxng/searxng](https://github.com/searxng/searxng)
  - OpenClaw plugin available: [github.com/keith-vs-kev/searxng-search](https://github.com/keith-vs-kev/searxng-search)

### LONG-TERM — Architecture

- [ ] **Migrate to smolagents** — replace shell-exec with CodeAgent:
  - `pip install smolagents`
  - New file: `~/.openclaw/workspace/dwight_agent.py`
  - Use `LiteLLMModel(model_id="ollama_chat/qwen3:14b")`
  - Define tools: `run_pipeline`, `search_trends`, `get_golden_opportunities`, `run_shell`, `search_web`, `read_file`, `write_file`
  - Source: [smolagents.org](https://smolagents.org/) | [github.com/huggingface/smolagents](https://github.com/huggingface/smolagents)
  - **Do this on a branch — test extensively before replacing live bot**

- [ ] **Parallelize pipeline** with asyncio:
  - Stage 1: `asyncio.gather()` across all 49 countries
  - Stage 2: `asyncio.Semaphore(5)` for SERP vetting
  - Stage 3: fan out with `asyncio.Semaphore(3)` (Ollama bottleneck)
  - Stages 4-6: keep sequential
  - Use `asyncio.Queue` between stages for streaming
  - Expected: 3-5x faster total pipeline execution

- [ ] **Add trend persistence scoring** to `validation.py`:
  - `pip install neuralprophet` ([neuralprophet.com](https://neuralprophet.com/))
  - New file: `~/.openclaw/workspace/trend_forecast.py`
  - Inputs: keyword, country, historical trend scores from `trends_all_history.jsonl`
  - Outputs: `persistence_probability` (0-1), `predicted_halflife_days`
  - Integrate into `validation.py` Stage 4: multiply `arbitrage_index` by `persistence_probability`
  - Add `persistence_score` and `predicted_halflife_days` to opportunity JSON output

- [ ] **Migrate JSON storage to SQLite** via oracle skill:
  - DB: `/Users/newmac/.openclaw/oracle.db`
  - Tables: `opportunities`, `trend_history`, `pipeline_runs`
  - Index on: `opportunities.tag`, `opportunities.date`, `trend_history(keyword, country)`
  - Update `validation.py` and `dashboard_builder.py` to write to SQLite
  - Do NOT delete original JSON files (some are marked permanent)

---

## Recent Fixes (Know These to Avoid Regressions)

| Date | Fix | File Changed |
|------|-----|-------------|
| 2026-02-28 | Disabled Telegram in gateway to fix 409 conflicts | `openclaw.json` |
| 2026-02-28 | Updated Node path to `/opt/homebrew/bin/node` (v25.6.1) | gateway plist |
| 2026-03-01 | Changed gateway primary from `qwen3-coder:30b` → `qwen3:8b` | `openclaw.json` |
| 2026-03-01 | Replaced dead OpenRouter models with live ones | `telegram_bot.py` |
| 2026-03-01 | Increased Ollama read timeout: 45s → 90s | `telegram_bot.py` |
| 2026-03-03 | Auto-sanitize backticks → `$(cmd)` in shell commands | `telegram_bot.py` line 151 |
| 2026-03-03 | Added SOUL.md rule: never use backtick substitution | `SOUL.md` |
| 2026-03-03 | Expanded META QUESTIONS triggers in SOUL.md | `SOUL.md` |

---

## Coding Standards for This Project

- **Python version**: Python 3 (`python3`, not `python`)
- **Shell syntax**: Always `$(cmd)` — never backtick substitution
- **Imports**: Check `~/.openclaw/venv/` for available packages before installing new ones
- **Logging**: Append errors to `~/.openclaw/workspace/error_log.jsonl` in JSON format `{"timestamp": ..., "stage": ..., "error": ...}`
- **No hardcoded API keys**: Read from `~/.openclaw/.env` using `python-dotenv` or `os.environ`
- **Test changes**: Always restart the bot after modifying `telegram_bot.py`
- **Verify model alignment**: After any model change, confirm `openclaw.json` and `telegram_bot.py` use the same Ollama model string

---

## Verified Source Links (Use These for Reference)

| Topic | Link |
|-------|------|
| Qwen3:14B on Ollama | [ollama.com/library/qwen3:14b](https://ollama.com/library/qwen3:14b) |
| bge-m3 on Ollama | [ollama.com/library/bge-m3](https://ollama.com/library/bge-m3) |
| Docker tool-calling benchmark | [docker.com/blog/local-llm-tool-calling-a-practical-evaluation](https://www.docker.com/blog/local-llm-tool-calling-a-practical-evaluation/) |
| Ollama env vars (KEEP_ALIVE etc.) | [docs.ollama.com/faq](https://docs.ollama.com/faq) |
| smolagents docs | [huggingface.co/docs/smolagents](https://huggingface.co/docs/smolagents/en/index) |
| LiteLLM proxy | [docs.litellm.ai/docs/proxy/quick_start](https://docs.litellm.ai/docs/proxy/quick_start) |
| LanceDB quickstart | [docs.lancedb.com/quickstart](https://docs.lancedb.com/quickstart) |
| NeuralProphet | [neuralprophet.com](https://neuralprophet.com/) |
| NeuralForecast (Nixtla) | [nixtlaverse.nixtla.io/neuralforecast/docs](https://nixtlaverse.nixtla.io/neuralforecast/docs/getting-started/introduction.html) |
| SearXNG | [searxng.org](https://searxng.org/) |
| DeepSeek V3.2 pricing | [openrouter.ai/deepseek/deepseek-v3.2/pricing](https://openrouter.ai/deepseek/deepseek-v3.2/pricing) |
| Qwen3.5 Plus pricing | [openrouter.ai/qwen/qwen3.5-plus-02-15](https://openrouter.ai/qwen/qwen3.5-plus-02-15) |
| Grok 4.1 Fast | [openrouter.ai/x-ai/grok-4.1-fast](https://openrouter.ai/x-ai/grok-4.1-fast) |
