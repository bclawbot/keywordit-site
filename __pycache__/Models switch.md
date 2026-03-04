# Dwight/OpenClaw — Claude Code Instructions

> Place this file at `~/.openclaw/workspace/CLAUDE.md`
> Read this entire file before doing anything. Then work through the TASKS section in order.

---

## What You Are Working On

You are the AI coder for **Dwight** — a self-hosted OpenClaw agent stack on a MacBook Pro M1 Max (32GB, macOS Darwin 24.5.0 ARM64). Operator: **Bclawa** (UTC+2).

Dwight is a Telegram bot (`@TheMediaBuyer_bot`) that runs a 6-stage media buying arbitrage research pipeline, finding high-CPC / low-saturation keyword opportunities across 20+ countries and delivering them to Telegram.

Your job is to implement the upgrades in the TASKS section below. Work through them in order. Do not skip steps. Do not change anything not mentioned in a task.

---

## Absolute Rules — Read First, Never Break

1. **Gateway model MUST always match bot model.** `openclaw.json` primary model and the Ollama model in `telegram_bot.py` must be identical strings at all times. Mismatch = 60s delays on every request.

2. **Never re-enable Telegram in the OpenClaw gateway.** It was deliberately disabled to fix 409 conflicts. Do not touch this setting.

3. **Never use backtick command substitution.** All shell commands must use `$(cmd)` syntax. Never generate `` `cmd` `` in any code, prompt, or script.

4. **Never delete or truncate these files:**
   - `~/.openclaw/workspace/trends_all_history.jsonl`
   - `~/.openclaw/workspace/explosive_trends_history.jsonl`

5. **Do not run `heartbeat.py`** unless explicitly asked by the operator. It triggers the full pipeline and sends Telegram notifications.

6. **Never read or print `~/.openclaw/.env`** — it contains live API credentials.

7. **After any change to `telegram_bot.py` or `openclaw.json`**, always restart both services:
   ```bash
   rm -f /tmp/dwight_bot.lock
   launchctl unload ~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist
   launchctl load ~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist
   launchctl unload ~/Library/LaunchAgents/ai.openclaw.gateway.plist
   launchctl load ~/Library/LaunchAgents/ai.openclaw.gateway.plist
   ```
   Then verify: `pgrep -fl telegram_bot.py` — must show exactly 1 process.

---

## Current System State (as of 2026-03-03)

| What | Current Value |
|------|--------------|
| Primary LLM | `qwen3:8b` via Ollama |
| Cloud Fallback 1 | `google/gemma-3-27b-it:free` via OpenRouter |
| Cloud Fallback 2 | `stepfun/step-3.5-flash:free` via OpenRouter |
| Gateway port | 18789 |
| Ollama port | 11434 |
| Bot file | `~/.openclaw/workspace/telegram_bot.py` |
| Main config | `~/.openclaw/openclaw.json` |
| Oracle DB | `/Users/newmac/.openclaw/oracle.db` |
| Node path | `/opt/homebrew/bin/node` (v25.6.1) |

---

## Key File Paths

```
~/.openclaw/openclaw.json
~/.openclaw/workspace/telegram_bot.py
~/.openclaw/workspace/SOUL.md
~/.openclaw/workspace/heartbeat.py
~/.openclaw/workspace/trends_scraper.py
~/.openclaw/workspace/trends_postprocess.py
~/.openclaw/workspace/vetting.py
~/.openclaw/workspace/validation.py
~/.openclaw/workspace/dashboard_builder.py
~/.openclaw/workspace/reflection.py
~/.openclaw/workspace/golden_opportunities.json
~/.openclaw/workspace/error_log.jsonl
~/Library/LaunchAgents/ai.openclaw.dwight-bot.plist
~/Library/LaunchAgents/ai.openclaw.gateway.plist
~/.openclaw/logs/gateway.log
```

---

## TASKS — Implement These In Order

Mark each task `[x]` when complete. If a task fails, stop and report the exact error before continuing.

---

### TASK 1 — Ollama Performance Flags
**Status**: [ ] Not started

Set the following environment variables so Ollama keeps models permanently loaded and uses memory efficiently. Cold starts are currently causing 60-second delays.

**Step 1**: Add to `~/.zshrc` (or `~/.bashrc` if that's the active shell):
```bash
export OLLAMA_KEEP_ALIVE=-1
export OLLAMA_MAX_LOADED_MODELS=3
export OLLAMA_FLASH_ATTENTION=1
export OLLAMA_KV_CACHE_TYPE=q8_0
```

**Step 2**: Source the file: `source ~/.zshrc`

**Step 3**: Add the same four variables to the `EnvironmentVariables` dict inside `~/Library/LaunchAgents/ai.openclaw.gateway.plist`:
```xml
<key>EnvironmentVariables</key>
<dict>
    <key>OLLAMA_KEEP_ALIVE</key><string>-1</string>
    <key>OLLAMA_MAX_LOADED_MODELS</key><string>3</string>
    <key>OLLAMA_FLASH_ATTENTION</key><string>1</string>
    <key>OLLAMA_KV_CACHE_TYPE</key><string>q8_0</string>
</dict>
```

**Verify**: Run `echo $OLLAMA_KEEP_ALIVE` — must return `-1`.

---

### TASK 2 — Download New Models
**Status**: [ ] Not started

Download the upgraded primary LLM and the new embedding model.

```bash
ollama pull qwen3:14b
ollama pull bge-m3
```

After both complete, pre-warm them:
```bash
curl -s -X POST http://localhost:11434/api/generate \
  -d '{"model":"qwen3:14b","prompt":"hi","stream":false,"options":{"num_predict":1}}'

curl -s -X POST http://localhost:11434/api/generate \
  -d '{"model":"bge-m3","prompt":"hi","stream":false,"options":{"num_predict":1}}'
```

**Verify**: `ollama ps` — both `qwen3:14b` and `bge-m3` must show as loaded.

**Why qwen3:14b**: Docker's benchmark of 21 models across 3,570 test cases shows Qwen3:14B scores 0.971 F1 on tool calling — near GPT-4's 0.974. Current qwen3:8b scores 0.919. At ~9GB it fits comfortably with bge-m3 (~2GB), leaving ~10GB headroom on 32GB. Source: https://www.docker.com/blog/local-llm-tool-calling-a-practical-evaluation/

---

### TASK 3 — Switch Primary Model to Qwen3:14B
**Status**: [ ] Not started

**CRITICAL**: Both files below must be updated to the same model string, or Ollama will constantly swap models and cause 60-second delays on every request.

**Step 1**: Read `~/.openclaw/openclaw.json`. Find the primary/local model setting and change every instance of `qwen3:8b` to `qwen3:14b`. Save.

**Step 2**: Read `~/.openclaw/workspace/telegram_bot.py`. Find where the Ollama model is configured (likely a variable named `OLLAMA_MODEL`, `model`, or `LOCAL_MODEL`). Change `qwen3:8b` to `qwen3:14b`. Save.

**Step 3**: Restart both services (use the commands from the Rules section above).

**Step 4**: Verify the bot responds on Telegram — send `/status` to `@TheMediaBuyer_bot`.

**Note**: Keep `qwen3:8b` available in Ollama — do not run `ollama rm qwen3:8b`. It stays as a fast secondary.

---

### TASK 4 — Upgrade Cloud Fallback Chain
**Status**: [ ] Not started

Replace the unreliable free-tier fallback with a paid budget model. Cost: ~$5-15/month for production-grade reliability.

**Step 1**: Read `~/.openclaw/workspace/telegram_bot.py`. Find the fallback model configuration.

**Step 2**: Change Fallback 1 from `google/gemma-3-27b-it:free` to `deepseek/deepseek-v3.2`.

**Step 3**: Keep Fallback 2 (`stepfun/step-3.5-flash:free`) as the absolute last resort — do not remove it.

Final fallback order in the bot:
1. `qwen3:14b` via Ollama (local, primary) — $0
2. `deepseek/deepseek-v3.2` via OpenRouter — $0.28/M tokens
3. `stepfun/step-3.5-flash:free` via OpenRouter — $0, last resort only

**Step 4**: Restart both services and verify.

**Why DeepSeek V3.2**: $0.28/$0.42 per 1M tokens, 164K context window, purpose-built agentic task synthesis pipeline, excellent at function calling and structured output. Source: https://openrouter.ai/deepseek/deepseek-v3.2/pricing

---

### TASK 5 — Add Boot-Time Model Preloading
**Status**: [ ] Not started

Create a startup script that warms both models at login so there's never a cold start after reboot.

**Step 1**: Create `~/.openclaw/workspace/preload_models.sh`:
```bash
#!/bin/bash
# Wait for Ollama to be available (max 60s)
for i in {1..30}; do
    curl -s http://localhost:11434/api/tags > /dev/null 2>&1 && break
    sleep 2
done

# Pre-warm primary LLM
curl -s -X POST http://localhost:11434/api/generate \
    -d '{"model":"qwen3:14b","prompt":"hi","stream":false,"options":{"num_predict":1}}' \
    > /dev/null 2>&1

# Pre-warm embedding model
curl -s -X POST http://localhost:11434/api/generate \
    -d '{"model":"bge-m3","prompt":"hi","stream":false,"options":{"num_predict":1}}' \
    > /dev/null 2>&1

echo "$(date): Models preloaded" >> ~/.openclaw/logs/preload.log
```

**Step 2**: `chmod +x ~/.openclaw/workspace/preload_models.sh`

**Step 3**: Create `~/Library/LaunchAgents/ai.openclaw.preload-models.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>ai.openclaw.preload-models</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/newmac/.openclaw/workspace/preload_models.sh</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/newmac/.openclaw/logs/preload.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/newmac/.openclaw/logs/preload.err.log</string>
</dict>
</plist>
```

**Step 4**: `launchctl load ~/Library/LaunchAgents/ai.openclaw.preload-models.plist`

---

### TASK 6 — Install LanceDB and Build Semantic Search
**Status**: [ ] Not started

Add a vector database so Dwight can deduplicate trends and semantically search past opportunities. This eliminates redundant SERP lookups in Stage 2.

**Step 1**: Install LanceDB:
```bash
pip install lancedb --break-system-packages
```

**Step 2**: Create `~/.openclaw/workspace/vector_store.py` with these exact functions:

```python
import lancedb
import json
import requests
from pathlib import Path

DB_PATH = Path.home() / ".openclaw" / "vector_db"
OLLAMA_URL = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "bge-m3"

def _get_db():
    DB_PATH.mkdir(parents=True, exist_ok=True)
    return lancedb.connect(str(DB_PATH))

def embed_text(text: str) -> list:
    resp = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text}, timeout=30)
    resp.raise_for_status()
    return resp.json()["embedding"]

def add_trend(keyword: str, country: str, date: str, source: str, score: float, raw_text: str = ""):
    db = _get_db()
    embedding = embed_text(f"{keyword} {country} {raw_text[:200]}")
    table_name = "trends"
    data = [{"keyword": keyword, "country": country, "date": date,
              "source": source, "score": score, "vector": embedding}]
    if table_name in db.table_names():
        db.open_table(table_name).add(data)
    else:
        db.create_table(table_name, data=data)

def is_duplicate(keyword: str, country: str, threshold: float = 0.85) -> bool:
    db = _get_db()
    if "trends" not in db.table_names():
        return False
    embedding = embed_text(f"{keyword} {country}")
    results = db.open_table("trends").search(embedding).limit(1).to_list()
    if not results:
        return False
    return results[0].get("_distance", 1.0) < (1.0 - threshold)

def search_opportunities(query: str, top_k: int = 10) -> list:
    db = _get_db()
    if "opportunities" not in db.table_names():
        return []
    embedding = embed_text(query)
    return db.open_table("opportunities").search(embedding).limit(top_k).to_list()

def add_opportunity(keyword: str, country: str, arbitrage_index: float, tag: str, raw: dict):
    db = _get_db()
    embedding = embed_text(f"{keyword} {country} {tag}")
    data = [{"keyword": keyword, "country": country,
              "arbitrage_index": arbitrage_index, "tag": tag,
              "raw_json": json.dumps(raw), "vector": embedding}]
    if "opportunities" in db.table_names():
        db.open_table("opportunities").add(data)
    else:
        db.create_table("opportunities", data=data)
```

**Step 3**: Create `~/.openclaw/workspace/index_history.py` to index existing data:

```python
#!/usr/bin/env python3
"""One-time script to index existing trend history into LanceDB."""
import json
from pathlib import Path
from vector_store import add_trend, add_opportunity

WORKSPACE = Path.home() / ".openclaw" / "workspace"

def index_trends():
    for fname in ["trends_all_history.jsonl", "explosive_trends_history.jsonl"]:
        fpath = WORKSPACE / fname
        if not fpath.exists():
            print(f"Skipping {fname} — not found")
            continue
        count = 0
        with open(fpath) as f:
            for line in f:
                try:
                    rec = json.loads(line.strip())
                    keyword = rec.get("keyword") or rec.get("title", "")
                    country = rec.get("country", "unknown")
                    date = rec.get("date") or rec.get("pubDate", "")
                    if keyword:
                        add_trend(keyword, country, str(date), fname, rec.get("traffic", 0))
                        count += 1
                        if count % 100 == 0:
                            print(f"  {fname}: indexed {count} entries...")
                except Exception as e:
                    continue
        print(f"Done: {fname} — {count} entries indexed")

def index_opportunities():
    fpath = WORKSPACE / "golden_opportunities.json"
    if not fpath.exists():
        print("No golden_opportunities.json found")
        return
    with open(fpath) as f:
        opps = json.load(f)
    if isinstance(opps, dict):
        opps = opps.get("opportunities", list(opps.values()))
    count = 0
    for opp in opps:
        keyword = opp.get("keyword", "")
        country = opp.get("country", "unknown")
        if keyword:
            add_opportunity(keyword, country, opp.get("arbitrage_index", 0),
                          opp.get("tag", ""), opp)
            count += 1
    print(f"Done: golden_opportunities — {count} entries indexed")

if __name__ == "__main__":
    print("Indexing trend history...")
    index_trends()
    print("Indexing opportunities...")
    index_opportunities()
    print("All done.")
```

**Step 4**: Run the indexer: `python3 ~/.openclaw/workspace/index_history.py`

**Step 5**: Integrate deduplication into `trends_postprocess.py`. Add at the top:
```python
import sys
sys.path.insert(0, str(Path.home() / ".openclaw" / "workspace"))
from vector_store import is_duplicate, add_trend
```
Then after filtering explosive trends (≥20k), before yielding/returning each trend, add:
```python
if is_duplicate(keyword, country):
    # Log to error_log.jsonl and skip
    with open(ERROR_LOG, "a") as f:
        f.write(json.dumps({"timestamp": str(datetime.now()),
                            "stage": "trends_postprocess",
                            "reason": "semantic_duplicate",
                            "keyword": keyword, "country": country}) + "\n")
    continue
```

---

### TASK 7 — Install LiteLLM Proxy for Smart Model Routing
**Status**: [ ] Not started

Replace the custom fallback logic in `telegram_bot.py` with LiteLLM Proxy. This gives automatic retries, output validation, and token logging in one config file.

**Step 1**:
```bash
pip install 'litellm[proxy]' --break-system-packages
```

**Step 2**: Create `~/.openclaw/litellm_config.yaml`:
```yaml
model_list:
  - model_name: dwight-primary
    litellm_params:
      model: ollama/qwen3:14b
      api_base: http://localhost:11434

  - model_name: dwight-fallback-paid
    litellm_params:
      model: openrouter/deepseek/deepseek-v3.2
      api_key: os.environ/OPENROUTER_API_KEY

  - model_name: dwight-fallback-free
    litellm_params:
      model: openrouter/stepfun/step-3.5-flash:free
      api_key: os.environ/OPENROUTER_API_KEY

router_settings:
  routing_strategy: usage-based-routing
  num_retries: 2
  fallbacks:
    - dwight-primary: [dwight-fallback-paid, dwight-fallback-free]

general_settings:
  master_key: sk-dwight-local
```

**Step 3**: Create `~/Library/LaunchAgents/ai.openclaw.litellm.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>ai.openclaw.litellm</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/python3</string>
        <string>-m</string>
        <string>litellm</string>
        <string>--config</string>
        <string>/Users/newmac/.openclaw/litellm_config.yaml</string>
        <string>--port</string>
        <string>4000</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>OPENROUTER_API_KEY</key>
        <string>READ_FROM_ENV</string>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/newmac/.openclaw/logs/litellm.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/newmac/.openclaw/logs/litellm.err.log</string>
</dict>
</plist>
```

**Step 4**: `launchctl load ~/Library/LaunchAgents/ai.openclaw.litellm.plist`

**Step 5**: Update `telegram_bot.py` — change all LLM calls to point at `http://localhost:4000` instead of directly calling Ollama/OpenRouter. The API format is OpenAI-compatible (same request structure, just different base URL).

**Verify**: `curl http://localhost:4000/v1/models` — must return a JSON list of `dwight-primary`, `dwight-fallback-paid`, `dwight-fallback-free`.

---

### TASK 8 — Add Trend Persistence Scoring to Pipeline Stage 4
**Status**: [ ] Not started

Stage 4 currently scores trends by CPC + saturation but doesn't predict whether a trend will last long enough to be worth pursuing. This adds a persistence probability to the arbitrage score.

**Step 1**:
```bash
pip install neuralprophet --break-system-packages
```

**Step 2**: Create `~/.openclaw/workspace/trend_forecast.py`:
```python
#!/usr/bin/env python3
"""Predict trend persistence using NeuralProphet."""
import json
import warnings
from pathlib import Path
from datetime import datetime
warnings.filterwarnings("ignore")

HISTORY_FILE = Path.home() / ".openclaw" / "workspace" / "trends_all_history.jsonl"

def _load_series(keyword: str, country: str) -> list:
    """Load time-series data for a keyword-country pair."""
    series = []
    if not HISTORY_FILE.exists():
        return series
    with open(HISTORY_FILE) as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                kw = rec.get("keyword") or rec.get("title", "")
                ct = rec.get("country", "unknown")
                if kw.lower() == keyword.lower() and ct == country:
                    date = rec.get("date") or rec.get("pubDate", "")
                    traffic = rec.get("traffic", 0)
                    if date and traffic:
                        series.append({"ds": str(date)[:10], "y": float(traffic)})
            except Exception:
                continue
    return sorted(series, key=lambda x: x["ds"])

def predict_persistence(keyword: str, country: str, horizon_days: int = 7) -> dict:
    """
    Returns:
        persistence_probability: float 0-1, probability trend stays above 20k
        predicted_halflife_days: int, estimated days until trend drops by half
    """
    series = _load_series(keyword, country)

    # Cold start — not enough history
    if len(series) < 5:
        return {"persistence_probability": 0.5, "predicted_halflife_days": 3,
                "confidence": "low", "data_points": len(series)}

    try:
        import pandas as pd
        from neuralprophet import NeuralProphet

        df = pd.DataFrame(series).drop_duplicates("ds").sort_values("ds")
        df["ds"] = pd.to_datetime(df["ds"])

        m = NeuralProphet(epochs=50, batch_size=16, learning_rate=0.01,
                          seasonality_mode="multiplicative", verbose=False)
        m.fit(df, freq="D", progress=None)

        future = m.make_future_dataframe(df, periods=horizon_days)
        forecast = m.predict(future)

        predicted_values = forecast["yhat1"].tail(horizon_days).values
        threshold = 20000
        days_above = sum(1 for v in predicted_values if v >= threshold)
        persistence_prob = days_above / horizon_days

        # Estimate half-life from decay rate
        current = float(df["y"].iloc[-1]) if len(df) > 0 else threshold
        half_life = horizon_days
        for i, v in enumerate(predicted_values):
            if v <= current / 2:
                half_life = i + 1
                break

        return {"persistence_probability": round(persistence_prob, 3),
                "predicted_halflife_days": half_life,
                "confidence": "medium" if len(series) >= 10 else "low",
                "data_points": len(series)}

    except Exception as e:
        return {"persistence_probability": 0.5, "predicted_halflife_days": 3,
                "confidence": "error", "error": str(e), "data_points": len(series)}
```

**Step 3**: Integrate into `validation.py` (Stage 4). After computing `arbitrage_index` for each opportunity, add:
```python
from trend_forecast import predict_persistence

persistence = predict_persistence(keyword, country)
opportunity["persistence_score"] = persistence["persistence_probability"]
opportunity["predicted_halflife_days"] = persistence["predicted_halflife_days"]
# Weight the final score: arbitrage × persistence
opportunity["weighted_score"] = opportunity["arbitrage_index"] * persistence["persistence_probability"]
```

**Verify**: Run one pipeline manually (`python3 validation.py` if it can run standalone, or check `golden_opportunities.json` after a full run) — entries must contain `persistence_score` and `weighted_score` fields.

---

### TASK 9 — Migrate to smolagents Framework
**Status**: [ ] Not started
**⚠️ DO THIS ON A GIT BRANCH. Do not replace the live bot until testing is complete.**

The current shell-exec mechanism is the root cause of backtick errors, fragile output parsing, and the hard 5-retry limit. smolagents replaces it with Python code execution — 30% fewer LLM steps, no shell formatting issues.

**Step 1**:
```bash
pip install smolagents --break-system-packages
git -C ~/.openclaw/workspace checkout -b smolagents-migration
```

**Step 2**: Create `~/.openclaw/workspace/dwight_agent.py`:
```python
#!/usr/bin/env python3
"""Dwight agent — smolagents CodeAgent replacing shell-exec mechanism."""
import subprocess
import json
from pathlib import Path
from smolagents import CodeAgent, LiteLLMModel, tool

WORKSPACE = Path.home() / ".openclaw" / "workspace"
SOUL_PATH = WORKSPACE / "SOUL.md"

# Load system prompt from SOUL.md
SYSTEM_PROMPT = SOUL_PATH.read_text() if SOUL_PATH.exists() else "You are Dwight, a media buying arbitrage agent."

@tool
def run_shell(command: str) -> str:
    """Run a shell command and return stdout+stderr. Never use backtick substitution."""
    # Sanitize backticks
    command = command.replace("`", "$(").replace("`", ")")
    result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=90)
    output = result.stdout + result.stderr
    return output[:4000] if len(output) > 4000 else output

@tool
def run_pipeline() -> str:
    """Run the full 6-stage media buying research pipeline via heartbeat.py."""
    return run_shell(f"python3 {WORKSPACE}/heartbeat.py")

@tool
def get_golden_opportunities(top_k: int = 10) -> str:
    """Return the top-k golden opportunities from the latest pipeline run."""
    fpath = WORKSPACE / "golden_opportunities.json"
    if not fpath.exists():
        return "No golden_opportunities.json found. Run the pipeline first."
    with open(fpath) as f:
        data = json.load(f)
    opps = data if isinstance(data, list) else list(data.values())
    opps_sorted = sorted(opps, key=lambda x: x.get("arbitrage_index", 0), reverse=True)
    return json.dumps(opps_sorted[:top_k], indent=2)

@tool
def read_file(path: str) -> str:
    """Read a file from the OpenClaw workspace. Returns content as string."""
    fpath = Path(path).expanduser()
    if not fpath.exists():
        return f"File not found: {path}"
    return fpath.read_text()[:8000]

@tool
def write_file(path: str, content: str) -> str:
    """Write content to a file in the OpenClaw workspace."""
    fpath = Path(path).expanduser()
    fpath.parent.mkdir(parents=True, exist_ok=True)
    fpath.write_text(content)
    return f"Written: {path} ({len(content)} chars)"

@tool
def search_past_opportunities(query: str) -> str:
    """Semantic search over past golden opportunities using vector store."""
    try:
        import sys
        sys.path.insert(0, str(WORKSPACE))
        from vector_store import search_opportunities
        results = search_opportunities(query, top_k=5)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Vector search unavailable: {e}"

# Initialize the agent
model = LiteLLMModel(model_id="ollama_chat/qwen3:14b",
                     api_base="http://localhost:11434")

dwight = CodeAgent(
    tools=[run_shell, run_pipeline, get_golden_opportunities,
           read_file, write_file, search_past_opportunities],
    model=model,
    system_prompt=SYSTEM_PROMPT,
    max_steps=10
)

def run(user_message: str) -> str:
    """Entry point — pass a user message, get a response."""
    return dwight.run(user_message)

if __name__ == "__main__":
    # Quick test
    print(dwight.run("What is your current status?"))
```

**Step 3**: Test the agent:
```bash
python3 ~/.openclaw/workspace/dwight_agent.py
```
It should respond with status information without errors.

**Step 4**: Only after successful testing — modify `telegram_bot.py` to route incoming messages through `dwight_agent.run()` instead of the direct LLM → shell-exec loop. Keep the existing Telegram command handlers (`/start`, `/reset`, `/status`, `/tasks`, `/cancel`) intact.

**Step 5**: Test with Telegram before merging to main branch.

---

### TASK 10 — Parallelise Pipeline Stages 1 and 2
**Status**: [ ] Not started
**Run after Task 9 is complete and stable.**

Stage 1 (scrapes 49 countries sequentially) and Stage 2 (vets each keyword one by one) are the biggest bottlenecks. Making them async gives 3-5x faster total pipeline runs.

**Step 1**: `pip install aiohttp --break-system-packages`

**Step 2**: Refactor `trends_scraper.py` — wrap the per-country scrape loop in `asyncio.gather()` with a semaphore:
```python
import asyncio
import aiohttp

async def scrape_country(session, country_code, semaphore):
    async with semaphore:
        # existing single-country scrape logic here, using session instead of requests
        pass

async def scrape_all():
    semaphore = asyncio.Semaphore(10)  # max 10 concurrent
    async with aiohttp.ClientSession() as session:
        tasks = [scrape_country(session, cc, semaphore) for cc in COUNTRY_CODES]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]
```

**Step 3**: Refactor `vetting.py` — wrap the per-keyword SERP check in `asyncio.gather()` with a stricter semaphore:
```python
semaphore = asyncio.Semaphore(5)  # max 5 concurrent SERP checks (respect rate limits)
```

**Step 4**: Add `asyncio.Queue` handoff between Stage 1 and Stage 2 so vetting starts as soon as the first trends arrive, before all 49 countries have been scraped.

**Verify**: Time a full pipeline run before and after — `time python3 heartbeat.py`.

---

## Coding Standards

- Python 3 only (`python3`, never `python`)
- Shell commands: always `$(cmd)`, never backtick substitution
- Error logging: append to `~/.openclaw/workspace/error_log.jsonl` in format: `{"timestamp": "...", "stage": "...", "error": "..."}`
- No hardcoded secrets: read from environment or `~/.openclaw/.env` via `python-dotenv`
- After any change to `telegram_bot.py` or `openclaw.json`: restart both services and verify with `pgrep -fl telegram_bot.py`
- Test in isolation before touching the live bot

---

## Verified Reference Links

| Topic | URL |
|-------|-----|
| Qwen3:14B — Ollama page | https://ollama.com/library/qwen3:14b |
| bge-m3 — Ollama page | https://ollama.com/library/bge-m3 |
| Docker tool-calling benchmark | https://www.docker.com/blog/local-llm-tool-calling-a-practical-evaluation/ |
| Ollama env vars (KEEP_ALIVE etc.) | https://docs.ollama.com/faq |
| smolagents docs | https://huggingface.co/docs/smolagents/en/index |
| smolagents GitHub | https://github.com/huggingface/smolagents |
| LiteLLM proxy | https://docs.litellm.ai/docs/proxy/quick_start |
| LanceDB quickstart | https://docs.lancedb.com/quickstart |
| NeuralProphet | https://neuralprophet.com/ |
| NeuralForecast (Nixtla) | https://nixtlaverse.nixtla.io/neuralforecast/docs/getting-started/introduction.html |
| DeepSeek V3.2 pricing | https://openrouter.ai/deepseek/deepseek-v3.2/pricing |
| Qwen3.5 Plus pricing | https://openrouter.ai/qwen/qwen3.5-plus-02-15 |
| Grok 4.1 Fast | https://openrouter.ai/x-ai/grok-4.1-fast |
| SearXNG | https://searxng.org/ |
