#!/usr/bin/env python3
"""
Telegram bot — Dwight (Quantitative Arbitrageur)
Primary:  Ollama  qwen3:14b  @ localhost:11434
Fallback: LiteLLM proxy (port 4000) → OpenRouter deepseek-v3.2 → stepfun free
System prompt: loaded from SOUL.md (same directory as this script)

Execution layer:
  LLM responds with:  exec → python3 /path/to/script.py
  Bot actually runs the command, feeds output back to LLM, returns summary.

  For scheduled/periodic execution:
  exec → python3 /path/to/script.py | 600   (runs every 600 seconds)
  Use /tasks to list, /cancel to stop background tasks.
"""

import asyncio
import concurrent.futures
import fcntl
import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
from pathlib import Path
from typing import Optional

import httpx
from telegram import Update
from telegram.constants import ChatAction
from telegram.error import Conflict, NetworkError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ── Load .env ─────────────────────────────────────────────────────────────────

def _load_env(path: str) -> None:
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass

_load_env(str(Path.home() / ".openclaw" / ".env"))

# ── Config ────────────────────────────────────────────────────────────────────

TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
OPENROUTER_KEY    = os.getenv("OPENROUTER_API_KEY", "")

LITELLM_BASE       = "http://localhost:4000"   # LiteLLM proxy — handles all model routing
LITELLM_MODEL      = "dwight-primary"          # routes: qwen3:14b → deepseek-v3.2 → stepfun
LITELLM_API_KEY    = "sk-dwight-local"         # master key from litellm_config.yaml

OLLAMA_BASE       = "http://localhost:11434"
OLLAMA_MODEL      = "qwen3:14b"         # primary local model — tool calling F1=0.971
OLLAMA_MODEL_BIG  = "qwen3-coder:30b"  # available but too slow for chat

OPENROUTER_BASE    = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL   = "deepseek/deepseek-v3.2"        # primary cloud fallback — $0.28/M, reliable
OPENROUTER_MODEL2  = "stepfun/step-3.5-flash:free"  # secondary cloud fallback — last resort

SOUL_PATH         = Path(__file__).parent / "SOUL.md"
SKILLS_DIR        = Path.home() / ".openclaw" / "skills"
WORKSPACE         = Path(__file__).parent
LOCK_FILE         = "/tmp/dwight_bot.lock"

MAX_HISTORY_PAIRS = 6       # keep last 12 messages — avoids context blowup
MAX_EXEC_LOOPS    = 5       # max LLM→exec→LLM iterations per message
EXEC_TIMEOUT      = 60      # seconds per inline command (fast-fail, user gets a response)
EXEC_BG_TIMEOUT   = 300     # seconds for background scheduled tasks
MAX_OUTPUT_CHARS  = 4000    # truncate exec output before sending to LLM

_OLLAMA_TIMEOUT     = httpx.Timeout(connect=10, read=90, write=10, pool=5)
_OPENROUTER_TIMEOUT = httpx.Timeout(connect=10, read=120, write=10, pool=5)

# Exec pattern:  exec → <cmd>          (run once)
#                exec → <cmd> | 600    (run every 600s)
# Supports multiline commands (e.g. inline python3 -c "...") —
# captures until blank line, next exec →, or end of string.
EXEC_RE = re.compile(
    r"exec\s*[→>]\s*(.+?)(?:\s*\|\s*(\d+))?(?=\n\s*\n|\nexec\s*[→>]|\Z)",
    re.DOTALL | re.IGNORECASE,
)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("dwight")

# ── State ─────────────────────────────────────────────────────────────────────

sessions: dict[int, list[dict]]        = {}
last_backend: dict[int, str]           = {}
bg_tasks: dict[int, list[asyncio.Task]] = {}   # user_id → background tasks

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def load_system_prompt() -> str:
    if SOUL_PATH.exists():
        text = SOUL_PATH.read_text(encoding="utf-8").strip()
        log.info("System prompt loaded from %s (%d chars)", SOUL_PATH, len(text))
    else:
        log.warning("SOUL.md not found — using fallback.")
        text = "You are a helpful assistant."

    # Append installed skills from ~/.openclaw/skills/*/SKILL.md (frontmatter stripped)
    _fm_re = re.compile(r"^---\s*\n.*?\n---\s*\n", re.DOTALL)
    def _strip_fm(t: str) -> str:
        return _fm_re.sub("", t, count=1).strip()

    skill_files = sorted(SKILLS_DIR.glob("*/SKILL.md")) if SKILLS_DIR.exists() else []
    if skill_files:
        skill_blocks = [_strip_fm(sf.read_text(encoding="utf-8")) for sf in skill_files]
        text += "\n\n## INSTALLED SKILLS\n\n" + "\n\n---\n\n".join(skill_blocks)
        log.info("Loaded %d skills from %s", len(skill_files), SKILLS_DIR)

    return text

SYSTEM_PROMPT: str = load_system_prompt()

# ── Phase 3.1: CodeAgent integration (opt-in) ─────────────────────────────
_CODE_AGENT = None
try:
    from agents.dwight_code_agent import DwightCodeAgent
    _CODE_AGENT = DwightCodeAgent()
    log.info("[STARTUP] CodeAgent loaded — pipeline commands will use structured execution")
except Exception as _ca_err:
    log.info("[STARTUP] CodeAgent not available (%s) — using exec fallback", _ca_err)

# ── Phase 1.4: Dynamic skill loader ───────────────────────────────────────

ALWAYS_LOAD_SKILLS = ['arbitrage-knowledge', 'pipeline-runner', 'media-researcher']
CONDITIONAL_SKILLS = {
    r'reddit|subreddit': 'reddit-scraper',
    r'trend|scrape|google trends': 'trends-scraper',
    r'keyword|cpc|ads|kw': 'keyword-expander',
    r'dashboard|chart|visual': 'dashboard-builder',
    r'deploy|hosting|publish': 'deployment',
    r'cost|spend|budget|api': 'cost-tracker',
    r'validate|vet|serp|duckduck': 'vetting',
    r'reflect|memory|learn': 'reflection',
    r'angle|rsoc|hook': 'angle-engine',
}

_fm_re_skill = re.compile(r"^---\s*\n.*?\n---\s*\n", re.DOTALL)

def load_skills_for_message(message: str) -> str:
    """Load only relevant skills based on message keywords instead of all skills."""
    if not SKILLS_DIR.exists():
        return ""
    active = list(ALWAYS_LOAD_SKILLS)
    for pattern, skill in CONDITIONAL_SKILLS.items():
        if re.search(pattern, message, re.IGNORECASE):
            if skill not in active:
                active.append(skill)
    content = []
    for name in active:
        f = SKILLS_DIR / name / 'SKILL.md'
        if f.exists():
            text = f.read_text(encoding='utf-8')
            text = _fm_re_skill.sub("", text, count=1).strip()
            content.append(text)
    return '\n\n---\n\n'.join(content) if content else ""

# ── Phase 1.2: Model parity assertion ─────────────────────────────────────

GATEWAY_CONFIG = Path(os.path.expanduser('~/.openclaw/openclaw.json'))
EXPECTED_BOT_MODEL = OLLAMA_MODEL  # qwen3:14b

def assert_model_parity():
    """Hard-fail at startup if openclaw.json model != expected bot model.
    Prevents 60-second model reload delays on every Telegram interaction."""
    if not GATEWAY_CONFIG.exists():
        log.warning("[STARTUP] openclaw.json not found — skipping parity check")
        return
    try:
        with open(GATEWAY_CONFIG) as f:
            config = json.load(f)
        gateway_model = config.get('model') or config.get('default_model', '')
    except Exception as e:
        log.warning("[STARTUP] Could not read openclaw.json: %s", e)
        return

    # Check Ollama is reachable and has the model loaded
    try:
        r = httpx.get(f'{OLLAMA_BASE}/api/tags', timeout=5)
        loaded = [m['name'] for m in r.json().get('models', [])]
        if EXPECTED_BOT_MODEL not in loaded:
            log.warning("[STARTUP] Model %s not loaded in Ollama (loaded: %s)",
                        EXPECTED_BOT_MODEL, loaded[:5])
    except Exception as e:
        log.warning("[STARTUP] Ollama unreachable: %s", e)

    if gateway_model and gateway_model != EXPECTED_BOT_MODEL:
        raise RuntimeError(
            f'[STARTUP ABORT] Model mismatch: openclaw.json={gateway_model!r} '
            f'vs bot={EXPECTED_BOT_MODEL!r}. Fix openclaw.json before starting.'
        )
    log.info("[STARTUP] Model parity OK: %s", EXPECTED_BOT_MODEL)

# ── Single-instance lock ──────────────────────────────────────────────────────

def _acquire_lock():
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit(
            f"Another instance is already running (lock: {LOCK_FILE}).\n"
            "Kill it first:  pkill -f telegram_bot.py"
        )
    return lock_fd

# ── Execution engine ──────────────────────────────────────────────────────────

# Allowed binaries for the exec engine. Commands starting with anything else
# are rejected to prevent LLM hallucinations or prompt injections from running
# arbitrary programs (e.g. curl|sh, rm -rf /, etc.).
_ALLOWED_BINARIES = frozenset({
    "python3", "python",
    "cat", "head", "tail", "wc", "ls", "pwd", "date", "echo", "grep", "find",
    "jq", "sqlite3", "git", "pip3", "pip",
    "ollama", "curl",  # curl kept for Ollama health probes
    "launchctl", "pgrep", "pkill",
})

# Pipeline scripts that are always allowed (resolved to absolute paths)
_ALLOWED_SCRIPT_DIRS = (
    str(WORKSPACE),
    str(Path.home() / ".openclaw"),
)


def _validate_command(cmd: str) -> str | None:
    """Validate a command string against the allowlist.

    Returns None if the command is safe, or an error message if rejected.
    """
    # Strip backtick substitution (legacy sanitization)
    cmd_clean = re.sub(r'`([^`]*)`', r'$(\1)', cmd)

    # Reject dangerous shell metacharacters that enable chaining/injection.
    # We allow: pipes to head/tail/grep (common and useful), but NOT arbitrary pipes.
    # The shell=False approach below prevents these from being interpreted anyway,
    # but we reject them explicitly to give clear error messages.
    dangerous_patterns = [
        (r';\s*\S', "semicolon command chaining"),
        (r'&&', "'&&' command chaining"),
        (r'\|\|', "'||' command chaining"),
        (r'>\s*/', "output redirection to absolute path"),
        (r'>>', "append redirection"),
        (r'\$\(', "command substitution"),
        (r'`', "backtick substitution"),
    ]
    for pattern, desc in dangerous_patterns:
        if re.search(pattern, cmd_clean):
            return f"Blocked: {desc} not allowed in exec commands"

    try:
        parts = shlex.split(cmd_clean)
    except ValueError as e:
        return f"Blocked: malformed command ({e})"

    if not parts:
        return "Blocked: empty command"

    binary = Path(parts[0]).name  # extract basename (python3 from /usr/bin/python3)

    if binary in _ALLOWED_BINARIES:
        return None

    # Allow scripts under workspace or .openclaw dirs
    resolved = parts[0]
    if any(resolved.startswith(d) for d in _ALLOWED_SCRIPT_DIRS):
        return None

    return f"Blocked: '{binary}' is not in the allowed command list"


async def run_command(cmd: str, timeout: int = EXEC_TIMEOUT) -> str:
    """Run a validated command in the workspace directory and return its output."""
    # Validate before executing
    rejection = _validate_command(cmd)
    if rejection:
        log.warning("EXEC BLOCKED: %s — %s", cmd[:80], rejection)
        return f"[{rejection}]"

    log.info("EXEC: %s", cmd)
    loop = asyncio.get_event_loop()
    env = {**os.environ, "HOME": str(Path.home())}
    try:
        parts = shlex.split(cmd)
        result = await asyncio.wait_for(
            loop.run_in_executor(
                _executor,
                lambda: subprocess.run(
                    parts,
                    capture_output=True,
                    text=True,
                    cwd=str(WORKSPACE),
                    env=env,
                ),
            ),
            timeout=timeout,
        )
        out = (result.stdout + result.stderr).strip()
        if not out:
            out = f"[exit {result.returncode}, no output]"
        elif len(out) > MAX_OUTPUT_CHARS:
            out = out[:MAX_OUTPUT_CHARS] + f"\n... [truncated — {len(out) - MAX_OUTPUT_CHARS} more chars]"
        log.info("EXEC done (exit=%d, %d chars)", result.returncode, len(out))
        return out
    except asyncio.TimeoutError:
        return f"[timed out after {timeout}s]"
    except Exception as exc:
        return f"[error running command: {exc}]"


async def _run_scheduled(
    bot,
    chat_id: int,
    user_id: int,
    cmd: str,
    interval: int,
) -> None:
    """Background task: run cmd every `interval` seconds, report via Telegram."""
    iteration = 0
    while True:
        iteration += 1
        log.info("Scheduled task iter=%d user=%d cmd=%s", iteration, user_id, cmd)
        output = await run_command(cmd, timeout=EXEC_BG_TIMEOUT)

        # Ask LLM to summarise the output
        messages = (
            [{"role": "system", "content": SYSTEM_PROMPT}]
            + sessions.get(user_id, [])
            + [{"role": "user", "content": f"[scheduled exec output — run #{iteration}]\n$ {cmd}\n{output}\n\nSummarise results concisely."}]
        )
        try:
            summary = await _llm_async_call(messages, stage="telegram_bot/scheduled")
        except _LLMError:
            summary = output

        for chunk in _split_message(f"[Update #{iteration}]\n{summary}"):
            try:
                await bot.send_message(chat_id=chat_id, text=chunk)
            except Exception as exc:
                log.warning("Failed to send scheduled update: %s", exc)

        await asyncio.sleep(interval)


def _cancel_user_tasks(user_id: int) -> int:
    tasks = bg_tasks.pop(user_id, [])
    for t in tasks:
        t.cancel()
    return len(tasks)

# ── LLM backends (delegated to shared llm_client.py async_call) ──────────────

from llm_client import async_call as _llm_async_call, LLMError as _LLMError


async def _llm(messages: list[dict]) -> tuple[Optional[str], str]:
    """Call LLM with 4-tier async fallback via shared llm_client."""
    try:
        reply = await _llm_async_call(messages, stage="telegram_bot/llm")
        return reply, "llm_client (auto-fallback)"
    except _LLMError:
        return None, "none"


async def get_reply(
    user_id: int,
    user_message: str,
    bot=None,
    chat_id: int = 0,
) -> tuple[str, str]:
    history = sessions.setdefault(user_id, [])
    history.append({"role": "user", "content": user_message})

    max_msgs = MAX_HISTORY_PAIRS * 2
    if len(history) > max_msgs:
        history[:] = history[-max_msgs:]

    backend = "none"
    reply = ""
    appended_this_turn = False   # track if we already appended reply in loop

    for iteration in range(MAX_EXEC_LOOPS):
        # Phase 1.4: Dynamic skill loading based on message content
        dynamic_skills = load_skills_for_message(user_message)
        sys_prompt = SYSTEM_PROMPT
        if dynamic_skills:
            sys_prompt = SYSTEM_PROMPT + "\n\n## ACTIVE SKILLS\n\n" + dynamic_skills
        messages = [{"role": "system", "content": sys_prompt}] + history
        reply, backend = await _llm(messages)

        if reply is None:
            reply = "Both backends failed. Check Ollama and OpenRouter credentials."
            backend = "none"
            break

        matches = list(EXEC_RE.finditer(reply))
        if not matches:
            # No exec commands — final response
            break

        # LLM wants to execute something
        history.append({"role": "assistant", "content": reply})
        appended_this_turn = True

        exec_results = []
        for m in matches:
            cmd = m.group(1).strip()
            interval_str = m.group(2)

            if interval_str and bot and chat_id:
                interval = int(interval_str)
                task = asyncio.create_task(
                    _run_scheduled(bot, chat_id, user_id, cmd, interval)
                )
                bg_tasks.setdefault(user_id, []).append(task)
                exec_results.append(
                    f"[scheduled] {cmd} — will run every {interval}s and report here"
                )
                log.info("Scheduled task created: %s every %ds", cmd, interval)
            else:
                if bot and chat_id:
                    try:
                        preview = cmd.split("\n")[0][:80]
                        await bot.send_message(
                            chat_id=chat_id,
                            text=f"⚙️ `{preview}`",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                # Phase 3.1: Try CodeAgent for pipeline stage commands
                output = None
                if _CODE_AGENT and re.search(r'python3?\s+\S+\.py', cmd):
                    try:
                        output = await asyncio.to_thread(_CODE_AGENT.run_stage_by_cmd, cmd)
                    except Exception as _ca_e:
                        log.warning("CodeAgent failed (%s) — falling back to exec", _ca_e)
                        output = None
                if output is None:
                    output = await run_command(cmd)
                exec_results.append(f"$ {cmd}\n{output}")

        combined = "\n\n---\n\n".join(exec_results)
        history.append({"role": "user", "content": f"[exec output]\n{combined}"})

    if not appended_this_turn:
        history.append({"role": "assistant", "content": reply})

    last_backend[user_id] = backend
    return reply, backend

# ── Typing indicator ──────────────────────────────────────────────────────────

async def _keep_typing(bot, chat_id: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        except Exception:
            pass
        await asyncio.sleep(4)

# ── Telegram handlers ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    _cancel_user_tasks(uid)
    sessions[uid] = []
    last_backend.pop(uid, None)
    name = update.effective_user.first_name or "operator"
    await update.message.reply_text(
        f"Dwight online, {name}. Session cleared.\n"
        "Commands: /reset /status /tasks /cancel"
    )


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    cancelled = _cancel_user_tasks(uid)
    sessions[uid] = []
    last_backend.pop(uid, None)
    msg = "Session history cleared."
    if cancelled:
        msg += f" {cancelled} background task(s) cancelled."
    await update.message.reply_text(msg)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    history = sessions.get(uid, [])
    turns = len(history) // 2
    backend = last_backend.get(uid, "none yet")
    tasks = bg_tasks.get(uid, [])
    active = sum(1 for t in tasks if not t.done())
    await update.message.reply_text(
        f"Primary:        Ollama — {OLLAMA_MODEL}\n"
        f"Fallback:       OpenRouter — {OPENROUTER_MODEL}\n"
        f"Last used:      {backend}\n"
        f"History:        {turns} turn(s) ({len(history)} messages)\n"
        f"Background tasks: {active} active\n"
        f"System prompt:  {len(SYSTEM_PROMPT)} chars"
    )


async def cmd_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    tasks = bg_tasks.get(uid, [])
    active = [t for t in tasks if not t.done()]
    if not active:
        await update.message.reply_text("No background tasks running.")
    else:
        await update.message.reply_text(
            f"{len(active)} background task(s) running.\n"
            "Use /cancel to stop all of them."
        )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    cancelled = _cancel_user_tasks(uid)
    if cancelled:
        await update.message.reply_text(f"Cancelled {cancelled} background task(s).")
    else:
        await update.message.reply_text("No background tasks to cancel.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid  = update.effective_user.id
    text = (update.message.text or "").strip()
    if not text:
        return

    stop_typing = asyncio.Event()
    typing_task = asyncio.create_task(
        _keep_typing(context.bot, update.effective_chat.id, stop_typing)
    )

    try:
        log.info("User %d: %s", uid, text[:80])
        reply, backend = await get_reply(
            uid, text,
            bot=context.bot,
            chat_id=update.effective_chat.id,
        )
        log.info("Reply via %s (%d chars)", backend, len(reply))
    except Exception as exc:
        log.error("get_reply error: %s", exc, exc_info=True)
        if sessions.get(uid):
            sessions[uid].pop()
        reply = "Internal error. Try again in a moment."
    finally:
        stop_typing.set()
        typing_task.cancel()

    try:
        for chunk in _split_message(reply):
            await update.message.reply_text(chunk)
    except Exception as exc:
        log.error("Failed to send reply: %s", exc)


def _split_message(text: str, limit: int = 4096) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        chunks.append(text[:limit])
        text = text[limit:]
    return chunks

# ── PTB error handler ─────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        log.warning("Telegram 409 Conflict — waiting for previous session to expire.")
    elif isinstance(err, (NetworkError, TimedOut)):
        log.warning("Transient network error (will retry): %s", err)
    else:
        log.error("Unhandled PTB error: %s", err, exc_info=context.error)

# ── Graceful shutdown ────────────────────────────────────────────────────────

_lock_fd = None  # set in main() after lock acquisition

def _shutdown_handler(signum, frame):
    """Clean up background tasks, subprocesses, and lock file on SIGTERM/SIGINT."""
    sig_name = signal.Signals(signum).name
    log.info("Received %s — shutting down gracefully", sig_name)
    # Cancel all background tasks
    for uid, tasks in bg_tasks.items():
        for t in tasks:
            t.cancel()
    bg_tasks.clear()
    # Release lock file
    if _lock_fd:
        try:
            fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            _lock_fd.close()
        except Exception:
            pass
    try:
        os.unlink(LOCK_FILE)
    except FileNotFoundError:
        pass
    sys.exit(0)

# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not TELEGRAM_TOKEN:
        raise SystemExit("TELEGRAM_TOKEN not set. Add it to ~/.openclaw/.env")
    if not OPENROUTER_KEY:
        log.warning("OPENROUTER_API_KEY not set — fallback will fail.")

    global _lock_fd
    _lock_fd = _acquire_lock()

    # Register graceful shutdown handlers
    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    # Phase 1.2: Assert model parity before starting
    assert_model_parity()

    log.info(
        "Starting Dwight bot (primary=%s, fallback=%s)",
        OLLAMA_MODEL, OPENROUTER_MODEL,
    )

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("reset",  cmd_reset))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("tasks",  cmd_tasks))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    log.info("Polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
