import json
import os
import re
import time
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent

from llm_client import generate as _llm_generate  # noqa: E402
from money_flow_classifier import ARCHETYPES as _MF_ARCHETYPES  # noqa: E402
SNAP = BASE / "latest_trends.json"
EXP  = BASE / "explosive_trends.json"
EXP_LOG = BASE / "explosive_trends_history.jsonl"  # append-only
ERROR_LOG = BASE / "error_log.jsonl"
DEDUP_LOG = BASE / "dedup_log.jsonl"

# R2-C1: checkpoint + liveness.
CHECKPOINT_EVERY = 200
ALIVE_DIR = Path(os.environ.get("HEARTBEAT_ALIVE_DIR", Path.home() / ".openclaw" / "logs"))
ALIVE_FILE = ALIVE_DIR / "trends_postprocess.alive"


def _atomic_save(target: Path, data) -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


def _touch_alive() -> None:
    try:
        ALIVE_DIR.mkdir(parents=True, exist_ok=True)
        ALIVE_FILE.touch()
    except Exception:
        pass

# ── Phase 0.1: Commercial Intent Prefilter ────────────────────────────────────
SIGNAL_WEIGHTS_PATH = Path(os.path.expanduser('~/.openclaw/signal_weights.json'))
FALSE_POS_LOG = BASE / "false_positive_log.jsonl"

# Phase 2.3: Config version stamp
try:
    from config import pipeline_config
    _CONFIG_VERSION = pipeline_config.version
except Exception:
    _CONFIG_VERSION = "unknown"

NON_COMMERCIAL_PATTERNS = re.compile(
    r'\b(died|dead|death|obituary|rip|killed|murder|arrested|'
    r'election|vote|voted|president|senator|shooting|earthquake|'
    r'hurricane|flood|wildfire|war|ceasefire|protest|riot|'
    r'scored|won|lost|champion|playoffs|super bowl|world cup|'
    r'born|birthday|anniversary|married|divorced|pregnant|'
    r'trailer|episode|season|netflix|hbo|stream|watch|'
    r'meme|viral|trend|twitter|tiktok)\b', re.IGNORECASE
)
COMMERCIAL_SIGNALS = re.compile(
    r'\b(insurance|lawyer|attorney|mortgage|loan|credit|rehab|'
    r'treatment|software|saas|pricing|buy|hire|cost|best|'
    r'review|vs|alternative|cheap|affordable|service|tool|'
    r'platform|agency|consultant|repair|install|fix)\b', re.IGNORECASE
)

def load_signal_weights():
    """Load dynamic per-country thresholds written by reflection.py."""
    if SIGNAL_WEIGHTS_PATH.exists():
        try:
            return json.loads(SIGNAL_WEIGHTS_PATH.read_text(encoding='utf-8'))
        except Exception:
            pass
    return {'min_cpc_override': {}, 'golden_rate_floor': 0.001}

def commercial_intent_prefilter(trend, country_code, weights):
    """
    3-layer commercial intent filter.
    Returns (result, reason):
      True, reason  = pass (commercial signal detected)
      False, reason = drop (non-commercial)
      None, reason  = ambiguous (needs LLM triage in future)
    """
    title = (trend.get('term', '') + ' ' + trend.get('description', '')).strip()

    # Layer 1: Regex hard-kill — non-commercial patterns
    # EXCEPTION: exempt keywords that match a money-flow archetype.
    # These look non-commercial on the surface but drive real ad spend
    # (e.g., "hurricane" → insurance, home_services, legal).
    if NON_COMMERCIAL_PATTERNS.search(title):
        has_money_flow_archetype = any(pat.search(title) for pat in _MF_ARCHETYPES.values())
        if not has_money_flow_archetype:
            return False, 'regex_noncommercial'

    # Layer 2: Dynamic CPC threshold from signal_weights.json
    country_weights = weights.get(country_code, {})
    if isinstance(country_weights, dict):
        override_cpc = country_weights.get('min_cpc')
        if override_cpc and trend.get('estimated_cpc', 0) < override_cpc:
            return False, f'cpc_below_threshold_{override_cpc}'

    # Layer 3: Commercial signal whitelist — pass immediately
    if COMMERCIAL_SIGNALS.search(title):
        return True, 'keyword_commercial_signal'

    return None, 'ambiguous_needs_llm'


# ── Layer 3: LLM triage for ambiguous trends ────────────────────────────────
_LLM_TRIAGE_MAX = 20     # max ambiguous trends to classify per run
_LLM_TRIAGE_TIMEOUT = 8  # seconds per Ollama call

def _llm_classify_commercial(title: str) -> bool:
    """Ask local LLM whether a trend title has commercial intent.
    Returns True if LLM says commercial, False otherwise.
    Falls back to True (pass-through) on any error to avoid false negatives."""
    try:
        prompt = (
            "You are a commercial intent classifier. "
            "Does the following trending topic have commercial/transactional intent "
            "(someone might buy a product, hire a service, or compare tools)? "
            f"Topic: \"{title}\"\n"
            "Answer ONLY 'yes' or 'no'."
        )
        answer = _llm_generate(prompt, max_tokens=10, temperature=0.1, timeout="fast", stage="trends_postprocess/classify")
        return answer.strip().lower().startswith("yes")
    except Exception:
        return True  # fail-open: ambiguous trends pass through on LLM error


try:
    from vector_store import is_duplicate, add_trend, maintenance as _vs_maintenance, health_check as _vs_health_check
    _VECTOR_STORE_AVAILABLE = True
except Exception:
    _VECTOR_STORE_AVAILABLE = False
    _vs_maintenance = None
    _vs_health_check = None

def score(t):
    val = str(t.get("traffic", "0")).replace("+","").replace(",","")
    val = val.replace("K","000").replace("M","000000")
    try:
        return int(val)
    except ValueError:
        return 0


def main():
    global _VECTOR_STORE_AVAILABLE
    data = json.loads(SNAP.read_text())

    # ── Apply commercial intent prefilter BEFORE explosive filter + LanceDB ───────
    _weights = load_signal_weights()
    _filtered = []
    _ambiguous = []
    _fp_dropped = 0

    with FALSE_POS_LOG.open('a', encoding='utf-8') as _fp_log:
        for trend in data:
            country = trend.get('geo', 'US')
            result, reason = commercial_intent_prefilter(trend, country, _weights)
            if result is True:
                _filtered.append(trend)
            elif result is None:
                # Ambiguous — pass through for now (LLM triage deferred to Phase 1)
                _ambiguous.append(trend)
            else:
                # Dropped — log to false_positive_log.jsonl
                _fp_log.write(json.dumps({
                    'title': trend.get('term', ''),
                    'country': country,
                    'reason': reason,
                    'estimated_cpc': trend.get('estimated_cpc', 0),
                    'traffic': trend.get('traffic', ''),
                    'ts': trend.get('fetched_at', datetime.now().isoformat()),
                    'config_version': _CONFIG_VERSION,
                }) + '\n')
                _fp_dropped += 1

    # Layer 3: LLM triage for ambiguous trends (capped at _LLM_TRIAGE_MAX per run)
    _llm_passed = []
    _llm_rejected = 0
    _llm_triaged = 0
    _llm_batch = _ambiguous[:_LLM_TRIAGE_MAX]
    _llm_overflow = _ambiguous[_LLM_TRIAGE_MAX:]  # pass through untriaged

    for _amb_trend in _llm_batch:
        _amb_title = (_amb_trend.get('term', '') + ' ' + _amb_trend.get('description', '')).strip()
        _llm_triaged += 1
        if _llm_classify_commercial(_amb_title):
            _llm_passed.append(_amb_trend)
        else:
            _llm_rejected += 1
            try:
                with FALSE_POS_LOG.open('a', encoding='utf-8') as _fp_llm:
                    _fp_llm.write(json.dumps({
                        'title': _amb_trend.get('term', ''),
                        'country': _amb_trend.get('geo', 'US'),
                        'reason': 'llm_triage_reject',
                        'estimated_cpc': _amb_trend.get('estimated_cpc', 0),
                        'ts': _amb_trend.get('fetched_at', datetime.now().isoformat()),
                        'config_version': _CONFIG_VERSION,
                    }) + '\n')
            except Exception:
                pass

    if _llm_triaged:
        print(f"LLM triage: {_llm_triaged} classified, {_llm_rejected} rejected, "
              f"{len(_llm_passed)} passed, {len(_llm_overflow)} overflow (pass-through)")
    data = _filtered + _llm_passed + _llm_overflow

    if _fp_dropped:
        print(f"Commercial intent filter: {_fp_dropped} non-commercial trends dropped, "
              f"{len(_filtered)} commercial, {len(_ambiguous)} ambiguous passed")

    explosive = [x for x in data if score(x) >= 20000]
    explosive_sorted = sorted(explosive, key=score, reverse=True)

    # Health check: verify LanceDB trends table and embedding endpoint
    if _VECTOR_STORE_AVAILABLE and _vs_health_check:
        _hc = _vs_health_check()
        if not _hc.get("embedding_ok"):
            print(f"  [LanceDB] Embedding endpoint unhealthy: {_hc.get('embedding_error', 'unknown')}")
            _VECTOR_STORE_AVAILABLE = False
        elif "trends" not in _hc.get("tables", []):
            print("  [LanceDB] trends table missing — will be created on first add_trend()")

    deduped_explosive = []
    skipped_semantic = 0
    _vs_consecutive_errors = 0
    _VS_ERROR_THRESHOLD = 3
    _t0 = time.time()
    _total = len(explosive_sorted)
    _touch_alive()
    for _i, rec in enumerate(explosive_sorted, 1):
        rec['explosive_score'] = score(rec)
        rec['marked_at'] = datetime.now().isoformat()
        keyword = rec.get("term", "")
        country = rec.get("geo", "unknown")
        if _VECTOR_STORE_AVAILABLE and keyword:
            try:
                if is_duplicate(keyword, country):
                    skipped_semantic += 1
                    with DEDUP_LOG.open("a") as f:
                        f.write(json.dumps({
                            "timestamp": datetime.now().isoformat(),
                            "stage": "trends_postprocess",
                            "reason": "semantic_duplicate",
                            "keyword": keyword,
                            "country": country,
                        }) + "\n")
                    continue
                add_trend(keyword, country, rec.get("fetched_at", ""), rec.get("source", ""), score(rec))
                _vs_consecutive_errors = 0
            except Exception as _vs_err:
                _vs_consecutive_errors += 1
                if _vs_consecutive_errors >= _VS_ERROR_THRESHOLD:
                    print(f"  [LanceDB] {_VS_ERROR_THRESHOLD} consecutive errors — disabling dedup for this run")
                    _VECTOR_STORE_AVAILABLE = False
                else:
                    print(f"  [LanceDB] Error indexing '{keyword}': {_vs_err}")
                try:
                    with ERROR_LOG.open("a") as _ef:
                        _ef.write(json.dumps({
                            "timestamp": datetime.now().isoformat(),
                            "stage": "trends_postprocess/vector_store",
                            "error": str(_vs_err),
                            "keyword": keyword,
                            "country": country,
                        }) + "\n")
                except Exception:
                    pass
        deduped_explosive.append(rec)

        if _i % CHECKPOINT_EVERY == 0:
            _atomic_save(EXP, deduped_explosive)
            _touch_alive()
            print(f"[trends_postprocess] checkpoint {_i}/{_total} saved "
                  f"({len(deduped_explosive)} after dedup, {int(time.time()-_t0)}s elapsed)",
                  flush=True)

    if skipped_semantic:
        print(f"Skipped {skipped_semantic} semantic duplicates via LanceDB")

    explosive_sorted = deduped_explosive
    _atomic_save(EXP, explosive_sorted)
    _touch_alive()

    with EXP_LOG.open("a") as f:
        for rec in explosive_sorted:
            f.write(json.dumps(rec) + "\n")

    print(f"Saved {len(explosive_sorted)} explosive trends to {EXP}")
    print(f"Appended to history log: {EXP_LOG}")

    # Compact LanceDB to prevent _versions directory bloat
    if _VECTOR_STORE_AVAILABLE and _vs_maintenance:
        try:
            _vs_maintenance()
        except Exception as _maint_err:
            print(f"  [LanceDB] Maintenance error: {_maint_err}")


if __name__ == "__main__":
    main()
