import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

BASE          = Path(__file__).resolve().parent
HISTORY_LOG   = BASE / "trends_all_history.jsonl"
VALIDATED_FILE = BASE / "validated_opportunities.json"
MEMORY_FILE   = BASE / "MEMORY.md"

# ── Load history ────────────────────────────────────────────────────────────
if not HISTORY_LOG.exists():
    print(f"⚠️  {HISTORY_LOG} not found — run trends_scraper.py first")
    raise SystemExit(1)

cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

old_terms = set()
country_term_map = {}  # term -> geo

with HISTORY_LOG.open() as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        fetched_raw = rec.get("fetched_at", "")
        try:
            fetched_dt = datetime.fromisoformat(fetched_raw)
            if fetched_dt.tzinfo is None:
                fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if fetched_dt < cutoff:
            term = rec.get("term", "").lower()
            old_terms.add(term)
            country_term_map[term] = rec.get("geo", "??")

# ── Load validated keywords ─────────────────────────────────────────────────
validated_keywords = set()
golden_keywords    = set()
country_golden     = {}   # geo -> {golden, total}
country_fp         = {}   # geo -> fp count

if VALIDATED_FILE.exists():
    validated = json.loads(VALIDATED_FILE.read_text())
    for rec in validated:
        kw = rec.get("keyword", "").lower()
        validated_keywords.add(kw)
        st = rec.get("source_trend", "").lower()
        if st:
            validated_keywords.add(st)
        geo = rec.get("country", "??")
        country_golden.setdefault(geo, {"golden": 0, "total": 0})
        country_golden[geo]["total"] += 1
        if rec.get("tag") == "GOLDEN_OPPORTUNITY":
            golden_keywords.add(kw)
            country_golden[geo]["golden"] += 1

# ── Identify false positives ────────────────────────────────────────────────
false_positives = old_terms - validated_keywords
fp_list = sorted(false_positives)

for term in fp_list:
    geo = country_term_map.get(term, "??")
    country_fp[geo] = country_fp.get(geo, 0) + 1

# ── Signal weights per country (golden_rate) ────────────────────────────────
signal_weights = {}
for geo, counts in country_golden.items():
    total = counts["total"]
    golden = counts["golden"]
    signal_weights[geo] = round(golden / total, 2) if total else 0.0

# ── Update MEMORY.md ────────────────────────────────────────────────────────
now_str = datetime.now().isoformat(timespec="seconds")

new_section = f"""
## Reflection — {now_str}

### False Positives (trends >48h old, never reached validation)
Total: {len(fp_list)}
"""
if fp_list:
    for term in fp_list[:30]:  # cap to avoid bloat
        geo = country_term_map.get(term, "??")
        new_section += f"- [{geo}] {term}\n"
    if len(fp_list) > 30:
        new_section += f"  ...and {len(fp_list) - 30} more\n"

new_section += "\n### False Positives by Country\n"
for geo, cnt in sorted(country_fp.items(), key=lambda x: -x[1])[:15]:
    new_section += f"- {geo}: {cnt} FP\n"

new_section += "\n### Golden Opportunities by Country\n"
for geo, counts in sorted(country_golden.items(), key=lambda x: -x[1]["golden"])[:15]:
    new_section += f"- {geo}: {counts['golden']} golden / {counts['total']} total\n"

new_section += "\n### Signal Weights (golden_rate per country, 0.0–1.0)\n"
for geo, weight in sorted(signal_weights.items(), key=lambda x: -x[1])[:20]:
    new_section += f"- {geo}: {weight:.2f}\n"

new_section += "\n---\n"

MAX_MEMORY_LINES = 400

if MEMORY_FILE.exists():
    existing = MEMORY_FILE.read_text()
    lines = existing.splitlines()
    if len(lines) > MAX_MEMORY_LINES:
        # Preserve the static header (everything before the first ## Reflection section)
        header_end = 0
        for i, line in enumerate(lines):
            if line.startswith("## Reflection"):
                header_end = i
                break
        header = "\n".join(lines[:header_end]) if header_end > 0 else "# OpenClaw Memory\n"
        recent = "\n".join(lines[-MAX_MEMORY_LINES:])
        existing = header + "\n\n" + recent + "\n"
else:
    existing = "# OpenClaw Memory\n\n"

MEMORY_FILE.write_text(existing + new_section)

# ── Phase 1.1: Export signal_weights.json (closed feedback loop) ─────────────

TIER_CPC = {
    'US': 0.5, 'GB': 0.45, 'CA': 0.45, 'AU': 0.45, 'DE': 0.40, 'FR': 0.35,
    'JP': 0.25, 'KR': 0.20, 'IT': 0.20, 'ES': 0.18, 'NL': 0.25, 'SE': 0.25,
    'BR': 0.08, 'MX': 0.06, 'IN': 0.03, 'ID': 0.03, 'TH': 0.04, 'PH': 0.02,
    'PL': 0.10, 'TR': 0.05, 'ZA': 0.15,
    'NG': 0.05, 'KE': 0.05, 'EG': 0.06, 'SA': 0.08, 'BD': 0.04, 'PK': 0.04,
}

def _get_tier_min_cpc(country):
    return TIER_CPC.get(country, 0.10)

def export_signal_weights():
    """Write per-country dynamic thresholds to signal_weights.json.
    Read by trends_postprocess.py (Stage 1b) at startup."""
    weights = {}
    for geo, counts in country_golden.items():
        total  = counts['total']
        golden = counts['golden']
        if total < 100:
            continue  # skip countries with insufficient data (plan: HAVING total > 100)
        golden_rate = golden / total if total > 0 else 0.0
        base_cpc = _get_tier_min_cpc(geo)

        if golden_rate < 0.001:
            weights[geo] = {
                'min_cpc': round(base_cpc * 1.5, 3),
                'golden_rate': round(golden_rate, 6),
                'filter_mode': 'max_strict',
            }
        elif golden_rate > 0.01:
            weights[geo] = {
                'min_cpc': round(base_cpc * 0.8, 3),
                'golden_rate': round(golden_rate, 6),
                'filter_mode': 'relaxed',
            }
        else:
            weights[geo] = {
                'min_cpc': base_cpc,
                'golden_rate': round(golden_rate, 6),
                'filter_mode': 'strict',
            }

    weights['updated_at'] = datetime.now(timezone.utc).isoformat()
    out_path = Path(os.path.expanduser('~/.openclaw/signal_weights.json'))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(weights, indent=2), encoding='utf-8')
    country_count = sum(1 for k in weights if k != 'updated_at')
    print(f"  [Signal Weights] Exported signal_weights.json — {country_count} countries")

try:
    export_signal_weights()
except Exception as _sw_err:
    print(f"  ⚠️  signal_weights export failed: {_sw_err}")

# ── Summary ─────────────────────────────────────────────────────────────────
print(
    f"✅ Reflection complete: {len(old_terms)} old trends, "
    f"{len(false_positives)} false positives, "
    f"{len(golden_keywords)} golden — MEMORY.md updated"
)
