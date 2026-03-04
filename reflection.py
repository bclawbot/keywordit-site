import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

BASE          = Path("/Users/newmac/.openclaw/workspace")
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

if MEMORY_FILE.exists():
    existing = MEMORY_FILE.read_text()
else:
    existing = "# OpenClaw Memory\n\n"

MEMORY_FILE.write_text(existing + new_section)

# ── Summary ─────────────────────────────────────────────────────────────────
print(
    f"✅ Reflection complete: {len(old_terms)} old trends, "
    f"{len(false_positives)} false positives, "
    f"{len(golden_keywords)} golden — MEMORY.md updated"
)
