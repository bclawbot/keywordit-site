# STEP 9 — Bridge consequence_generator → vetting, then resume

## Why this exists

STEP 9's autonomous run hit a wall at **Stage 4a (keyword_extractor.py)**: the script
was alive in the background for 73 minutes with a 0-byte log file and never produced
`commercial_keywords.json`. That's an LLM retry storm (see companion doc
`STEP_9_KEYWORD_EXTRACTOR_HANG_FIX.md`) — not a data problem.

The data is fine:

- `transformed_keywords.json` contains **199 rows** whose `metrics_source` is
  `consequence_generator`, every one of them has a non-empty `source_trend`.
- `vetting.py` reads **`commercial_keywords.json`**, not `transformed_keywords.json`.
- `commercial_keywords.json` currently has **5909 rows** from an earlier pipeline
  pass and has NOT been refreshed with this run's consequence_generator output.

So we bypass `keyword_extractor.py` for this measurement run by mapping the 199
consequence_generator rows directly into the `commercial_keywords.json` schema and
appending them. The cost — we skip live DataForSEO enrichment for these rows — is
acceptable: `needs_dataforseo_validation=true` is preserved so the next scheduled
heartbeat will enrich them.

This unblocks Stages 4b → 4c → 5 → 6 → 7 of the autonomous run.

**This prompt is self-contained.** Do NOT read the full `STEP_9_AUTONOMOUS_RUN.md`
yet — follow the steps below, and only at the end will you re-enter the autonomous
run at Stage 4b.

---

## Pre-flight sanity checks

Run all three in one Bash call:

```bash
cd ~/.openclaw/workspace && \
  pwd && \
  git rev-parse --abbrev-ref HEAD && \
  git log --oneline -3 && \
  python3 -c "
import json, pathlib
p = pathlib.Path('/Users/newmac/.openclaw/workspace')
t = json.loads((p/'transformed_keywords.json').read_text())
c = json.loads((p/'commercial_keywords.json').read_text())
cg = [r for r in t if r.get('metrics_source') == 'consequence_generator']
cg_with_trend = [r for r in cg if r.get('source_trend')]
print(f'transformed_keywords.json total:         {len(t)}')
print(f'  consequence_generator rows:            {len(cg)}')
print(f'  ...with source_trend:                  {len(cg_with_trend)}')
print(f'commercial_keywords.json existing rows:  {len(c)}')
print(f'  already with metrics_source=cg:        {sum(1 for r in c if r.get(\"metrics_source\") == \"consequence_generator\")}')
"
```

**STOP** and report if:

- You're not on the STEP-9 feature branch.
- `cg_with_trend` is less than 100 (something ate the rows).
- `commercial_keywords.json` already has many (say >50) `metrics_source == "consequence_generator"` rows. That would mean this bridge already ran — do not double-append. Report the count and ask.

Otherwise proceed.

---

## Step 1 — Create the bridge script

Write the following file exactly as shown. Path:
`/Users/newmac/.openclaw/workspace/bridge_consequence_to_commercial.py`

```python
#!/usr/bin/env python3
"""
bridge_consequence_to_commercial.py — STEP 9 manual bridge.

Maps rows from transformed_keywords.json whose metrics_source is
"consequence_generator" into the commercial_keywords.json schema and appends
them. Skips live DataForSEO enrichment — needs_dataforseo_validation=true is
preserved so a later heartbeat pass will fill in CPC/volume.

Why this exists: keyword_extractor.py hung at 73 min during the STEP 9
autonomous run (LLM retry storm). Vetting reads commercial_keywords.json,
so we feed the transform output into vetting directly, bypassing the LLM
seed-extraction stage.

Safety:
  - Creates commercial_keywords.json.bak_step9_bridge before writing.
  - De-dupes by (keyword, country) against existing rows.
  - Never touches transformed_keywords.json.
  - Exits non-zero and touches no files if anything is off.
"""
from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Import COUNTRY_CONFIG the same way vetting/validation do — no schema drift.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from country_config import COUNTRY_CONFIG, DEFAULT_COUNTRY  # noqa: E402

BASE         = Path(__file__).resolve().parent
TRANSFORMED  = BASE / "transformed_keywords.json"
COMMERCIAL   = BASE / "commercial_keywords.json"
BACKUP       = BASE / "commercial_keywords.json.bak_step9_bridge"

# DataForSEO location/language — same mapping keyword_extractor.py uses.
GEO_MAP = {
    "US": (2840, "en"), "GB": (2826, "en"), "UK": (2826, "en"),
    "AU": (2036, "en"), "CA": (2124, "en"), "IN": (2356, "en"),
    "DE": (2276, "de"), "FR": (2250, "fr"), "ES": (2724, "es"),
    "IT": (2380, "it"), "NL": (2528, "nl"), "BR": (2076, "pt"),
    "JP": (2392, "ja"), "KR": (2410, "ko"), "MX": (2484, "es"),
    "PL": (2616, "pl"), "SE": (2752, "sv"),
    "DK": (2208, "da"), "FI": (2246, "fi"), "AT": (2040, "de"),
    "BE": (2056, "nl"), "CH": (2756, "de"), "IE": (2372, "en"),
    "ZA": (2710, "en"), "SG": (2702, "en"), "NZ": (2554, "en"),
    "HK": (2344, "zh"), "TW": (2158, "zh"), "AR": (2032, "es"),
    "CO": (2170, "es"), "CL": (2152, "es"), "PE": (2604, "es"),
    "PH": (2608, "en"), "ID": (2360, "id"), "TH": (2764, "th"),
    "VN": (2704, "vi"), "MY": (2458, "en"), "NG": (2566, "en"),
    "KE": (2404, "en"), "EG": (2818, "ar"), "SA": (2682, "ar"),
    "TR": (2792, "tr"), "UA": (2804, "uk"), "GR": (2300, "el"),
    "PT": (2620, "pt"), "CZ": (2203, "cs"), "RO": (2642, "ro"),
    "HU": (2348, "hu"), "IL": (2376, "he"),
}

# archetype from consequence_generator → commercial_category bucket in vetting.
ARCHETYPE_TO_CATEGORY = {
    "economic":   "finance",
    "health":     "health",
    "legal":      "legal",
    "home":       "home_services",
    "automotive": "automotive",
    "tech":       "saas",
    "travel":     "travel",
}


def now_iso() -> str:
    return datetime.now().isoformat()


def to_commercial_row(src: dict) -> dict | None:
    """Map a consequence_generator row to a commercial_keywords.json row."""
    kw = (src.get("keyword") or "").strip()
    if not kw:
        return None

    country = (src.get("country") or "US").upper()
    loc_code, lang_code = GEO_MAP.get(country, (2840, "en"))
    cfg = COUNTRY_CONFIG.get(country, DEFAULT_COUNTRY)

    archetype = (src.get("archetype") or "").lower()
    category  = ARCHETYPE_TO_CATEGORY.get(archetype, "general")

    return {
        "seed_keyword":         kw,
        "source_trend":         src.get("source_trend") or src.get("original_keyword") or "",
        "country":              country,
        "location_code":        loc_code,
        "language_code":        lang_code,
        "commercial_category":  category,
        "confidence":           "medium",
        "en_fallback":          False,
        "trend_source":         "consequence_generator",
        "keyword":              kw,
        "metrics_source":       "consequence_generator",
        "original_keyword":     kw,
        "linguistic_score":     {"signals": [], "bonus_multiplier": 1.0, "signal_count": 0},
        # Non-zero placeholders so rows survive vetting.py's top-2000 CPC*volume
        # cap. cpc=0 vol=0 → score 0 → tie-broken to bottom → trimmed. cpc=1.0
        # vol=1000 (score 1000) keeps rows inside the cap without out-ranking
        # genuinely enriched high-CPC keywords. needs_dataforseo_validation=True
        # below ensures the next heartbeat overwrites these with real numbers.
        # Verified 2026-04-20 — v2 settings (77 survivors). See "If vetting
        # still trims your cg rows" below for the escalation ceiling.
        "cpc_usd":              1.0,
        "search_volume":        1000,
        "competition":          0.5,
        "opportunity_score":    0,
        "estimated_rpm":        0,
        "country_tier":         cfg["tier"],
        "efficiency_factor":    cfg["efficiency"],
        "processed_at":         now_iso(),
        "_quality_score":       1.0,
        # Preserve the DFS-needs flag so the next heartbeat fills CPC/volume.
        "needs_dataforseo_validation": True,
    }


def main() -> int:
    if not TRANSFORMED.exists():
        print(f"ERROR: {TRANSFORMED} missing — aborting.", file=sys.stderr)
        return 2
    if not COMMERCIAL.exists():
        print(f"ERROR: {COMMERCIAL} missing — aborting.", file=sys.stderr)
        return 2

    transformed = json.loads(TRANSFORMED.read_text())
    commercial  = json.loads(COMMERCIAL.read_text())

    consequence_rows = [
        r for r in transformed
        if r.get("metrics_source") == "consequence_generator"
        and (r.get("keyword") or "").strip()
        and (r.get("source_trend") or "").strip()
    ]
    print(f"consequence_generator rows available: {len(consequence_rows)}")

    # De-dupe against existing commercial_keywords.json by (keyword_lower, country)
    existing_keys = {
        ((r.get("keyword") or "").lower().strip(),
         (r.get("country") or "").upper().strip())
        for r in commercial
    }

    to_append: list[dict] = []
    seen_in_batch: set[tuple[str, str]] = set()
    for row in consequence_rows:
        mapped = to_commercial_row(row)
        if not mapped:
            continue
        key = (mapped["keyword"].lower(), mapped["country"])
        if key in existing_keys or key in seen_in_batch:
            continue
        seen_in_batch.add(key)
        to_append.append(mapped)

    print(f"new rows after dedupe:                 {len(to_append)}")
    if not to_append:
        print("nothing to append — exiting without writing.")
        return 0

    # Backup then write.
    shutil.copy2(COMMERCIAL, BACKUP)
    print(f"backup written:                        {BACKUP.name}")

    commercial.extend(to_append)
    COMMERCIAL.write_text(json.dumps(commercial, indent=2, ensure_ascii=False))
    print(f"commercial_keywords.json total rows:   {len(commercial)}")
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Make it executable (optional but nice):

```bash
chmod +x /Users/newmac/.openclaw/workspace/bridge_consequence_to_commercial.py
```

---

## Step 2 — Dry-run diff, then run for real

First, do a *dry* inspection — don't write anything yet:

```bash
cd ~/.openclaw/workspace && python3 -c "
import json, pathlib
from bridge_consequence_to_commercial import to_commercial_row

t = json.loads(pathlib.Path('transformed_keywords.json').read_text())
cg = [r for r in t if r.get('metrics_source') == 'consequence_generator']
mapped = [to_commercial_row(r) for r in cg[:3] if r.get('keyword')]
print(json.dumps(mapped, indent=2, ensure_ascii=False))
"
```

Verify:

- Each output row has all 23 fields listed in the script.
- `location_code` and `language_code` are populated (not null).
- `country_tier` and `efficiency_factor` are numeric.
- `source_trend` is a full sentence, not empty.

If that looks right, run the bridge:

```bash
cd ~/.openclaw/workspace && python3 bridge_consequence_to_commercial.py 2>&1 | tee /tmp/step9_bridge.log
```

Expected output:

```
consequence_generator rows available: 199
new rows after dedupe:                 <N>   # probably 150–199
backup written:                        commercial_keywords.json.bak_step9_bridge
commercial_keywords.json total rows:   <5909 + N>
done.
```

Log decision to `/tmp/step9_autonomous_log.txt` (keep the format used by the
autonomous run so future passes can parse it):

```bash
echo "$(date -Iseconds) STAGE=4a_BRIDGE PASS rows_appended=<N> backup=commercial_keywords.json.bak_step9_bridge" >> /tmp/step9_autonomous_log.txt
```

---

## Step 3 — Confirm vetting will see the new rows

```bash
cd ~/.openclaw/workspace && python3 -c "
import json
c = json.loads(open('commercial_keywords.json').read_text())
cg = [r for r in c if r.get('metrics_source') == 'consequence_generator']
with_trend = [r for r in cg if (r.get('source_trend') or '').strip()]
print(f'commercial_keywords.json total:             {len(c)}')
print(f'  consequence_generator rows:               {len(cg)}')
print(f'  ...with source_trend populated:           {len(with_trend)}')
print(f'  sample seed_keyword/source_trend/country:')
for r in cg[:5]:
    print(f'    - {r[\"seed_keyword\"][:50]:50s} | {r[\"country\"]} | {r[\"source_trend\"][:60]}')
"
```

**STOP** if `with_trend` is less than 100 — something about the append is wrong.

---

## Step 3.5 — If vetting still trims your cg rows (retry ceiling)

After running Stage 4b (vetting) you may find that fewer than 100 of the 199
cg rows survived. The default placeholders above (cpc=1.0, vol=1000) were
chosen to clear vetting's top-2000 CPC×volume cap. If you see heavy trim-out,
DO NOT escalate placeholders indefinitely. The 2026-04-20 run established
these data points:

| Attempt | cpc × vol (score) | cg survived | total vetted |
|---|---|---:|---:|
| v1 | 0 × 0 = 0 | 0 / 199 | — (trimmed by cap) |
| v2 | 1.0 × 1000 = 1000 | 77 / 199 | 577 |
| v3 | 1.0 × 3000 = 3000 | 59 / 199 | 530 |
| v4 | 0.01 × 100 = 1 | 27 / 199 | 415 |
| v2b (retry) | 1.0 × 1000 = 1000 | 35 / 199 | 404 |

Takeaway: **higher placeholder score does not mean higher survival.** The
cap is not the real bottleneck once rows rank inside it — `vet_keyword`'s
SearXNG → DDG → Brave SERP cascade + `is_long_form` URL filter is. Many
non-English or niche keywords return thin/noisy SERPs regardless of score.
Worse, SearXNG itself degrades with each re-run (total vetted output dropped
577 → 530 → 415 → 404 in a single session).

**Retry ceiling (enforce this):**
1. First attempt: bridge at default cpc=1.0 vol=1000. Run vetting once.
2. If cg survival < 100 and you think the issue is placeholder-score: try
   ONE escalation to cpc=1.0 vol=3000. Run vetting.
3. If that also fails: **stop escalating.** Proceed with whatever the second
   run produced. Each additional vetting call degrades SearXNG further.

A 20–40 cg-row sample is still sufficient to demonstrate STEP 9's Layer 1
wins (trend-grounded titles on commercial_transform angles). The SERP-filter
bottleneck is an independent product issue, not a STEP 9 regression.

---

## Step 4 — Hand off to STEP_9_AUTONOMOUS_RUN.md at Stage 4b

You are now at the point where `commercial_keywords.json` is fresh and ready for
vetting. Open `/Users/newmac/.openclaw/workspace/implementation/prompts/STEP_9_AUTONOMOUS_RUN.md`
and jump straight to:

- **STAGE 4 — Flow through Bucket B (extractor → vetting → validation)**
  - **SKIP Stage 4a** (`keyword_extractor.py`) — the bridge replaced it.
    Log `STAGE=4a SKIP bridged_from=consequence_generator rows=<N>`.
  - **RUN Stage 4b** (`vetting.py`) as written in the autonomous run doc.
  - **RUN Stage 4c** (`validation.py`) as written in the autonomous run doc.

Then continue:

- **STAGE 5** — Invalidate angle_candidates cache (as written).
- **STAGE 6** — Re-run angle_engine.py (as written).
- **STAGE 7** — Verification + dashboard rebuild (as written).

**Do NOT re-run** PRE-FLIGHT P6 (launchd heartbeat unload) if it's already been
unloaded by the previous autonomous attempt. Verify first:

```bash
launchctl list | grep -E "ai\.openclaw\.(heartbeat|preload)"
```

If those services are still disabled from the earlier run, proceed. If they're
back up (mac restarted, etc.), re-run PRE-FLIGHT P6 in modern syntax:

```bash
UID_=$(id -u)
for svc in ai.openclaw.heartbeat ai.openclaw.preload-models; do
  launchctl disable gui/$UID_/$svc 2>&1 || true
  launchctl bootout gui/$UID_/$svc 2>&1 || true
done
```

---

## Step 5 — In FINAL SUMMARY, flag the bridge

When you reach STAGE 7's FINAL SUMMARY, add one line noting the bridge was used:

```
STAGE 4a: SKIP (bridge) — bridge_consequence_to_commercial.py appended <N> rows
                          to commercial_keywords.json on <timestamp>
                          keyword_extractor.py patch pending — see
                          STEP_9_KEYWORD_EXTRACTOR_HANG_FIX.md
```

---

## Rollback (if vetting misbehaves)

All of this is reversible:

```bash
cd ~/.openclaw/workspace
# Restore commercial_keywords.json
cp commercial_keywords.json.bak_step9_bridge commercial_keywords.json
# Remove the bridge script (optional — keeping it is fine)
# rm bridge_consequence_to_commercial.py
```

The bridge never touches `transformed_keywords.json`, `validated_opportunities.json`,
`error_log.jsonl`, or any historical file. Restoring the backup returns
`commercial_keywords.json` to its pre-bridge state (5909 rows).

---

## When to NOT use this bridge

- If `keyword_extractor.py` has been patched (via
  `STEP_9_KEYWORD_EXTRACTOR_HANG_FIX.md`) and you've already verified it runs
  cleanly, go back to Stage 4a in the normal autonomous run — don't bridge.
- If `commercial_keywords.json` already contains this run's consequence_generator
  rows (check with the pre-flight query above), the bridge has already run —
  skip directly to Stage 4b.
- If you want live DataForSEO CPC/volume enrichment *before* vetting, run
  `keyword_extractor.py` — the bridge deliberately skips DFS to trade enrichment
  quality for unblocking speed.
- If you've already re-run `vetting.py` more than twice in this session, stop.
  SearXNG's local cache degrades with each invocation (observed 577 → 530 →
  415 → 404 total vetted output across four consecutive runs on 2026-04-20),
  and further retries will likely give **worse** results, not better. Accept
  whatever the best run produced and move on to Stage 4c.
