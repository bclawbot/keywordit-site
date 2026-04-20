# STEP 9 — Autonomous Run Prompt (Steps 2–7) — v2

> **Copy the fenced block below into Claude Code once. It runs Steps 2–7 of the playbook end-to-end with fail-fast checkpoints and a full log.**
>
> Prerequisites: STEP 9 Step 1 commits are landed (6d59018, 2860ed5, 3f47e4c on `fix/sprint-2-4-timeout-v3`). Baseline from §2 captured to `implementation/prompts/_baseline_step9.txt`.
>
> Expected elapsed time: 2–4 hours, mostly waiting. Safe to leave unattended.
>
> Reference: `implementation/prompts/STEP_9_PLAYBOOK.md`
>
> **What changed in v2 vs v1:**
> 1. Explicit background-execution pattern (Bash tool has a 10-min foreground cap — long stages MUST use `run_in_background: true` + Monitor).
> 2. Working-directory anchor, branch check, and explicit guidance on Claude Code's fresh-shell-per-Bash-call behaviour (caffeinate PID via tempfile).
> 3. Ollama pre-warm check before Stage 6 and again at Stage 6 start (Stage 4 can evict the model).
> 4. `caffeinate -dimsu` to prevent Mac sleep during the run.
> 5. Launchd heartbeat/preload unload in pre-flight, reload in Stage 7 — prevents concurrent `angle_candidates.json` writes.
> 6. Freshness check on `validated_opportunities.json` uses file **mtime**, not a row-level timestamp (no `validated_at`/`vetted_at` coupling to this run).
> 7. `error_log.jsonl` tail between stages.
> 8. SKIP_LLM regression threshold compared to STEP 9 smoke baseline (~11).
> 9. Resume-from-stage guidance.

---

```
Autonomous end-to-end run of STEP 9 Playbook Steps 2–7. Run all stages in
sequence. Each stage has a fail-fast checkpoint — if a checkpoint fails, STOP,
write the failure reason to the run log, and do NOT proceed to later stages.

HARD RULES (from CLAUDE.md — do not violate):
- Do NOT run heartbeat.py at any point (Rule 5).
- Do NOT touch trends_all_history.jsonl or explosive_trends_history.jsonl (Rule 4).
- Do NOT re-enable Telegram in the gateway (Rule 2).
- Do NOT push. Do NOT commit anything unless a stage explicitly says to.
- Do NOT use backtick command substitution. Always $(cmd) (Rule 3).

EXECUTION RULES (Claude Code specific):
- The Bash tool foreground timeout is 10 minutes. Any command expected to take
  longer than ~8 minutes MUST be launched with run_in_background: true and then
  streamed via the Monitor tool until the process exits. Apply this pattern to:
    Stage 2a (trends_scraper)      — up to 15 min
    Stage 3  (consequence_generator) — up to 60 min
    Stage 4a (keyword_extractor)    — up to 30 min
    Stage 4b (vetting)              — up to 30 min
    Stage 4c (validation)           — up to 30 min
    Stage 6  (angle_engine)         — up to 2h 30min

  Short commands (snapshots, checkpoints, grep, small Python one-liners) run
  foreground.

- IMPORTANT: Claude Code's Bash tool opens a fresh shell per call. `cd`
  (working directory) persists across calls, but shell-local state —
  exported env vars, $!, activated venvs — does NOT. Mitigations:

  (a) Python: use plain `python3` as-is throughout this prompt. The
      operator's Mac already has `python3` on PATH with all pipeline
      deps available (verified during STEP 9 Step 1 smoke). If P4
      (test pass) fails with ImportError, THEN and only then fall back
      to activating a venv — the fix at that point is:
        source "$HOME/.openclaw/venv/bin/activate" && python3 -m pytest ...
      in the same Bash call.

  (b) caffeinate: launching in background with $! only works within the
      same Bash call. Write the PID to a tempfile so Stage 7's cleanup
      Bash call can read it:
        caffeinate -dimsu &
        echo $! > /tmp/step9auto_caffeinate.pid

  (c) Working dir: re-run `cd "$(git rev-parse --show-toplevel)"` at the
      start of every Bash call that does real work — cheap insurance,
      and auto-corrects if something accidentally cd'd into a subdir.

PROGRESS LOG:
Write all stage progress and checkpoint results to:
  implementation/prompts/_autonomous_run_step9.log

Format every log line as:
  [YYYY-MM-DD HH:MM:SS] STAGE=<N> <EVENT> — <detail>

At the end (or on failure) append a SUMMARY block with the full outcome table.

RESUME SUPPORT:
If a previous autonomous run stopped mid-way and you're resuming, you may
pass RESUME_FROM=<N> (e.g. RESUME_FROM=4) and skip stages below N. The
PRE-FLIGHT block still runs. Default: RESUME_FROM=2 (run everything).

====================================================================
PRE-FLIGHT (always runs, even if resuming)
====================================================================

P0. Environment setup (do these two commands in ONE Bash call before
    anything else so $! resolves correctly for caffeinate):
      cd "$(git rev-parse --show-toplevel)"
      python3 -c "import sys; print('python:', sys.executable)"
      caffeinate -dimsu &
      echo $! > /tmp/step9auto_caffeinate.pid
      echo "caffeinate pid=$(cat /tmp/step9auto_caffeinate.pid)"

P1. Working directory + branch:
      cd "$(git rev-parse --show-toplevel)" && pwd
      git rev-parse --abbrev-ref HEAD
    Expect: repo root path; branch = fix/sprint-2-4-timeout-v3
    If branch differs, STOP and log PREFLIGHT_FAIL.

P2. Confirm the three STEP 9 commits are at HEAD:
      git log --oneline -6
    Expect (in this order at the top):
      3f47e4c angle_engine: drop fb_intel competitor angles with 'best' in title
      2860ed5 STEP 9: trend-grounding fixes + compliance 'best' removal
      6d59018 angle_engine: prefer article_content source over ad creative in fb_intel matching
    If any are missing, STOP and log PREFLIGHT_FAIL.

P3. Confirm baseline exists:
      ls -la implementation/prompts/_baseline_step9.txt
    If missing, STOP and log PREFLIGHT_FAIL — baseline needed for Stage 7 diff.

P4. Confirm tests pass:
      python3 -m pytest tests/test_best_banned_in_titles.py -q
    Expect 3 passed. If not, STOP and log PREFLIGHT_FAIL.

P5. Ollama health + model pre-warm:
      curl -sf http://localhost:11434/api/tags | python3 -c "import sys,json; \
        tags = json.load(sys.stdin)['models']; \
        names = {t['name'].split(':')[0]+':'+t['name'].split(':')[1] \
                 if ':' in t['name'] else t['name'] for t in tags}; \
        print('loaded tags:', sorted(names)); \
        assert any('qwen3:14b' in n for n in names), 'qwen3:14b NOT PULLED'"
      # Warm it so the first real call doesn't eat 60s of model load:
      curl -s -X POST http://localhost:11434/api/generate \
        -H 'Content-Type: application/json' \
        -d '{"model":"qwen3:14b","prompt":"hi","stream":false,
             "options":{"num_predict":1,"num_ctx":32768}}' \
        -o /tmp/step9auto_ollama_warm.json --max-time 120
      python3 -c "import json; d=json.load(open('/tmp/step9auto_ollama_warm.json')); \
        print('warm ok, load_duration_ms=', d.get('load_duration',0)//1_000_000)"
    If any of this fails, STOP and log PREFLIGHT_FAIL — Ollama not ready.

P6. Unload launchd heartbeat + preload so they don't collide mid-run.
    macOS 15 / Darwin 24.x requires the modern `launchctl disable + bootout`
    sequence — the legacy `launchctl unload` is permission-denied from an
    unprivileged shell. Use modern syntax first; fall back only on older macOS.

    Modern (macOS 11+; what you almost certainly want):
      UID_=$(id -u)
      for svc in ai.openclaw.heartbeat ai.openclaw.preload-models; do
          launchctl disable "gui/$UID_/$svc" 2>&1 | head -1
          launchctl bootout  "gui/$UID_/$svc" 2>&1 | head -1
          echo "processed $svc"
      done

    Legacy fallback (macOS 10.x, kept for reference — DO NOT USE on 11+):
      # for svc in ai.openclaw.heartbeat ai.openclaw.preload-models; do
      #     launchctl list | grep -q "$svc" && \
      #         launchctl unload "$HOME/Library/LaunchAgents/$svc.plist" 2>/dev/null \
      #         && echo "unloaded $svc" || echo "$svc not loaded (ok)"
      # done

    Note in the log which services were disabled+booted-out so Stage 7 can
    re-enable + bootstrap them. The bot (ai.openclaw.dwight-bot) and gateway
    (ai.openclaw.gateway) stay up.

    If modern syntax is ALSO permission-denied (sandboxed macOS variants):
    check the heartbeat's next firing time via the plist's StartCalendarInterval.
    If firing is >4h away AND the run will finish before then, log
    P6_SKIPPED_SAFE. Otherwise stop and ask the operator — running without
    unload-equivalent risks a Stage 1b rewrite wiping your money-flow enrichment.

P7. Capture "before autonomous run" snapshot:
      python3 - <<'PY' > implementation/prompts/_autonomous_before.txt
      import json, collections, re
      from pathlib import Path
      ac = json.loads(Path("angle_candidates.json").read_text())
      records = ac if isinstance(ac, list) else list(ac.values())
      by_sig = collections.defaultdict(lambda: {"total": 0, "with_trend": 0})
      for r in records:
          sig = (r.get("discovery_context") or {}).get("signal_type", "unknown")
          by_sig[sig]["total"] += 1
          if (r.get("source_trend") or "").strip():
              by_sig[sig]["with_trend"] += 1
      for sig, c in sorted(by_sig.items(), key=lambda x: -x[1]["total"]):
          pct = 100 * c["with_trend"] / max(c["total"], 1)
          print(f"{sig:25s}  {c['with_trend']:6d} / {c['total']:6d}  ({pct:5.1f}%)")
      rx = re.compile(r"\bbest\b", re.I)
      total = sum(len(r.get("selected_angles") or []) for r in records)
      hits = sum(1 for r in records for a in (r.get("selected_angles") or [])
                 if rx.search(a.get("angle_title", "")))
      print(f"\nTitles with standalone 'best': {hits} / {total}")
      PY

P8. Note error_log.jsonl starting line count (for between-stage deltas):
      wc -l error_log.jsonl 2>/dev/null | awk '{print $1}' > /tmp/step9auto_errlog_start.txt
      cat /tmp/step9auto_errlog_start.txt

====================================================================
STAGE 2 — Refresh trends if stale
====================================================================

1. Check mtime of explosive_trends.json:
     python3 -c "import os, time; age = (time.time() - os.path.getmtime('explosive_trends.json'))/3600; print(f'{age:.1f}h old')"

2. If age < 24h: log STAGE=2 SKIP — fresh enough. Proceed to Stage 3.

3. If age >= 24h OR file missing, run in background:
     a. Launch in background:
           python3 trends_scraper.py > /tmp/step9auto_stage2a.log 2>&1
        with run_in_background: true. Then Monitor until exit.
        Expected runtime ~5–12 min. Hard ceiling 15 min.
     b. Foreground (short):
           python3 trends_postprocess.py 2>&1 | tee /tmp/step9auto_stage2b.log
     c. Verify freshness:
           ls -la explosive_trends.json
        mtime must be within the last 2 min.

Between-stage error check:
     end=$(wc -l < error_log.jsonl 2>/dev/null || echo 0)
     start=$(cat /tmp/step9auto_errlog_start.txt)
     new=$((end - start))
     echo "STAGE 2 added $new error_log lines"
     if [ "$new" -gt 0 ]; then
         tail -n "$new" error_log.jsonl | head -20
     fi

CHECKPOINT 2:
- explosive_trends.json exists AND (was already fresh < 24h OR was just
  rewritten by 2a+2b) → PASS
- Stage 2a or 2b exited non-zero / Traceback at tail of log → FAIL, STOP
- After running 2a+2b, explosive_trends.json mtime NOT within last 2 min
  → FAIL (scraper ran but postprocess produced no file), STOP

====================================================================
STAGE 3 — Run consequence_generator.py
====================================================================

1. Before-snapshot (record file size and row count for baseline, plus mtime
   to use as a safer "new row" proxy than fetched_at field):
     python3 - <<'PY' | tee -a implementation/prompts/_autonomous_run_step9.log
     import json, os
     from pathlib import Path
     p = Path("transformed_keywords.json")
     rows = json.loads(p.read_text()) if p.exists() else []
     cg = [r for r in rows if r.get("metrics_source") == "consequence_generator"]
     wt = [r for r in cg if (r.get("source_trend") or "").strip()]
     mtime_before = os.path.getmtime(p) if p.exists() else 0
     Path("/tmp/step9auto_tk_mtime_before.txt").write_text(str(mtime_before))
     print(f"BEFORE: {len(cg)} cg rows, {len(wt)} with source_trend, mtime={mtime_before}")
     PY

2. Run in background:
     python3 consequence_generator.py > /tmp/step9auto_stage3.log 2>&1
   with run_in_background: true. Expected runtime 20–50 min. Hard ceiling 60 min.

3. After-snapshot — count NEW rows by comparing fetched_at against the run's
   own start time (which we captured implicitly by mtime_before); use file
   mtime fallback if fetched_at is not present on any row:
     python3 - <<'PY' | tee -a implementation/prompts/_autonomous_run_step9.log
     import json, os, datetime
     from pathlib import Path
     mtime_before = float(Path("/tmp/step9auto_tk_mtime_before.txt").read_text())
     cutoff = datetime.datetime.fromtimestamp(mtime_before).isoformat()
     rows = json.loads(Path("transformed_keywords.json").read_text())
     cg = [r for r in rows if r.get("metrics_source") == "consequence_generator"]
     wt = [r for r in cg if (r.get("source_trend") or "").strip()]
     # NEW = rows with fetched_at after pre-run snapshot
     new_rows = [r for r in cg if r.get("fetched_at", "") > cutoff]
     new_with_trend = [r for r in new_rows if (r.get("source_trend") or "").strip()]
     print(f"AFTER: {len(cg)} cg rows, {len(wt)} with source_trend")
     print(f"NEW (fetched_at > {cutoff}): {len(new_rows)} rows, "
           f"{len(new_with_trend)} with source_trend")
     Path("/tmp/step9auto_new_count.txt").write_text(str(len(new_with_trend)))
     PY

4. error_log delta:
     end=$(wc -l < error_log.jsonl 2>/dev/null || echo 0)
     start=$(cat /tmp/step9auto_errlog_start.txt)
     new=$((end - start))
     echo "cumulative error_log delta through stage 3: $new"

CHECKPOINT 3:
- NEW rows > 0 AND NEW rows with source_trend > 0 → PASS
- NEW rows > 0 AND NEW rows with source_trend == 0 → FAIL (STEP 9 §3.1
  regressed: consequence_generator is not populating source_trend on new
  output). Log STAGE=3 FAIL_REGRESSION with first 3 new rows dumped and STOP.
- NEW rows == 0 → SOFT_FAIL (no new consequences generated; classifier
  found nothing actionable in explosive_trends.json). Log STAGE=3
  SOFT_FAIL and STOP — downstream stages have no fresh input.

====================================================================
STAGE 4 — Flow through Bucket B (extractor → vetting → validation)
====================================================================

For each sub-stage, run in background, stream with Monitor, then check for
fatal traceback. Brave 429s in vetting.py are NOT fatal — ignore them per
CLAUDE.md prior fixes.

4a. python3 keyword_extractor.py > /tmp/step9auto_stage4a.log 2>&1
    (background; ceiling 30 min)

4b. python3 vetting.py > /tmp/step9auto_stage4b.log 2>&1
    (background; ceiling 30 min; 429s in log are fine)

4c. python3 validation.py > /tmp/step9auto_stage4c.log 2>&1
    (background; ceiling 30 min)

After each sub-stage, fatal check:
     tail -30 /tmp/step9auto_stage4X.log
     grep -q "^Traceback" /tmp/step9auto_stage4X.log && echo FATAL || echo OK

After all three complete, freshness check on validated_opportunities.json
using mtime (NOT vetted_at — vetted_at tracks the ORIGINAL vetting event,
not this run):
     python3 - <<'PY' | tee -a implementation/prompts/_autonomous_run_step9.log
     import json, os, datetime
     from pathlib import Path
     p = Path("validated_opportunities.json")
     if not p.exists():
         print("validated_opportunities.json: NOT FOUND — Stage 4 did not produce output")
         Path("/tmp/step9auto_validated_new.txt").write_text("0")
         raise SystemExit(0)
     stat = p.stat()
     age_min = (datetime.datetime.now().timestamp() - stat.st_mtime) / 60
     data = json.loads(p.read_text())
     items = data if isinstance(data, list) else list(data.values())
     # Count rows ultimately sourced from consequence_generator that carry source_trend.
     # IMPORTANT: validation.py OVERWRITES metrics_source to "google_keyword_planner"
     # during DFS fallback; the preserved identifier is trend_source. Check all four
     # fields — missing trend_source gave a false-zero SOFT_FAIL on 2026-04-20.
     ct = [r for r in items
           if (r.get("metrics_source") == "consequence_generator"
               or r.get("source") == "consequence_generator"
               or r.get("data_source") == "consequence_generator"
               or r.get("trend_source") == "consequence_generator")
           and (r.get("source_trend") or "").strip()]
     print(f"validated_opportunities.json mtime age: {age_min:.1f} min")
     print(f"total items: {len(items)}, consequence-transform rows with source_trend: {len(ct)}")
     Path("/tmp/step9auto_validated_new.txt").write_text(str(len(ct)))
     PY

CHECKPOINT 4:
- No Traceback in any sub-stage log AND mtime < 60 min AND ct_with_trend > 0
  → PASS
- ct_with_trend == 0 → SOFT_FAIL (consequence_generator rows got filtered
  out by vetting or validation). Log STAGE=4 SOFT_FAIL with tail of each
  sub-stage log and STOP.
- mtime > 60 min → the file didn't rewrite, meaning validation produced
  no new output → SOFT_FAIL, STOP.

====================================================================
STAGE 5 — Invalidate angle_candidates cache
====================================================================

1. Back up:
     cp angle_candidates.json angle_candidates.json.bak_step9_round2

2. Identify keyword|country pairs to invalidate: rows in
   validated_opportunities.json that (a) came from consequence_generator
   AND (b) have a source_trend AND (c) were refreshed in the last few
   hours (mtime within 4h):
     python3 - <<'PY' | tee -a implementation/prompts/_autonomous_run_step9.log
     import json, os, time
     from pathlib import Path
     vo = Path("validated_opportunities.json")
     if not vo.exists():
         print("validated_opportunities.json missing; nothing to invalidate")
         Path("/tmp/step9auto_invalidated.txt").write_text("0")
         raise SystemExit(0)
     # Only do this if vo was refreshed recently, else we'd blow away rows
     # that Stage 4 didn't actually touch this run.
     age_h = (time.time() - vo.stat().st_mtime) / 3600
     if age_h > 6:
         print(f"vo is {age_h:.1f}h old; skipping invalidation (not from this run)")
         Path("/tmp/step9auto_invalidated.txt").write_text("0")
         raise SystemExit(0)
     data = json.loads(vo.read_text())
     items = data if isinstance(data, list) else list(data.values())
     target_keys = set()
     for r in items:
         is_cg = (r.get("metrics_source") == "consequence_generator"
                  or r.get("source") == "consequence_generator"
                  or r.get("data_source") == "consequence_generator")
         has_trend = bool((r.get("source_trend") or "").strip())
         if is_cg and has_trend:
             k = (str(r.get("keyword", "")).lower().strip(),
                  str(r.get("country", "")).upper().strip())
             if k[0]:
                 target_keys.add(k)
     print(f"Invalidation targets: {len(target_keys)} keyword|country pairs")

     ac = json.loads(Path("angle_candidates.json").read_text())
     if isinstance(ac, list):
         before = len(ac)
         ac = [r for r in ac
               if (str(r.get("keyword", "")).lower().strip(),
                   str(r.get("country", "")).upper().strip()) not in target_keys]
         Path("angle_candidates.json").write_text(json.dumps(ac, indent=2))
         removed = before - len(ac)
     else:
         before = len(ac)
         ac = {k: v for k, v in ac.items()
               if (str(v.get("keyword", "")).lower().strip(),
                   str(v.get("country", "")).upper().strip()) not in target_keys}
         Path("angle_candidates.json").write_text(json.dumps(ac, indent=2))
         removed = before - len(ac)
     print(f"angle_candidates.json: {before} -> {before - removed}, removed {removed}")
     Path("/tmp/step9auto_invalidated.txt").write_text(str(removed))
     PY

CHECKPOINT 5:
- removed > 0 → PASS
- removed == 0 → SOFT_PASS (targets weren't cached; angle_engine will
  generate them fresh anyway). Proceed to Stage 6.

====================================================================
STAGE 6 — Re-run angle_engine.py
====================================================================

1. Re-confirm Ollama is still warm (re-run P5 warm curl; ceiling 30s).

2. Launch in background:
     python3 angle_engine.py > /tmp/step9auto_stage6.stdout.log \
                              2> /tmp/step9auto_stage6.stderr.log
   with run_in_background: true. Expected runtime 1h 30min – 2h 30min.
   Hard ceiling 2h 45min (9900s).

3. Summarize:
     python3 - <<'PY' | tee -a implementation/prompts/_autonomous_run_step9.log
     import re
     from pathlib import Path
     stdout = Path("/tmp/step9auto_stage6.stdout.log").read_text()
     stderr = Path("/tmp/step9auto_stage6.stderr.log").read_text()
     m = re.search(r"\[angle_engine\] (\d+) new / (\d+) cached", stdout)
     new_count = m.group(1) if m else "?"
     cached_count = m.group(2) if m else "?"
     skip_llm = stderr.count("SKIP_LLM") + stdout.count("SKIP_LLM")
     fb_dropped = stdout.count("fb_intel angle(s) for 'best'")
     errors = len([l for l in stdout.splitlines() if "ERROR" in l])
     tracebacks = stdout.count("Traceback") + stderr.count("Traceback")
     # Ratio, not count — SKIP_LLM count scales linearly with batch size, so an
     # absolute threshold flags every production-size batch as a regression.
     # The gate is "what fraction of fresh records were skipped?" A healthy
     # run sees ~50-80% skips because keyword_expansion + commercial_intent
     # signals legitimately have no source_trend after STEP 9 §3.3.
     skip_ratio = (skip_llm / int(new_count)) if str(new_count).isdigit() and int(new_count) > 0 else 0.0
     print(f"STAGE 6 summary:")
     print(f"  new / cached: {new_count} / {cached_count}")
     print(f"  SKIP_LLM events: {skip_llm}  (ratio: {skip_ratio:.2f})")
     print(f"  fb_intel best-drops: {fb_dropped}")
     print(f"  ERROR lines: {errors}")
     print(f"  Tracebacks: {tracebacks}")
     PY

CHECKPOINT 6:
- angle_engine completed (not timed out) AND Tracebacks == 0 AND
  ERROR lines < 20 AND skip_ratio < 0.90  → PASS
- Hard-ceiling timeout hit → FAIL, log STAGE=6 TIMEOUT, STOP.
- Tracebacks > 0 → FAIL, log first traceback and STOP.
- skip_ratio >= 0.90 AND new_count > 10 → SOFT_FAIL (more than 90% of fresh
  records skipped LLM; suggests source_trend threading broke upstream and
  the signal-type gate is firing on everything). Log, STOP.
- ERROR lines >= 20 → SOFT_FAIL. Log first 10, STOP.
- Diagnostic: if skip_llm count ≈ records-with-empty-source_trend count, the
  gate is working as designed (high ratio is expected for keyword_expansion-
  heavy batches) — not a regression. Verify by cross-checking the fresh-set
  source_trend distribution before declaring SOFT_FAIL.

====================================================================
STAGE 7 — Verification + dashboard rebuild
====================================================================

1. Write after-run snapshot (same shape as before):
     python3 - <<'PY' > implementation/prompts/_autonomous_after.txt
     import json, collections, re
     from pathlib import Path
     ac = json.loads(Path("angle_candidates.json").read_text())
     records = ac if isinstance(ac, list) else list(ac.values())
     by_sig = collections.defaultdict(lambda: {"total": 0, "with_trend": 0})
     for r in records:
         sig = (r.get("discovery_context") or {}).get("signal_type", "unknown")
         by_sig[sig]["total"] += 1
         if (r.get("source_trend") or "").strip():
             by_sig[sig]["with_trend"] += 1
     for sig, c in sorted(by_sig.items(), key=lambda x: -x[1]["total"]):
         pct = 100 * c["with_trend"] / max(c["total"], 1)
         print(f"{sig:25s}  {c['with_trend']:6d} / {c['total']:6d}  ({pct:5.1f}%)")
     rx = re.compile(r"\bbest\b", re.I)
     total = sum(len(r.get("selected_angles") or []) for r in records)
     hits = sum(1 for r in records for a in (r.get("selected_angles") or [])
                if rx.search(a.get("angle_title", "")))
     print(f"\nTitles with standalone 'best': {hits} / {total}")
     PY

2. Compare:
     echo '=== DIFF vs pre-STEP 9 baseline ==='
     diff implementation/prompts/_baseline_step9.txt \
          implementation/prompts/_autonomous_after.txt || true
     echo
     echo '=== DIFF vs before-autonomous snapshot ==='
     diff implementation/prompts/_autonomous_before.txt \
          implementation/prompts/_autonomous_after.txt || true

3. Sample 10 fresh commercial_transform titles:
     python3 - <<'PY' > implementation/prompts/_autonomous_samples.txt
     import json, random
     from pathlib import Path
     ac = json.loads(Path("angle_candidates.json").read_text())
     recs = ac if isinstance(ac, list) else list(ac.values())
     ct = [r for r in recs
           if (r.get("discovery_context") or {}).get("signal_type") == "commercial_transform"
           and (r.get("source_trend") or "").strip()]
     random.seed(42)
     sample = random.sample(ct, min(10, len(ct)))
     for r in sample:
         print(f'\nkeyword={r.get("keyword","?")}  country={r.get("country","?")}')
         print(f'  source_trend: {(r.get("source_trend","") or "")[:100]}')
         for a in (r.get("selected_angles") or [])[:2]:
             print(f'    [{a.get("angle_type","?"):25s}] {a.get("angle_title","")}')
     PY

4. Rebuild dashboard (foreground, ~1–2 min, non-fatal):
     python3 dashboard_builder.py 2>&1 | tee /tmp/step9auto_stage7_dashboard.log

5. Reload launchd services we unloaded in P6:
     for svc in ai.openclaw.heartbeat ai.openclaw.preload-models; do
         test -f "$HOME/Library/LaunchAgents/$svc.plist" && \
             launchctl load "$HOME/Library/LaunchAgents/$svc.plist" && \
             echo "loaded $svc" || echo "$svc plist missing, skipped"
     done

6. Kill caffeinate (PID was written to /tmp in pre-flight):
     if [ -f /tmp/step9auto_caffeinate.pid ]; then
         kill "$(cat /tmp/step9auto_caffeinate.pid)" 2>/dev/null \
             && echo "caffeinate stopped" \
             || echo "caffeinate already gone"
         rm -f /tmp/step9auto_caffeinate.pid
     fi
     # Belt-and-suspenders:
     pkill -u "$USER" caffeinate 2>/dev/null || true

CHECKPOINT 7:
- after snapshot written → PASS (measurement-only stage).

====================================================================
FINAL SUMMARY
====================================================================

Append to implementation/prompts/_autonomous_run_step9.log and paste into
the reply:

  ====================================================================
  STEP 9 AUTONOMOUS RUN — SUMMARY
  ====================================================================
  Started:  <preflight timestamp>
  Finished: <now timestamp>
  Branch:   <git rev-parse --abbrev-ref HEAD>
  Head:     <git rev-parse HEAD>

  Stage outcomes:
    STAGE 2: <PASS|SKIP|FAIL>      detail
    STAGE 3: <PASS|FAIL|SOFT_FAIL> N new rows, M with source_trend
    STAGE 4: <PASS|SOFT_FAIL>      N consequence rows in validated, with trend
    STAGE 5: <PASS|SOFT_PASS>      N keywords invalidated
    STAGE 6: <PASS|FAIL|SOFT_FAIL> N angles regenerated; SKIP_LLM=X; fb_drops=Y
    STAGE 7: <PASS>                measurement only

  Key metrics:
    'best' standalone titles — pre-STEP 9: <_baseline_step9.txt>
                                post-run:  <_autonomous_after.txt>
    commercial_transform coverage before run: X.X%
    commercial_transform coverage after run:  Y.Y%
    error_log.jsonl delta during run: N lines

  Artifacts:
    implementation/prompts/_autonomous_before.txt
    implementation/prompts/_autonomous_after.txt
    implementation/prompts/_autonomous_samples.txt
    implementation/prompts/_autonomous_run_step9.log
    angle_candidates.json.bak_step9_round2  (rollback point)

  Next action suggested:
    <e.g. "review _autonomous_samples.txt — all 10 titles reference their
    source_trend, commit round-2 artifacts"
    OR
    "STAGE 3 SOFT_FAIL: explosive_trends.json had no classifiable items;
    rerun once next heartbeat produces new trends">

====================================================================
ROLLBACK (if anything went fatal mid-run)
====================================================================

Commits from Step 1 are safe; do NOT revert them.

If angle_candidates.json looks corrupted:
    cp angle_candidates.json.bak_step9_round2 angle_candidates.json
(restores the pre-Stage-6 state of this autonomous run)
    -- OR --
    cp angle_candidates.json.bak_step9 angle_candidates.json
(nuclear: all the way back to pre-STEP 9 smoke state)

transformed_keywords.json rows added by consequence_generator are additive;
leave them — they'll just become cached input for future runs.

Do NOT force-delete anything under data/ or any *_history.jsonl files.

If Stage 6 timed out: partial angle_candidates.json has a mix of old+new.
Next manual `python3 angle_engine.py` run will finish the remaining keys.
Not a rollback situation.

If you stopped at a soft-fail checkpoint, you can resume from the next
stage by setting RESUME_FROM=<next stage> on the re-run prompt.

====================================================================
BEGIN
====================================================================

Start with PRE-FLIGHT. Proceed stage by stage. Log everything.
If ANY checkpoint logs FAIL or SOFT_FAIL, STOP and report — do not
try to self-heal. If you hit a tool-timeout on a long-running stage,
that is a bug in this prompt — note it and STOP.
```

---

## What to expect when it finishes

A successful run leaves you with:

- `implementation/prompts/_autonomous_before.txt` — snapshot at start
- `implementation/prompts/_autonomous_after.txt` — snapshot at end
- `implementation/prompts/_autonomous_samples.txt` — 10-title eyeball sample
- `implementation/prompts/_autonomous_run_step9.log` — full timestamped log
- `angle_candidates.json.bak_step9_round2` — restore point

Claude Code will paste the SUMMARY block into its reply. Look for:

- `commercial_transform coverage` jumped from ~6% baseline to meaningfully higher (target 20–50% aggregate, higher on newly-processed rows; historical rows drag the average down)
- `Titles with 'best'` is 0 or very close to 0
- No FAIL or SOFT_FAIL on stages 3, 4, 6 (stages 2 and 5 can legitimately SKIP/SOFT_PASS)
- 10 sample titles visibly reference their `source_trend` instead of formulaic "A Closer Look at X" patterns

## When to use this vs step-by-step

Use this autonomous prompt when:
- You have a 2–4 hour window where you can leave your laptop alone
- The code changes have been smoke-verified (they have — STEP 9 Step 1 confirmed)
- You want to see the full delta in one shot

Use step-by-step (the original playbook) when:
- It's the first production run and you want eyes on each checkpoint
- You want to bail early if Stage 3 shows weak lift
- You're debugging a specific stage
