# QA Harness — regression ledger

Every past QA finding has at least one test or monitor. This file is the index
a future developer uses when they see a suspicious log line and wonder "is
this a known shape or a brand-new one?"

## Run-2 findings (Apr 21, 2026)

Source: `PRODUCT_AUDIT_run2.md` + `QA_RUN_REPORT_run2.md`.
Fix branch: `fix/sprint-2-4-timeout-v3`.

| Finding | One-line symptom | Sprint | Regression test | Live monitor |
|---|---|:--:|---|---|
| **R2-C1** | `trends_postprocess` 3600s timeout; yesterday's trends shipped as today's. | 2 | `tests/test_regression_index.py::TestR2_C1_*`, `tests/test_pipeline_watchdog.py` | `pipeline_watchdog` alive-file SLA; `freshness_monitors.check_explosive_trends_freshness` |
| **R2-C2** | `money_flow_classifier` 900s budget kept burning. | 2 | `tests/test_regression_index.py::TestR2_C2_*` | watchdog (stage_alive SLA 180s) |
| **R2-C3** | `keyword_extractor` 80-min dead zone, zero stdout. | 2 | `tests/test_regression_index.py::TestR2_C3_*`, `tests/test_pipeline_watchdog.py` | watchdog (600s SLA) + per-batch alive touch |
| **R2-C4** | FB storage dropped 100% of new ads (`int.replace`). | 1 | `tests/test_fb_storage.py` (5 cases), `tests/test_regression_index.py::TestR2_C4_*` | `freshness_monitors.check_fb_intel_freshness` |
| **R2-C5** | FB `enrich_keywords` ran 13.5s, made 0 API calls. | 1 | `tests/test_regression_index.py::TestR2_C5_*` | Sprint-1 degraded-log branch `[enrich_keywords] degraded: reason=` |
| **R2-C6** | FB `article_analysis` 0 matches (downstream of C4/C5). | 1 | `tests/test_regression_index.py::TestR2_C6_*` | recovered when C4/C5 fire |
| **R2-C7** | Railway 500s for 96h, no operator alert. | 3 | `tests/test_alerts_and_monitors.py` (8 cases), `tests/test_regression_index.py::TestR2_C7_*` | `lib.alerts` + `config/sync.muted` switch + `ok→degraded` transition alert |
| **R2-C8** | Unscheduled heartbeat re-entry, no PPID attribution. | 3 | `tests/test_regression_index.py::TestR2_C8_*` | entry-point log line + `docs/heartbeat_triggers.md` |

## Supporting guard-rails from later sprints

- **Sprint 4 data-drift** — `tests/test_schema_and_taxonomy.py`. Catches the next
  `angle_candidates` Title-Case bifurcation and unknown reddit categories
  (`keyword_mention` was the last one to slip through). Sidecar schemas on
  every artifact so readers can assert version compatibility.
- **Sprint 5 product surface** — `tests/test_dashboard_enrich.py`. Pins the
  `US/AU/GB/CA/DE` pinned-chip order, since-last-run cutoff semantics, and
  the angle/reddit/fb_intel joins.

## How to add a new row

When a QA audit produces a new finding:

1. Write a test (unit or smoke) that reproduces the bug.
2. Land the fix on a new branch.
3. Add a row here pointing at the test and the fix commit SHA.
4. Link both in the PR description.

## Commit discipline

One logical change = one commit. Commit message format per
`fix_plan/00_ORCHESTRATOR.md` §9. Every commit references the QA finding ID
(e.g., `R2-C4`) so `git log --grep=R2-` becomes an operator timeline.

## Test budget

- **Unit layer** (`pytest tests/ --ignore=tests/smoke`): completes in < 10s.
  Runs on every PR (CI gate).
- **Smoke layer** (`pytest tests/smoke/`): Ollama-dependent, runs on the local
  M1 box only. Not required in CI.

## What's deliberately NOT here

- 100% code coverage.
- Front-end tests for the dashboard (scope in `fix_plan/00_ORCHESTRATOR.md` §8).
- Property-based or fuzz testing — overkill for this codebase's size.
- Rewriting tests as Claude moves — the sprint plan is the spec; deviation
  goes into the sprint file in the same commit.
