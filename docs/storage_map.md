# Storage Map

Five stores power the pipeline. They are intentionally not consolidated — the
cost of a migration exceeds the cost of documenting where each one lives.
See orchestrator §8 for the anti-scope rationale.

## 1. JSON artifacts — `~/.openclaw/workspace/*.json`
Canonical pipeline I/O. Written by stages, read by `dashboard_builder.py`
and (when enabled) `sync_to_backend.py`. Every artifact has a matching
`<name>.schema.json` sidecar from Sprint 4 — see `lib/schema_version.py`
for the authoritative `ARTIFACT_SCHEMAS` map (name → version, source stage).

| Artifact | Writer | Readers |
|---|---|---|
| `latest_trends.json` | `trends_scraper.py` | `trends_postprocess.py` |
| `explosive_trends.json` | `trends_postprocess.py`, `money_flow_classifier.py` | `keyword_expander.py`, `keyword_extractor.py`, `vetting.py`, dashboard |
| `expanded_keywords.json` | `keyword_expander.py` | `commercial_keyword_transformer.py` |
| `transformed_keywords.json` | `commercial_keyword_transformer.py`, `consequence_generator.py` | `keyword_extractor.py` |
| `commercial_keywords.json` | `keyword_extractor.py`, `intel_bridge.py` | `vetting.py` |
| `vetted_opportunities.json` | `vetting.py` | `validation.py` |
| `validated_opportunities.json` | `validation.py` | dashboard, sync |
| `golden_opportunities.json` | `validation.py` | dashboard, sync |
| `angle_candidates.json` | `angle_engine.py` | dashboard, sync |
| `reddit_intelligence.json` | `reddit_intelligence.py` | `keyword_extractor.py`, dashboard |
| `signal_weights.json` | `reflection.py` | `trends_postprocess.py` (CPC gate), `country_config.py` (min_cpc) |

**On `signal_weights.json`:** the Apr 21 product audit flagged this as an
orphan. It is not. `trends_postprocess.load_signal_weights()` reads it on
every run, and `country_config.load_country_config()` reads it to derive the
dynamic per-country `min_cpc`. Keeping it because both readers are live and
used by the pipeline we ship.

## 2. SQLite — `dwight/fb_intelligence/data/fb_intelligence.db`
FB Ad Library scrapes. Owned by `dwight/fb_intelligence/storage.py`. Read
by `dwight/fb_intelligence/analyzer.py`, `api_client.py`, and the Intel tab.
Does not participate in the 10-stage keyword pipeline.

## 3. LanceDB — `~/.openclaw/vector_db/`
Trend embeddings + dedup cache. Owned by `vector_store.py`. Only
`trends_postprocess.py` writes; `trends_postprocess.py` and (optionally)
`validation.py` read.

## 4. Logs — `~/.openclaw/logs/`
Forensics. Not pipeline data. See `docs/operator_switches.md` for the files
the operator actually toggles (`stale/`, `.alive`, `alerts.jsonl`,
`.sync_last_status.json`).

## 5. Oracle SQLite — `~/.openclaw/oracle.db`
Dwight-bot long-term memory via the `oracle` skill. Separate from pipeline
state; documented here only so the count reads as five.

## SQLite/JSON mismatch warning
`dashboard_builder.py` computes `SQLite vs current-run JSON` and warns on
a divergence > 10 rows. This is intentional: the JSON state reflects the
*current run*, SQLite accumulates *all historical runs*. Treat the warning
as informational. If you want it silenced, project the current-run subset
from the SQLite side before comparing — don't widen the threshold.

## Sidecar format
```json
{
  "schema_version": "1.1",
  "generated_at":   "2026-04-21T21:00:00+00:00",
  "source_stage":   "trends_postprocess",
  "artifact":       "explosive_trends.json",
  "record_count":   410
}
```

A bumped `schema_version` means readers must revalidate. The Sprint 6
harness asserts each reader handles all versions we currently write.
