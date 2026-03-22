#!/usr/bin/env python3
"""
Tests for all 12 discrepancy fixes from the master plan audit.
Avoids importing validation.py as module (it runs heavy module-level code).
Instead uses source inspection + isolated function tests.
Run: python3 test_discrepancy_fixes.py
"""
import sys, os, re, textwrap

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DIR)

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name} — {detail}")

def read_src(fname):
    return open(os.path.join(DIR, fname)).read()


# ══════════════════════════════════════════════════════════════════════════════
# Read all source files once
val_src = read_src("validation.py")
ke_src  = read_src("keyword_extractor.py")
cc_src  = read_src("country_config.py")
cp_src  = read_src("cpc_cache.py")
vt_src  = read_src("vetting.py")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #1: _bulk_kd_gate threshold ═══")
check("kd >= 15 in _bulk_kd_gate", "if kd >= 15:" in val_src, "Still uses old threshold")
check("kd >= 40 NOT in _bulk_kd_gate", "if kd >= 40:" not in val_src, "Old threshold still present")
check("Print says KD ≥ 15", "KD ≥ 15" in val_src, "Print statement not updated")
check("KD ≥ 40 NOT in print", "KD ≥ 40" not in val_src, "Old print still present")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #2: classify_emerging exists and is called before hard gates ═══")
check("classify_emerging function defined", "def classify_emerging(" in val_src)
check("5 signal checks in classify_emerging",
      all(s in val_src for s in ["trend_velocity", "kd_discovery_zone", "cpc_above_vertical",
                                  "auction_heating", "multi_period_growth"]),
      "Missing one or more signals")
check("KVSI promotion implemented", 'kvsi_val >= 0.5' in val_src)
# Verify classify_emerging runs BEFORE _apply_hard_gates in the main loop
main_loop_start = val_src.find("for opp in vetted:")
if main_loop_start > 0:
    loop_body = val_src[main_loop_start:]
    classify_pos = loop_body.find("classify_emerging(")
    hardgate_pos = loop_body.find("_apply_hard_gates(")
    check("classify_emerging called BEFORE _apply_hard_gates in main loop",
          0 < classify_pos < hardgate_pos,
          f"classify_pos={classify_pos}, hardgate_pos={hardgate_pos}")
    check("emerging_tag injected into enrichment",
          'enrichment["emerging_tag"]' in loop_body)
else:
    check("Main loop found", False, "Could not find 'for opp in vetted:'")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #3: Budget tracking wired in ═══")
# cpc_cache.py has the functions
check("pre_flight_budget_check defined in cpc_cache", "def pre_flight_budget_check(" in cp_src)
check("increment_usd_spent defined in cpc_cache", "def increment_usd_spent(" in cp_src)
check("_migrate_api_usage defined in cpc_cache", "def _migrate_api_usage(" in cp_src)

# keyword_extractor.py imports and calls them
check("ke imports pre_flight_budget_check", "pre_flight_budget_check" in ke_src)
check("ke imports increment_usd_spent", "increment_usd_spent" in ke_src)
check("ke calls pre_flight_budget_check", "pre_flight_budget_check(" in ke_src)
check("ke calls increment_usd_spent", "increment_usd_spent(" in ke_src)

# validation.py imports and calls them
check("val imports from cpc_cache", "from cpc_cache import pre_flight_budget_check" in val_src)
pfbc_count = val_src.count("pre_flight_budget_check(")
ius_count  = val_src.count("increment_usd_spent(")
check(f"val calls pre_flight_budget_check ≥2 times", pfbc_count >= 2,
      f"Found {pfbc_count} calls")
check(f"val calls increment_usd_spent ≥2 times", ius_count >= 2,
      f"Found {ius_count} calls")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #4: CPC field mismatch — cpc_high_usd vs htpb floor ═══")
check("_apply_hard_gates has cpc_high_usd param",
      "def _apply_hard_gates(keyword: str, country: str, cpc_usd: float,\n"
      "                      cpc_high_usd: float" in val_src,
      "Missing cpc_high_usd parameter")
check("Gate compares cpc_high_usd against floor", "(cpc_high_usd or 0) < floor" in val_src)
check("Call site passes cpc_high", "keyword, country, cpc_usd, cpc_high, competition" in val_src)

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #5: classify_emerging uses KD 15-44 range ═══")
check("KD discovery zone 15-44", "15 <= kd <= 44" in val_src, "Should use 15-44 range")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #6: Gate order ═══")
# Extract gate numbers and their positions in source
gate_pattern = re.findall(r"# Gate (\d+) — (\w+)", val_src)
if len(gate_pattern) >= 7:
    actual = [(num, label) for num, label in gate_pattern[:7]]
    check("Gate 1 = language (Wrong language)",
          actual[0] == ("1", "Wrong"), f"Got Gate {actual[0]}")
    check("Gate 2 = Intent",
          actual[1] == ("2", "Intent"), f"Got Gate {actual[1]}")
    check("Gate 3 = KD",
          actual[2] == ("3", "KD"), f"Got Gate {actual[2]}")
    check("Gate 4 = SERP (SSR)",
          actual[3] == ("4", "SERP"), f"Got Gate {actual[3]}")
    check("Gate 5 = CPC",
          actual[4] == ("5", "CPC"), f"Got Gate {actual[4]}")
    check("Gate 6 = Paid (competition)",
          actual[5] == ("6", "Paid"), f"Got Gate {actual[5]}")
    check("Gate 7 = Volume",
          actual[6] == ("7", "Volume"), f"Got Gate {actual[6]}")
else:
    check("Found 7 gates", False, f"Found only {len(gate_pattern)} gates")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #7: kd_score in RSOC weights ═══")
check("EVERGREEN has kd weight", '"kd":' in val_src.split("_RSOC_WEIGHTS")[1][:300])
check("kd weight = 0.10", '"kd":          0.10' in val_src)
check("_compute_kd_score defined", "def _compute_kd_score(" in val_src)
check("kd_score in component_scores", '"kd":          _compute_kd_score(' in val_src)

# Verify EVERGREEN weights sum to 1.0 via parsing
ev_block = val_src.split('"EVERGREEN":')[1].split("}")[0]
weights = [float(x) for x in re.findall(r':\s+([\d.]+)', ev_block)]
check(f"EVERGREEN weights sum to 1.0 (got {sum(weights):.2f})",
      abs(sum(weights) - 1.0) < 0.001)

em_block = val_src.split('"EMERGING":')[1].split("}")[0]
em_weights = [float(x) for x in re.findall(r':\s+([\d.]+)', em_block)]
check(f"EMERGING weights sum to 1.0 (got {sum(em_weights):.2f})",
      abs(sum(em_weights) - 1.0) < 0.001)

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #8: _compute_cpc_score thresholds ═══")
check("$2 tier present", "if b >=  2:" in val_src, "Missing $2 tier")
check("$0.50 tier present", "if b >= 0.5:" in val_src, "Missing $0.50 tier")
check("$2.50 tier removed", "if b >= 2.5:" not in val_src, "Old $2.50 threshold still present")

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #9: 0.6x informational+commercial multiplier ═══")
check("0.6 multiplier in compute_rsoc_score",
      "intent_raw *= 0.6" in val_src,
      "Missing 0.6x multiplier application")
check("Multiplier conditioned on informational",
      'main_intent == "informational"' in val_src.split("def compute_rsoc_score")[1].split("def ")[0])

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #10: language_code not hardcoded ═══")
check("_dfs_language helper defined", "def _dfs_language(" in val_src)
check("_DFS_LANG map defined", "_DFS_LANG = {" in val_src)

# Check that the API call sites use _dfs_language(country) instead of "en"
# Extract bulk_kd_gate function source
bulk_kd_src = val_src.split("def _bulk_kd_gate(")[1].split("\ndef ")[0]
check("_bulk_kd_gate uses _dfs_language", "_dfs_language(country)" in bulk_kd_src,
      "Still has hardcoded language")
check('_bulk_kd_gate no hardcoded "en"',
      '"language_code": "en"' not in bulk_kd_src and
      "'language_code': 'en'" not in bulk_kd_src)

labs_batch_src = val_src.split("def _fetch_dataforseo_labs_batch(")[1].split("\ndef ")[0]
check("_fetch_dataforseo_labs_batch uses _dfs_language",
      "_dfs_language(country)" in labs_batch_src)

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #11: DFS_SEEDS_PER_EXPAND_TASK in country_config ═══")
check("DFS_SEEDS_PER_EXPAND_TASK defined",
      "DFS_SEEDS_PER_EXPAND_TASK" in cc_src)
check("Value = 5",
      "DFS_SEEDS_PER_EXPAND_TASK    = 5" in cc_src or
      "DFS_SEEDS_PER_EXPAND_TASK = 5" in cc_src)

# ══════════════════════════════════════════════════════════════════════════════
print("\n═══ Fix #12: News filter false-positive protection ═══")
# Test the actual function
from vetting import is_news_headline

# Should be filtered (news)
check("'trump arrested' → news", is_news_headline("trump arrested"))
check("'shooting downtown' → news", is_news_headline("shooting downtown"))
check("'earthquake today' → news", is_news_headline("earthquake today"))

# Should NOT be filtered (commercial products with context words)
check("'fire extinguisher' → NOT news", not is_news_headline("fire extinguisher"))
check("'storm door' → NOT news", not is_news_headline("storm door"))
check("'flood insurance' → NOT news", not is_news_headline("flood insurance"))
check("'fire alarm' → NOT news", not is_news_headline("fire alarm"))
check("'weather proof jacket' → NOT news", not is_news_headline("weather proof jacket"))
check("'best fire pit' → NOT news", not is_news_headline("best fire pit"))

# Should NOT be filtered (unrelated keywords)
check("'best vpn' → NOT news", not is_news_headline("best vpn"))
check("'buy laptop online' → NOT news", not is_news_headline("buy laptop online"))


# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'═' * 60}")
print(f"  RESULTS: {passed} passed, {failed} failed out of {passed + failed} tests")
print(f"{'═' * 60}")
sys.exit(0 if failed == 0 else 1)
