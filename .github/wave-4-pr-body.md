## Summary

Wave 4 ships **19 finding commits + 1 regen commit** across UX, contrast, font-size floor, dashboard interaction polish, and login/register UX. Touches `dashboard_template.html`, `dashboard_builder.py`, `login.html`, `admin.html`, `chat_widget.html`, `intelligence.html`. Backend not in scope.

**F-009 / F-010 default decision applied:** ship empty-state copy (tabs stay visible), per the operator default in the GO doc.

---

### Visual / contrast / font

| Finding | Fix | Commit |
|---|---|---|
| **F-070** | `[data-theme="light"] #opportunity-table th { color: rgb(74,74,90); }` — ~8:1 contrast on light `--bg-raised`. | `f00c7ec` |
| **F-071** | `.tab-btn.active { font-size: 14px; }` + light-theme color `rgb(15,118,110)` (~5:1 on white). | `d86c752` |
| **F-072** | `[data-theme="light"] .btn-signout { color: rgb(74,74,90); }` — ~7:1 on light. | `e7e289f` |
| **F-073** | Mechanical sweep of every `font-size: 10px` → `font-size: 12px` across 31 occurrences in dashboard CSS. Zero remaining (verified post-regen). | `e38b39c` |
| **F-064** | `aria-pressed` on intelligence period-filter buttons + dashboard exp-filter buttons; click handlers flip in lockstep with `.active` class. | `d3b1455` |

### Login / register UX

| Finding | Fix | Commit |
|---|---|---|
| **F-002** | New `setBtnBusy(btn, busy, busyText)` helper — `disabled`, `aria-busy="true"`, label swap, inline CSS spinner. Wired on both login and register submits. | `b6796a5` |
| **F-018** | Per-field inline errors on Request Access (4 fields). `aria-describedby` to a new `<span class="field-error">`. Submit-then-validate pattern; submit button never disabled (a11y antipattern). `textContent` for server-driven copy (XSS-safe). | `ba20553` |
| **F-019** | Post-submit confirmation block (`role="status"` only, no `aria-live`). Replaces the form with the operator-supplied copy + the user's email rendered via `textContent`. | `d58ae85` |

### Dashboard interaction

| Finding | Fix | Commit |
|---|---|---|
| **F-030** | 300ms trailing-edge debounce on `#global-search` input handler. Cheap cues (clear-button visibility) stay synchronous. | `4beca6a` |
| **F-026** | `kv-golden` and `kv-emerging` KPI tiles → `<button>` with `data-kpi-filter-tag` toggling the matching filter-panel checkbox. `aria-pressed` reflects active state. Other tiles intentionally stay as `<div>` (no matching tag filter). | `05f29bf` |
| **F-068** | Pipeline-ribbon counters GKP / DFS / Unscored → `<button>` filter toggles routed through the existing filter-panel checkboxes. Errors counter NOT made drillable — no error-tag filter exists today (per brief: ship 3, defer Errors). | `a4fa560` |
| **F-028** | `<div id="toast-region" role="status">` + `showToast()` helper. ⌘ button click copies + toasts; `stopPropagation` keeps F-034 row→drawer independent. `textContent` for the keyword in the toast. | `f7a2a46` |
| **F-034** | Row click → `openDrawer` (existing wiring). Added `openDrawer(item, trigger)` arg so click + keyboard `Enter` pass the row's ⌘ button as the focus-restoration target — fixes the Wave 2 F-044 known limitation where ESC-close fell back to `<body>`. | `5db7f68` |
| **F-075** | Active-filter chips → `<button class="af-pill">` with `aria-label="Remove <filter> filter"`. Whole chip is clickable; ✕ glyph is decorative (`aria-hidden`). | `4ea7c0a` |

### Other pages

| Finding | Fix | Commit |
|---|---|---|
| **F-008** | New module-level `_pendingUsers` cache in admin.html. `renderAll()` computes the pending filter once after the API fetch; both `renderStats` (counter + badge) and `renderUsers` (table) read from it. Defensive consolidation — single source. | `f67e089` |
| **F-036** | `aria-label="Reset conversation"` on `#chat-clear`, `aria-label="Close chat"` on `#chat-close`. | `8c1ae6b` |
| **F-065** | DURABILITY + VELOCITY metric tooltips with WAI-ARIA Tooltip pattern. CSS `:hover`/`:focus-within` show; document-level keydown handler closes on Escape. Operator-approved copy. | `395640d` |
| **F-009** | Performance tab empty-state copy ("Performance — coming soon") replaces the previous ops-only CLI command instructions. Hydrated branch (rich Performance UI) unchanged for when data is present. | `d9b51f9` |
| **F-010** | Pipeline tab empty-state copy + corrected tab class from `tab-panel` to `tab-content` so the tab show/hide JS works. Rich pipeline body kept as dead code below the early return for future re-enable. | `6425cfa` |

### Regen

`python3 dashboard_builder.py` ran clean. **31/31 markers pass** the static check (the two diff-verification "miss" entries were data-conditional code paths — F-009's empty-state and F-064's exp-filter buttons only render when their data isn't / is present in the worktree; the code is in place and verified in the source files). Commit `e9225d8`.

---

## Verification — Part A (Claude Code, locally) ✅

`python3 -m http.server 8080` against the regenerated worktree, plus a Python port of the structural-marker scan across all 5 in-scope files: **31/31 markers verified.** F-073 sweep: zero remaining `font-size: 10px` in `dashboard.html`. Ports of the F-018 invalid-then-valid flow, F-064 aria-pressed lockstep, F-068 sys-counter routing, F-026 KPI sync, F-008 single-source verified by code review.

axe-core static check: not run autonomously (the harness can't drive a real Chrome). Color-contrast count is the metric to watch on Part B — F-070/71/72/73 should drop the audit's pre-existing color-contrast violations substantially.

## Verification — Part B (operator + Cowork on CF preview)

Preview URL pattern: `https://wave-4-polish.keywordit-site.pages.dev/<page>`

- Re-run axe-core on dashboard / login / admin / intelligence. Color-contrast count should approach zero (Wave 4 paid the contrast bill).
- Lighthouse Accessibility on dashboard — target ≥ 95.
- Smoke: KPI click filters, ⌘ icon copies + toast, row click opens drawer (no copy), chip click removes filter, register inline errors on first invalid submit, date filter has only one active button, search feels snappier, Escape on metric-info dismisses tooltip.

---

Closes F-002, F-008, F-009, F-010, F-018, F-019, F-026, F-028, F-030, F-034, F-036, F-064, F-065, F-068, F-070, F-071, F-072, F-073, F-075
F-009 / F-010: empty-state shipped (operator default; tabs stay visible)
F-030: 300ms debounce stopgap only — full perf fix tracked under F-001
F-034: minimal drawer content — rich score breakdown stays under F-061 (deferred)
F-068: GKP / DFS / Unscored counters drillable; Errors deferred (no error-tag filter exists today — flagged in commit body)
F-018: server password / email / username errors routed by keyword match to the field-error span; `textContent` only, no `innerHTML` — server can't inject HTML

### Deferred from Wave 4 (no commits)

- F-020 (gh-pages landing) — bundled with F-050 follow-up
- F-032 (filter option lists) — needs backend
- F-061 (score transparency) — out-of-scope
- F-069 (on-demand pipeline run) — needs job queue
- F-076 (specific login errors) — security review needed

Brief tripwires summary (none triggered):
- F-006-style subtle handler refactor → N/A this wave.
- F-026 filter mechanism → exists, verified.
- F-068 Errors filter → does NOT exist; followed brief decision (ship other 3, defer Errors).
- F-073 font sweep → no intentional-sizing comments encountered, full sweep applied.
- F-018 server error string → `textContent` everywhere, no `innerHTML`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
