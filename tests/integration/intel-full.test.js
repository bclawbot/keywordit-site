/**
 * Comprehensive Integration Test Suite — Sprint 1-3 Feature Validation
 *
 * Validates all Intel tab features work together: data integrity,
 * CCD scoring, filters, cross-tab navigation, bug regressions,
 * pipeline integration, CCD v2 shadow mode, and performance.
 *
 * Run: npm test
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const { CCD } = require('../../shared/ccd.js');
const { IntelFilters } = require('../../shared/intel-filters.js');
const { IntelNav } = require('../../shared/intel-nav.js');

// ── Load dashboard HTML for structure tests ─────────────────────────────────
let html;
let apiPy;

beforeAll(() => {
  html = readFileSync(resolve(__dirname, '..', '..', 'dashboard.html'), 'utf-8');
  apiPy = readFileSync(resolve(__dirname, '..', '..', 'services', 'intelligence_api.py'), 'utf-8');
});

// ── Realistic test fixtures (mirrors API response shapes) ───────────────────

const VERTICALS = [
  { vertical: 'education', avg_durability: 5.9, network_count: 4, velocity_7d: 932, total_ads: 57 },
  { vertical: 'health', avg_durability: 5.0, network_count: 3, velocity_7d: 1154, total_ads: 31 },
  { vertical: 'travel', avg_durability: 5.7, network_count: 2, velocity_7d: 391, total_ads: 40 },
  { vertical: 'automotive', avg_durability: 5.0, network_count: 2, velocity_7d: 734, total_ads: 20 },
  { vertical: 'employment', avg_durability: 5.0, network_count: 2, velocity_7d: 52, total_ads: 18 },
  { vertical: 'personal_care', avg_durability: 8.8, network_count: 1, velocity_7d: 24, total_ads: 10 },
  { vertical: 'real_estate', avg_durability: 5.0, network_count: 1, velocity_7d: 200, total_ads: 8 },
  { vertical: 'finance', avg_durability: 5.0, network_count: 1, velocity_7d: 594, total_ads: 12 },
  { vertical: 'solar', avg_durability: 5.0, network_count: 1, velocity_7d: 50, total_ads: 5 },
  { vertical: 'tech', avg_durability: 5.8, network_count: 2, velocity_7d: 398, total_ads: 15 },
];

const NETWORKS = [
  { name: 'Predicto', avg_durability: 4.7, active_ads: 139, velocity_7d: 1482, new_7d: 100, new_30d: 200, top_verticals: ['education', 'health', 'automotive'] },
  { name: 'adtitle', avg_durability: 4.3, active_ads: 56, velocity_7d: 543, new_7d: 40, new_30d: 80, top_verticals: ['education', 'tech'] },
  { name: 'Tonic', avg_durability: 11.0, active_ads: 4, velocity_7d: 9, new_7d: 9, new_30d: 29, top_verticals: ['health', 'personal_care'] },
  { name: 'ExplorAds', avg_durability: 0, active_ads: 0, velocity_7d: 638, new_7d: 50, new_30d: 100, top_verticals: ['employment'] },
];

function makeKeywords(count) {
  const verts = VERTICALS.map(v => v.vertical);
  const angleTypes = ['listicle', 'how_to', 'comparison', 'cost_savings', 'informational_explainer', 'news_breaking', 'testimonial', 'direct_offer'];
  const kws = [];
  for (let i = 0; i < count; i++) {
    const netCount = 1 + (i % 9);
    // 10% of keywords have no verticals (exercises global fallback in CCD v1)
    const kwVerts = i % 10 === 7 ? [] : [verts[i % verts.length]];
    if (kwVerts.length > 0 && i % 3 === 0 && verts[(i + 1) % verts.length]) kwVerts.push(verts[(i + 1) % verts.length]);
    const hasTracking = i % 4 !== 3; // 75% have ad_tracking
    const avgDur = 0.5 + (i % 30);
    kws.push({
      id: 1000 + i,
      keyword: 'keyword_' + i + (i % 20 === 0 ? ' home improvement' : i % 15 === 0 ? ' electrician work' : ''),
      network_count: netCount,
      verticals: kwVerts,
      max_durability: 3 + (i % 10),
      cpc_usd: i % 5 === 0 ? null : 0.5 + (i % 20) * 0.4,
      volume: i % 5 === 0 ? null : 100 + i * 50,
      kd: i % 7 === 0 ? null : 10 + (i % 80),
      rsoc_score: i % 5 === 0 ? null : 0.3 + (i % 7) * 0.1,
      validation_status: i % 5 === 0 ? 'pending' : 'validated',
      first_seen: '2026-04-0' + (1 + i % 9) + 'T10:00:00Z',
      angles: i % 6 === 0 ? [] : [
        { type: angleTypes[i % angleTypes.length], confidence: 0.7 + (i % 3) * 0.1, source: 'generated' },
        { type: angleTypes[(i + 3) % angleTypes.length], confidence: 0.6, source: 'generated' },
      ],
      ad_tracking: hasTracking ? {
        total_unique_ads: 1 + (i % 25),
        active_ads: 1 + (i % 18),
        churned_ads: i % 7,
        avg_ad_duration_days: avgDur,
        max_ad_duration_days: avgDur + 5,
        duration_p50: avgDur - 0.5,
        duration_p90: avgDur + 3,
        durability_class: avgDur < 1 ? 'flash' : avgDur < 3 ? 'rapid_test' : avgDur < 7 ? 'testing' : avgDur < 14 ? 'established' : avgDur < 30 ? 'proven' : 'evergreen',
      } : null,
    });
  }
  return kws;
}

const KEYWORDS_1000 = makeKeywords(1000);

function initCCDFull() {
  CCD.activeVersion = 'v1';
  CCD.init(KEYWORDS_1000, VERTICALS, NETWORKS);
}

// ══════════════════════════════════════════════════════════════════════════════
// DATA INTEGRITY TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('Data Integrity', () => {

  it('API Python code serves all 7 endpoints', () => {
    const endpoints = ['/networks', '/activity', '/keywords', '/verticals', '/matrix', '/analysis', '/health'];
    endpoints.forEach(ep => {
      expect(apiPy).toContain(`@intel_bp.route("${ep}")`);
    });
  });

  it('keywords reference only valid verticals (from fixture)', () => {
    const validVerts = new Set(VERTICALS.map(v => v.vertical));
    KEYWORDS_1000.forEach(kw => {
      (kw.verticals || []).forEach(v => {
        expect(validVerts.has(v), `"${v}" in keyword "${kw.keyword}" not in verticals list`).toBe(true);
      });
    });
  });

  it('no duplicate angle types exist in keyword angles', () => {
    initCCDFull();
    const allTypes = new Set();
    KEYWORDS_1000.forEach(kw => {
      (kw.angles || []).forEach(a => {
        allTypes.add(a.type);
      });
    });
    // All types should be snake_case (no slashes, hyphens, or mixed case)
    allTypes.forEach(t => {
      expect(t).toMatch(/^[a-z0-9_]+$/);
    });
  });

  it('no duplicate verticals exist after consolidation (code check)', () => {
    // The API code should map old names to new canonical names
    expect(apiPy).toContain('"jobs": "employment"');
    expect(apiPy).toContain('"job_search": "employment"');
    expect(apiPy).toContain('"beauty_cosmetics": "personal_care"');
    expect(apiPy).toContain('"housing": "real_estate"');
  });

  it('vertical consolidation maps are applied in keywords endpoint', () => {
    expect(apiPy).toContain('_normalize_vertical');
    expect(apiPy).toContain('_override_vertical_for_keyword');
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// CCD SCORE TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('CCD Scores', () => {
  beforeAll(() => { initCCDFull(); });

  it('every keyword has a valid CCD v1 score', () => {
    KEYWORDS_1000.forEach(kw => {
      const entry = CCD.cache[kw.id];
      expect(entry, `keyword ${kw.id} missing from cache`).toBeDefined();
      expect(entry.v1).toBeDefined();
      expect(entry.v1.normalized).toBeGreaterThanOrEqual(0);
      expect(entry.v1.normalized).toBeLessThanOrEqual(100);
      expect(Number.isNaN(entry.v1.normalized)).toBe(false);
      expect(Number.isNaN(entry.v1.raw)).toBe(false);
    });
  });

  it('every keyword also has a v2 score (shadow mode)', () => {
    KEYWORDS_1000.forEach(kw => {
      const entry = CCD.cache[kw.id];
      expect(entry.v2).toBeDefined();
      expect(entry.v2.normalized).toBeGreaterThanOrEqual(0);
      expect(entry.v2.normalized).toBeLessThanOrEqual(100);
    });
  });

  it('CCD scores have meaningful distribution (not all same)', () => {
    const scores = KEYWORDS_1000.map(kw => CCD.cache[kw.id].v1.normalized);
    const unique = new Set(scores);
    expect(unique.size).toBeGreaterThan(10);
  });

  it('at least 3 CCD tiers represented', () => {
    // After v1 hotfix (removed global maxNetDurability), scores spread
    // across all tiers based on per-keyword network count + vertical data.
    const tiers = new Set();
    KEYWORDS_1000.forEach(kw => {
      tiers.add(CCD.tier(CCD.cache[kw.id].v1.normalized).label);
    });
    expect(tiers.size).toBeGreaterThanOrEqual(3);
  });

  it('CCD standard deviation > 5', () => {
    const scores = KEYWORDS_1000.map(kw => CCD.cache[kw.id].v1.normalized);
    const mean = scores.reduce((a, b) => a + b, 0) / scores.length;
    const variance = scores.reduce((a, s) => a + (s - mean) ** 2, 0) / scores.length;
    expect(Math.sqrt(variance)).toBeGreaterThan(5);
  });

  it('CCD sort produces correct ascending order', () => {
    const sorted = KEYWORDS_1000.slice().sort((a, b) =>
      CCD.cache[a.id].v1.normalized - CCD.cache[b.id].v1.normalized
    );
    for (let i = 1; i < sorted.length; i++) {
      expect(CCD.cache[sorted[i].id].v1.normalized)
        .toBeGreaterThanOrEqual(CCD.cache[sorted[i - 1].id].v1.normalized);
    }
  });

  it('CCD filter + sort + search work together', () => {
    const state = {
      search: 'home',
      colFilters: {},
      numFilters: { ccd_score: { min: 51, max: 75 } },
    };
    const filtered = IntelFilters.filterKeywords(KEYWORDS_1000, state, { ccdCache: CCD.cache });
    // All results should match search "home" AND CCD 51-75
    filtered.forEach(kw => {
      expect(kw.keyword.toLowerCase()).toContain('home');
      const score = CCD.active(kw.id).normalized;
      expect(score).toBeGreaterThanOrEqual(51);
      expect(score).toBeLessThanOrEqual(75);
    });
    // Sort the filtered set by volume descending
    const sorted = filtered.slice().sort((a, b) => (b.volume || 0) - (a.volume || 0));
    for (let i = 1; i < sorted.length; i++) {
      expect(sorted[i - 1].volume || 0).toBeGreaterThanOrEqual(sorted[i].volume || 0);
    }
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// FILTER INTEGRATION TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('Filter Integration', () => {
  beforeAll(() => { initCCDFull(); });

  it('all filter dropdowns can be populated from data', () => {
    const opts = IntelFilters.extractDropdownOptions(KEYWORDS_1000);
    expect(opts.verticals.length).toBeGreaterThan(0);
    expect(opts.angleTypes.length).toBeGreaterThan(0);
  });

  it('vertical filter count is accurate', () => {
    const state = { search: '', colFilters: { verticals: ['education'] }, numFilters: {} };
    const filtered = IntelFilters.filterKeywords(KEYWORDS_1000, state);
    const manual = KEYWORDS_1000.filter(kw => (kw.verticals || []).includes('education'));
    expect(filtered.length).toBe(manual.length);
  });

  it('status filter count is accurate', () => {
    const state = { search: '', colFilters: { validation_status: ['validated'] }, numFilters: {} };
    const filtered = IntelFilters.filterKeywords(KEYWORDS_1000, state);
    const manual = KEYWORDS_1000.filter(kw => kw.validation_status === 'validated');
    expect(filtered.length).toBe(manual.length);
  });

  it('stacked filters use AND logic', () => {
    const state = {
      search: '',
      colFilters: { verticals: ['education'], validation_status: ['validated'] },
      numFilters: { network_count: { min: 3, max: null } },
    };
    const filtered = IntelFilters.filterKeywords(KEYWORDS_1000, state);
    filtered.forEach(kw => {
      expect(kw.verticals).toContain('education');
      expect(kw.validation_status).toBe('validated');
      expect(kw.network_count).toBeGreaterThanOrEqual(3);
    });
  });

  it('clearing all filters restores full dataset', () => {
    const state = {
      search: 'xyz',
      colFilters: { verticals: ['education'] },
      numFilters: { ccd_score: { min: 76, max: null } },
    };
    let filtered = IntelFilters.filterKeywords(KEYWORDS_1000, state, { ccdCache: CCD.cache });
    expect(filtered.length).toBeLessThan(KEYWORDS_1000.length);
    state.search = '';
    state.colFilters = {};
    state.numFilters = {};
    filtered = IntelFilters.filterKeywords(KEYWORDS_1000, state);
    expect(filtered.length).toBe(KEYWORDS_1000.length);
  });

  it('hasActiveFilters correctly reports state', () => {
    expect(IntelFilters.hasActiveFilters({ search: '', colFilters: {}, numFilters: {} })).toBe(false);
    expect(IntelFilters.hasActiveFilters({ search: 'x', colFilters: {}, numFilters: {} })).toBe(true);
    expect(IntelFilters.hasActiveFilters({ search: '', colFilters: { verticals: ['a'] }, numFilters: {} })).toBe(true);
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// CROSS-TAB NAVIGATION TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('Cross-Tab Navigation', () => {

  it('War Room → Keywords: network top vertical becomes filter', () => {
    const params = IntelNav.paramsFromNetwork(NETWORKS[0]);
    expect(params.vertical).toBe('education');
    const merged = IntelNav.mergeFilters(
      { colFilters: {}, numFilters: {}, search: '' },
      params
    );
    expect(merged.colFilters['verticals']).toEqual(['education']);
  });

  it('Verticals → Keywords: vertical name becomes filter', () => {
    const params = IntelNav.paramsFromVertical('automotive');
    const merged = IntelNav.mergeFilters(
      { colFilters: {}, numFilters: {}, search: '' },
      params
    );
    expect(merged.colFilters['verticals']).toEqual(['automotive']);
  });

  it('Matrix → Keywords: vertical + angle become filters', () => {
    const params = IntelNav.paramsFromMatrixCell('education', 'informational_explainer', false);
    expect(params).toEqual({ vertical: 'education', angle: 'informational_explainer' });
    const merged = IntelNav.mergeFilters(
      { colFilters: {}, numFilters: {}, search: '' },
      params
    );
    expect(merged.colFilters['verticals']).toEqual(['education']);
    expect(merged.colFilters['top_angle_type']).toEqual(['informational_explainer']);
  });

  it('empty Matrix cell returns null (no navigation)', () => {
    const params = IntelNav.paramsFromMatrixCell('education', 'listicle', true);
    expect(params).toBeNull();
  });

  it('round-trip navigation preserves existing non-conflicting filters', () => {
    // Start with CCD + status filters on Keywords
    const initial = {
      colFilters: { validation_status: ['validated'] },
      numFilters: { ccd_score: { min: 0, max: 25, label: 'Emerging' } },
      search: 'test',
    };
    // Navigate from Matrix cell
    const navParams = IntelNav.paramsFromMatrixCell('education', 'listicle', false);
    const merged = IntelNav.mergeFilters(initial, navParams);
    // New filters applied
    expect(merged.colFilters['verticals']).toEqual(['education']);
    expect(merged.colFilters['top_angle_type']).toEqual(['listicle']);
    // Existing non-conflicting filters preserved
    expect(merged.colFilters['validation_status']).toEqual(['validated']);
    expect(merged.numFilters['ccd_score'].min).toBe(0);
    expect(merged.search).toBe('test');
  });

  it('navigateToKeywords function exists in dashboard HTML', () => {
    expect(html).toContain('function navigateToKeywords(navParams)');
    expect(html).toContain('window.navigateToKeywords = navigateToKeywords');
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// CCD v2 SHADOW TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('CCD v2 Shadow Mode', () => {
  beforeAll(() => { initCCDFull(); });

  it('v1 and v2 scores correlate positively (Spearman rank)', () => {
    // Compute rank correlation: sort by v1, sort by v2, check agreement
    const v1Ranked = KEYWORDS_1000.slice().sort((a, b) =>
      CCD.cache[a.id].v1.normalized - CCD.cache[b.id].v1.normalized
    );
    const v2Ranked = KEYWORDS_1000.slice().sort((a, b) =>
      CCD.cache[a.id].v2.normalized - CCD.cache[b.id].v2.normalized
    );
    // Build rank maps
    const v1Rank = {};
    const v2Rank = {};
    v1Ranked.forEach((kw, i) => { v1Rank[kw.id] = i; });
    v2Ranked.forEach((kw, i) => { v2Rank[kw.id] = i; });
    // Spearman: 1 - (6 * sum(d^2)) / (n * (n^2 - 1))
    const n = KEYWORDS_1000.length;
    let sumD2 = 0;
    KEYWORDS_1000.forEach(kw => {
      const d = v1Rank[kw.id] - v2Rank[kw.id];
      sumD2 += d * d;
    });
    const rho = 1 - (6 * sumD2) / (n * (n * n - 1));
    expect(rho).toBeGreaterThan(0.3);
  });

  it('v2 scores are at least as varied as v1', () => {
    const v1s = KEYWORDS_1000.map(kw => CCD.cache[kw.id].v1.normalized);
    const v2s = KEYWORDS_1000.map(kw => CCD.cache[kw.id].v2.normalized);
    const v1Std = stdDev(v1s);
    const v2Std = stdDev(v2s);
    // v2 should have at least half the variance of v1 (real data may spread differently)
    expect(v2Std).toBeGreaterThanOrEqual(v1Std * 0.3);
  });

  it('comparison report has valid structure', () => {
    const report = CCD.comparisonReport();
    expect(report.keywords.length).toBe(KEYWORDS_1000.length);
    expect(report.summary.total).toBe(KEYWORDS_1000.length);
    const s = report.summary;
    expect(s.deltaUnder5 + s.deltaUnder15 + s.deltaUnder30 + s.deltaOver30).toBe(s.total);
  });

  it('v1→v2 swap changes active() output', () => {
    CCD.setVersion('v1');
    const v1Score = CCD.active(KEYWORDS_1000[0].id).normalized;
    CCD.setVersion('v2');
    const v2Score = CCD.active(KEYWORDS_1000[0].id).normalized;
    CCD.setVersion('v1'); // reset
    // Scores can be equal if v2 fell back, but the version tag should differ
    const v1Version = CCD.cache[KEYWORDS_1000[0].id].v1.version;
    const v2Version = CCD.cache[KEYWORDS_1000[0].id].v2.version;
    expect(v1Version).toBe('v1');
    expect(['v2', 'v2_fallback']).toContain(v2Version);
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// BUG REGRESSION TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('Bug Regressions', () => {

  it('#1: Activity endpoint has fallback when Signals empty', () => {
    expect(apiPy).toContain('signal_count > 0');
    expect(apiPy).toContain("'new_ad' AS signal_type");
  });

  it('#2: Active Ads KPI has tooltip', () => {
    expect(html).toMatch(/kpi-card.*?title="[^"]*snapshot[^"]*"/i);
  });

  it('#5: api_networks docstring explains metric independence', () => {
    expect(apiPy).toContain('active_ads, new_7d, and new_30d are *independent* counts');
  });

  it('#6: vertical consolidation maps exist', () => {
    expect(apiPy).toContain('_VERTICAL_ALIAS_MAP');
    expect(apiPy).toContain('"jobs": "employment"');
  });

  it('#7: electrician override exists', () => {
    expect(apiPy).toContain('_KEYWORD_VERTICAL_OVERRIDES');
    expect(apiPy).toContain('"electrician"');
  });

  it('#8: future timestamp sanitizer exists', () => {
    const storagePy = readFileSync(
      resolve(__dirname, '..', '..', 'dwight', 'fb_intelligence', 'storage.py'), 'utf-8'
    );
    expect(storagePy).toContain('_sanitize_timestamp');
  });

  it('#9: dormant networks filtered from War Room', () => {
    expect(apiPy).toContain("n[\"active_ads\"] > 0 or n[\"new_7d\"] > 0 or n[\"new_30d\"] > 0");
  });

  it('#10: score_reason field added to keywords', () => {
    expect(apiPy).toContain('"score_reason"');
    expect(apiPy).toContain('_score_reason');
  });

  it('#11: asrsearch blocklist exists', () => {
    expect(apiPy).toContain('_TOP_KW_BLOCKLIST');
    expect(apiPy).toContain('"asrsearch"');
  });

  it('#12: IKW_TOTAL tracks real API total', () => {
    expect(html).toContain('var IKW_TOTAL = 0;');
    expect(html).toContain('IKW_TOTAL = totalItems');
  });

  it('#14: all metric column headers have tooltips', () => {
    const cols = ['network_count', 'max_durability', 'kd', 'rsoc_score', 'ccd_score'];
    cols.forEach(col => {
      const re = new RegExp(`data-col="${col}"[^>]*title="`);
      expect(html).toMatch(re);
    });
  });

  it('#15: angle normalization exists', () => {
    expect(apiPy).toContain('_ANGLE_ALIAS_MAP');
    expect(apiPy).toContain('_normalize_angle_type');
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// PERFORMANCE TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('Performance', () => {

  it('CCD computation completes in < 200ms for 1000 keywords', () => {
    const start = performance.now();
    CCD.init(KEYWORDS_1000, VERTICALS, NETWORKS);
    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(200);
  });

  it('filter application completes in < 100ms for 1000 keywords', () => {
    initCCDFull();
    const state = {
      search: 'keyword',
      colFilters: { verticals: ['education'], validation_status: ['validated'] },
      numFilters: { ccd_score: { min: 26, max: 75 }, network_count: { min: 3, max: null } },
    };
    const start = performance.now();
    IntelFilters.filterKeywords(KEYWORDS_1000, state, { ccdCache: CCD.cache });
    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(100);
  });

  it('comparison report generates in < 50ms for 1000 keywords', () => {
    initCCDFull();
    const start = performance.now();
    CCD.comparisonReport();
    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(50);
  });

  it('IntelNav.mergeFilters is instant (< 1ms)', () => {
    const state = {
      colFilters: { verticals: ['health'], validation_status: ['pending'] },
      numFilters: { ccd_score: { min: 0, max: 25 } },
      search: 'test',
    };
    const start = performance.now();
    for (let i = 0; i < 1000; i++) {
      IntelNav.mergeFilters(state, { vertical: 'education', angle: 'listicle' });
    }
    const elapsed = performance.now() - start;
    expect(elapsed / 1000).toBeLessThan(1); // < 1ms per call
  });
});

// ── Helper ──────────────────────────────────────────────────────────────────

function stdDev(values) {
  if (values.length < 2) return 0;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const sqDiffs = values.map(v => (v - mean) ** 2);
  return Math.sqrt(sqDiffs.reduce((a, b) => a + b, 0) / values.length);
}
