/**
 * CCD (Competitor Campaign Density) v1 Scoring Module — Unit Tests
 *
 * Run: npm test
 */

import { describe, it, expect, beforeEach } from 'vitest';

// The CCD module uses var + module.exports for browser/Node compat
const { CCD } = require('../shared/ccd.js');

// ── Test fixtures ──────────────────────────────────────────────────────────

const SAMPLE_VERTICALS = [
  { vertical: 'education', avg_durability: 5.9, network_count: 4, velocity_7d: 932, total_ads: 57 },
  { vertical: 'health', avg_durability: 5.0, network_count: 3, velocity_7d: 1154, total_ads: 31 },
  { vertical: 'travel', avg_durability: 5.7, network_count: 2, velocity_7d: 391, total_ads: 40 },
  { vertical: 'auto', avg_durability: 5.0, network_count: 2, velocity_7d: 734, total_ads: 20 },
  { vertical: 'home_improvement', avg_durability: 5.0, network_count: 1, velocity_7d: 768, total_ads: 15 },
  { vertical: 'finance', avg_durability: 5.0, network_count: 1, velocity_7d: 594, total_ads: 12 },
  { vertical: 'real_estate', avg_durability: 5.0, network_count: 1, velocity_7d: 200, total_ads: 8 },
  { vertical: 'solar', avg_durability: 5.0, network_count: 1, velocity_7d: 50, total_ads: 5 },
  { vertical: 'government', avg_durability: 5.0, network_count: 1, velocity_7d: 30, total_ads: 3 },
  { vertical: 'immigration', avg_durability: 5.0, network_count: 1, velocity_7d: 4, total_ads: 2 },
];

const SAMPLE_NETWORKS = [
  { name: 'Predicto', avg_durability: 4.7, active_ads: 139, velocity_7d: 1482.0 },
  { name: 'adtitle', avg_durability: 4.3, active_ads: 56, velocity_7d: 543.0 },
  { name: 'Other', avg_durability: 4.9, active_ads: 17, velocity_7d: 720.0 },
  { name: 'Tonic', avg_durability: 11.0, active_ads: 4, velocity_7d: 9.0 },
  { name: 'ExplorAds', avg_durability: 0, active_ads: 0, velocity_7d: 638.0 },
  { name: 'Visymo', avg_durability: 0, active_ads: 0, velocity_7d: 1729.0 },
];

const HIGH_NETWORK_KEYWORD = {
  id: 562,
  keyword: 'home improvement',
  network_count: 9,
  verticals: ['home_improvement', 'government', 'health', 'real_estate', 'solar'],
  max_durability: 5.0,
};

const LOW_NETWORK_KEYWORD = {
  id: 999,
  keyword: 'immigration lawyer',
  network_count: 1,
  verticals: ['immigration'],
  max_durability: 3.0,
};

const MISSING_VERTICAL_KEYWORD = {
  id: 888,
  keyword: 'quantum computing',
  network_count: 2,
  verticals: ['nonexistent_vertical'],
  max_durability: 1.0,
};

const EMPTY_VERTICALS_KEYWORD = {
  id: 777,
  keyword: 'generic term',
  network_count: 1,
  verticals: [],
  max_durability: 0,
};

const ALL_KEYWORDS = [
  HIGH_NETWORK_KEYWORD,
  LOW_NETWORK_KEYWORD,
  MISSING_VERTICAL_KEYWORD,
  EMPTY_VERTICALS_KEYWORD,
];

// ── Tests ──────────────────────────────────────────────────────────────────

describe('CCD Scoring Module', () => {
  beforeEach(() => {
    CCD.init(ALL_KEYWORDS, SAMPLE_VERTICALS, SAMPLE_NETWORKS);
  });

  describe('CCD.compute — high network keyword scores high', () => {
    it('should produce a high normalized score for a keyword with 9 networks and 5 verticals', () => {
      const result = CCD.cache[HIGH_NETWORK_KEYWORD.id].v1;
      expect(result).toBeDefined();
      expect(result.normalized).toBeGreaterThan(60);
      expect(result.confidence).toBe('high');

      const tier = CCD.tier(result.normalized);
      expect(['Saturated', 'Oversaturated']).toContain(tier.label);
    });

    it('should have correct breakdown structure', () => {
      const result = CCD.cache[HIGH_NETWORK_KEYWORD.id].v1;
      expect(result.breakdown).toBeDefined();
      expect(result.breakdown.networks.count).toBe(9);
      expect(result.breakdown.networks.contrib).toBe(315); // 9 * 35
      expect(result.breakdown.vert_durability.matched).toBe(5);
      expect(result.breakdown.vert_durability.avg).toBeCloseTo(5.0, 1);
      expect(result.breakdown.global_fallback).toBe(false);
      // v1 hotfix: net_durability removed from breakdown
      expect(result.breakdown.net_durability).toBeUndefined();
    });
  });

  describe('CCD.compute — low network keyword scores low', () => {
    it('should produce a lower normalized score than the high-network keyword', () => {
      const result = CCD.cache[LOW_NETWORK_KEYWORD.id].v1;
      expect(result).toBeDefined();
      const highResult = CCD.cache[HIGH_NETWORK_KEYWORD.id].v1;
      expect(result.normalized).toBeLessThan(highResult.normalized);
      // 1-network + 1 high-durability vertical lands in Saturated with tuned weights
      expect(['Competitive', 'Saturated']).toContain(CCD.tier(result.normalized).label);
      expect(highResult.normalized - result.normalized).toBeGreaterThanOrEqual(30);
    });

    it('should have medium confidence with 1 vertical matched', () => {
      const result = CCD.cache[LOW_NETWORK_KEYWORD.id].v1;
      expect(result.confidence).toBe('medium');
    });
  });

  describe('CCD.compute — missing verticals fallback', () => {
    it('should fallback to global median durability (1.0) for unmatched verticals', () => {
      const result = CCD.cache[MISSING_VERTICAL_KEYWORD.id].v1;
      expect(result).toBeDefined();
      // Has verticals but none matched the map — uses global median 1.0
      expect(result.breakdown.vert_durability.avg).toBe(1.0);
      expect(result.breakdown.vert_durability.matched).toBe(0);
      expect(result.breakdown.global_fallback).toBe(false); // not global fallback — has verticals, just unmatched
      expect(result.confidence).toBe('low');
    });
  });

  describe('CCD.compute — empty verticals', () => {
    it('should not crash and return a score with confidence "low"', () => {
      const result = CCD.cache[EMPTY_VERTICALS_KEYWORD.id].v1;
      expect(result).toBeDefined();
      expect(typeof result.normalized).toBe('number');
      expect(result.confidence).toBe('low');
      expect(Number.isNaN(result.normalized)).toBe(false);
      expect(Number.isNaN(result.raw)).toBe(false);
    });
  });

  describe('CCD.normalize — clamps to 0-100 (MIN=10, MAX=200)', () => {
    it('should return 0 for raw = 0', () => {
      expect(CCD.normalize(0)).toBe(0);
    });

    it('should return 0 for raw at MIN threshold (10)', () => {
      expect(CCD.normalize(10)).toBe(0);
    });

    it('should return 100 for raw = 500 (above MAX)', () => {
      expect(CCD.normalize(500)).toBe(100);
    });

    it('should return 100 for raw at MAX threshold (200)', () => {
      expect(CCD.normalize(200)).toBe(100);
    });

    it('should return a value between 0 and 100 for raw = 105', () => {
      const result = CCD.normalize(105);
      expect(result).toBeGreaterThan(0);
      expect(result).toBeLessThan(100);
      // (105 - 10) / (200 - 10) * 100 = 95/190 * 100 = 50
      expect(result).toBe(50);
    });
  });

  describe('CCD.tier — correct tier boundaries', () => {
    it('0 should be Emerging', () => {
      expect(CCD.tier(0).label).toBe('Emerging');
      expect(CCD.tier(0).color).toBe('#22c55e');
    });

    it('25 should be Emerging (inclusive upper bound)', () => {
      expect(CCD.tier(25).label).toBe('Emerging');
    });

    it('26 should be Competitive', () => {
      expect(CCD.tier(26).label).toBe('Competitive');
      expect(CCD.tier(26).color).toBe('#84cc16');
    });

    it('50 should be Competitive (inclusive upper bound)', () => {
      expect(CCD.tier(50).label).toBe('Competitive');
    });

    it('51 should be Saturated', () => {
      expect(CCD.tier(51).label).toBe('Saturated');
      expect(CCD.tier(51).color).toBe('#f97316');
    });

    it('75 should be Saturated (inclusive upper bound)', () => {
      expect(CCD.tier(75).label).toBe('Saturated');
    });

    it('76 should be Oversaturated', () => {
      expect(CCD.tier(76).label).toBe('Oversaturated');
      expect(CCD.tier(76).color).toBe('#dc2626');
    });

    it('100 should be Oversaturated', () => {
      expect(CCD.tier(100).label).toBe('Oversaturated');
    });

    it('each tier should have bg property', () => {
      expect(CCD.tier(0).bg).toBe('#f0fdf4');
      expect(CCD.tier(30).bg).toBe('#f7fee7');
      expect(CCD.tier(60).bg).toBe('#fff7ed');
      expect(CCD.tier(80).bg).toBe('#fef2f2');
    });
  });

  describe('CCD.init — builds correct lookup maps', () => {
    it('should populate verticalMap from verticals data', () => {
      expect(Object.keys(CCD.verticalMap).length).toBe(SAMPLE_VERTICALS.length);
      expect(CCD.verticalMap['education']).toBeDefined();
      expect(CCD.verticalMap['education'].avg_durability).toBe(5.9);
      expect(CCD.verticalMap['education'].network_count).toBe(4);
      expect(CCD.verticalMap['education'].velocity_7d).toBe(932);
    });

    it('should populate networkMap from networks data', () => {
      expect(Object.keys(CCD.networkMap).length).toBe(SAMPLE_NETWORKS.length);
      expect(CCD.networkMap['Predicto']).toBeDefined();
      expect(CCD.networkMap['Predicto'].avg_durability).toBe(4.7);
    });

    it('should populate cache for all keywords', () => {
      expect(Object.keys(CCD.cache).length).toBe(ALL_KEYWORDS.length);
      ALL_KEYWORDS.forEach(kw => {
        expect(CCD.cache[kw.id]).toBeDefined();
      });
    });
  });

  describe('CCD.init — handles empty data gracefully', () => {
    it('should not crash with empty arrays', () => {
      expect(() => CCD.init([], [], [])).not.toThrow();
      expect(Object.keys(CCD.verticalMap).length).toBe(0);
      expect(Object.keys(CCD.networkMap).length).toBe(0);
      expect(Object.keys(CCD.cache).length).toBe(0);
    });

    it('should not crash with null/undefined arrays', () => {
      expect(() => CCD.init(null, null, null)).not.toThrow();
      expect(() => CCD.init(undefined, undefined, undefined)).not.toThrow();
    });

    it('should reset state on re-init', () => {
      CCD.init(ALL_KEYWORDS, SAMPLE_VERTICALS, SAMPLE_NETWORKS);
      const firstScore = CCD.cache[HIGH_NETWORK_KEYWORD.id].v1.normalized;
      CCD.init(ALL_KEYWORDS, SAMPLE_VERTICALS, SAMPLE_NETWORKS);
      expect(CCD.cache[HIGH_NETWORK_KEYWORD.id].v1.normalized).toBe(firstScore);
    });
  });

  describe('CCD — score distribution and sanity checks', () => {
    it('should produce no NaN scores (v1)', () => {
      Object.values(CCD.cache).forEach(entry => {
        expect(Number.isNaN(entry.v1.raw)).toBe(false);
        expect(Number.isNaN(entry.v1.normalized)).toBe(false);
      });
    });

    it('should produce all scores in 0-100 range (v1)', () => {
      Object.values(CCD.cache).forEach(entry => {
        expect(entry.v1.normalized).toBeGreaterThanOrEqual(0);
        expect(entry.v1.normalized).toBeLessThanOrEqual(100);
      });
    });

    it('should produce at least 2 different tiers across all keywords', () => {
      const tiers = new Set();
      Object.values(CCD.cache).forEach(entry => {
        tiers.add(CCD.tier(entry.v1.normalized).label);
      });
      expect(tiers.size).toBeGreaterThanOrEqual(2);
    });

    it('should not produce identical scores for all keywords (unlike broken durability)', () => {
      const scores = Object.values(CCD.cache).map(e => e.v1.normalized);
      const uniqueScores = new Set(scores);
      expect(uniqueScores.size).toBeGreaterThan(1);
    });

    it('high-network keyword should score higher than low-network keyword', () => {
      const highScore = CCD.cache[HIGH_NETWORK_KEYWORD.id].v1.normalized;
      const lowScore = CCD.cache[LOW_NETWORK_KEYWORD.id].v1.normalized;
      expect(highScore).toBeGreaterThan(lowScore);
    });
  });

  describe('CCD.compute — formula correctness (v1 tuned weights)', () => {
    it('should compute raw score correctly for a known input', () => {
      // Manual calculation for LOW_NETWORK_KEYWORD (immigration, 1 network):
      // networkCount = 1, networks_contrib = 35 * 1 = 35
      // verticals = ['immigration'] → immigration has avg_durability=5.0 → matched=1
      // avgVertDurability = 5.0, vert_dur_contrib = 15 * 5.0 = 75
      // velocity = immigration.velocity_7d = 4, velocity_contrib = 0.03 * 4 = 0.12
      // consensus: immigration.network_count=1 → 1/7 ≈ 0.143, consensus_contrib = 20 * 0.143 ≈ 2.86
      // raw = 35 + 75 + 0.12 + 2.86 ≈ 112.98
      const result = CCD.cache[LOW_NETWORK_KEYWORD.id].v1;
      expect(result.raw).toBeCloseTo(112.98, 0);
    });
  });

  describe('CCD v1 tuned — tier distribution', () => {
    it('0-vertical keyword uses global fallback and scores Emerging', () => {
      // EMPTY_VERTICALS_KEYWORD: network_count=1, verticals=[]
      // Global fallback: avgVertDurability=1.0, velocity=10, avgConsensus=0.3
      // raw = 35*1 + 15*1.0 + 0.03*10 + 20*0.3 = 35+15+0.3+6 = 56.3
      // normalized = ((56.3-10)/190)*100 ≈ 24
      const result = CCD.cache[EMPTY_VERTICALS_KEYWORD.id].v1;
      expect(result.breakdown.global_fallback).toBe(true);
      expect(result.confidence).toBe('low');
      expect(result.normalized).toBeLessThanOrEqual(25); // Emerging tier
      expect(CCD.tier(result.normalized).label).toBe('Emerging');
    });

    it('1-network 1-vertical keyword scores higher than 0-vertical with same network count', () => {
      // LOW_NETWORK_KEYWORD (1 net, 1 vert) vs EMPTY_VERTICALS_KEYWORD (1 net, 0 verts)
      const withVert = CCD.cache[LOW_NETWORK_KEYWORD.id].v1;
      const noVert = CCD.cache[EMPTY_VERTICALS_KEYWORD.id].v1;
      expect(withVert.normalized).toBeGreaterThan(noVert.normalized);
    });

    it('v1 scores distribute across at least 3 tiers with varied keywords', () => {
      const tiers = new Set();
      ALL_KEYWORDS.forEach(kw => {
        tiers.add(CCD.tier(CCD.cache[kw.id].v1.normalized).label);
      });
      // Tuned: empty→Competitive(26), missing→Competitive(39), low→Saturated(54), high→Oversaturated(100)
      expect(tiers.size).toBeGreaterThanOrEqual(3);
    });
  });
});
