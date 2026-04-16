/**
 * CCD (Competitor Campaign Density) Scoring Module — v1 + v2 with shadow mode.
 *
 * v1: Proxy score using network count + vertical-level durability + velocity + consensus.
 * v2: Real per-keyword score using ad_tracking data (avg_ad_duration, max_ad_duration).
 *
 * Shadow mode: computes BOTH scores, displays whichever version is active.
 *
 * Usage (browser):
 *   CCD.init(keywords, verticals, networks);       // shadow mode by default
 *   CCD.activeVersion;                              // 'v1' or 'v2'
 *   CCD.setVersion('v2');                           // swap to v2
 *   var result = CCD.cache[keyword.id];             // { v1: {...}, v2: {...} }
 *   var active = CCD.active(keyword.id);            // returns whichever version is active
 *   var t = CCD.tier(active.normalized);
 *   var report = CCD.comparisonReport();            // { keywords: [...], summary: {...} }
 *
 * Usage (tests — Node/Vitest):
 *   const { CCD } = require('./shared/ccd.js');
 */

var CCD = {
  verticalMap: {},
  networkMap: {},
  cache: {},
  _maxNetDurability: 0,
  activeVersion: 'v1',

  /**
   * Initialize CCD with keyword, vertical, and network data.
   * Computes both v1 and v2 scores for every keyword (shadow mode).
   */
  init: function (keywords, verticals, networks) {
    this.verticalMap = {};
    this.networkMap = {};
    this.cache = {};
    this._maxNetDurability = 0;

    var i;
    if (verticals && verticals.length) {
      for (i = 0; i < verticals.length; i++) {
        var v = verticals[i];
        this.verticalMap[v.vertical] = {
          avg_durability: v.avg_durability || 0,
          network_count: v.network_count || 0,
          velocity_7d: v.velocity_7d || 0,
          total_ads: v.total_ads || 0,
        };
      }
    }

    if (networks && networks.length) {
      for (i = 0; i < networks.length; i++) {
        var n = networks[i];
        var dur = n.avg_durability || 0;
        this.networkMap[n.name] = {
          avg_durability: dur,
          active_ads: n.active_ads || 0,
          velocity_7d: n.velocity_7d || 0,
        };
        if (dur > this._maxNetDurability) {
          this._maxNetDurability = dur;
        }
      }
    }

    // Detect version from URL param (?ccd=v2)
    if (typeof window !== 'undefined' && window.location) {
      try {
        var params = new URLSearchParams(window.location.search);
        var ccdParam = params.get('ccd');
        if (ccdParam === 'v2' || ccdParam === 'v1') {
          this.activeVersion = ccdParam;
        }
      } catch (e) { /* ignore */ }
    }

    if (keywords && keywords.length) {
      for (i = 0; i < keywords.length; i++) {
        var kw = keywords[i];
        this.cache[kw.id] = {
          v1: this.computeV1(kw),
          v2: this.computeV2(kw),
        };
      }
    }
  },

  /**
   * Get the active score for a keyword (based on activeVersion).
   */
  active: function (keywordId) {
    var entry = this.cache[keywordId];
    if (!entry) return null;
    return entry[this.activeVersion] || entry.v1;
  },

  /**
   * Switch active version. Returns the new version string.
   */
  setVersion: function (version) {
    if (version === 'v1' || version === 'v2') {
      this.activeVersion = version;
    }
    return this.activeVersion;
  },

  // ── v1: Proxy score (vertical/network durability) ─────────────────────────

  computeV1: function (keyword) {
    var networkCount = keyword.network_count || 0;
    var verts = keyword.verticals || [];

    var vertDurabilities = [];
    var j;
    for (j = 0; j < verts.length; j++) {
      var vData = this.verticalMap[verts[j]];
      if (vData && vData.avg_durability > 0) {
        vertDurabilities.push(vData.avg_durability);
      }
    }
    var avgVertDurability =
      vertDurabilities.length > 0
        ? vertDurabilities.reduce(function (a, b) { return a + b; }, 0) / vertDurabilities.length
        : 4.5;

    var maxNetDurability = this._maxNetDurability;

    var velocity = 0;
    for (j = 0; j < verts.length; j++) {
      var vd = this.verticalMap[verts[j]];
      if (vd) velocity += vd.velocity_7d || 0;
    }

    var consensusSum = 0;
    var consensusCount = 0;
    for (j = 0; j < verts.length; j++) {
      var vc = this.verticalMap[verts[j]];
      if (vc) {
        consensusSum += (vc.network_count || 0) / 7;
        consensusCount++;
      }
    }
    var avgConsensus = consensusCount > 0 ? consensusSum / consensusCount : 0;

    var networksContrib = 25 * networkCount;
    var vertDurContrib = 20 * avgVertDurability;
    var netDurContrib = 10 * maxNetDurability;
    var velocityContrib = 0.01 * velocity;
    var consensusContrib = 15 * avgConsensus;

    var raw = networksContrib + vertDurContrib + netDurContrib + velocityContrib + consensusContrib;

    var confidence =
      vertDurabilities.length >= 2 ? 'high'
        : vertDurabilities.length === 1 ? 'medium'
          : 'low';

    return {
      version: 'v1',
      raw: Math.round(raw * 100) / 100,
      normalized: this.normalize(raw),
      confidence: confidence,
      breakdown: {
        networks: { count: networkCount, contrib: _r(networksContrib) },
        vert_durability: { avg: _r(avgVertDurability), matched: vertDurabilities.length, contrib: _r(vertDurContrib) },
        net_durability: { max: _r(maxNetDurability), contrib: _r(netDurContrib) },
        velocity: { sum: _r(velocity), contrib: _r(velocityContrib) },
        consensus: { avg: _r(avgConsensus), contrib: _r(consensusContrib) },
      },
    };
  },

  // ── v2: Real per-keyword ad_tracking score ────────────────────────────────

  computeV2: function (keyword) {
    var networkCount = keyword.network_count || 0;
    var at = keyword.ad_tracking;

    // If ad_tracking is missing or empty, fall back to v1
    if (!at || !at.total_unique_ads) {
      var fallback = this.computeV1(keyword);
      fallback.version = 'v2_fallback';
      fallback.confidence = 'low';
      return fallback;
    }

    var avgAdDuration = at.avg_ad_duration_days || 0;
    var maxAdDuration = at.max_ad_duration_days || 0;

    // Velocity from verticals (same as v1 — no per-keyword velocity yet)
    var verts = keyword.verticals || [];
    var velocity = 0;
    var j;
    for (j = 0; j < verts.length; j++) {
      var vd = this.verticalMap[verts[j]];
      if (vd) velocity += vd.velocity_7d || 0;
    }

    // Consensus ratio (same as v1)
    var consensusSum = 0;
    var consensusCount = 0;
    for (j = 0; j < verts.length; j++) {
      var vc = this.verticalMap[verts[j]];
      if (vc) {
        consensusSum += (vc.network_count || 0) / 7;
        consensusCount++;
      }
    }
    var avgConsensus = consensusCount > 0 ? consensusSum / consensusCount : 0;

    // v2 formula: uses REAL per-keyword ad durations
    var networksContrib = 25 * networkCount;
    var avgDurContrib = 20 * avgAdDuration;
    var maxDurContrib = 10 * maxAdDuration;
    var velocityContrib = 0.01 * velocity;
    var consensusContrib = 15 * avgConsensus;

    var raw = networksContrib + avgDurContrib + maxDurContrib + velocityContrib + consensusContrib;

    // v2 confidence based on ad_tracking depth
    var totalAds = at.total_unique_ads || 0;
    var confidence =
      (totalAds >= 10 && networkCount >= 3) ? 'high'
        : (totalAds >= 3 || networkCount >= 2) ? 'medium'
          : 'low';

    return {
      version: 'v2',
      raw: Math.round(raw * 100) / 100,
      normalized: this.normalize(raw),
      confidence: confidence,
      breakdown: {
        networks: { count: networkCount, contrib: _r(networksContrib) },
        avg_ad_duration: { days: _r(avgAdDuration), contrib: _r(avgDurContrib) },
        max_ad_duration: { days: _r(maxAdDuration), contrib: _r(maxDurContrib) },
        velocity: { sum: _r(velocity), contrib: _r(velocityContrib) },
        consensus: { avg: _r(avgConsensus), contrib: _r(consensusContrib) },
      },
    };
  },

  // ── Backwards-compatible compute() — returns active version ───────────────

  compute: function (keyword) {
    return this.activeVersion === 'v2' ? this.computeV2(keyword) : this.computeV1(keyword);
  },

  // ── Shadow comparison report ──────────────────────────────────────────────

  /**
   * Generate a comparison report between v1 and v2 scores.
   * Returns { keywords: [...], summary: { total, deltaUnder5, deltaUnder15, deltaUnder30, deltaOver30 } }
   */
  comparisonReport: function () {
    var keywords = [];
    var deltas = [];
    var ids = Object.keys(this.cache);

    for (var i = 0; i < ids.length; i++) {
      var id = ids[i];
      var entry = this.cache[id];
      if (!entry || !entry.v1 || !entry.v2) continue;

      var v1Score = entry.v1.normalized;
      var v2Score = entry.v2.normalized;
      var delta = v2Score - v1Score;
      var absDelta = Math.abs(delta);
      var v1Tier = this.tier(v1Score).label;
      var v2Tier = this.tier(v2Score).label;

      keywords.push({
        id: id,
        v1: v1Score,
        v2: v2Score,
        delta: delta,
        absDelta: absDelta,
        v1Tier: v1Tier,
        v2Tier: v2Tier,
        tierChanged: v1Tier !== v2Tier,
        v2Version: entry.v2.version,
      });
      deltas.push(absDelta);
    }

    var total = deltas.length;
    var under5 = 0, under15 = 0, under30 = 0, over30 = 0;
    for (var d = 0; d < deltas.length; d++) {
      if (deltas[d] < 5) under5++;
      else if (deltas[d] < 15) under15++;
      else if (deltas[d] < 30) under30++;
      else over30++;
    }

    return {
      keywords: keywords,
      summary: {
        total: total,
        deltaUnder5: under5,
        deltaUnder5Pct: total ? Math.round((under5 / total) * 100) : 0,
        deltaUnder15: under15,
        deltaUnder15Pct: total ? Math.round((under15 / total) * 100) : 0,
        deltaUnder30: under30,
        deltaUnder30Pct: total ? Math.round((under30 / total) * 100) : 0,
        deltaOver30: over30,
        deltaOver30Pct: total ? Math.round((over30 / total) * 100) : 0,
      },
    };
  },

  // ── Shared utilities ──────────────────────────────────────────────────────

  normalize: function (raw) {
    var MIN = 50;
    var MAX = 350;
    return Math.round(
      Math.max(0, Math.min(100, ((raw - MIN) / (MAX - MIN)) * 100))
    );
  },

  tier: function (score) {
    if (score <= 25)
      return { label: 'Emerging', color: '#22c55e', bg: '#f0fdf4' };
    if (score <= 50)
      return { label: 'Competitive', color: '#84cc16', bg: '#f7fee7' };
    if (score <= 75)
      return { label: 'Saturated', color: '#f97316', bg: '#fff7ed' };
    return { label: 'Oversaturated', color: '#dc2626', bg: '#fef2f2' };
  },
};

function _r(n) { return Math.round(n * 100) / 100; }

// Support both browser (global) and Node/Vitest (ESM export)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { CCD: CCD };
}
