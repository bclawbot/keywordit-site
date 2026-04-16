/**
 * Intel Keyword Filters — Pure filter logic for the Keywords tab.
 *
 * Provides filterKeywords() which applies search, column filters,
 * and numeric range filters to an array of keyword objects.
 *
 * Used by dashboard.html's ikwApplyFilters() and by Vitest tests.
 */

var IntelFilters = {

  /**
   * Apply all filters to the keyword dataset.
   *
   * @param {Array} data - Full keyword array (IKW_DATA)
   * @param {Object} state - Filter state: { search, colFilters, numFilters }
   *   colFilters: { column_name: ['allowed1', 'allowed2', ...] }
   *   numFilters: { column_name: { min, max, label } }
   * @param {Object} [opts] - Optional: { ccdCache, dateFrom, dateTo }
   * @returns {Array} Filtered keyword array
   */
  filterKeywords: function (data, state, opts) {
    if (!data || !data.length) return [];
    opts = opts || {};
    var filtered = data.slice();
    var search = (state.search || '').toLowerCase();
    var colFilters = state.colFilters || {};
    var numFilters = state.numFilters || {};

    // Text search
    if (search) {
      filtered = filtered.filter(function (k) {
        var kw = (k.keyword || '').toLowerCase();
        var verts = (k.verticals || []).join(' ').toLowerCase();
        var topAngle = k.angles && k.angles[0] ? (k.angles[0].type || '').toLowerCase() : '';
        return kw.indexOf(search) !== -1 || verts.indexOf(search) !== -1 || topAngle.indexOf(search) !== -1;
      });
    }

    // Date range filter
    if (opts.dateFrom || opts.dateTo) {
      var df = opts.dateFrom || '';
      var dt = opts.dateTo || '';
      filtered = filtered.filter(function (k) {
        var kDate = (k.first_seen || k.created_at || '').slice(0, 10);
        if (!kDate) return true;
        if (df && kDate < df) return false;
        if (dt && kDate > dt) return false;
        return true;
      });
    }

    // Column filters (enum / array membership)
    var colKeys = Object.keys(colFilters);
    for (var ci = 0; ci < colKeys.length; ci++) {
      var cKey = colKeys[ci];
      var allowed = colFilters[cKey];
      if (!allowed || !allowed.length) continue;
      filtered = filtered.filter(function (k) {
        var raw = IntelFilters._colValRaw(k, cKey, opts.ccdCache);
        if (Array.isArray(raw)) {
          if (!raw.length) return allowed.indexOf('') !== -1;
          for (var ai = 0; ai < raw.length; ai++) {
            if (allowed.indexOf(String(raw[ai])) !== -1) return true;
          }
          return false;
        }
        return allowed.indexOf(String(raw)) !== -1;
      });
    }

    // Numeric range filters
    var numKeys = Object.keys(numFilters);
    for (var ni = 0; ni < numKeys.length; ni++) {
      var nKey = numKeys[ni];
      var range = numFilters[nKey];
      if (!range) continue;
      filtered = filtered.filter(function (k) {
        var val = IntelFilters._colValRaw(k, nKey, opts.ccdCache);
        if (val == null) val = 0;
        val = Number(val) || 0;
        if (range.min != null && val < range.min) return false;
        if (range.max != null && val > range.max) return false;
        return true;
      });
    }

    return filtered;
  },

  /**
   * Get raw column value for a keyword (mirrors ikwColValRaw in dashboard.html).
   */
  _colValRaw: function (k, col, ccdCache) {
    if (col === 'verticals') return k.verticals || [];
    if (col === 'validation_status') return k.validation_status || 'pending';
    if (col === 'angle_count') return (k.angles || []).length;
    if (col === 'top_angle_type') return k.angles && k.angles[0] ? k.angles[0].type || '' : '';
    if (col === 'ccd_score') {
      if (ccdCache && ccdCache[k.id]) {
        // Support both old ({normalized}) and new ({v1:{normalized}, v2:{normalized}}) cache shapes
        var entry = ccdCache[k.id];
        if (entry.normalized != null) return entry.normalized;
        // New shadow-mode shape: use CCD.active() if available, else v1
        var active = (typeof CCD !== 'undefined' && CCD.active) ? CCD.active(k.id) : (entry.v1 || entry);
        return active ? active.normalized : 0;
      }
      return 0;
    }
    return k[col];
  },

  /**
   * Parse bar filter selections into colFilters/numFilters state.
   *
   * @param {Object} selections - { vertical, status, angle, ccd, netmin }
   * @returns {Object} { colFilters, numFilters }
   */
  parseBarSelections: function (selections) {
    var colFilters = {};
    var numFilters = {};

    if (selections.vertical) {
      colFilters['verticals'] = [selections.vertical];
    }
    if (selections.status) {
      colFilters['validation_status'] = [selections.status];
    }
    if (selections.angle) {
      colFilters['top_angle_type'] = [selections.angle];
    }
    if (selections.ccd) {
      var ccdRanges = {
        'emerging': { min: 0, max: 25, label: 'Emerging (0-25)' },
        'competitive': { min: 26, max: 50, label: 'Competitive (26-50)' },
        'saturated': { min: 51, max: 75, label: 'Saturated (51-75)' },
        'oversaturated': { min: 76, max: null, label: 'Oversaturated (76-100)' }
      };
      if (ccdRanges[selections.ccd]) {
        numFilters['ccd_score'] = ccdRanges[selections.ccd];
      }
    }
    if (selections.netmin) {
      numFilters['network_count'] = { min: parseInt(selections.netmin), max: null, label: selections.netmin + '+' };
    }

    return { colFilters: colFilters, numFilters: numFilters };
  },

  /**
   * Extract unique values for dropdown population.
   *
   * @param {Array} data - Keyword array
   * @returns {Object} { verticals: [...], angleTypes: [...] }
   */
  extractDropdownOptions: function (data) {
    var vertSet = {};
    var angleSet = {};
    for (var i = 0; i < data.length; i++) {
      var vs = data[i].verticals || [];
      for (var j = 0; j < vs.length; j++) {
        if (vs[j]) vertSet[vs[j]] = (vertSet[vs[j]] || 0) + 1;
      }
      var angles = data[i].angles || [];
      if (angles.length && angles[0].type) {
        var aType = angles[0].type;
        angleSet[aType] = (angleSet[aType] || 0) + 1;
      }
    }
    return {
      verticals: Object.keys(vertSet).sort().map(function (v) { return { value: v, count: vertSet[v] }; }),
      angleTypes: Object.keys(angleSet).sort().map(function (a) { return { value: a, count: angleSet[a] }; })
    };
  },

  /**
   * Check if any filters are currently active.
   */
  hasActiveFilters: function (state) {
    return !!(
      state.search ||
      Object.keys(state.colFilters || {}).length > 0 ||
      Object.keys(state.numFilters || {}).length > 0
    );
  }
};

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { IntelFilters: IntelFilters };
}
