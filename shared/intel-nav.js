/**
 * Intel Cross-Tab Navigation — Builds filter state for navigateToKeywords().
 *
 * Pure logic for merging navigation filters with existing keyword filters.
 * Used by dashboard.html's navigateToKeywords() and by Vitest tests.
 *
 * Usage (browser):
 *   var merged = IntelNav.mergeFilters(currentState, { vertical: 'education', angle: 'listicle' });
 *   // merged = { colFilters: { verticals: ['education'], top_angle_type: ['listicle'], ...existing }, numFilters: {...existing} }
 *
 * Usage (tests):
 *   const { IntelNav } = require('./shared/intel-nav.js');
 */

var IntelNav = {

  /**
   * Merge navigation params into existing filter state.
   * Navigation params OVERRIDE conflicting filters but PRESERVE non-conflicting ones.
   *
   * @param {Object} currentState - { colFilters: {...}, numFilters: {...}, search: '' }
   * @param {Object} navParams - { vertical, angle, status, ccdTier }
   * @returns {Object} New state: { colFilters, numFilters, search }
   */
  mergeFilters: function (currentState, navParams) {
    var colFilters = {};
    var numFilters = {};

    // Copy existing filters
    var ck = Object.keys(currentState.colFilters || {});
    for (var i = 0; i < ck.length; i++) {
      colFilters[ck[i]] = (currentState.colFilters[ck[i]] || []).slice();
    }
    var nk = Object.keys(currentState.numFilters || {});
    for (var j = 0; j < nk.length; j++) {
      var nf = currentState.numFilters[nk[j]];
      numFilters[nk[j]] = nf ? { min: nf.min, max: nf.max, label: nf.label } : nf;
    }

    // Apply navigation overrides
    if (navParams.vertical) {
      colFilters['verticals'] = [navParams.vertical];
    }
    if (navParams.angle) {
      colFilters['top_angle_type'] = [navParams.angle];
    }
    if (navParams.status) {
      colFilters['validation_status'] = [navParams.status];
    }
    if (navParams.ccdTier) {
      var ccdRanges = {
        'emerging': { min: 0, max: 25, label: 'Emerging (0-25)' },
        'competitive': { min: 26, max: 50, label: 'Competitive (26-50)' },
        'saturated': { min: 51, max: 75, label: 'Saturated (51-75)' },
        'oversaturated': { min: 76, max: null, label: 'Oversaturated (76-100)' },
      };
      if (ccdRanges[navParams.ccdTier]) {
        numFilters['ccd_score'] = ccdRanges[navParams.ccdTier];
      }
    }

    return {
      colFilters: colFilters,
      numFilters: numFilters,
      search: currentState.search || '',
    };
  },

  /**
   * Extract navigation params from a War Room network row.
   * Uses the network's top verticals to filter keywords.
   *
   * @param {Object} network - { name, top_verticals: [...], ... }
   * @returns {Object} navParams for mergeFilters
   */
  paramsFromNetwork: function (network) {
    var topVerts = network.top_verticals || [];
    if (topVerts.length > 0) {
      return { vertical: topVerts[0] };
    }
    return {};
  },

  /**
   * Extract navigation params from a Verticals leaderboard card.
   *
   * @param {string} verticalName
   * @returns {Object} navParams
   */
  paramsFromVertical: function (verticalName) {
    return { vertical: verticalName };
  },

  /**
   * Extract navigation params from a Matrix cell.
   *
   * @param {string} vertical
   * @param {string} angle
   * @param {boolean} isEmpty - true if the cell is a gap
   * @returns {Object|null} navParams, or null if cell is empty
   */
  paramsFromMatrixCell: function (vertical, angle, isEmpty) {
    if (isEmpty) return null;
    return { vertical: vertical, angle: angle };
  },
};

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { IntelNav: IntelNav };
}
