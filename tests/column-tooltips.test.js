/**
 * Bug #14 — Column Tooltips Tests
 *
 * Verifies that all metric columns in the Intel Keywords table
 * have tooltip (title) attributes with descriptive text.
 *
 * Run: npm test
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

let html;

beforeAll(() => {
  html = readFileSync(resolve(__dirname, '..', 'dashboard.html'), 'utf-8');
});

describe('Bug #14 — Column tooltips', () => {
  const expectedTooltips = {
    network_count: 'Number of ad networks where this keyword appears in competitor campaigns',
    max_durability: 'Average number of days ads for this keyword stay active',
    kd: 'Keyword Difficulty',
    rsoc_score: 'Revenue per Search on Outclick',
    ccd_score: 'Competitor Campaign Density',
  };

  it('all metric columns have tooltip text', () => {
    for (const [col, expectedSubstring] of Object.entries(expectedTooltips)) {
      // Find the <th> tag with data-col="..."
      const pattern = new RegExp(
        `<th[^>]*data-col="${col}"[^>]*title="([^"]+)"`,
      );
      const match = html.match(pattern);
      expect(match, `Column ${col} should have a title attribute`).not.toBeNull();
      expect(match[1]).toContain(expectedSubstring);
    }
  });

  it('Networks column tooltip mentions "ad networks"', () => {
    const match = html.match(/data-col="network_count"[^>]*title="([^"]+)"/);
    expect(match).not.toBeNull();
    expect(match[1].toLowerCase()).toContain('ad networks');
  });

  it('Durability column tooltip mentions "days"', () => {
    const match = html.match(/data-col="max_durability"[^>]*title="([^"]+)"/);
    expect(match).not.toBeNull();
    expect(match[1].toLowerCase()).toContain('days');
  });

  it('KD column tooltip mentions "0-100"', () => {
    const match = html.match(/data-col="kd"[^>]*title="([^"]+)"/);
    expect(match).not.toBeNull();
    expect(match[1]).toContain('0-100');
  });

  it('RSOC column tooltip mentions "monetization"', () => {
    const match = html.match(/data-col="rsoc_score"[^>]*title="([^"]+)"/);
    expect(match).not.toBeNull();
    expect(match[1].toLowerCase()).toContain('monetization');
  });

  it('CCD column tooltip mentions "Beta"', () => {
    const match = html.match(/data-col="ccd_score"[^>]*title="([^"]+)"/);
    expect(match).not.toBeNull();
    expect(match[1]).toContain('Beta');
  });
});
