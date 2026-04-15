import test from 'node:test';
import assert from 'node:assert/strict';

import { extractSegmentFromName } from '../src/transforms';

test('maps HS-Jessica to home_services', () => {
  assert.equal(
    extractSegmentFromName('ON - PAIR 9 and 3 - HOME SERVICES - (SAM) X Jessica'),
    'home_services',
  );
});

test('covers new keyword buckets added by the segment audit fix', () => {
  assert.equal(extractSegmentFromName('ON - Pair 11 - ACCOUNTING - (EYVER)'), 'finance_accounting');
  assert.equal(extractSegmentFromName('ON - Pair 10 - Manufacturing (CARLOS)'), 'manufacturing');
  assert.equal(extractSegmentFromName('OFF - Pair 6 - Consulting (Work) - Capflux - (SHAAN)'), 'consulting');
  assert.equal(extractSegmentFromName('Elevate Benefits - Non profit 1 (IDO)'), 'nonprofit');
  assert.equal(extractSegmentFromName('OLD - KD10 - Fundora - ECOM(TOMI)'), 'ecommerce');
  assert.equal(extractSegmentFromName('Spanish-speaking owners - test'), 'spanish_speaking');
});

test('widens synonym coverage for existing buckets without regressing general', () => {
  assert.equal(extractSegmentFromName('ON - Pair 12 - Home Services (Marcos)'), 'home_services');
  assert.equal(extractSegmentFromName('Mental Health - Pair 9 - RG2253/RG2254 (BRENDAN)'), 'healthcare');
  assert.equal(extractSegmentFromName('ON - Pair 13 - Shops (CARLOS)'), 'retail');
  assert.equal(extractSegmentFromName('ON - A - COURIER - LAUTARO'), 'trucking');
  assert.equal(extractSegmentFromName('GENERAL - William'), 'general');
});

test('routes legacy home improvement names into home_services', () => {
  assert.equal(
    extractSegmentFromName('OLD - KD2 - BrightFunds - Home Improvement(TOMI)'),
    'home_services',
  );
  assert.equal(
    extractSegmentFromName('OLD - Pair 17 - Fieldstone Growth - Home Improvement (LEO)'),
    'home_services',
  );
  assert.equal(
    extractSegmentFromName('OLD - B - HOME IMPROVEMENT - LAUTARO'),
    'home_services',
  );
});

test('preserves clean Presidents and CEOs matches while industry keywords still win first', () => {
  assert.equal(extractSegmentFromName('ALL CEOs USA - 10-50,000 employees 5'), 'ceos');
  assert.equal(extractSegmentFromName('Presidents - 1'), 'presidents');
  assert.equal(
    extractSegmentFromName('Owner/Founder/CEO Self Employed Landscaping'),
    'landscaping',
  );
});

test('keeps promoted unapproved segments mapped in the classifier', () => {
  assert.equal(extractSegmentFromName('Pair X - Cleaning - (CM) Y'), 'cleaning');
  assert.equal(extractSegmentFromName('Pair X - Advertising - (CM) Y'), 'advertising');
  assert.equal(extractSegmentFromName('Pair X - Automotive - (CM) Y'), 'automotive');
  assert.equal(extractSegmentFromName('Pair X - Insurance - (CM) Y'), 'insurance');
});
