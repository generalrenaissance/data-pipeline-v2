import test from 'node:test';
import assert from 'node:assert/strict';

import {
  normalizeCmName,
  parseCmName,
  resolveCampaignManager,
  resolveCmFromTags,
  WORKSPACE_CM_DEFAULTS,
  WORKSPACE_CM_HARD_RULES,
} from '../src/transforms';

const parserCases: Array<{ input: string; expected: string | null }> = [
  { input: 'ON - PAIR 13,14 - RESTAURANT - (SAM) X NM', expected: 'SAM' },
  { input: 'ON - PAIR 8 - General 2 (MARCOS) NM', expected: 'MARCOS' },
  { input: '3485-3489 - Adv(TOMI) (copy) x MA', expected: 'TOMI' },
  { input: 'OFF - General S PAIR 31 (ANDRES) Y', expected: 'ANDRES' },
  { input: 'OLD - Pair 21 - Financial Center USA - Beauty (LEO)_NP', expected: 'LEO' },
  { input: 'OLD - 1O - CONSTRUCTION (SAMUEL)', expected: 'SAMUEL' },
  { input: 'ON - PAIR 19 - CEO (BRENDAN) MD', expected: 'BRENDAN' },
  { input: 'OFF - Pair - General (Alex) (copy) (copy)', expected: 'ALEX' },
  { input: 'ON - Chiro - RG4514 - Alex (copy)', expected: 'ALEX' },
  { input: 'OFF 898-903 - Alex - Presidents 4- (Bentora Capital) (copy)', expected: 'ALEX' },
  { input: 'OFF - B - ACCOUNTING R496 - LAUTARO', expected: 'LAUTARO' },
  { input: 'OLD - NEW - A - PROP MANAGEMENT - LAUTARO (copy) (copy)', expected: 'LAUTARO' },
  { input: 'General GMAPS - Pair 8 - Quickcred - SHAAN', expected: 'SHAAN' },
  { input: 'OLD - CLEANING Pair 5 (ANDRES) X', expected: 'ANDRES' },
  { input: 'ON - PAIR 1 - RESTAURANTS - (SAM) X', expected: 'SAM' },
  { input: 'OLD - PAIR 2 - General A (ANDRES) NO TAGS, TO RE LAUNCH', expected: 'ANDRES' },
  { input: 'ON - HVAC - RG4521 - Alex (copy)', expected: 'ALEX' },
  { input: 'Big Think Capital - App missing', expected: null },
  { input: 'My Campaign', expected: null },
  { input: 'HealthSphere - w/Employees', expected: null },
  { input: 'Ellen - GBC (Application Out) NP', expected: null },
  { input: 'RG3555 - TBD(TOMI) x MA', expected: 'TOMI' },
];

test('parseCmName handles live parser shapes from Spec 5', () => {
  for (const { input, expected } of parserCases) {
    assert.equal(parseCmName(input), expected, input);
  }
});

test('normalizeCmName maps aliases to the canonical CM list', () => {
  const aliases: Record<string, string> = {
    SAMUEL: 'SAMUEL',
    "Samuel's Campaigns": 'SAMUEL',
    MARCO: 'MARCOS',
    "Marco's Campaigns": 'MARCOS',
    ANDRE: 'ANDRES',
    "Andre's Campaigns": 'ANDRES',
    "Andres's Campaigns": 'ANDRES',
    "Carlos' Campaigns": 'CARLOS',
    'Eyver Campaigns': 'EYVER',
    "Leo's Campaigns OFF": 'LEO',
    'LAUTARO CAMPAIGNS': 'LAUTARO',
    "LAUTARO'S CAMPAIGNS": 'LAUTARO',
  };

  for (const [raw, expected] of Object.entries(aliases)) {
    assert.equal(normalizeCmName(raw), expected, raw);
  }
});

test('resolveCmFromTags only returns a CM for unambiguous ownership tags', () => {
  assert.equal(resolveCmFromTags(["Alex's Campaigns"]), 'ALEX');
  assert.equal(resolveCmFromTags(["Samuel's Campaigns", 'RG2213']), 'SAMUEL');
  assert.equal(resolveCmFromTags(["Leo's Campaigns OFF", 'Pair 21']), 'LEO');
  assert.equal(resolveCmFromTags(["Alex's Campaigns", "Andres's Campaigns"]), null);
  assert.equal(resolveCmFromTags(['RG2213', 'Pair 21']), null);
});

test('resolveCampaignManager applies the canonical resolution order', () => {
  assert.equal(
    resolveCampaignManager('renaissance-3', 'OLD - Pair 3 - Healthcare (TOMI)', ["Tomi's Campaigns"]),
    'SAM',
  );
  assert.equal(
    resolveCampaignManager('renaissance-6', 'ON - Pair 1 - Retail - (SAM) X William', []),
    'SAM',
  );
  assert.equal(
    resolveCampaignManager('erc-1', 'ON - Pair 12 - BTC/GQ - (SAM) X William', []),
    'SAM',
  );
  assert.equal(
    resolveCampaignManager('erc-1', 'Advertising - Google + Others (IDO)', []),
    'IDO',
  );
  assert.equal(
    resolveCampaignManager('renaissance-1', 'OFF - General S PAIR 31 (ANDRES) Y', []),
    'ANDRES',
  );
  assert.equal(
    resolveCampaignManager('outlook-2', 'OLD - Pair 21 - Financial Center USA - Beauty', ["Leo's Campaigns"]),
    'LEO',
  );
  assert.equal(
    resolveCampaignManager('the-eagles', '[OLD] - Pair 9 - GYM (SAMUEL) NP', []),
    'SAMUEL',
  );
  assert.equal(
    resolveCampaignManager('renaissance-6', 'ON - Pair 2 - SHOPS/RETAIL (SAMUEL) NP', []),
    'SAMUEL',
  );
  assert.equal(
    resolveCampaignManager('renaissance-2', 'Audience Lab - Intent', []),
    'EYVER',
  );
  assert.equal(
    resolveCampaignManager('warm-leads', 'Big Think Capital - App missing', []),
    null,
  );
});

test('workspace defaults reflect the safe mixed-workspace changes from the spec', () => {
  assert.equal(WORKSPACE_CM_HARD_RULES['renaissance-3'], 'SAM');
  assert.equal(WORKSPACE_CM_HARD_RULES['renaissance-6'], 'SAM');
  assert.equal(WORKSPACE_CM_DEFAULTS['renaissance-1'], null);
  assert.equal(WORKSPACE_CM_DEFAULTS['outlook-2'], null);
  assert.equal(WORKSPACE_CM_DEFAULTS['the-eagles'], null);
  assert.equal(WORKSPACE_CM_DEFAULTS['renaissance-2'], 'EYVER');
  assert.equal(WORKSPACE_CM_DEFAULTS['koi-and-destroy'], 'TOMI');
});
