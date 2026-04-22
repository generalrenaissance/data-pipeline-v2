import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildResolverContext,
  decodeHtmlEntities,
  normalizeMeetingName,
  resolveCampaignName,
  type CampaignRecord,
} from '../src/meetings-matcher';

const campaigns: CampaignRecord[] = [
  { campaign_id: 'kd5', name: 'OLD - KD5 - Fundora - CEOs(TOMI)' },
  { campaign_id: 'health5', name: 'OLD - Health Pair 5 (ANDRES) X' },
  { campaign_id: 'qualify', name: 'old ✅ RG49/RG50/RG51 - Qualify - Construction (CARLOS)' },
  { campaign_id: 'hvac-np', name: 'ON - PAIR 4 - HVAC (MARCOS)_NP' },
  { campaign_id: 'auto-ido', name: 'Auto - Google + others (IDO)' },
  { campaign_id: 'outlook-ido', name: 'OLD - Construction 2 - Outlook (IDO)' },
  { campaign_id: 'pair-5', name: 'ON - CLEANING Pair 5 (ANDRES) X' },
  { campaign_id: 'credora-smb', name: 'RG998-RG1001 - Credora - SMB 1188 - (EYVER)' },
  { campaign_id: 'eyver', name: 'ON - Pair 3 - Healthcare (EYVER)' },
  { campaign_id: 'tomi', name: 'ON - Pair 3 - Healthcare (TOMI)' },
  { campaign_id: 'roof-plumb', name: 'ON - Pair 13 - Roofing & Plumbing (Marcos)' },
];

test('strict matcher accepts state-prefix-only differences', () => {
  const context = buildResolverContext(campaigns, []);

  assert.deepEqual(resolveCampaignName('KD5 - Fundora - CEOs(TOMI)', context), {
    kind: 'match',
    rawName: 'KD5 - Fundora - CEOs(TOMI)',
    campaignId: 'kd5',
    campaignName: 'OLD - KD5 - Fundora - CEOs(TOMI)',
    matchMethod: 'strict_auto',
    matchConfidence: 0.98,
  });

  assert.deepEqual(resolveCampaignName('ON - Health Pair 5 (ANDRES) X', context), {
    kind: 'match',
    rawName: 'ON - Health Pair 5 (ANDRES) X',
    campaignId: 'health5',
    campaignName: 'OLD - Health Pair 5 (ANDRES) X',
    matchMethod: 'strict_auto',
    matchConfidence: 0.98,
  });

  assert.deepEqual(resolveCampaignName('RG49/RG50/RG51 - Qualify - Construction (CARLOS)', context), {
    kind: 'match',
    rawName: 'RG49/RG50/RG51 - Qualify - Construction (CARLOS)',
    campaignId: 'qualify',
    campaignName: 'old ✅ RG49/RG50/RG51 - Qualify - Construction (CARLOS)',
    matchMethod: 'strict_auto',
    matchConfidence: 0.98,
  });
});

test('alias matcher handles approved non-prefix mappings', () => {
  const context = buildResolverContext(campaigns, [
    { alias: 'Construction 2 - Outlook', campaign_id: 'outlook-ido' },
  ]);

  const resolution = resolveCampaignName('Construction 2 - Outlook', context);
  assert.equal(resolution.kind, 'match');
  if (resolution.kind !== 'match') return;
  assert.equal(resolution.matchMethod, 'alias');
  assert.equal(resolution.campaignId, 'outlook-ido');
});

test('strict matcher rejects extra suffixes and falls back to manual queue', () => {
  const context = buildResolverContext(campaigns, []);

  const hvac = resolveCampaignName('ON - PAIR 4 - HVAC (MARCOS)', context);
  assert.equal(hvac.kind, 'queue');
  if (hvac.kind !== 'queue') return;
  assert.equal(hvac.queueReason, 'no_match');

  const auto = resolveCampaignName('Auto - Google + others', context);
  assert.equal(auto.kind, 'queue');
  if (auto.kind !== 'queue') return;
  assert.equal(auto.queueReason, 'no_match');
});

test('hard rejects block different pair, same-rg-different-product, and different CM matches', () => {
  const context = buildResolverContext(campaigns, []);

  const pairMismatch = resolveCampaignName('ON - CLEANING Pair 3 (ANDRES) X', context);
  assert.equal(pairMismatch.kind, 'queue');
  if (pairMismatch.kind !== 'queue') return;
  assert.equal(pairMismatch.queueReason, 'hard_reject');

  const rgMismatch = resolveCampaignName('RG998-RG1001 - Credora - MCA - (EYVER)', context);
  assert.equal(rgMismatch.kind, 'queue');
  if (rgMismatch.kind !== 'queue') return;
  assert.equal(rgMismatch.queueReason, 'hard_reject');

  const cmMismatch = resolveCampaignName('ON - Pair 3 - Healthcare (LEO)', context);
  assert.equal(cmMismatch.kind, 'queue');
  if (cmMismatch.kind !== 'queue') return;
  assert.equal(cmMismatch.queueReason, 'hard_reject');
});

test('linkedin rows are ignored', () => {
  const context = buildResolverContext(campaigns, []);
  assert.deepEqual(resolveCampaignName('LinkedIn - Pair 1 - General', context), {
    kind: 'ignore',
    rawName: 'LinkedIn - Pair 1 - General',
    queueReason: 'ignored_linkedin',
  });
});

test('normalization handles html entities, dash variants, and ampersand spacing', () => {
  assert.equal(decodeHtmlEntities('Roofing &amp; Plumbing &#39;A&#39;'), "Roofing & Plumbing 'A'");
  assert.equal(
    normalizeMeetingName('ON\u2013 Pair 13 \u2013 Roofing &amp; Plumbing (Marcos)'),
    'ON- Pair 13 - Roofing & Plumbing (Marcos)',
  );

  const context = buildResolverContext(campaigns, []);
  const resolution = resolveCampaignName('ON- Pair 13 - Roofing &amp; Plumbing (Marcos)', context);
  assert.equal(resolution.kind, 'match');
});
