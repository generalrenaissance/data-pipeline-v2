import test from 'node:test';
import assert from 'node:assert/strict';

import {
  GHOST_CLEANUP_MAX_PER_WORKSPACE,
  buildGhostCleanupPlan,
  type WorkspaceCampaignRollup,
} from '../src/sync';

const sampleRollups: WorkspaceCampaignRollup[] = [
  {
    campaign_id: 'keep-1',
    campaign_name: 'Keep 1',
    workspace_id: 'equinox',
    status: '1',
    synced_at: '2026-04-16T00:00:00.000Z',
  },
  {
    campaign_id: 'ghost-1',
    campaign_name: 'Ghost 1',
    workspace_id: 'equinox',
    status: '1',
    synced_at: '2026-04-14T00:00:00.000Z',
  },
  {
    campaign_id: 'ghost-2',
    campaign_name: 'Ghost 2',
    workspace_id: 'equinox',
    status: '2',
    synced_at: '2026-04-14T00:00:00.000Z',
  },
];

test('buildGhostCleanupPlan returns only rollups missing from the fetched list', () => {
  const plan = buildGhostCleanupPlan(new Set(['keep-1']), sampleRollups);
  assert.equal(plan.skipReason, null);
  assert.deepEqual(
    plan.missing.map(row => row.campaign_id).sort(),
    ['ghost-1', 'ghost-2'],
  );
});

test('buildGhostCleanupPlan skips cleanup on an empty fetch result', () => {
  const plan = buildGhostCleanupPlan(new Set(), sampleRollups);
  assert.equal(plan.skipReason, 'empty_fetch');
  assert.deepEqual(plan.missing, []);
});

test('buildGhostCleanupPlan refuses cleanup when the missing set exceeds the cap', () => {
  const rollups = Array.from({ length: GHOST_CLEANUP_MAX_PER_WORKSPACE + 1 }, (_, index) => ({
    campaign_id: `ghost-${index}`,
    campaign_name: `Ghost ${index}`,
    workspace_id: 'renaissance-2',
    status: '1',
    synced_at: '2026-04-09T00:00:00.000Z',
  }));

  const plan = buildGhostCleanupPlan(new Set(), rollups, GHOST_CLEANUP_MAX_PER_WORKSPACE);
  assert.equal(plan.skipReason, 'empty_fetch');

  const overCapPlan = buildGhostCleanupPlan(
    new Set(['keep-only']),
    rollups,
    GHOST_CLEANUP_MAX_PER_WORKSPACE,
  );
  assert.equal(overCapPlan.skipReason, 'over_cap');
  assert.equal(overCapPlan.missing.length, GHOST_CLEANUP_MAX_PER_WORKSPACE + 1);
});
