import test from 'node:test';
import assert from 'node:assert/strict';

import {
  GHOST_CLEANUP_MAX_PER_WORKSPACE,
  buildStoredCampaignTags,
  buildGhostCleanupPlan,
  buildCampaignDataWinnerPickPlan,
  type CampaignDataDedupeRow,
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

test('buildStoredCampaignTags keeps raw cached tags for safekeeping only', () => {
  assert.deepEqual(
    buildStoredCampaignTags([' Pair 1 ', 'RG123', 'RG123']),
    ['Pair 1', 'RG123'],
  );
  assert.equal(buildStoredCampaignTags(undefined), null);
  assert.equal(buildStoredCampaignTags(['  ', '\t']), null);
});

// ---------------------------------------------------------------------------
// buildCampaignDataWinnerPickPlan — winner-pick coalesce planner
// ---------------------------------------------------------------------------
//
// Rule under test (must match SQL dedupe migrations 2026-04-26):
//   1. Highest emails_sent (NULLS treated as 0)
//   2. Tiebreak: most recent synced_at (DESC, NULLS LAST)
//   3. Multi-workspace name collisions are NEVER coalesced.
//   4. Loser is only flagged for deletion if its (workspace, name, step,
//      variant) group includes at least one new-row campaign_id (otherwise
//      the collision is among pre-existing rows untouched by this sync).

const allAll = (overrides: Partial<CampaignDataDedupeRow>): CampaignDataDedupeRow => ({
  campaign_id: 'placeholder',
  campaign_name: 'Pair 1 Restaurant',
  workspace_id: 'erc-1',
  step: '__ALL__',
  variant: '__ALL__',
  emails_sent: 0,
  synced_at: null,
  ...overrides,
});

test('winner-pick: highest emails_sent wins, regardless of synced_at', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'fresh-uuid', emails_sent: 0, synced_at: '2026-04-26T21:36:00Z' }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'historical-uuid', emails_sent: 12000, synced_at: '2026-04-26T20:00:00Z' }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, ['fresh-uuid']);
  assert.equal(plan.resolutions.length, 1);
  assert.equal(plan.resolutions[0].winner_id, 'historical-uuid');
});

test('winner-pick: synced_at breaks emails_sent ties (most recent wins)', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'fresh', emails_sent: 500, synced_at: '2026-04-26T22:00:00Z' }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'stale', emails_sent: 500, synced_at: '2026-04-25T10:00:00Z' }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, ['stale']);
  assert.equal(plan.resolutions[0].winner_id, 'fresh');
});

test('winner-pick: NULL emails_sent treated as 0', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'has-sends', emails_sent: 1, synced_at: '2026-04-25T00:00:00Z' }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'null-sends', emails_sent: null, synced_at: '2026-04-26T00:00:00Z' }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, ['null-sends']);
  assert.equal(plan.resolutions[0].winner_id, 'has-sends');
});

test('winner-pick: NULL synced_at sorts last on tiebreak', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'has-time', emails_sent: 5, synced_at: '2026-04-26T00:00:00Z' }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'no-time', emails_sent: 5, synced_at: null }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, ['no-time']);
});

test('winner-pick: multi-workspace name collisions are never coalesced', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'erc1-uuid', workspace_id: 'erc-1', emails_sent: 100 }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'r3-uuid', workspace_id: 'renaissance-3', emails_sent: 999 }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, []);
  assert.equal(plan.resolutions.length, 0);
});

test('winner-pick: pure-existing collision (no new-row campaign_id) is left alone', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'unrelated', campaign_name: 'Other Campaign', emails_sent: 10 }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'old-a', campaign_name: 'Pair 1 Restaurant', emails_sent: 10 }),
    allAll({ campaign_id: 'old-b', campaign_name: 'Pair 1 Restaurant', emails_sent: 5 }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, []);
  assert.equal(plan.resolutions.length, 0);
});

test('winner-pick: same campaign_id present in both new and existing is not a collision', () => {
  const sharedId = 'same-uuid';
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: sharedId, emails_sent: 250, synced_at: '2026-04-26T22:00:00Z' }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: sharedId, emails_sent: 240, synced_at: '2026-04-26T21:00:00Z' }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, []);
  assert.equal(plan.resolutions.length, 0);
});

test('winner-pick: handles N-way collision and emits all losers', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'fresh', emails_sent: 0, synced_at: '2026-04-26T22:00:00Z' }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'mid', emails_sent: 50, synced_at: '2026-04-26T20:00:00Z' }),
    allAll({ campaign_id: 'big', emails_sent: 5000, synced_at: '2026-04-26T19:00:00Z' }),
    allAll({ campaign_id: 'tiny', emails_sent: 1, synced_at: '2026-04-26T21:00:00Z' }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.equal(plan.resolutions.length, 1);
  assert.equal(plan.resolutions[0].winner_id, 'big');
  assert.deepEqual(plan.loserCampaignIds.sort(), ['fresh', 'mid', 'tiny']);
});

test('winner-pick: campaign_name with embedded spaces does not corrupt grouping', () => {
  // Regression for an earlier delimiter-based key that split on spaces.
  // The campaign_name "Pair 12 RG3458 General A" has 4 internal spaces.
  const newRows: CampaignDataDedupeRow[] = [
    allAll({
      campaign_id: 'fresh',
      campaign_name: 'Pair 12 RG3458 General A',
      workspace_id: 'erc-1',
      emails_sent: 0,
      synced_at: '2026-04-26T22:00:00Z',
    }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({
      campaign_id: 'old',
      campaign_name: 'Pair 12 RG3458 General A',
      workspace_id: 'erc-1',
      emails_sent: 5000,
      synced_at: '2026-04-25T22:00:00Z',
    }),
    // Different name that shares a prefix — must NOT collide.
    allAll({
      campaign_id: 'unrelated',
      campaign_name: 'Pair 12 RG3458 General B',
      workspace_id: 'erc-1',
      emails_sent: 9999,
    }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  assert.deepEqual(plan.loserCampaignIds, ['fresh']);
  assert.equal(plan.resolutions.length, 1);
  assert.equal(plan.resolutions[0].campaign_name, 'Pair 12 RG3458 General A');
  assert.equal(plan.resolutions[0].winner_id, 'old');
});

test('winner-pick: per-step rows are partitioned independently from __ALL__', () => {
  const newRows: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'fresh', emails_sent: 0 }),
    allAll({ campaign_id: 'fresh', step: '1', variant: 'A', emails_sent: 0 }),
  ];
  const existing: CampaignDataDedupeRow[] = [
    allAll({ campaign_id: 'old', emails_sent: 100 }),
    allAll({ campaign_id: 'old', step: '1', variant: 'A', emails_sent: 80 }),
    // Same campaign_name but step=2 — different group, no collision.
    allAll({ campaign_id: 'unrelated', step: '2', variant: 'A', emails_sent: 999 }),
  ];
  const plan = buildCampaignDataWinnerPickPlan(newRows, existing);
  // 'fresh' loses to 'old' on both __ALL__ and step=1.
  assert.deepEqual(plan.loserCampaignIds.sort(), ['fresh']);
  // Two resolutions: one per step/variant tuple where 'fresh' collides.
  assert.equal(plan.resolutions.length, 2);
});
