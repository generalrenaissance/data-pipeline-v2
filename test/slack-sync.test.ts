import test from 'node:test';
import assert from 'node:assert/strict';

import { buildResolverContext, buildResolvedQueuePatch, type CampaignRecord } from '../src/meetings-matcher';
import { applyManualResolutions } from '../src/slack-sync';

const campaigns: CampaignRecord[] = [
  { campaign_id: 'live-1', name: 'ON - PAIR 1 - RETAIL - (SAM) X William' },
  { campaign_id: 'live-2', name: 'ON - PAIR 14 - OUTLOOK - AURORA ACCOUNTS - GENERAL 1 - (EYVER)' },
];

function createDb(overrides?: {
  queueRows?: unknown[];
}) {
  const calls = {
    selectAll: [] as Array<[string, string]>,
    upsert: [] as Array<[string, unknown[], string]>,
    update: [] as Array<[string, string, Record<string, unknown>]>,
  };

  return {
    calls,
    db: {
      async selectAll(table: string, params: string) {
        calls.selectAll.push([table, params]);
        if (table === 'meetings_unmatched_queue') {
          return overrides?.queueRows ?? [];
        }
        throw new Error(`unexpected selectAll ${table}`);
      },
      async upsert(table: string, rows: unknown[], onConflict: string) {
        calls.upsert.push([table, rows, onConflict]);
      },
      async update(table: string, params: string, patch: Record<string, unknown>) {
        calls.update.push([table, params, patch]);
      },
      async rpc() {
        return null;
      },
    },
  };
}

test('buildResolvedQueuePatch marks auto-resolved rows applied immediately', () => {
  const patch = buildResolvedQueuePatch('live-1');
  assert.equal(patch.review_status, 'resolved');
  assert.equal(patch.resolved_campaign_id, 'live-1');
  assert.equal(typeof patch.applied_at, 'string');
});

test('applyManualResolutions upserts alias, patches raw rows, and marks queue applied', async () => {
  const { db, calls } = createDb({
    queueRows: [
      {
        campaign_name_raw: 'P1 Retail Sam',
        resolved_campaign_id: 'live-1',
        review_status: 'resolved',
        applied_at: null,
      },
    ],
  });
  const context = buildResolverContext(campaigns, []);

  const applied = await applyManualResolutions(db as any, context);

  assert.equal(applied, 1);
  assert.equal(calls.upsert.length, 1);
  assert.deepEqual(calls.upsert[0][0], 'campaign_aliases');
  assert.deepEqual(calls.upsert[0][2], 'alias');
  assert.deepEqual(calls.update[0][0], 'meetings_booked_raw');
  assert.match(calls.update[0][1], /campaign_name_raw=eq\.P1%20Retail%20Sam&campaign_id=is\.null/);
  assert.equal(calls.update[0][2].campaign_id, 'live-1');
  assert.equal(calls.update[0][2].match_method, 'manual_alias');
  assert.equal(calls.update[1][0], 'meetings_unmatched_queue');
  assert.equal(typeof calls.update[1][2].applied_at, 'string');
});

test('applyManualResolutions skips invalid campaign ids and leaves queue unapplied', async () => {
  const { db, calls } = createDb({
    queueRows: [
      {
        campaign_name_raw: 'Unknown Raw',
        resolved_campaign_id: 'missing',
        review_status: 'resolved',
        applied_at: null,
      },
    ],
  });
  const context = buildResolverContext(campaigns, []);

  const applied = await applyManualResolutions(db as any, context);

  assert.equal(applied, 0);
  assert.equal(calls.upsert.length, 0);
  assert.equal(calls.update.length, 0);
});

test('applyManualResolutions is idempotent when no unapplied resolved rows exist', async () => {
  const { db, calls } = createDb({ queueRows: [] });
  const context = buildResolverContext(campaigns, []);

  const applied = await applyManualResolutions(db as any, context);

  assert.equal(applied, 0);
  assert.equal(calls.upsert.length, 0);
  assert.equal(calls.update.length, 0);
});
