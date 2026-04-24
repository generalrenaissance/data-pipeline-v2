import test from 'node:test';
import assert from 'node:assert/strict';

import {
  resolveCampaignTagSources,
  selectWorkspacesForRefresh,
} from '../src/campaign-tags';

test('resolveCampaignTagSources backfills cache rows from campaign detail tags when cache is missing', () => {
  const resolved = resolveCampaignTagSources(undefined, ['RG_123', 'Pair 1', 'RG_123']);

  assert.equal(resolved.shouldBackfillCache, true);
  assert.deepEqual(resolved.cachedTags, []);
  assert.deepEqual(resolved.resolvedTags, ['Pair 1', 'RG_123']);
});

test('resolveCampaignTagSources does not backfill when cache already has mapping tags', () => {
  const resolved = resolveCampaignTagSources(['RG_123'], ['Pair 1']);

  assert.equal(resolved.shouldBackfillCache, false);
  assert.deepEqual(resolved.cachedTags, ['RG_123']);
  assert.deepEqual(resolved.resolvedTags, ['Pair 1', 'RG_123']);
});

test('selectWorkspacesForRefresh shards deterministically when no explicit filter is provided', () => {
  const keyMap = {
    bravo: 'b',
    alpha: 'a',
    delta: 'd',
    charlie: 'c',
  };

  assert.deepEqual(
    Object.keys(selectWorkspacesForRefresh(keyMap, new Set(), 0, 2)),
    ['alpha', 'charlie'],
  );
  assert.deepEqual(
    Object.keys(selectWorkspacesForRefresh(keyMap, new Set(), 1, 2)),
    ['bravo', 'delta'],
  );
});

test('selectWorkspacesForRefresh honors explicit workspace filters over sharding', () => {
  const keyMap = {
    alpha: 'a',
    bravo: 'b',
    charlie: 'c',
  };

  assert.deepEqual(
    Object.keys(selectWorkspacesForRefresh(keyMap, new Set(['charlie', 'alpha']), 1, 3)),
    ['alpha', 'charlie'],
  );
});
