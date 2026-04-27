import test from 'node:test';
import assert from 'node:assert/strict';

import {
  computeStatus,
  computeFreshness,
  computeRrPct,
  eventTypeForTransition,
  ELIGIBILITY,
  THRESHOLDS,
  STALE_DATA_HOURS,
  type MetricSnapshot,
} from '../src/infra/score-domain-rr';

const NOW = new Date('2026-04-26T22:00:00Z');
const FRESH_SYNC = new Date('2026-04-26T20:00:00Z'); // 2h ago
const STALE_SYNC = new Date('2026-04-24T00:00:00Z'); // ~70h ago

function metric(overrides: Partial<MetricSnapshot> = {}): MetricSnapshot {
  return {
    domain: 'example.co',
    provider_group: 'google_otd',
    is_free_mail: false,
    sent_total: 1000,
    reply_count_total: 10,
    auto_reply_count_total: 0,
    rr_pct: 1.0,
    source_max_synced_at: FRESH_SYNC,
    inbox_count: 5,
    active_inbox_count: 5,
    ...overrides,
  };
}

// =========================================================================
// computeStatus — bucket coverage (4 buckets × 2 providers = 8 cases)
// =========================================================================

test('computeStatus: google_otd RR=1.5% → great', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 15, rr_pct: 1.5 });
  assert.equal(computeStatus(m, { now: NOW }), 'great');
});

test('computeStatus: google_otd RR=0.85% → good', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 9, rr_pct: 0.85 });
  assert.equal(computeStatus(m, { now: NOW }), 'good');
});

test('computeStatus: google_otd RR=0.6% → warmup', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 6, rr_pct: 0.6 });
  assert.equal(computeStatus(m, { now: NOW }), 'warmup');
});

test('computeStatus: google_otd RR=0.3% → retire', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 3, rr_pct: 0.3 });
  assert.equal(computeStatus(m, { now: NOW }), 'retire');
});

test('computeStatus: outlook RR=2.0% → great', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 1500, reply_count_total: 30, rr_pct: 2.0 });
  assert.equal(computeStatus(m, { now: NOW }), 'great');
});

test('computeStatus: outlook RR=0.85% → good', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 2000, reply_count_total: 17, rr_pct: 0.85 });
  assert.equal(computeStatus(m, { now: NOW }), 'good');
});

test('computeStatus: outlook RR=0.55% → warmup', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 1500, reply_count_total: 8, rr_pct: 0.55 });
  assert.equal(computeStatus(m, { now: NOW }), 'warmup');
});

test('computeStatus: outlook RR=0.2% → retire', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 1500, reply_count_total: 3, rr_pct: 0.2 });
  assert.equal(computeStatus(m, { now: NOW }), 'retire');
});

// =========================================================================
// Eligibility gate
// =========================================================================

test('computeStatus: google_otd below 700-send gate → unscored', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 699, reply_count_total: 50, rr_pct: 7.15 });
  assert.equal(computeStatus(m, { now: NOW }), 'unscored');
});

test('computeStatus: outlook below 1000-send gate → unscored', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 999, reply_count_total: 50, rr_pct: 5.0 });
  assert.equal(computeStatus(m, { now: NOW }), 'unscored');
});

test('computeStatus: google_otd at exactly 700 sends with 1.5% RR → great (gate inclusive)', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 700, reply_count_total: 11, rr_pct: 1.571 });
  assert.equal(computeStatus(m, { now: NOW }), 'great');
});

test('computeStatus: outlook at exactly 1000 sends with 1.5% RR → great (gate inclusive)', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 1000, reply_count_total: 15, rr_pct: 1.5 });
  assert.equal(computeStatus(m, { now: NOW }), 'great');
});

// =========================================================================
// Special states (free_mail, unknown_provider, sync_zero, stale_data)
// =========================================================================

test('computeStatus: free-mail domain → free_mail_excluded (regardless of RR)', () => {
  const m = metric({ is_free_mail: true, sent_total: 5000, reply_count_total: 100, rr_pct: 2.0 });
  assert.equal(computeStatus(m, { now: NOW }), 'free_mail_excluded');
});

test('computeStatus: unknown provider → unknown_provider', () => {
  const m = metric({ provider_group: 'unknown', sent_total: 5000, reply_count_total: 50, rr_pct: 1.0 });
  assert.equal(computeStatus(m, { now: NOW }), 'unknown_provider');
});

test('computeStatus: sent_total=0 with fresh source → sync_zero', () => {
  const m = metric({ sent_total: 0, reply_count_total: 0, rr_pct: null, source_max_synced_at: FRESH_SYNC });
  assert.equal(computeStatus(m, { now: NOW }), 'sync_zero');
});

test('computeStatus: sent_total=0 with stale source → stale_data', () => {
  const m = metric({ sent_total: 0, reply_count_total: 0, rr_pct: null, source_max_synced_at: STALE_SYNC });
  assert.equal(computeStatus(m, { now: NOW }), 'stale_data');
});

test('computeStatus: sent_total=0 with null source_max_synced_at → stale_data (treat as ancient)', () => {
  const m = metric({ sent_total: 0, reply_count_total: 0, rr_pct: null, source_max_synced_at: null });
  assert.equal(computeStatus(m, { now: NOW }), 'stale_data');
});

test('computeStatus: source data 70h stale (sent>0) → stale_data', () => {
  const m = metric({ source_max_synced_at: STALE_SYNC });
  assert.equal(computeStatus(m, { now: NOW }), 'stale_data');
});

// =========================================================================
// Edge cases on bucket boundaries
// =========================================================================

test('computeStatus: google_otd exactly 0.5% RR → warmup (>= 0.5 is warmup, not retire)', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 5, rr_pct: 0.5 });
  assert.equal(computeStatus(m, { now: NOW }), 'warmup');
});

test('computeStatus: google_otd exactly 0.75% RR → good (>= 0.75 is good)', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 8, rr_pct: 0.75 });
  assert.equal(computeStatus(m, { now: NOW }), 'good');
});

test('computeStatus: google_otd exactly 1.0% RR → good (1.0 is upper bound of good, not great)', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 1000, reply_count_total: 10, rr_pct: 1.0 });
  assert.equal(computeStatus(m, { now: NOW }), 'good');
});

test('computeStatus: google_otd 1.001% RR → great (strictly > 1.0)', () => {
  const m = metric({ provider_group: 'google_otd', sent_total: 10000, reply_count_total: 101, rr_pct: 1.01 });
  assert.equal(computeStatus(m, { now: NOW }), 'great');
});

test('computeStatus: outlook exactly 0.45% RR → warmup', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 2000, reply_count_total: 9, rr_pct: 0.45 });
  assert.equal(computeStatus(m, { now: NOW }), 'warmup');
});

test('computeStatus: outlook exactly 0.7% RR → good', () => {
  const m = metric({ provider_group: 'outlook', sent_total: 2000, reply_count_total: 14, rr_pct: 0.7 });
  assert.equal(computeStatus(m, { now: NOW }), 'good');
});

test('computeStatus: source exactly at 48h boundary (sent>0) → still scored, not stale', () => {
  const at48h = new Date(NOW.getTime() - STALE_DATA_HOURS * 3600_000);
  const m = metric({ source_max_synced_at: at48h });
  assert.equal(computeStatus(m, { now: NOW }), 'good'); // 1.0 RR, eligible
});

test('computeStatus: source 48h+1ms stale (sent>0) → stale_data', () => {
  const justOver = new Date(NOW.getTime() - STALE_DATA_HOURS * 3600_000 - 1);
  const m = metric({ source_max_synced_at: justOver });
  assert.equal(computeStatus(m, { now: NOW }), 'stale_data');
});

// =========================================================================
// Status precedence: free_mail > unknown_provider > stale/sync > eligibility > bucket
// =========================================================================

test('computeStatus: free_mail beats unknown_provider', () => {
  const m = metric({ is_free_mail: true, provider_group: 'unknown' });
  assert.equal(computeStatus(m, { now: NOW }), 'free_mail_excluded');
});

test('computeStatus: unknown_provider beats stale_data', () => {
  const m = metric({ provider_group: 'unknown', source_max_synced_at: STALE_SYNC });
  assert.equal(computeStatus(m, { now: NOW }), 'unknown_provider');
});

// =========================================================================
// computeRrPct — fallback when rr_pct is null
// =========================================================================

test('computeRrPct: uses precomputed rr_pct when present', () => {
  const m = metric({ rr_pct: 1.5, sent_total: 1000, reply_count_total: 10 });
  assert.equal(computeRrPct(m), 1.5);
});

test('computeRrPct: falls back to reply_count/sent_total*100 when rr_pct is null', () => {
  const m = metric({ rr_pct: null, sent_total: 1000, reply_count_total: 12 });
  assert.equal(computeRrPct(m), 1.2);
});

test('computeRrPct: returns 0 when sent_total is 0', () => {
  const m = metric({ sent_total: 0, reply_count_total: 0, rr_pct: null });
  assert.equal(computeRrPct(m), 0);
});

// =========================================================================
// computeFreshness
// =========================================================================

test('computeFreshness: sync_zero status → freshness=sync_zero', () => {
  const m = metric({ sent_total: 0, source_max_synced_at: FRESH_SYNC });
  assert.equal(computeFreshness(m, 'sync_zero', { now: NOW }), 'sync_zero');
});

test('computeFreshness: null source_max_synced_at → unknown', () => {
  const m = metric({ source_max_synced_at: null });
  assert.equal(computeFreshness(m, 'good', { now: NOW }), 'unknown');
});

test('computeFreshness: source within 48h → fresh', () => {
  const m = metric({ source_max_synced_at: FRESH_SYNC });
  assert.equal(computeFreshness(m, 'good', { now: NOW }), 'fresh');
});

test('computeFreshness: source over 48h stale → stale', () => {
  const m = metric({ source_max_synced_at: STALE_SYNC });
  assert.equal(computeFreshness(m, 'stale_data', { now: NOW }), 'stale');
});

// =========================================================================
// eventTypeForTransition — matches the 2026-04-25 seed pattern
// =========================================================================

test('eventTypeForTransition: warmup → warmup_entered', () => {
  assert.equal(eventTypeForTransition('warmup'), 'warmup_entered');
});

test('eventTypeForTransition: retire → retire_entered', () => {
  assert.equal(eventTypeForTransition('retire'), 'retire_entered');
});

test('eventTypeForTransition: sync_zero → sync_zero_entered', () => {
  assert.equal(eventTypeForTransition('sync_zero'), 'sync_zero_entered');
});

test('eventTypeForTransition: unknown_provider → unknown_provider_entered', () => {
  assert.equal(eventTypeForTransition('unknown_provider'), 'unknown_provider_entered');
});

test('eventTypeForTransition: great → status_change', () => {
  assert.equal(eventTypeForTransition('great'), 'status_change');
});

test('eventTypeForTransition: good → status_change', () => {
  assert.equal(eventTypeForTransition('good'), 'status_change');
});

test('eventTypeForTransition: unscored → null (silent — no event)', () => {
  assert.equal(eventTypeForTransition('unscored'), null);
});

test('eventTypeForTransition: free_mail_excluded → null (silent)', () => {
  assert.equal(eventTypeForTransition('free_mail_excluded'), null);
});

test('eventTypeForTransition: stale_data → null (silent)', () => {
  assert.equal(eventTypeForTransition('stale_data'), null);
});

// =========================================================================
// Constants — sanity checks against the spec
// =========================================================================

test('ELIGIBILITY: google_otd=700 sends, outlook=1000 sends', () => {
  assert.equal(ELIGIBILITY.google_otd, 700);
  assert.equal(ELIGIBILITY.outlook, 1000);
});

test('THRESHOLDS: google_otd great=1.0 good=0.75 warmup=0.5', () => {
  assert.equal(THRESHOLDS.google_otd.great, 1.0);
  assert.equal(THRESHOLDS.google_otd.good, 0.75);
  assert.equal(THRESHOLDS.google_otd.warmup, 0.5);
});

test('THRESHOLDS: outlook great=1.0 good=0.7 warmup=0.45', () => {
  assert.equal(THRESHOLDS.outlook.great, 1.0);
  assert.equal(THRESHOLDS.outlook.good, 0.7);
  assert.equal(THRESHOLDS.outlook.warmup, 0.45);
});

test('STALE_DATA_HOURS = 48', () => {
  assert.equal(STALE_DATA_HOURS, 48);
});
