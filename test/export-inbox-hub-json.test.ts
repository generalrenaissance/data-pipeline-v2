import { strict as assert } from 'node:assert';
import { test } from 'node:test';

import { buildPayload, validateRowShape } from '../scripts/export-inbox-hub-json';

const baseRow = {
  tag: 'RG3527',
  offer: 'Funding',
  sheet_status: 'Active',
  email_provider: 'Outlook',
  provider_group: 'outlook',
  group_name: null,
  pair: '38',
  infra_type: 'MailIn',
  accounts_expected: 3960,
  cold_per_account: 3,
  warmup_per_account: 10,
  expected_daily_cold: 11_880,
  expected_domain_count: 40,
  accounts_per_domain: 99,
  tag_value: 1,
  low_rr: false,
  warmup_emails_daily: 39_600,
  need_warmup: false,
  row_confidence: 'high',
  sheet_synced_at: '2026-04-28T06:34:17.686Z',
};

test('validateRowShape accepts allowlisted columns', () => {
  assert.doesNotThrow(() => validateRowShape(baseRow));
});

test('validateRowShape rejects denied PII columns', () => {
  const bad = { ...baseRow, campaign_manager: 'Jane Doe' };
  assert.throws(() => validateRowShape(bad), /SECURITY: denied column/);
});

test('validateRowShape rejects denied infra-exposure columns', () => {
  const bad = { ...baseRow, brand_domain: 'someinfra.co' };
  assert.throws(() => validateRowShape(bad), /SECURITY: denied column/);
});

test('validateRowShape rejects denied operational columns', () => {
  const bad = { ...baseRow, billing_date: '2026-04-01' };
  assert.throws(() => validateRowShape(bad), /SECURITY: denied column/);
});

test('validateRowShape rejects raw_row jsonb', () => {
  const bad = { ...baseRow, raw_row: { campaign_manager: 'leak' } };
  assert.throws(() => validateRowShape(bad), /SECURITY: denied column/);
});

test('validateRowShape rejects unknown columns (forces explicit allow/deny)', () => {
  const bad = { ...baseRow, future_unknown_field: 'something' };
  assert.throws(() => validateRowShape(bad), /SECURITY: unknown column/);
});

test('buildPayload emits stable shape with row_count and allowed_columns', () => {
  const generatedAt = new Date('2026-04-28T12:00:00.000Z');
  const payload = buildPayload([baseRow], generatedAt);
  assert.equal(payload.generated_at, '2026-04-28T12:00:00.000Z');
  assert.equal(payload.source, 'public.infra_sheet_registry');
  assert.equal(payload.sheet_synced_at, '2026-04-28T06:34:17.686Z');
  assert.equal(payload.row_count, 1);
  assert.equal(payload.rows.length, 1);
  assert.ok(payload.allowed_columns.includes('tag'));
  assert.ok(payload.allowed_columns.includes('accounts_per_domain'));
});

test('buildPayload handles empty result set', () => {
  const payload = buildPayload([]);
  assert.equal(payload.row_count, 0);
  assert.equal(payload.sheet_synced_at, null);
  assert.equal(payload.rows.length, 0);
});

test('buildPayload still validates every row, not just the first', () => {
  const goodRow = { ...baseRow };
  const badRow = { ...baseRow, technical: 'leaked' };
  assert.throws(() => buildPayload([goodRow, badRow]), /SECURITY: denied column/);
});
