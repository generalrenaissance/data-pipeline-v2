import test from 'node:test';
import assert from 'node:assert/strict';

import { buildAccountTagRows, probeAccountTagMappings } from '../src/infra/account-tags';
import { buildDomainRegistryRows } from '../src/infra/domain-registry';
import { parseBrandRows, parseCancelledRows, parseSheetDump } from '../src/infra/sheet-registry';
import type { InstantlyClient } from '../src/instantly';

test('sheet registry: Funding header validates row 0 column G', async () => {
  const parsed = await parseSheetDump('/tmp/renaissance_sheet', new Date('2026-04-25T00:00:00Z'));
  const fundingRows = parsed.sheetRows.filter(r => r.source_tab === 'Funding');
  assert.ok(fundingRows.length > 100);
  assert.ok(fundingRows.some(r => r.campaign_manager === 'Leo'));
  assert.ok(parsed.warnings.some(w => w.includes('RG1879')));
});

test('sheet registry: parses brand allocation five-column blocks', () => {
  const rows = [
    ['BRANDING'],
    ['Business Funding', '', '', '', '', 'ERC'],
    ['x', '', 'Business Funding Brands'],
    ['Name', 'Domain', 'Group Assigned', 'Date Created', 'Setup?', 'Name', 'Domain', 'Group Assigned', 'Date Created', 'Setup?'],
    ['Flex Group', 'https://flexfundingroup.com/', 'A', 'Dec 11, 2024', 'Ready', 'ERC Masters', 'ertcmasters.com'],
  ];
  const parsed = parseBrandRows(rows, '2026-04-25T00:00:00Z');
  assert.equal(parsed.length, 2);
  assert.equal(parsed[0]!.brand_domain, 'flexfundingroup.com');
  assert.equal(parsed[1]!.offer, 'ERC');
});

test('sheet registry: cancelled rows detect date-looking operational fields', () => {
  const rows = [
    ['OFFER', 'Tag/Group', 'Accounts', 'Cold Emails', 'Warmup Emails', 'Status', 'Campaign Manager', 'Inbox Manager', 'Warmup Start Date', 'Workspace', 'Branding', 'Domain Purchase Date', 'Type', 'Technical', 'Batch'],
    ['Funding', 'RG1', 100, 3, 10, 'Cancelled', 'Leo', 'Frank', 'Jul 1, 2025', 'Jul 2, 2025', 'Jul 3, 2025', '', 'Panel', 'Tech', 'B1'],
  ];
  const parsed = parseCancelledRows(rows, '2026-04-25T00:00:00Z');
  assert.equal(parsed[0]!.row_confidence, 'low');
  assert.ok(parsed[0]!.row_warnings.some(w => w.includes('workspace looks like date')));
});

test('account tags: probe finds account resource type from email resource_id matches', async () => {
  const client = {
    apiCallCount: 3,
    rateLimitEvents: 0,
    getAccountsRaw: async () => [{ email: 'a@example.com' }],
    getTagMap: async () => new Map([['tag1', 'RG100']]),
    getAllCustomTagMappings: async () => [
      { id: 'm1', tag_id: 'tag1', resource_id: 'campaign1', resource_type: 2 },
      { id: 'm2', tag_id: 'tag1', resource_id: 'a@example.com', resource_type: 1 },
    ],
  } as unknown as InstantlyClient;
  const result = await probeAccountTagMappings('renaissance-6', client);
  assert.equal(result.verdict, 'ACCOUNT_TAGS_AVAILABLE');
  assert.equal(result.chosenResourceType, 1);
  assert.deepEqual(result.sampleAccountEmails, ['a@example.com']);
});

test('account tags: build rows uses email resource_id and preserves raw resource_id', async () => {
  const client = {
    getAccountsRaw: async () => [{ email: 'a@example.com' }],
    getTagMap: async () => new Map([['tag1', 'RG100']]),
    getAllCustomTagMappings: async () => [
      { id: 'm1', tag_id: 'tag1', resource_id: 'A@Example.com', resource_type: 1 },
      { id: 'm2', tag_id: 'tag1', resource_id: 'campaign1', resource_type: 2 },
    ],
  } as unknown as InstantlyClient;
  const result = await buildAccountTagRows('x', client, 1, '2026-04-25T00:00:00Z');
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0]!.account_email, 'a@example.com');
  assert.equal(result.rows[0]!.resource_id, 'A@Example.com');
  assert.equal(result.rows[0]!.domain, 'example.com');
});

test('domain registry: mapped, unmapped, mixed, and cancelled status rules', () => {
  const baseAccount = {
    workspace_name: 'Renaissance 6',
    provider_group: 'google_otd' as const,
    account_status: 'active',
    is_free_mail: false,
  };
  const rows = buildDomainRegistryRows({
    builtAt: '2026-04-25T00:00:00Z',
    accounts: [
      { ...baseAccount, account_email: 'a@mapped.com', domain: 'mapped.com', workspace_slug: 'renaissance-6' },
      { ...baseAccount, account_email: 'b@untagged.com', domain: 'untagged.com', workspace_slug: 'renaissance-6' },
      { ...baseAccount, account_email: 'c@mixed.com', domain: 'mixed.com', workspace_slug: 'renaissance-6' },
      { ...baseAccount, account_email: 'd@mixed.com', domain: 'mixed.com', workspace_slug: 'renaissance-6' },
      { ...baseAccount, account_email: 'e@cancelled.com', domain: 'cancelled.com', workspace_slug: 'renaissance-6' },
    ],
    mappings: [
      { workspace_slug: 'renaissance-6', account_email: 'a@mapped.com', resource_id: 'a@mapped.com', domain: 'mapped.com', tag_id: 't1', tag_label: 'RG1' },
      { workspace_slug: 'renaissance-6', account_email: 'c@mixed.com', resource_id: 'c@mixed.com', domain: 'mixed.com', tag_id: 't1', tag_label: 'RG1' },
      { workspace_slug: 'renaissance-6', account_email: 'd@mixed.com', resource_id: 'd@mixed.com', domain: 'mixed.com', tag_id: 't2', tag_label: 'RG2' },
      { workspace_slug: 'renaissance-6', account_email: 'e@cancelled.com', resource_id: 'e@cancelled.com', domain: 'cancelled.com', tag_id: 't3', tag_label: 'RG3' },
    ],
    sheetRows: [
      { tag: 'RG1', offer: 'Funding', campaign_manager: 'Leo', workspace_name: 'R6', workspace_slug: 'renaissance-6', sheet_status: 'Active', brand_name: 'Brand A', brand_domain: 'a.com', infra_type: 'Panel', inbox_manager: 'Frank', group_name: 'Group A', pair: '1', email_provider: 'Google', batch: 'B1', accounts_expected: 100, expected_daily_cold: 1000, accounts_per_domain: 2, expected_domain_count: 50, domain_purchase_date: '2026-04-01', low_rr: false, row_confidence: 'high' },
      { tag: 'RG2', offer: 'Funding', campaign_manager: 'Andres', workspace_name: 'R6', workspace_slug: 'renaissance-6', sheet_status: 'Active', brand_name: 'Brand B', brand_domain: 'b.com', infra_type: 'Panel', inbox_manager: 'Frank', group_name: 'Group A', pair: '2', email_provider: 'Outlook', batch: 'B2', accounts_expected: 100, expected_daily_cold: 1000, accounts_per_domain: 4, expected_domain_count: 25, domain_purchase_date: '2026-04-02', low_rr: true, row_confidence: 'high' },
    ],
    cancelledRows: [{ tag: 'RG3', row_confidence: 'medium' }],
  });
  const byDomain = new Map(rows.map(r => [r.domain, r]));
  assert.equal(byDomain.get('mapped.com')!.mapping_status, 'mapped');
  assert.equal(byDomain.get('mapped.com')!.primary_campaign_manager, 'Leo');
  assert.deepEqual(byDomain.get('mapped.com')!.email_providers, ['Google']);
  assert.deepEqual(byDomain.get('mapped.com')!.batches, ['B1']);
  assert.deepEqual(byDomain.get('mapped.com')!.accounts_per_domain_values, [2]);
  assert.deepEqual(byDomain.get('mapped.com')!.expected_domain_count_values, [50]);
  assert.deepEqual(byDomain.get('mapped.com')!.domain_purchase_dates, ['2026-04-01']);
  assert.deepEqual(byDomain.get('mapped.com')!.low_rr_flags, [false]);
  assert.equal(byDomain.get('untagged.com')!.mapping_status, 'no_account_tags');
  assert.equal(byDomain.get('mixed.com')!.mapping_status, 'mixed_cm');
  assert.equal(byDomain.get('cancelled.com')!.mapping_status, 'cancelled');
});
