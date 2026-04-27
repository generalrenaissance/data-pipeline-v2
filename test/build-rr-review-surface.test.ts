import test from 'node:test';
import assert from 'node:assert/strict';

import { buildMarkdownReport, toCsv, type ReviewDataset } from '../scripts/build-rr-review-surface';

const DATASET: ReviewDataset = {
  generatedAt: new Date('2026-04-27T15:00:00Z'),
  reportDate: '2026-04-27',
  freshness: {
    infra_domain_metrics_last_refresh: new Date('2026-04-27T14:00:00Z'),
    domain_rr_state_last_evaluated: new Date('2026-04-27T14:58:00Z'),
    state_count: 3,
  },
  distribution: [
    { current_status: 'retire', provider_group: 'outlook', domain_count: 1, sent_7d: 1000 },
    { current_status: 'warmup', provider_group: 'google_otd', domain_count: 1, sent_7d: 500 },
    { current_status: 'unknown_provider', provider_group: 'unknown', domain_count: 1, sent_7d: 10 },
  ],
  rowsByStatus: {
    great: [],
    good: [],
    warmup: [
      {
        domain: 'warm.example',
        provider_group: 'google_otd',
        current_status: 'warmup',
        status_since: new Date('2026-04-27T12:00:00Z'),
        last_rr_pct: 0.6,
        last_sent_total: 900,
        last_reply_count: 5,
        sent_7d: 500,
        replies_7d: 3,
        active_account_count: 4,
        source_max_synced_at: new Date('2026-04-27T14:00:00Z'),
        data_freshness_status: 'fresh',
      },
    ],
    retire: [
      {
        domain: 'dead.example',
        provider_group: 'outlook',
        current_status: 'retire',
        status_since: new Date('2026-04-27T12:00:00Z'),
        last_rr_pct: 0.2,
        last_sent_total: 5000,
        last_reply_count: 10,
        sent_7d: 1000,
        replies_7d: 2,
        active_account_count: 91,
        source_max_synced_at: new Date('2026-04-27T14:00:00Z'),
        data_freshness_status: 'fresh',
      },
    ],
    unscored: [],
    sync_zero: [],
    stale_data: [],
    unknown_provider: [
      {
        domain: 'unknown.example',
        provider_group: 'unknown',
        current_status: 'unknown_provider',
        status_since: new Date('2026-04-27T12:00:00Z'),
        last_rr_pct: 1.1,
        last_sent_total: 100,
        last_reply_count: 1,
        sent_7d: 10,
        replies_7d: 0,
        active_account_count: 1,
        source_max_synced_at: new Date('2026-04-27T14:00:00Z'),
        data_freshness_status: 'fresh',
      },
    ],
    free_mail_excluded: [],
  },
  retireAccounts: [
    {
      domain: 'dead.example',
      account_email: 'sender@dead.example',
      workspace_slug: 'outlook-1',
      account_status: 'active',
      provider_code_raw: 3,
      last_seen_at: new Date('2026-04-27T14:00:00Z'),
    },
  ],
};

test('buildMarkdownReport includes required sections and tables', () => {
  const markdown = buildMarkdownReport(DATASET, {
    report: '/Users/sam/Documents/Claude Code/Renaissance/deliverables/2026-04-27-domain-rr-review.md',
    retireCsv: '/Users/sam/Documents/Claude Code/Renaissance/deliverables/domain-rr/retire-domains-2026-04-27.csv',
    warmupCsv: '/Users/sam/Documents/Claude Code/Renaissance/deliverables/domain-rr/warmup-domains-2026-04-27.csv',
    unknownProviderCsv: '/Users/sam/Documents/Claude Code/Renaissance/deliverables/domain-rr/unknown-provider-domains-2026-04-27.csv',
    staleDataCsv: '/Users/sam/Documents/Claude Code/Renaissance/deliverables/domain-rr/stale-data-domains-2026-04-27.csv',
    retireAccountsCsv: '/Users/sam/Documents/Claude Code/Renaissance/deliverables/domain-rr/retire-domain-active-accounts-2026-04-27.csv',
  });

  assert.match(markdown, /^# Domain RR Review - 2026-04-27/);
  assert.match(markdown, /## Freshness/);
  assert.match(markdown, /## Status distribution/);
  assert.match(markdown, /\| Status \| google_otd \| outlook \| total \| 7d sends \|/);
  assert.match(markdown, /## Top 50 retire domains/);
  assert.match(markdown, /\| dead\.example \| outlook \| 1,000 \| 2 \| 0\.2000 \| 91 \|/);
  assert.match(markdown, /## 1 unknown_provider domains/);
});

test('toCsv writes stable header order and escapes commas', () => {
  const csv = toCsv(
    [
      {
        domain: 'example.com',
        provider_group: 'outlook',
        current_status: 'retire',
        status_since: '2026-04-27T00:00:00Z',
        last_rr_pct: 0.2,
        last_sent_total: 10,
        last_reply_count: 1,
        sent_7d: 5,
        replies_7d: 0,
        active_account_count: 2,
        source_max_synced_at: '2026-04-27T00:00:00Z',
        data_freshness_status: 'fresh,verified',
      },
    ],
    [
      'domain',
      'provider_group',
      'current_status',
      'status_since',
      'last_rr_pct',
      'last_sent_total',
      'last_reply_count',
      'sent_7d',
      'replies_7d',
      'active_account_count',
      'source_max_synced_at',
      'data_freshness_status',
    ],
  );

  assert.equal(
    csv,
    'domain,provider_group,current_status,status_since,last_rr_pct,last_sent_total,last_reply_count,sent_7d,replies_7d,active_account_count,source_max_synced_at,data_freshness_status\nexample.com,outlook,retire,2026-04-27T00:00:00Z,0.2,10,1,5,0,2,2026-04-27T00:00:00Z,"fresh,verified"\n',
  );
});
