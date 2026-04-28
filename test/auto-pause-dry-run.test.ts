import test from 'node:test';
import assert from 'node:assert/strict';

import {
  applyAutoPauseGuards,
  buildMarkdownReport,
  toCsv,
  type AccountRow,
  type AutoPauseDataset,
  type DomainRow,
} from '../scripts/auto-pause-dry-run';

const NOW = new Date('2026-04-28T03:10:00Z');

function domain(overrides: Partial<DomainRow> = {}): DomainRow {
  return {
    domain: 'dead.example',
    provider_group: 'outlook',
    current_status: 'retire',
    status_since: new Date('2026-04-25T00:00:00Z'),
    last_rr_pct: 0.2,
    last_sent_total: 5000,
    source_max_synced_at: new Date('2026-04-28T02:30:00Z'),
    is_free_mail: false,
    sent_7d: 700,
    replies_7d: 1,
    active_account_count: 1,
    status_since_age_hours: 75,
    cooldown_status: 'PASS',
    ...overrides,
  };
}

function account(overrides: Partial<AccountRow> = {}): AccountRow {
  return {
    domain: 'dead.example',
    account_email: 'sender@dead.example',
    workspace_slug: 'outlook-1',
    provider_code_raw: 3,
    provider_group: 'outlook',
    account_status: 'active',
    daily_limit: 30,
    last_seen_at: new Date('2026-04-28T02:00:00Z'),
    domain_status_since: new Date('2026-04-25T00:00:00Z'),
    domain_status_since_age_hours: 75,
    cooldown_status: 'PASS',
    domain_last_rr_pct: 0.2,
    domain_last_sent_total: 5000,
    ...overrides,
  };
}

function dataset(overrides: Partial<AutoPauseDataset> = {}): AutoPauseDataset {
  return {
    generatedAt: NOW,
    reportDate: '2026-04-28',
    freshness: {
      infra_domain_metrics_last_refresh: new Date('2026-04-28T02:30:00Z'),
      domain_rr_state_last_evaluated: new Date('2026-04-28T03:00:00Z'),
    },
    retireDomains: [domain()],
    targetDomains: [],
    wouldPauseDomains: [],
    skippedDomains: [],
    wouldPauseAccounts: [account()],
    cooldownSkippedAccounts: [],
    ...overrides,
  };
}

test('freshness gate trips: stale global source yields FAIL and zero would-pause-now accounts', () => {
  const guarded = applyAutoPauseGuards(dataset({
    freshness: {
      infra_domain_metrics_last_refresh: new Date('2026-04-26T02:30:00Z'),
      domain_rr_state_last_evaluated: new Date('2026-04-28T03:00:00Z'),
    },
  }));

  const markdown = buildMarkdownReport(guarded);
  assert.match(markdown, /Freshness gate: FAIL/);
  assert.match(markdown, /Would-pause-NOW \(passed cooldown\): 0 accounts across 0 domains/);
  assert.equal(guarded.wouldPauseDomains.length, 0);
  assert.equal(guarded.wouldPauseAccounts.length, 0);
  assert.equal(guarded.targetDomains[0]?.cooldown_status, 'SKIP');
});

test('cooldown rows are flagged, not excluded from CSV input', () => {
  const guarded = applyAutoPauseGuards(dataset({
    retireDomains: [
      domain({
        status_since: new Date('2026-04-27T12:00:00Z'),
        status_since_age_hours: 15,
        cooldown_status: 'SKIP',
      }),
    ],
    wouldPauseAccounts: [
      account({
        domain_status_since: new Date('2026-04-27T12:00:00Z'),
        domain_status_since_age_hours: 15,
        cooldown_status: 'SKIP',
      }),
    ],
  }));

  assert.equal(guarded.targetDomains.length, 1);
  assert.equal(guarded.wouldPauseDomains.length, 0);
  assert.equal(guarded.wouldPauseAccounts.length, 0);
  assert.equal(guarded.cooldownSkippedAccounts.length, 1);
  assert.equal(guarded.cooldownSkippedAccounts[0]?.cooldown_status, 'SKIP');
});

test('cooldown PASS rows are flagged and included in would-pause-now', () => {
  const guarded = applyAutoPauseGuards(dataset({
    retireDomains: [
      domain({
        status_since: new Date('2026-04-25T00:00:00Z'),
        status_since_age_hours: 75,
        cooldown_status: 'PASS',
      }),
    ],
    wouldPauseAccounts: [
      account({
        domain_status_since: new Date('2026-04-25T00:00:00Z'),
        domain_status_since_age_hours: 75,
        cooldown_status: 'PASS',
      }),
    ],
  }));

  assert.equal(guarded.targetDomains.length, 1);
  assert.equal(guarded.wouldPauseDomains.length, 1);
  assert.equal(guarded.wouldPauseAccounts.length, 1);
  assert.equal(guarded.wouldPauseAccounts[0]?.cooldown_status, 'PASS');
});

test('active-only filter excludes non-active accounts from CSV input', () => {
  const guarded = applyAutoPauseGuards(dataset({
    wouldPauseAccounts: [
      account({ account_email: 'active@dead.example' }),
      account({ account_email: 'paused@dead.example', account_status: 'paused' }),
    ],
  }));

  assert.deepEqual(guarded.wouldPauseAccounts.map(row => row.account_email), ['active@dead.example']);
});

test('CSV row shape has required columns and no null key fields', () => {
  const guarded = applyAutoPauseGuards(dataset());
  const columns = [
    'domain',
    'account_email',
    'workspace_slug',
    'provider_code_raw',
    'provider_group',
    'account_status',
    'daily_limit',
    'last_seen_at',
    'domain_status_since',
    'domain_status_since_age_hours',
    'cooldown_status',
    'domain_last_rr_pct',
    'domain_last_sent_total',
  ] as const;
  const csv = toCsv(guarded.wouldPauseAccounts, columns);

  assert.match(csv, /^domain,account_email,workspace_slug,provider_code_raw,provider_group,account_status,daily_limit,last_seen_at,domain_status_since,domain_status_since_age_hours,cooldown_status,domain_last_rr_pct,domain_last_sent_total\n/);
  const row = csv.trim().split('\n')[1].split(',');
  assert.equal(row[0], 'dead.example');
  assert.equal(row[1], 'sender@dead.example');
  assert.equal(row[2], 'outlook-1');
  assert.equal(row[10], 'PASS');
});

test('markdown report shape includes required sections and ISO dates', () => {
  const markdown = buildMarkdownReport(applyAutoPauseGuards(dataset()));

  assert.match(markdown, /^# Auto-Pause Dry-Run - 2026-04-28/);
  assert.match(markdown, /## Header/);
  assert.match(markdown, /Generated at: 2026-04-28T03:10:00\.000Z/);
  assert.match(markdown, /## Summary/);
  assert.match(markdown, /## Cooldown breakdown/);
  assert.match(markdown, /## Distribution by provider \(cooldown PASS only\)/);
  assert.match(markdown, /## Top 50 would-pause-NOW domains \(by 7-day send volume; cooldown=PASS only\)/);
  assert.match(markdown, /## Top 25 cooldown-skipped domains \(review for early signals\)/);
  assert.match(markdown, /## Confidence flags/);
});

test('summary numbers reconcile PASS plus SKIP to total retire active accounts', () => {
  const guarded = applyAutoPauseGuards(dataset({
    retireDomains: [
      domain({ domain: 'pass.example', cooldown_status: 'PASS', active_account_count: 1 }),
      domain({
        domain: 'skip.example',
        status_since: new Date('2026-04-27T12:00:00Z'),
        status_since_age_hours: 15,
        cooldown_status: 'SKIP',
        active_account_count: 1,
      }),
    ],
    wouldPauseAccounts: [
      account({ domain: 'pass.example', account_email: 'sender@pass.example', cooldown_status: 'PASS' }),
      account({
        domain: 'skip.example',
        account_email: 'sender@skip.example',
        domain_status_since: new Date('2026-04-27T12:00:00Z'),
        domain_status_since_age_hours: 15,
        cooldown_status: 'SKIP',
      }),
    ],
  }));

  const total = guarded.wouldPauseAccounts.length + guarded.cooldownSkippedAccounts.length;
  assert.equal(total, 2);
  assert.equal(guarded.targetDomains.length, 2);
});
