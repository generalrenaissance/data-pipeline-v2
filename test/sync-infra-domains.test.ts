import test from 'node:test';
import assert from 'node:assert/strict';

import {
  inventory,
  metricsIncremental,
  rebuildAggregates,
  type SyncDeps,
} from '../src/infra/sync-infra-domains';
import type { InstantlyClient } from '../src/instantly';
import type { SupabaseClient } from '../src/supabase';
import type { Account } from '../src/types';
import type { AccountDailyMetric } from '../src/infra/types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

interface UpsertCall {
  table: string;
  rows: unknown[];
  onConflict: string;
}

function makeFakeSupabase(opts: {
  selectAllByTable?: Record<string, unknown[]>;
} = {}): { sb: SupabaseClient; upserts: UpsertCall[] } {
  const upserts: UpsertCall[] = [];
  const sb: Partial<SupabaseClient> = {
    upsert: async (table: string, rows: unknown[], onConflict: string) => {
      upserts.push({ table, rows: [...rows], onConflict });
    },
    select: async () => [],
    selectAll: async (table: string) => {
      const rows = opts.selectAllByTable?.[table] ?? [];
      return rows;
    },
    insert: async () => {},
    insertIgnore: async () => {},
    delete: async () => {},
    update: async () => {},
    rpc: async () => null,
  };
  return { sb: sb as SupabaseClient, upserts };
}

interface FakeClientPlan {
  accounts?: Account[];
  daily?: AccountDailyMetric[];
  apiCalls?: number;
  rateLimitEvents?: number;
}

function makeFakeClient(plan: FakeClientPlan): InstantlyClient {
  const stub: Partial<InstantlyClient> = {
    apiCallCount: plan.apiCalls ?? 1,
    rateLimitEvents: plan.rateLimitEvents ?? 0,
    getAccountsRaw: async () => plan.accounts ?? [],
    getWorkspaceAccountDailyAnalytics: async () => plan.daily ?? [],
  };
  return stub as InstantlyClient;
}

function fixedNow(iso: string): () => Date {
  const d = new Date(iso);
  return () => d;
}

// ---------------------------------------------------------------------------
// Inventory tests
// ---------------------------------------------------------------------------

test('inventory: upserts to infra_accounts with correct provider_group from slug', async () => {
  const { sb, upserts } = makeFakeSupabase();
  const plansBySlug: Record<string, FakeClientPlan> = {
    'outlook-3': {
      accounts: [
        { email: 'a@tryunsecuredhq.co', provider_code: 3, status: 'active' },
        { email: 'b@tryunsecuredhq.co', provider_code: 3, status: 'active' },
      ],
      apiCalls: 2,
    },
    'renaissance-3': {
      accounts: [{ email: 'c@example.com', provider_code: 3, status: 'paused' }],
      apiCalls: 1,
    },
  };
  const deps: SyncDeps = {
    keyMap: { 'outlook-3': 'k1', 'renaissance-3': 'k2' },
    supabase: sb,
    makeClient: (key: string) => {
      if (key === 'k1') return makeFakeClient(plansBySlug['outlook-3']!);
      if (key === 'k2') return makeFakeClient(plansBySlug['renaissance-3']!);
      throw new Error(`unknown key ${key}`);
    },
    now: fixedNow('2026-04-24T12:00:00Z'),
  };

  const stats = await inventory(deps, {});
  assert.equal(stats.workspaceCount, 2);
  assert.equal(stats.accountsSeen, 3);
  assert.equal(stats.errors.length, 0);
  assert.equal(stats.apiCalls, 3);

  // Two upsert calls (one per workspace).
  assert.equal(upserts.length, 2);
  for (const call of upserts) {
    assert.equal(call.table, 'infra_accounts');
    assert.equal(call.onConflict, 'account_email');
  }
  const outlookRows = upserts.find(c =>
    (c.rows as Array<Record<string, unknown>>).every(r => r.workspace_slug === 'outlook-3'),
  )!;
  for (const r of outlookRows.rows as Array<Record<string, unknown>>) {
    assert.equal(r.provider_group, 'outlook');
    assert.equal(r.is_free_mail, false);
    assert.equal(r.domain, 'tryunsecuredhq.co');
    assert.equal(r.provider_code_raw, 3);
    assert.ok(!('first_seen_at' in r), 'first_seen_at must be omitted');
    assert.equal(r.last_seen_at, '2026-04-24T12:00:00.000Z');
    assert.equal(r.api_synced_at, '2026-04-24T12:00:00.000Z');
  }
  const renRows = upserts.find(c =>
    (c.rows as Array<Record<string, unknown>>).every(r => r.workspace_slug === 'renaissance-3'),
  )!;
  for (const r of renRows.rows as Array<Record<string, unknown>>) {
    assert.equal(r.provider_group, 'google_otd');
  }
});

test('inventory: skips EXCLUDED_SLUGS even when present in keyMap', async () => {
  const { sb, upserts } = makeFakeSupabase();
  let madeFor: string[] = [];
  const deps: SyncDeps = {
    keyMap: { personal: 'kp', 'sam-test': 'ks', 'outlook-1': 'k1' },
    supabase: sb,
    makeClient: (key: string) => {
      madeFor.push(key);
      return makeFakeClient({ accounts: [{ email: 'x@example.com' }], apiCalls: 1 });
    },
  };

  const stats = await inventory(deps, {});
  assert.equal(stats.workspaceCount, 1);
  assert.deepEqual(madeFor, ['k1']);
  assert.equal(upserts.length, 1);
});

test('inventory: respects workspaceFilter and skips even matching EXCLUDED_SLUGS', async () => {
  const { sb } = makeFakeSupabase();
  let made = 0;
  const deps: SyncDeps = {
    keyMap: { personal: 'kp', 'outlook-1': 'k1', 'outlook-2': 'k2' },
    supabase: sb,
    makeClient: () => {
      made++;
      return makeFakeClient({ accounts: [], apiCalls: 1 });
    },
  };

  // Filter to outlook-1 only.
  const stats = await inventory(deps, { workspaceFilter: 'outlook-1' });
  assert.equal(stats.workspaceCount, 1);
  assert.equal(made, 1);

  // Filter targeting an excluded slug should still skip it.
  made = 0;
  const stats2 = await inventory(deps, { workspaceFilter: 'personal' });
  assert.equal(stats2.workspaceCount, 0);
  assert.equal(made, 0);
});

test('inventory: sets is_free_mail=true for free-mail domains', async () => {
  const { sb, upserts } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: { 'outlook-1': 'k1' },
    supabase: sb,
    makeClient: () =>
      makeFakeClient({
        accounts: [
          { email: 'sam@gmail.com', provider_code: 3 },
          { email: 'sam@tryunsecuredhq.co', provider_code: 3 },
        ],
        apiCalls: 1,
      }),
  };

  await inventory(deps, {});
  const rows = upserts[0]!.rows as Array<Record<string, unknown>>;
  const gmail = rows.find(r => r.domain === 'gmail.com')!;
  const company = rows.find(r => r.domain === 'tryunsecuredhq.co')!;
  assert.equal(gmail.is_free_mail, true);
  assert.equal(company.is_free_mail, false);
});

test('inventory: increments apiCalls and rateLimitEvents counters', async () => {
  const { sb } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: { 'outlook-1': 'k1', 'outlook-2': 'k2' },
    supabase: sb,
    makeClient: (key: string) =>
      makeFakeClient({
        accounts: [{ email: 'a@example.com' }],
        apiCalls: key === 'k1' ? 5 : 7,
        rateLimitEvents: key === 'k1' ? 1 : 0,
      }),
  };
  const stats = await inventory(deps, {});
  assert.equal(stats.apiCalls, 12);
  assert.equal(stats.rateLimitEvents, 1);
});

// ---------------------------------------------------------------------------
// metricsIncremental tests
// ---------------------------------------------------------------------------

test('metricsIncremental: upserts to infra_account_daily_metrics with provider_group from slug not row', async () => {
  const { sb, upserts } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: { 'outlook-3': 'k1' },
    supabase: sb,
    makeClient: () =>
      makeFakeClient({
        daily: [
          {
            date: '2026-04-23',
            email_account: 'a@tryunsecuredhq.co',
            sent: 5,
            bounced: 0,
            contacted: 5,
            new_leads_contacted: 5,
            opened: 0,
            unique_opened: 0,
            replies: 1,
            unique_replies: 1,
            replies_automatic: 0,
            unique_replies_automatic: 0,
            clicks: 0,
            unique_clicks: 0,
          },
        ],
        apiCalls: 1,
      }),
    now: fixedNow('2026-04-24T12:00:00Z'),
  };

  const stats = await metricsIncremental(deps, { days: 7 });
  assert.equal(stats.accountMetricRows, 1);
  const dailyUpserts = upserts.filter(c => c.table === 'infra_account_daily_metrics');
  assert.equal(dailyUpserts.length, 1);
  const row = (dailyUpserts[0]!.rows[0] as Record<string, unknown>);
  // Slug-derived, never row-derived.
  assert.equal(row.provider_group, 'outlook');
  assert.equal(row.workspace_slug, 'outlook-3');
  assert.equal(row.account_email, 'a@tryunsecuredhq.co');
  assert.equal(row.metric_date, '2026-04-23');
  assert.equal(row.sent, 5);
  assert.equal(row.replies, 1);
});

test('metricsIncremental: bumps errors when phantom empty-email row leaks past client filter', async () => {
  const { sb } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: { 'outlook-3': 'k1' },
    supabase: sb,
    makeClient: () =>
      makeFakeClient({
        daily: [
          // Defense-in-depth: simulate a client-level filter failure to make
          // sure the sync layer detects and reports the leak.
          { date: '2026-04-23', email_account: '', sent: 0 } as unknown as AccountDailyMetric,
          {
            date: '2026-04-23',
            email_account: 'a@example.com',
            sent: 1,
            bounced: 0,
            contacted: 1,
            new_leads_contacted: 1,
            opened: 0,
            unique_opened: 0,
            replies: 0,
            unique_replies: 0,
            replies_automatic: 0,
            unique_replies_automatic: 0,
            clicks: 0,
            unique_clicks: 0,
          },
        ],
        apiCalls: 1,
      }),
    now: fixedNow('2026-04-24T12:00:00Z'),
  };
  const stats = await metricsIncremental(deps, { days: 7 });
  assert.equal(stats.accountMetricRows, 1);
  assert.equal(stats.errors.length, 1);
  assert.match(stats.errors[0]!, /phantom empty-email row/);
});

test('metricsIncremental: skips EXCLUDED_SLUGS', async () => {
  const { sb } = makeFakeSupabase();
  let madeFor: string[] = [];
  const deps: SyncDeps = {
    keyMap: { 'sam-test': 'kx', 'outlook-1': 'k1' },
    supabase: sb,
    makeClient: (key: string) => {
      madeFor.push(key);
      return makeFakeClient({ daily: [], apiCalls: 1 });
    },
    now: fixedNow('2026-04-24T12:00:00Z'),
  };
  await metricsIncremental(deps, { days: 7 });
  assert.deepEqual(madeFor, ['k1']);

  madeFor = [];
  await metricsIncremental(deps, { workspaceFilter: 'sam-test', days: 7 });
  assert.deepEqual(madeFor, []);
});

// ---------------------------------------------------------------------------
// rebuildAggregates tests
// ---------------------------------------------------------------------------

function makeAccountDailyRow(args: {
  account_email: string;
  metric_date: string;
  domain: string;
  workspace_slug: string;
  provider_group: 'outlook' | 'google_otd' | 'unknown';
  sent: number;
  replies: number;
  replies_automatic?: number;
  api_synced_at?: string;
}): Record<string, unknown> {
  return {
    account_email: args.account_email,
    metric_date: args.metric_date,
    domain: args.domain,
    workspace_slug: args.workspace_slug,
    provider_group: args.provider_group,
    sent: args.sent,
    replies: args.replies,
    replies_automatic: args.replies_automatic ?? 0,
    api_synced_at: args.api_synced_at ?? '2026-04-24T12:00:00Z',
  };
}

test('rebuildAggregates: zero-fills weekend gaps in date window', async () => {
  // Account sends Mon-Fri but not Sat/Sun. Assert all 7 dates appear after
  // aggregation.
  const accountDailyRows = [
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-20', // Monday
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 3,
      replies: 0,
    }),
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-21',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 3,
      replies: 1,
    }),
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-22',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 3,
      replies: 0,
    }),
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-23',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 3,
      replies: 0,
    }),
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-24',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 3,
      replies: 0,
    }),
    // Skips Sat 2026-04-25 and Sun 2026-04-26.
  ];
  const inventoryRows = [
    {
      account_email: 'a@example.com',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      provider_code_raw: 3,
      account_status: 'active',
      is_free_mail: false,
    },
  ];
  const { sb, upserts } = makeFakeSupabase({
    selectAllByTable: {
      infra_account_daily_metrics: accountDailyRows,
      infra_accounts: inventoryRows,
      // After Step A upsert we read this back for Step B; reuse the just-written
      // shape but as the fake just echoes selectAll, we emulate with the
      // post-aggregation data via the same map below.
      infra_domain_daily_metrics: [],
    },
  });

  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    now: fixedNow('2026-04-27T12:00:00Z'),
  };

  // Capture upserts so we can also inspect domain_daily.
  await rebuildAggregates(deps, {
    dateRange: { startDate: '2026-04-20', endDate: '2026-04-26' },
  });

  const dailyUpsert = upserts.find(c => c.table === 'infra_domain_daily_metrics')!;
  assert.ok(dailyUpsert, 'expected an infra_domain_daily_metrics upsert');
  const rows = dailyUpsert.rows as Array<Record<string, unknown>>;
  // 7 days × 1 (domain, provider) = 7 rows
  assert.equal(rows.length, 7);
  const dates = rows.map(r => r.metric_date as string).sort();
  assert.deepEqual(dates, [
    '2026-04-20',
    '2026-04-21',
    '2026-04-22',
    '2026-04-23',
    '2026-04-24',
    '2026-04-25',
    '2026-04-26',
  ]);
  // Weekends are zero-filled.
  const sat = rows.find(r => r.metric_date === '2026-04-25')!;
  const sun = rows.find(r => r.metric_date === '2026-04-26')!;
  assert.equal(sat.sent, 0);
  assert.equal(sat.replies, 0);
  assert.equal(sat.rr_pct, null);
  assert.equal(sun.sent, 0);
  assert.equal(sun.rr_pct, null);
});

test('rebuildAggregates: rr_pct = (replies/sent)*100 when sent>0, null when sent=0', async () => {
  const accountDailyRows = [
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-23',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 200,
      replies: 5,
    }),
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-24',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 0,
      replies: 0,
    }),
  ];
  const { sb, upserts } = makeFakeSupabase({
    selectAllByTable: {
      infra_account_daily_metrics: accountDailyRows,
      infra_accounts: [
        {
          account_email: 'a@example.com',
          domain: 'example.com',
          workspace_slug: 'outlook-3',
          provider_group: 'outlook',
          provider_code_raw: 3,
          account_status: 'active',
          is_free_mail: false,
        },
      ],
      infra_domain_daily_metrics: [],
    },
  });
  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    now: fixedNow('2026-04-25T12:00:00Z'),
  };
  await rebuildAggregates(deps, {
    dateRange: { startDate: '2026-04-23', endDate: '2026-04-24' },
  });
  const dailyUpsert = upserts.find(c => c.table === 'infra_domain_daily_metrics')!;
  const rows = dailyUpsert.rows as Array<Record<string, unknown>>;
  const r23 = rows.find(r => r.metric_date === '2026-04-23')!;
  const r24 = rows.find(r => r.metric_date === '2026-04-24')!;
  assert.equal(r23.sent, 200);
  assert.equal(r23.replies, 5);
  assert.equal(r23.rr_pct, (5 / 200) * 100); // 2.5
  assert.equal(r24.sent, 0);
  assert.equal(r24.rr_pct, null);
});

test('rebuildAggregates: lifetime aggregate sum matches per-day sum', async () => {
  // Two daily rows for a domain. The lifetime upsert into infra_domain_metrics
  // should reflect the sum of everything in infra_domain_daily_metrics for the
  // domain (which the fake returns).
  const accountDailyRows = [
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-23',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 100,
      replies: 1,
      replies_automatic: 2,
    }),
    makeAccountDailyRow({
      account_email: 'a@example.com',
      metric_date: '2026-04-24',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      sent: 50,
      replies: 0,
      replies_automatic: 1,
    }),
  ];
  // Pretend lifetime domain-daily already contains the same shape.
  const lifetimeDomainDaily = [
    {
      domain: 'example.com',
      metric_date: '2026-04-23',
      provider_group: 'outlook',
      sent: 100,
      replies: 1,
      replies_automatic: 2,
      api_synced_at: '2026-04-24T12:00:00Z',
    },
    {
      domain: 'example.com',
      metric_date: '2026-04-24',
      provider_group: 'outlook',
      sent: 50,
      replies: 0,
      replies_automatic: 1,
      api_synced_at: '2026-04-25T12:00:00Z',
    },
  ];
  const inventoryRows = [
    {
      account_email: 'a@example.com',
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      provider_code_raw: 3,
      account_status: 'active',
      is_free_mail: false,
    },
  ];
  const { sb, upserts } = makeFakeSupabase({
    selectAllByTable: {
      infra_account_daily_metrics: accountDailyRows,
      infra_accounts: inventoryRows,
      infra_domain_daily_metrics: lifetimeDomainDaily,
    },
  });
  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    now: fixedNow('2026-04-25T12:00:00Z'),
  };
  await rebuildAggregates(deps, {
    dateRange: { startDate: '2026-04-23', endDate: '2026-04-24' },
  });
  const lifetimeUpsert = upserts.find(c => c.table === 'infra_domain_metrics')!;
  assert.ok(lifetimeUpsert);
  const rows = lifetimeUpsert.rows as Array<Record<string, unknown>>;
  assert.equal(rows.length, 1);
  const r = rows[0]!;
  assert.equal(r.sent_total, 150);
  assert.equal(r.reply_count_total, 1);
  assert.equal(r.auto_reply_count_total, 3);
  assert.equal(r.rr_pct, (1 / 150) * 100);
  assert.equal(r.first_metric_date, '2026-04-23');
  assert.equal(r.last_metric_date, '2026-04-24');
  assert.equal(r.inbox_count, 1);
  assert.equal(r.active_inbox_count, 1);
  assert.equal(r.workspace_count, 1);
  assert.equal(r.is_free_mail, false);
  assert.equal(r.provider_group, 'outlook');
  assert.equal(r.dominant_provider_raw, 3);
  assert.equal(r.source_coverage_status, 'full');
});
