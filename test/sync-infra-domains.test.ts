import test from 'node:test';
import assert from 'node:assert/strict';

import {
  type AggregatePgClient,
  inventory,
  metricsIncremental,
  pickDominantProviderGroup,
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

interface QueryCall {
  text: string;
  values?: unknown[];
}

interface FakeAggregateClient extends AggregatePgClient {
  connected: boolean;
  ended: boolean;
  queries: QueryCall[];
}

function makeFakeAggregateClient(
  rows: Array<{ provider_group: string; domains_written: string }> = [
    { provider_group: 'unknown', domains_written: '0' },
  ],
): FakeAggregateClient {
  return {
    connected: false,
    ended: false,
    queries: [],
    async connect() {
      this.connected = true;
    },
    async query<T extends Record<string, unknown>>(text: string, values?: unknown[]) {
      this.queries.push({ text, values });
      return {
        rows: text.includes('rebuild_infra_aggregates') ? rows as T[] : [],
      };
    },
    async end() {
      this.ended = true;
    },
  };
}

function makeAggregateClientFactory(
  rows?: Array<{ provider_group: string; domains_written: string }>,
): { makePgClient: (connectionString: string) => AggregatePgClient; clients: FakeAggregateClient[] } {
  const clients: FakeAggregateClient[] = [];
  return {
    clients,
    makePgClient: () => {
      const client = makeFakeAggregateClient(rows);
      clients.push(client);
      return client;
    },
  };
}

process.env.PIPELINE_SUPABASE_DB_URL ??= 'postgres://test:test@localhost:5432/test';

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
    getWorkspaceAccountDailyAnalyticsAdaptive: async () => plan.daily ?? [],
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

test('inventory: upserts to infra_accounts with provider_group from provider_code', async () => {
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
      accounts: [{ email: 'c@example.com', provider_code: 1, status: 'paused' }],
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
    assert.equal(r.provider_code_raw, 1);
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

test('metricsIncremental: upserts to infra_account_daily_metrics with provider_group from inventory', async () => {
  const { sb, upserts } = makeFakeSupabase({
    selectAllByTable: {
      infra_accounts: [
        {
          account_email: 'a@tryunsecuredhq.co',
          provider_group: 'outlook',
        },
      ],
    },
  });
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
    makePgClient: makeAggregateClientFactory().makePgClient,
    now: fixedNow('2026-04-24T12:00:00Z'),
  };

  const stats = await metricsIncremental(deps, { days: 7 });
  assert.equal(stats.accountMetricRows, 1);
  const dailyUpserts = upserts.filter(c => c.table === 'infra_account_daily_metrics');
  assert.equal(dailyUpserts.length, 1);
  const row = (dailyUpserts[0]!.rows[0] as Record<string, unknown>);
  assert.equal(row.provider_group, 'outlook');
  assert.equal(row.workspace_slug, 'outlook-3');
  assert.equal(row.account_email, 'a@tryunsecuredhq.co');
  assert.equal(row.metric_date, '2026-04-23');
  assert.equal(row.sent, 5);
  assert.equal(row.replies, 1);
});

test('metricsIncremental: provider_group resolves cross-workspace via global infra_accounts lookup', async () => {
  // Regression test for the 2026-04-27 Step 2 follow-up bug: Instantly's
  // daily-metrics endpoint returns activity for accounts whose inventory
  // home is a *different* workspace than the API call. The writer must
  // still resolve provider_group correctly via the account's infra_accounts
  // row, not fall back to 'unknown'.
  //
  // Scenario: account 'a@tryclearyield.co' lives in renaissance-1 with
  // provider_group='google_otd'. Daily metrics endpoint called with
  // section-125-2's API key returns activity for that account. Expected:
  // upsert row has provider_group='google_otd' (from the global lookup),
  // not 'unknown' (which would happen with a workspace_slug-filtered lookup).
  const { sb, upserts } = makeFakeSupabase({
    selectAllByTable: {
      infra_accounts: [
        {
          account_email: 'a@tryclearyield.co',
          provider_group: 'google_otd',
          // workspace_slug field intentionally omitted to mirror the
          // filter-free SELECT shape used by loadAccountProviderGroups.
        },
      ],
    },
  });
  const deps: SyncDeps = {
    keyMap: { 'section-125-2': 'k_s125' },
    supabase: sb,
    makeClient: () =>
      makeFakeClient({
        daily: [
          {
            date: '2026-04-23',
            email_account: 'a@tryclearyield.co',
            sent: 5,
            bounced: 0,
            contacted: 5,
            new_leads_contacted: 5,
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
    makePgClient: makeAggregateClientFactory().makePgClient,
    now: fixedNow('2026-04-24T12:00:00Z'),
  };

  const stats = await metricsIncremental(deps, { days: 7 });
  assert.equal(stats.accountMetricRows, 1);
  const dailyUpserts = upserts.filter(c => c.table === 'infra_account_daily_metrics');
  assert.equal(dailyUpserts.length, 1);
  const row = dailyUpserts[0]!.rows[0] as Record<string, unknown>;
  // Critical: provider_group is the account's correct attribution, NOT 'unknown'
  assert.equal(row.provider_group, 'google_otd');
  // workspace_slug reflects the API call's workspace, which differs from inventory home
  assert.equal(row.workspace_slug, 'section-125-2');
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
    makePgClient: makeAggregateClientFactory().makePgClient,
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
    makePgClient: makeAggregateClientFactory().makePgClient,
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

test('pickDominantProviderGroup: highest sent volume wins over active account count', () => {
  assert.equal(
    pickDominantProviderGroup([
      { provider_group: 'google_otd', sent: 300, active_account_count: 3 },
      { provider_group: 'outlook', sent: 250, active_account_count: 5 },
    ]),
    'google_otd',
  );
});

test('pickDominantProviderGroup: equal sent volume uses active-account tiebreaker', () => {
  assert.equal(
    pickDominantProviderGroup([
      { provider_group: 'google_otd', sent: 300, active_account_count: 3 },
      { provider_group: 'outlook', sent: 300, active_account_count: 5 },
    ]),
    'outlook',
  );
});

test('pickDominantProviderGroup: equal sent and active count uses lexical tiebreaker', () => {
  assert.equal(
    pickDominantProviderGroup([
      { provider_group: 'outlook', sent: 300, active_account_count: 5 },
      { provider_group: 'google_otd', sent: 300, active_account_count: 5 },
    ]),
    'google_otd',
  );
});

test('rebuildAggregates: calls SQL function through direct pg with 15min timeout', async () => {
  const original = process.env.PIPELINE_SUPABASE_DB_URL;
  process.env.PIPELINE_SUPABASE_DB_URL = 'postgres://test:test@localhost:5432/test';
  const factory = makeAggregateClientFactory([
    { provider_group: 'google_otd', domains_written: '12' },
    { provider_group: 'outlook', domains_written: '7' },
  ]);
  const { sb } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    makePgClient: factory.makePgClient,
  };

  try {
    const result = await rebuildAggregates(deps, {
      workspaceFilter: 'renaissance-4',
      dateRange: { startDate: '2026-04-20', endDate: '2026-04-27' },
    });

    assert.equal(result.domainsWritten, 19);
    assert.deepEqual(result.errors, []);
    assert.equal(factory.clients.length, 1);
    const client = factory.clients[0]!;
    assert.equal(client.connected, true);
    assert.equal(client.ended, true);
    assert.deepEqual(client.queries, [
      { text: "set statement_timeout = '15min'", values: undefined },
      {
        text: 'select * from public.rebuild_infra_aggregates($1, $2, $3)',
        values: ['renaissance-4', '2026-04-20', '2026-04-27'],
      },
    ]);
  } finally {
    if (original === undefined) delete process.env.PIPELINE_SUPABASE_DB_URL;
    else process.env.PIPELINE_SUPABASE_DB_URL = original;
  }
});

test('rebuildAggregates: requires PIPELINE_SUPABASE_DB_URL', async () => {
  const original = process.env.PIPELINE_SUPABASE_DB_URL;
  delete process.env.PIPELINE_SUPABASE_DB_URL;
  const { sb } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    makePgClient: makeAggregateClientFactory().makePgClient,
  };

  try {
    await assert.rejects(
      () => rebuildAggregates(deps, {}),
      /Missing PIPELINE_SUPABASE_DB_URL/,
    );
  } finally {
    if (original !== undefined) process.env.PIPELINE_SUPABASE_DB_URL = original;
  }
});

test('rebuildAggregates: closes pg client when SQL call fails', async () => {
  const original = process.env.PIPELINE_SUPABASE_DB_URL;
  process.env.PIPELINE_SUPABASE_DB_URL = 'postgres://test:test@localhost:5432/test';
  const client = makeFakeAggregateClient();
  client.query = async function query<T extends Record<string, unknown>>(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    if (text.includes('rebuild_infra_aggregates')) throw new Error('boom');
    return { rows: [] as T[] };
  };
  const { sb } = makeFakeSupabase();
  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    makePgClient: () => client,
  };

  try {
    await assert.rejects(
      () => rebuildAggregates(deps, {}),
      /boom/,
    );
    assert.equal(client.ended, true);
  } finally {
    if (original === undefined) delete process.env.PIPELINE_SUPABASE_DB_URL;
    else process.env.PIPELINE_SUPABASE_DB_URL = original;
  }
});

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

test.skip('rebuildAggregates: zero-fills weekend gaps in date window', async () => {
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

test.skip('rebuildAggregates: rr_pct = (replies/sent)*100 when sent>0, null when sent=0', async () => {
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

test.skip('rebuildAggregates: dominant provider uses sent volume for daily and lifetime rows', async () => {
  const accountDailyRows: Array<Record<string, unknown>> = [];
  const inventoryRows: Array<Record<string, unknown>> = [];
  for (let i = 0; i < 3; i++) {
    const email = `g${i}@example.com`;
    accountDailyRows.push(
      makeAccountDailyRow({
        account_email: email,
        metric_date: '2026-04-24',
        domain: 'example.com',
        workspace_slug: 'renaissance-3',
        provider_group: 'google_otd',
        sent: 100,
        replies: 1,
      }),
    );
    inventoryRows.push({
      account_email: email,
      domain: 'example.com',
      workspace_slug: 'renaissance-3',
      provider_group: 'google_otd',
      provider_code_raw: 1,
      account_status: 'active',
      is_free_mail: false,
    });
  }
  for (let i = 0; i < 5; i++) {
    const email = `m${i}@example.com`;
    accountDailyRows.push(
      makeAccountDailyRow({
        account_email: email,
        metric_date: '2026-04-24',
        domain: 'example.com',
        workspace_slug: 'outlook-3',
        provider_group: 'outlook',
        sent: 50,
        replies: 0,
      }),
    );
    inventoryRows.push({
      account_email: email,
      domain: 'example.com',
      workspace_slug: 'outlook-3',
      provider_group: 'outlook',
      provider_code_raw: 3,
      account_status: 'active',
      is_free_mail: false,
    });
  }

  const { sb, upserts } = makeFakeSupabase({
    selectAllByTable: {
      infra_account_daily_metrics: accountDailyRows,
      infra_accounts: inventoryRows,
    },
  });
  const deps: SyncDeps = {
    keyMap: {},
    supabase: sb,
    makeClient: () => makeFakeClient({}),
    now: fixedNow('2026-04-25T12:00:00Z'),
  };

  await rebuildAggregates(deps, {
    dateRange: { startDate: '2026-04-24', endDate: '2026-04-24' },
  });

  const dailyUpsert = upserts.find(c => c.table === 'infra_domain_daily_metrics')!;
  const dailyRows = dailyUpsert.rows as Array<Record<string, unknown>>;
  assert.equal(dailyRows.length, 1);
  assert.equal(dailyRows[0]!.provider_group, 'google_otd');
  assert.equal(dailyRows[0]!.sent, 550);

  const lifetimeUpsert = upserts.find(c => c.table === 'infra_domain_metrics')!;
  const lifetimeRows = lifetimeUpsert.rows as Array<Record<string, unknown>>;
  assert.equal(lifetimeRows.length, 1);
  assert.equal(lifetimeRows[0]!.provider_group, 'google_otd');
  assert.equal(lifetimeRows[0]!.dominant_provider_raw, 1);
  assert.equal(lifetimeRows[0]!.sent_total, 550);
});

test.skip('rebuildAggregates: lifetime aggregate sum matches per-day sum', async () => {
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
