import test from 'node:test';
import assert from 'node:assert/strict';

import { InstantlyClient } from '../src/instantly';
import type { AccountDailyMetric } from '../src/infra/types';

type FetchCall = { url: string; init?: RequestInit };

function installFetchStub(handler: (call: FetchCall) => Response | Promise<Response>) {
  const calls: FetchCall[] = [];
  const original = globalThis.fetch;
  globalThis.fetch = (async (input: any, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    calls.push({ url, init });
    return handler({ url, init });
  }) as typeof fetch;
  return {
    calls,
    restore: () => {
      globalThis.fetch = original;
    },
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function metric(email: string, date: string, sent: number): AccountDailyMetric {
  return {
    date,
    email_account: email,
    sent,
    bounced: 0,
    contacted: sent,
    new_leads_contacted: sent,
    opened: 0,
    unique_opened: 0,
    replies: 0,
    unique_replies: 0,
    replies_automatic: 0,
    unique_replies_automatic: 0,
    clicks: 0,
    unique_clicks: 0,
  };
}

class PlannedAnalyticsClient extends InstantlyClient {
  requests: Array<{ startDate: string; endDate: string }> = [];

  constructor(private plan: Record<string, AccountDailyMetric[] | Error>) {
    super('test-key');
  }

  override async getWorkspaceAccountDailyAnalytics(params: {
    startDate: string;
    endDate: string;
  }): Promise<AccountDailyMetric[]> {
    this.requests.push(params);
    const result = this.plan[`${params.startDate}..${params.endDate}`];
    if (result instanceof Error) throw result;
    return result ?? [];
  }
}

test('getWorkspaceAccountDailyAnalytics filters out the phantom empty-email row', async () => {
  const stub = installFetchStub(() =>
    jsonResponse([
      { date: '2026-04-23', email_account: '', sent: 0, replies: 0 },
      { date: '2026-04-23', email_account: 'a@example.com', sent: 5, replies: 1 },
      { date: '2026-04-23', email_account: 'b@example.com', sent: 7, replies: 0 },
    ]),
  );
  try {
    const client = new InstantlyClient('test-key');
    const rows = await client.getWorkspaceAccountDailyAnalytics({
      startDate: '2026-04-17',
      endDate: '2026-04-23',
    });
    assert.equal(rows.length, 2);
    assert.deepEqual(
      rows.map(r => r.email_account).sort(),
      ['a@example.com', 'b@example.com'],
    );
  } finally {
    stub.restore();
  }
});

test('getWorkspaceAccountDailyAnalytics passes no email-filter params (Phase 0 finding)', async () => {
  const stub = installFetchStub(() => jsonResponse([]));
  try {
    const client = new InstantlyClient('test-key');
    await client.getWorkspaceAccountDailyAnalytics({
      startDate: '2026-04-17',
      endDate: '2026-04-23',
    });
    assert.equal(stub.calls.length, 1);
    const url = new URL(stub.calls[0]!.url);
    assert.equal(url.pathname, '/api/v2/accounts/analytics/daily');
    assert.equal(url.searchParams.get('start_date'), '2026-04-17');
    assert.equal(url.searchParams.get('end_date'), '2026-04-23');
    for (const banned of ['emails', 'email', 'email_account', 'account', 'accounts', 'emails[]']) {
      assert.equal(url.searchParams.has(banned), false, `expected no ${banned} param`);
    }
  } finally {
    stub.restore();
  }
});

test('getWorkspaceAccountDailyAnalytics increments apiCallCount once per call', async () => {
  const stub = installFetchStub(() => jsonResponse([]));
  try {
    const client = new InstantlyClient('test-key');
    assert.equal(client.apiCallCount, 0);
    await client.getWorkspaceAccountDailyAnalytics({
      startDate: '2026-04-17',
      endDate: '2026-04-23',
    });
    assert.equal(client.apiCallCount, 1);
    await client.getWorkspaceAccountDailyAnalytics({
      startDate: '2026-04-17',
      endDate: '2026-04-23',
    });
    assert.equal(client.apiCallCount, 2);
  } finally {
    stub.restore();
  }
});

test('getWorkspaceAccountDailyAnalyticsAdaptive splits 500 windows and dedupes combined rows', async () => {
  const client = new PlannedAnalyticsClient({
    '2026-04-20..2026-04-27': new Error(
      'Instantly 500 on GET /accounts/analytics/daily after 5 attempts: upstream error',
    ),
    '2026-04-20..2026-04-23': [
      metric('a@example.com', '2026-04-20', 3),
      metric('a@example.com', '2026-04-20', 3),
    ],
    '2026-04-24..2026-04-27': [metric('b@example.com', '2026-04-24', 4)],
  });

  const rows = await client.getWorkspaceAccountDailyAnalyticsAdaptive({
    startDate: '2026-04-20',
    endDate: '2026-04-27',
  });

  assert.deepEqual(client.requests, [
    { startDate: '2026-04-20', endDate: '2026-04-27' },
    { startDate: '2026-04-20', endDate: '2026-04-23' },
    { startDate: '2026-04-24', endDate: '2026-04-27' },
  ]);
  assert.deepEqual(
    rows.map(r => `${r.email_account}|${r.date}|${r.sent}`),
    ['a@example.com|2026-04-20|3', 'b@example.com|2026-04-24|4'],
  );
});

test('getWorkspaceAccountDailyAnalyticsAdaptive does not split successful full windows', async () => {
  const client = new PlannedAnalyticsClient({
    '2026-04-20..2026-04-27': [metric('a@example.com', '2026-04-20', 3)],
  });

  const rows = await client.getWorkspaceAccountDailyAnalyticsAdaptive({
    startDate: '2026-04-20',
    endDate: '2026-04-27',
  });

  assert.equal(rows.length, 1);
  assert.deepEqual(client.requests, [
    { startDate: '2026-04-20', endDate: '2026-04-27' },
  ]);
});

test('getWorkspaceAccountDailyAnalyticsAdaptive bubbles single-day 500', async () => {
  const client = new PlannedAnalyticsClient({
    '2026-04-27..2026-04-27': new Error(
      'Instantly 500 on GET /accounts/analytics/daily after 5 attempts: upstream error',
    ),
  });

  await assert.rejects(
    () =>
      client.getWorkspaceAccountDailyAnalyticsAdaptive({
        startDate: '2026-04-27',
        endDate: '2026-04-27',
      }),
    /Instantly 500/,
  );
  assert.deepEqual(client.requests, [
    { startDate: '2026-04-27', endDate: '2026-04-27' },
  ]);
});

test('getWorkspaceAccountDailyAnalyticsAdaptive preserves 429 behavior', async () => {
  const client = new PlannedAnalyticsClient({
    '2026-04-20..2026-04-27': new Error(
      'Instantly 429 on GET /accounts/analytics/daily after 5 attempts: rate limited',
    ),
  });

  await assert.rejects(
    () =>
      client.getWorkspaceAccountDailyAnalyticsAdaptive({
        startDate: '2026-04-20',
        endDate: '2026-04-27',
      }),
    /Instantly 429/,
  );
  assert.deepEqual(client.requests, [
    { startDate: '2026-04-20', endDate: '2026-04-27' },
  ]);
});

test('429 response increments rateLimitEvents and triggers retry', async () => {
  let callCount = 0;
  const stub = installFetchStub(() => {
    callCount++;
    if (callCount === 1) return new Response('rate limited', { status: 429 });
    return jsonResponse([{ date: '2026-04-23', email_account: 'a@example.com', sent: 3 }]);
  });
  try {
    const client = new InstantlyClient('test-key');
    const rows = await client.getWorkspaceAccountDailyAnalytics({
      startDate: '2026-04-17',
      endDate: '2026-04-23',
    });
    assert.equal(rows.length, 1);
    assert.equal(client.rateLimitEvents, 1);
    assert.equal(client.apiCallCount, 2);
    assert.equal(stub.calls.length, 2);
  } finally {
    stub.restore();
  }
});

test('5 consecutive 429s throws', async () => {
  const stub = installFetchStub(() => new Response('rate limited', { status: 429 }));
  try {
    const client = new InstantlyClient('test-key');
    await assert.rejects(
      () =>
        client.getWorkspaceAccountDailyAnalytics({
          startDate: '2026-04-17',
          endDate: '2026-04-23',
        }),
      /429/,
    );
    assert.equal(client.rateLimitEvents, 5);
    assert.equal(client.apiCallCount, 5);
    assert.equal(stub.calls.length, 5);
  } finally {
    stub.restore();
  }
});

test('custom tag mappings retry transient 502 responses', async () => {
  let callCount = 0;
  const stub = installFetchStub(() => {
    callCount++;
    if (callCount === 1) return new Response('bad gateway', { status: 502 });
    return jsonResponse({ items: [{ id: 'mapping-1', tag_id: 'tag-1', resource_id: 'campaign-1', resource_type: 2 }] });
  });
  try {
    const client = new InstantlyClient('test-key');
    const mappings = await client.getAllCustomTagMappings();
    assert.equal(mappings.length, 1);
    assert.equal(client.apiCallCount, 2);
    assert.equal(stub.calls.length, 2);
  } finally {
    stub.restore();
  }
});

test('getAccountsRaw with search includes the search query param', async () => {
  const stub = installFetchStub(() =>
    jsonResponse({
      items: [{ email: 'a@tryunsecuredhq.co', provider_code: 3 }],
    }),
  );
  try {
    const client = new InstantlyClient('test-key');
    const accounts = await client.getAccountsRaw({ search: 'tryunsecuredhq.co' });
    assert.equal(accounts.length, 1);
    assert.equal(accounts[0]!.email, 'a@tryunsecuredhq.co');
    assert.equal(accounts[0]!.provider_code, 3);
    assert.equal(stub.calls.length, 1);
    const url = new URL(stub.calls[0]!.url);
    assert.equal(url.pathname, '/api/v2/accounts');
    assert.equal(url.searchParams.get('search'), 'tryunsecuredhq.co');
    assert.equal(url.searchParams.get('limit'), '100');
  } finally {
    stub.restore();
  }
});

test('getAccountsRaw paginates across two pages when next_starting_after is set', async () => {
  let callCount = 0;
  const stub = installFetchStub(({ url }) => {
    callCount++;
    const u = new URL(url);
    if (callCount === 1) {
      assert.equal(u.searchParams.has('starting_after'), false);
      return jsonResponse({
        items: [{ email: 'a@example.com' }, { email: 'b@example.com' }],
        next_starting_after: 'cursor-1',
      });
    }
    assert.equal(u.searchParams.get('starting_after'), 'cursor-1');
    return jsonResponse({
      items: [{ email: 'c@example.com' }],
    });
  });
  try {
    const client = new InstantlyClient('test-key');
    const accounts = await client.getAccountsRaw();
    assert.equal(stub.calls.length, 2);
    assert.deepEqual(
      accounts.map(a => a.email),
      ['a@example.com', 'b@example.com', 'c@example.com'],
    );
  } finally {
    stub.restore();
  }
});
