import test from 'node:test';
import assert from 'node:assert/strict';

import { InstantlyClient } from '../src/instantly';

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
