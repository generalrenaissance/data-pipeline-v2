import type { InstantlyClient } from '../instantly';
import type { SupabaseClient } from '../supabase';
import type { Account } from '../types';
import type { AccountDailyMetric } from './types';

import { emailToDomain } from './domain-utils';
import { isFreeMailDomain } from './free-mail';
import {
  EXCLUDED_SLUGS,
  accountProviderCodeToGroup,
  type ProviderGroup,
} from './provider-routing';

export interface SyncDeps {
  keyMap: Record<string, string>;
  supabase: SupabaseClient;
  /** Optional run id for logging context. Not consumed by the module itself. */
  runId?: string;
  /** Injected to allow mocking in tests. */
  makeClient: (apiKey: string) => InstantlyClient;
  /** Injected for deterministic timestamps in tests; defaults to () => new Date(). */
  now?: () => Date;
}

export interface RunStats {
  workspaceCount: number;
  accountsSeen: number;
  accountMetricRows: number;
  domainsWritten: number;
  apiCalls: number;
  rateLimitEvents: number;
  durationMs: number;
  errors: string[];
}

interface InfraAccountUpsertRow {
  account_email: string;
  domain: string;
  workspace_slug: string;
  workspace_name: string | null;
  provider_code_raw: number | null;
  provider_group: ProviderGroup;
  account_status: string | null;
  warmup_status: string | null;
  daily_limit: number | null;
  sending_gap_seconds: number | null;
  first_name: string | null;
  last_name: string | null;
  is_free_mail: boolean;
  last_seen_at: string;
  api_synced_at: string;
  raw_account: Account;
}

interface InfraAccountDailyUpsertRow {
  account_email: string;
  metric_date: string;
  domain: string;
  workspace_slug: string;
  provider_group: ProviderGroup;
  sent: number;
  bounced: number;
  contacted: number;
  new_leads_contacted: number;
  opened: number;
  unique_opened: number;
  replies: number;
  unique_replies: number;
  replies_automatic: number;
  unique_replies_automatic: number;
  clicks: number;
  unique_clicks: number;
  api_synced_at: string;
}

interface InfraAccountRow {
  account_email: string;
  domain: string;
  workspace_slug: string;
  provider_group: ProviderGroup;
  provider_code_raw: number | null;
  account_status: string | null;
  is_free_mail: boolean;
}

interface InfraAccountProviderRow {
  account_email: string;
  provider_group: ProviderGroup;
}

interface InfraAccountDailyRow {
  account_email: string;
  metric_date: string;
  domain: string;
  workspace_slug: string;
  provider_group: ProviderGroup;
  sent: number;
  replies: number;
  replies_automatic: number;
  api_synced_at: string;
}

interface ProviderContribution {
  provider_group: ProviderGroup;
  sent: number;
  active_account_count: number;
}

function emptyStats(): RunStats {
  return {
    workspaceCount: 0,
    accountsSeen: 0,
    accountMetricRows: 0,
    domainsWritten: 0,
    apiCalls: 0,
    rateLimitEvents: 0,
    durationMs: 0,
    errors: [],
  };
}

function nowIso(deps: SyncDeps): string {
  return (deps.now ? deps.now() : new Date()).toISOString();
}

function inScopeWorkspaces(
  keyMap: Record<string, string>,
  workspaceFilter?: string,
): Array<[string, string]> {
  return Object.entries(keyMap).filter(([slug]) => {
    if (EXCLUDED_SLUGS.includes(slug)) return false;
    if (workspaceFilter && workspaceFilter !== slug) return false;
    return true;
  });
}

function pickStr(v: unknown): string | null {
  if (typeof v !== 'string') return null;
  const t = v.trim();
  return t.length === 0 ? null : t;
}

function pickNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return null;
}

export function pickDominantProviderGroup(
  contributions: ProviderContribution[],
): ProviderGroup {
  if (contributions.length === 0) return 'unknown';
  let best: ProviderContribution | null = null;
  for (const c of contributions) {
    if (!best) {
      best = c;
      continue;
    }
    if (c.sent > best.sent) {
      best = c;
      continue;
    }
    if (c.sent === best.sent && c.active_account_count > best.active_account_count) {
      best = c;
      continue;
    }
    if (
      c.sent === best.sent &&
      c.active_account_count === best.active_account_count &&
      c.provider_group < best.provider_group
    ) {
      best = c;
    }
  }
  return best?.provider_group ?? 'unknown';
}

// Instantly v2 returns account.status and account.warmup_status as integers.
// Spec section 8D references string statuses ('active' / 'paused' /
// 'connection_error'), so we map at ingest. Unknown ints are preserved
// stringified for forward compatibility.
function accountStatusFromInt(v: number | null): string | null {
  if (v === null) return null;
  if (v === 1) return 'active';
  if (v === 2) return 'paused';
  if (v === -1) return 'connection_error';
  if (v === -2) return 'connection_error';
  return String(v);
}

function warmupStatusFromInt(v: number | null): string | null {
  if (v === null) return null;
  if (v === 1) return 'active';
  if (v === 0) return 'paused';
  return String(v);
}

function buildAccountUpsertRow(
  account: Account,
  slug: string,
  syncedAt: string,
): InfraAccountUpsertRow | null {
  const email = typeof account.email === 'string' ? account.email.trim().toLowerCase() : '';
  if (!email) return null;
  const domain = emailToDomain(email);
  if (!domain) return null;
  const workspaceName =
    pickStr((account as Record<string, unknown>).workspace_name) ??
    pickStr((account as Record<string, unknown>).workspace);
  return {
    account_email: email,
    domain,
    workspace_slug: slug,
    workspace_name: workspaceName,
    provider_code_raw: pickNum(account.provider_code),
    provider_group: accountProviderCodeToGroup(pickNum(account.provider_code)),
    account_status: accountStatusFromInt(pickNum(account.status)),
    warmup_status: warmupStatusFromInt(pickNum(account.warmup_status)),
    daily_limit: pickNum((account as Record<string, unknown>).daily_limit),
    sending_gap_seconds: pickNum((account as Record<string, unknown>).sending_gap),
    first_name: pickStr((account as Record<string, unknown>).first_name),
    last_name: pickStr((account as Record<string, unknown>).last_name),
    is_free_mail: isFreeMailDomain(domain),
    last_seen_at: syncedAt,
    api_synced_at: syncedAt,
    raw_account: account,
  };
}

/**
 * Inventory sync — refresh `infra_accounts` for in-scope workspaces.
 *
 * Per spec section 5A, `first_seen_at` has a column default and is intentionally
 * omitted from upsert payloads so PostgREST `merge-duplicates` preserves the
 * original insert timestamp on conflict.
 */
export async function inventory(
  deps: SyncDeps,
  opts: { workspaceFilter?: string } = {},
): Promise<RunStats> {
  const start = Date.now();
  const stats = emptyStats();
  const workspaces = inScopeWorkspaces(deps.keyMap, opts.workspaceFilter);

  for (const [slug, key] of workspaces) {
    stats.workspaceCount++;
    const client = deps.makeClient(key);
    try {
      const accounts = await client.getAccountsRaw();
      const syncedAt = nowIso(deps);
      const rows: InfraAccountUpsertRow[] = [];
      for (const account of accounts) {
        const row = buildAccountUpsertRow(account, slug, syncedAt);
        if (row) rows.push(row);
      }
      if (rows.length > 0) {
        await deps.supabase.upsert('infra_accounts', rows, 'account_email');
      }
      stats.accountsSeen += rows.length;
    } catch (err) {
      stats.errors.push(
        `inventory ${slug}: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      stats.apiCalls += client.apiCallCount;
      stats.rateLimitEvents += client.rateLimitEvents;
    }
  }

  stats.durationMs = Date.now() - start;
  return stats;
}

async function pullMetricsForRange(
  deps: SyncDeps,
  opts: { workspaceFilter?: string; startDate: string; endDate: string },
  stats: RunStats,
): Promise<void> {
  const workspaces = inScopeWorkspaces(deps.keyMap, opts.workspaceFilter);

  for (const [slug, key] of workspaces) {
    stats.workspaceCount++;
    const client = deps.makeClient(key);
    try {
      const accountProviders = await loadAccountProviderGroups(deps, slug);
      const rows = await client.getWorkspaceAccountDailyAnalytics({
        startDate: opts.startDate,
        endDate: opts.endDate,
      });
      // Defense in depth: client already strips phantom rows; double-check and
      // bump errors if any leak through (per spec section 8E phantom-row rule).
      const filtered: AccountDailyMetric[] = [];
      let phantomLeaks = 0;
      for (const r of rows) {
        if (!r.email_account || r.email_account === '') {
          phantomLeaks++;
          continue;
        }
        filtered.push(r);
      }
      if (phantomLeaks > 0) {
        stats.errors.push(
          `${slug}: ${phantomLeaks} phantom empty-email row(s) leaked past client filter`,
        );
      }

      const syncedAt = nowIso(deps);
      const upsertRows: InfraAccountDailyUpsertRow[] = [];
      for (const r of filtered) {
        const email = r.email_account.trim().toLowerCase();
        const domain = emailToDomain(email);
        if (!domain) continue;
        const metricDate = String(r.date ?? '').trim();
        if (!metricDate) continue;
        upsertRows.push({
          account_email: email,
          metric_date: metricDate,
          domain,
          workspace_slug: slug,
          provider_group: accountProviders.get(email) ?? 'unknown',
          sent: r.sent ?? 0,
          bounced: r.bounced ?? 0,
          contacted: r.contacted ?? 0,
          new_leads_contacted: r.new_leads_contacted ?? 0,
          opened: r.opened ?? 0,
          unique_opened: r.unique_opened ?? 0,
          replies: r.replies ?? 0,
          unique_replies: r.unique_replies ?? 0,
          replies_automatic: r.replies_automatic ?? 0,
          unique_replies_automatic: r.unique_replies_automatic ?? 0,
          clicks: r.clicks ?? 0,
          unique_clicks: r.unique_clicks ?? 0,
          api_synced_at: syncedAt,
        });
      }
      if (upsertRows.length > 0) {
        await deps.supabase.upsert(
          'infra_account_daily_metrics',
          upsertRows,
          'account_email,metric_date',
        );
      }
      stats.accountMetricRows += upsertRows.length;
    } catch (err) {
      stats.errors.push(
        `metrics ${slug}: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      stats.apiCalls += client.apiCallCount;
      stats.rateLimitEvents += client.rateLimitEvents;
    }
  }
}

async function loadAccountProviderGroups(
  deps: SyncDeps,
  workspaceSlug: string,
): Promise<Map<string, ProviderGroup>> {
  const rows = (await deps.supabase.selectAll(
    'infra_accounts',
    `select=account_email,provider_group&workspace_slug=eq.${encodeURIComponent(workspaceSlug)}`,
  )) as InfraAccountProviderRow[];
  return new Map(rows.map(r => [r.account_email.trim().toLowerCase(), r.provider_group]));
}

function todayIso(deps: SyncDeps): string {
  const d = deps.now ? deps.now() : new Date();
  return d.toISOString().slice(0, 10);
}

function shiftDateIso(dateIso: string, deltaDays: number): string {
  const [y, m, d] = dateIso.split('-').map(n => parseInt(n, 10));
  const dt = new Date(Date.UTC(y!, (m! - 1), d!));
  dt.setUTCDate(dt.getUTCDate() + deltaDays);
  return dt.toISOString().slice(0, 10);
}

function enumerateDates(startDate: string, endDate: string): string[] {
  const out: string[] = [];
  let cur = startDate;
  while (cur <= endDate) {
    out.push(cur);
    cur = shiftDateIso(cur, 1);
  }
  return out;
}

export async function metricsIncremental(
  deps: SyncDeps,
  opts: { workspaceFilter?: string; days: number },
): Promise<RunStats> {
  const start = Date.now();
  const stats = emptyStats();
  const endDate = todayIso(deps);
  const startDate = shiftDateIso(endDate, -Math.max(0, opts.days));

  await pullMetricsForRange(
    deps,
    { workspaceFilter: opts.workspaceFilter, startDate, endDate },
    stats,
  );

  const agg = await rebuildAggregates(deps, {
    workspaceFilter: opts.workspaceFilter,
    dateRange: { startDate, endDate },
  });
  stats.domainsWritten = agg.domainsWritten;
  stats.durationMs = Date.now() - start;
  return stats;
}

export async function metricsBackfill(
  deps: SyncDeps,
  opts: { workspaceFilter?: string; startDate: string; endDate: string },
): Promise<RunStats> {
  const start = Date.now();
  const stats = emptyStats();

  await pullMetricsForRange(
    deps,
    {
      workspaceFilter: opts.workspaceFilter,
      startDate: opts.startDate,
      endDate: opts.endDate,
    },
    stats,
  );

  const agg = await rebuildAggregates(deps, {
    workspaceFilter: opts.workspaceFilter,
    dateRange: { startDate: opts.startDate, endDate: opts.endDate },
  });
  stats.domainsWritten = agg.domainsWritten;
  stats.durationMs = Date.now() - start;
  return stats;
}

function pickDominantProviderCodeRaw(
  inv: InfraAccountRow[],
  rows: InfraAccountDailyRow[],
): number | null {
  const codeByAccount = new Map<string, number>();
  const activeByCode = new Map<number, number>();
  for (const account of inv) {
    if (account.provider_code_raw === null) continue;
    codeByAccount.set(account.account_email, account.provider_code_raw);
    if (account.account_status === 'active') {
      activeByCode.set(
        account.provider_code_raw,
        (activeByCode.get(account.provider_code_raw) ?? 0) + 1,
      );
    }
  }
  const sentByCode = new Map<number, number>();
  for (const row of rows) {
    const code = codeByAccount.get(row.account_email);
    if (code === undefined) continue;
    sentByCode.set(code, (sentByCode.get(code) ?? 0) + (row.sent ?? 0));
  }
  const codes = new Set([...activeByCode.keys(), ...sentByCode.keys()]);
  let best: number | null = null;
  let bestSent = -1;
  let bestActive = -1;
  for (const code of codes) {
    const sent = sentByCode.get(code) ?? 0;
    const active = activeByCode.get(code) ?? 0;
    if (
      sent > bestSent ||
      (sent === bestSent && active > bestActive) ||
      (sent === bestSent && active === bestActive && (best === null || code < best))
    ) {
      best = code;
      bestSent = sent;
      bestActive = active;
    }
  }
  return best;
}

function activeAccountsByProvider(inv: InfraAccountRow[]): Map<ProviderGroup, number> {
  const counts = new Map<ProviderGroup, number>();
  for (const row of inv) {
    if (row.account_status !== 'active') continue;
    counts.set(row.provider_group, (counts.get(row.provider_group) ?? 0) + 1);
  }
  return counts;
}

function providerContributions(
  rows: InfraAccountDailyRow[],
  activeByProvider: Map<ProviderGroup, number>,
): ProviderContribution[] {
  const sentByProvider = new Map<ProviderGroup, number>();
  for (const row of rows) {
    sentByProvider.set(row.provider_group, (sentByProvider.get(row.provider_group) ?? 0) + (row.sent ?? 0));
  }
  const providers = new Set([...sentByProvider.keys(), ...activeByProvider.keys()]);
  return [...providers].map(provider_group => ({
    provider_group,
    sent: sentByProvider.get(provider_group) ?? 0,
    active_account_count: activeByProvider.get(provider_group) ?? 0,
  }));
}

export async function rebuildAggregates(
  deps: SyncDeps,
  opts: { workspaceFilter?: string; dateRange?: { startDate: string; endDate: string } } = {},
): Promise<{ domainsWritten: number }> {
  // ---------------- Step A: infra_domain_daily_metrics ----------------
  const dailyParams: string[] = [
    'select=account_email,metric_date,domain,workspace_slug,provider_group,sent,replies,replies_automatic,api_synced_at',
  ];
  if (opts.workspaceFilter) {
    dailyParams.push(`workspace_slug=eq.${encodeURIComponent(opts.workspaceFilter)}`);
  }
  if (opts.dateRange) {
    dailyParams.push(`metric_date=gte.${opts.dateRange.startDate}`);
    dailyParams.push(`metric_date=lte.${opts.dateRange.endDate}`);
  }
  const dailyRows = (await deps.supabase.selectAll(
    'infra_account_daily_metrics',
    dailyParams.join('&'),
  )) as InfraAccountDailyRow[];

  // Inventory rows for inbox/active/workspace counts and is_free_mail / provider_code_raw.
  const inventoryParams: string[] = [
    'select=account_email,domain,workspace_slug,provider_group,provider_code_raw,account_status,is_free_mail',
  ];
  if (opts.workspaceFilter) {
    inventoryParams.push(`workspace_slug=eq.${encodeURIComponent(opts.workspaceFilter)}`);
  }
  const inventoryRows = (await deps.supabase.selectAll(
    'infra_accounts',
    inventoryParams.join('&'),
  )) as InfraAccountRow[];

  const inventoryByDomain = new Map<string, InfraAccountRow[]>();
  for (const row of inventoryRows) {
    const list = inventoryByDomain.get(row.domain) ?? [];
    list.push(row);
    inventoryByDomain.set(row.domain, list);
  }

  const activeByDomainProvider = new Map<string, Map<ProviderGroup, number>>();
  for (const [domain, inv] of inventoryByDomain) {
    activeByDomainProvider.set(domain, activeAccountsByProvider(inv));
  }

  const rowsByDomain = new Map<string, InfraAccountDailyRow[]>();
  for (const r of dailyRows) {
    const list = rowsByDomain.get(r.domain) ?? [];
    list.push(r);
    rowsByDomain.set(r.domain, list);
  }
  const dominantProviderByDomain = new Map<string, ProviderGroup>();
  for (const [domain, rows] of rowsByDomain) {
    dominantProviderByDomain.set(
      domain,
      pickDominantProviderGroup(
        providerContributions(rows, activeByDomainProvider.get(domain) ?? new Map()),
      ),
    );
  }

  // Group account-daily into (domain, metric_date) buckets. provider_group is
  // the domain's dominant provider for the rebuild window.
  type Bucket = {
    domain: string;
    metric_date: string;
    sent: number;
    replies: number;
    replies_automatic: number;
    maxSyncedAt: string;
  };
  const buckets = new Map<string, Bucket>();
  const domainsSeen = new Set<string>();
  for (const r of dailyRows) {
    const key = `${r.domain}${r.metric_date}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        domain: r.domain,
        metric_date: r.metric_date,
        sent: 0,
        replies: 0,
        replies_automatic: 0,
        maxSyncedAt: r.api_synced_at,
      };
      buckets.set(key, b);
    }
    b.sent += r.sent ?? 0;
    b.replies += r.replies ?? 0;
    b.replies_automatic += r.replies_automatic ?? 0;
    if (r.api_synced_at > b.maxSyncedAt) b.maxSyncedAt = r.api_synced_at;
    domainsSeen.add(r.domain);
  }

  // Zero-fill: for each domain seen in data, ensure every
  // date in [startDate, endDate] has a row (per spec section 8E — absent dates
  // = zero, not missing).
  const dateRange = opts.dateRange;
  if (dateRange) {
    const allDates = enumerateDates(dateRange.startDate, dateRange.endDate);
    const nowSynced = nowIso(deps);
    for (const domain of domainsSeen) {
      for (const date of allDates) {
        const key = `${domain}${date}`;
        if (buckets.has(key)) continue;
        buckets.set(key, {
          domain,
          metric_date: date,
          sent: 0,
          replies: 0,
          replies_automatic: 0,
          maxSyncedAt: nowSynced,
        });
      }
    }
  }

  const domainCountsForDay = (domain: string): {
    inbox: number;
    active: number;
    workspaces: number;
  } => {
    const inv = inventoryByDomain.get(domain) ?? [];
    const inbox = inv.length;
    const active = inv.filter(r => r.account_status === 'active').length;
    const workspaces = new Set(inv.map(r => r.workspace_slug)).size;
    return { inbox, active, workspaces };
  };

  const domainDailyUpsert: Array<Record<string, unknown>> = [];
  for (const b of buckets.values()) {
    const counts = domainCountsForDay(b.domain);
    const rrPct = b.sent > 0 ? (b.replies / b.sent) * 100 : null;
    domainDailyUpsert.push({
      domain: b.domain,
      metric_date: b.metric_date,
      provider_group: dominantProviderByDomain.get(b.domain) ?? 'unknown',
      workspace_count: counts.workspaces,
      inbox_count: counts.inbox,
      active_inbox_count: counts.active,
      sent: b.sent,
      replies: b.replies,
      replies_automatic: b.replies_automatic,
      rr_pct: rrPct,
      api_synced_at: b.maxSyncedAt,
    });
  }
  if (domainDailyUpsert.length > 0) {
    await deps.supabase.upsert(
      'infra_domain_daily_metrics',
      domainDailyUpsert,
      'domain,metric_date',
    );
  }

  // ---------------- Step B: infra_domain_metrics (lifetime) ----------------
  const affectedDomains = new Set<string>();
  for (const b of buckets.values()) affectedDomains.add(b.domain);

  if (affectedDomains.size === 0) {
    return { domainsWritten: 0 };
  }

  // Pull lifetime account-daily rows for affected domains so the lifetime
  // dominant provider is based on account-level sent volume.
  const domainList = [...affectedDomains];
  const lifetimeRows: InfraAccountDailyRow[] = [];
  // Chunk the IN() filter to keep URLs sane.
  const CHUNK = 100;
  for (let i = 0; i < domainList.length; i += CHUNK) {
    const chunk = domainList.slice(i, i + CHUNK);
    const inList = chunk.map(d => `"${d.replace(/"/g, '\\"')}"`).join(',');
    const part = (await deps.supabase.selectAll(
      'infra_account_daily_metrics',
      `select=account_email,metric_date,domain,workspace_slug,provider_group,sent,replies,replies_automatic,api_synced_at&domain=in.(${inList})`,
    )) as InfraAccountDailyRow[];
    lifetimeRows.push(...part);
  }

  const lifetimeByDomain = new Map<string, InfraAccountDailyRow[]>();
  for (const r of lifetimeRows) {
    const list = lifetimeByDomain.get(r.domain) ?? [];
    list.push(r);
    lifetimeByDomain.set(r.domain, list);
  }

  const updatedAt = nowIso(deps);
  const domainMetricsUpsert: Array<Record<string, unknown>> = [];
  for (const domain of domainList) {
    const rows = lifetimeByDomain.get(domain) ?? [];
    let sentTotal = 0;
    let replyTotal = 0;
    let autoReplyTotal = 0;
    let firstDate: string | null = null;
    let lastDate: string | null = null;
    let maxSynced: string | null = null;
    for (const r of rows) {
      sentTotal += r.sent ?? 0;
      replyTotal += r.replies ?? 0;
      autoReplyTotal += r.replies_automatic ?? 0;
      if (firstDate === null || r.metric_date < firstDate) firstDate = r.metric_date;
      if (lastDate === null || r.metric_date > lastDate) lastDate = r.metric_date;
      if (maxSynced === null || r.api_synced_at > maxSynced) maxSynced = r.api_synced_at;
    }

    const inv = inventoryByDomain.get(domain) ?? [];
    const inboxCount = inv.length;
    const activeInboxCount = inv.filter(r => r.account_status === 'active').length;
    const workspaceCount = new Set(inv.map(r => r.workspace_slug)).size;
    const isFreeMail = inv.some(r => r.is_free_mail);
    const dominantRaw = pickDominantProviderCodeRaw(inv, rows);
    const providerGroupFinal = pickDominantProviderGroup(
      providerContributions(rows, activeAccountsByProvider(inv)),
    );

    let coverage: 'full' | 'partial' | 'unknown';
    if (inv.length === 0) {
      coverage = 'unknown';
    } else {
      const distinctMetricAccounts = new Set(
        rows.length > 0
          ? // we only know per-day rollups here; approximate via active inbox count
            // by checking lifetime sent presence: if any sent at all, treat as
            // full when counts agree.
            []
          : [],
      );
      // Use simpler proxy: 'full' when both inventory and metrics rows exist
      // and lifetime sent > 0; 'partial' if inventory exists but no sends yet.
      void distinctMetricAccounts;
      if (rows.length > 0 && sentTotal > 0) coverage = 'full';
      else coverage = 'partial';
    }

    const rrPct = sentTotal > 0 ? (replyTotal / sentTotal) * 100 : null;

    domainMetricsUpsert.push({
      domain,
      provider_group: providerGroupFinal,
      dominant_provider_raw: dominantRaw,
      workspace_count: workspaceCount,
      inbox_count: inboxCount,
      active_inbox_count: activeInboxCount,
      sent_total: sentTotal,
      reply_count_total: replyTotal,
      auto_reply_count_total: autoReplyTotal,
      rr_pct: rrPct,
      first_metric_date: firstDate,
      last_metric_date: lastDate,
      source_coverage_status: coverage,
      source_max_synced_at: maxSynced,
      is_free_mail: isFreeMail,
      updated_at: updatedAt,
    });
  }

  if (domainMetricsUpsert.length > 0) {
    await deps.supabase.upsert('infra_domain_metrics', domainMetricsUpsert, 'domain');
  }

  return { domainsWritten: domainMetricsUpsert.length };
}
