// Domain RR Scorer — pure logic + DB driver. Reads infra_domain_metrics,
// writes domain_rr_state + domain_rr_events.
//
// Spec: specs/2026-04-26-domain-rr-scorer-locate-or-rebuild.md
// Schema: data-pipeline-v2/sql/2026-04-24-domain-infra-sync.sql (5F + 5G)
//
// Direct pg client matches the rebuild_domain_registry pattern
// (src/infra/domain-registry.ts:rebuildDomainRegistry). Uses
// `set statement_timeout = '15min'` because Supabase's role-level default
// (2 min) trips before reading 40k metric rows + writing 40k state rows in
// the same transaction.

import { Client } from 'pg';

import type { ProviderGroup } from './provider-routing';

export const ELIGIBILITY: Readonly<Record<'google_otd' | 'outlook', number>> = {
  google_otd: 700,
  outlook: 1000,
};

export const THRESHOLDS: Readonly<Record<'google_otd' | 'outlook', { great: number; good: number; warmup: number }>> = {
  // > great = great; [good, great] = good; [warmup, good) = warmup; < warmup = retire
  google_otd: { great: 1.0, good: 0.75, warmup: 0.5 },
  outlook: { great: 1.0, good: 0.7, warmup: 0.45 },
};

export const STALE_DATA_HOURS = 48;

const UPSERT_CHUNK_SIZE = 500;
const EVENT_CHUNK_SIZE = 1000;

export type RrStatus =
  | 'great'
  | 'good'
  | 'warmup'
  | 'retire'
  | 'unscored'
  | 'unknown_provider'
  | 'free_mail_excluded'
  | 'stale_data'
  | 'sync_zero';

export type FreshnessStatus = 'fresh' | 'stale' | 'unknown' | 'sync_zero';

export interface MetricSnapshot {
  domain: string;
  provider_group: ProviderGroup;
  is_free_mail: boolean;
  sent_total: number;
  reply_count_total: number;
  auto_reply_count_total: number;
  rr_pct: number | null;
  source_max_synced_at: Date | null;
  inbox_count: number;
  active_inbox_count: number;
}

export interface ComputeStatusOptions {
  now?: Date;
}

export function computeRrPct(m: MetricSnapshot): number {
  if (m.sent_total === 0) return 0;
  if (m.rr_pct !== null && Number.isFinite(m.rr_pct)) return m.rr_pct;
  return (m.reply_count_total / m.sent_total) * 100;
}

export function computeStatus(m: MetricSnapshot, opts: ComputeStatusOptions = {}): RrStatus {
  if (m.is_free_mail) return 'free_mail_excluded';
  if (m.provider_group === 'unknown') return 'unknown_provider';

  const now = opts.now ?? new Date();
  const sourceAgeHours = m.source_max_synced_at
    ? (now.getTime() - m.source_max_synced_at.getTime()) / 3_600_000
    : Infinity;

  if (m.sent_total === 0) {
    return sourceAgeHours <= STALE_DATA_HOURS ? 'sync_zero' : 'stale_data';
  }
  if (sourceAgeHours > STALE_DATA_HOURS) return 'stale_data';

  const minSends = ELIGIBILITY[m.provider_group];
  if (m.sent_total < minSends) return 'unscored';

  const rr = computeRrPct(m);
  const t = THRESHOLDS[m.provider_group];
  if (rr > t.great) return 'great';
  if (rr >= t.good) return 'good';
  if (rr >= t.warmup) return 'warmup';
  return 'retire';
}

export function computeFreshness(
  m: MetricSnapshot,
  status: RrStatus,
  opts: ComputeStatusOptions = {},
): FreshnessStatus {
  if (status === 'sync_zero') return 'sync_zero';
  if (!m.source_max_synced_at) return 'unknown';
  const now = opts.now ?? new Date();
  const ageHours = (now.getTime() - m.source_max_synced_at.getTime()) / 3_600_000;
  return ageHours > STALE_DATA_HOURS ? 'stale' : 'fresh';
}

// Seed-pattern matching: the 2026-04-25 bulk seed wrote `*_entered` events for
// transitions into "actionable" buckets (warmup, retire, sync_zero,
// unknown_provider) and `status_change` events for transitions into great/good.
// Transitions into unscored / free_mail_excluded / stale_data are silent — those
// states are operationally inert.
const ENTERED_EVENT_STATUSES: ReadonlySet<RrStatus> = new Set([
  'warmup',
  'retire',
  'sync_zero',
  'unknown_provider',
]);
const SILENT_STATUSES: ReadonlySet<RrStatus> = new Set([
  'unscored',
  'free_mail_excluded',
  'stale_data',
]);

export function eventTypeForTransition(toStatus: RrStatus): string | null {
  if (SILENT_STATUSES.has(toStatus)) return null;
  if (ENTERED_EVENT_STATUSES.has(toStatus)) return `${toStatus}_entered`;
  return 'status_change';
}

export interface ScorerResult {
  metrics_read: number;
  domains_evaluated: number;
  state_inserts: number;
  state_updates: number;
  status_unchanged: number;
  events_written: number;
  status_distribution: Record<string, number>;
  duration_ms: number;
}

export interface ScorerOptions {
  dryRun?: boolean;
  client?: Client;
  now?: Date;
}

interface StateUpsertRow {
  domain: string;
  provider_group: ProviderGroup;
  current_status: RrStatus;
  status_since: Date;
  last_sent_total: number;
  last_reply_count: number;
  last_auto_reply_count: number;
  last_rr_pct: number;
  inbox_count: number;
  active_inbox_count: number;
  source_max_synced_at: Date | null;
  data_freshness_status: FreshnessStatus;
  last_evaluated_at: Date;
}

interface EventInsertRow {
  domain: string;
  event_type: string;
  from_status: RrStatus | null;
  to_status: RrStatus;
  sent_total: number;
  reply_count: number;
  rr_pct: number;
  reason: string | null;
}

export async function scoreDomainRr(opts: ScorerOptions = {}): Promise<ScorerResult> {
  const startedAt = Date.now();
  const dryRun = opts.dryRun ?? false;
  const now = opts.now ?? new Date();
  const ownClient = !opts.client;
  const client =
    opts.client ??
    new Client({
      connectionString: process.env.PIPELINE_SUPABASE_DB_URL,
    });

  if (ownClient) {
    if (!process.env.PIPELINE_SUPABASE_DB_URL) {
      throw new Error('Missing PIPELINE_SUPABASE_DB_URL');
    }
    await client.connect();
  }

  try {
    // Per memory/decisions.md 2026-04-26: function-level statement_timeout
    // doesn't override the outer caller's timer; the SET LOCAL pattern is
    // unreliable. Issue it as a session-level SET before any heavy query.
    await client.query("set statement_timeout = '15min'");

    const metricsResult = await client.query<{
      domain: string;
      provider_group: ProviderGroup;
      is_free_mail: boolean;
      sent_total: string;
      reply_count_total: string;
      auto_reply_count_total: string;
      rr_pct: string | null;
      source_max_synced_at: Date | null;
      inbox_count: number;
      active_inbox_count: number;
    }>(`
      select
        domain, provider_group, is_free_mail,
        sent_total, reply_count_total, auto_reply_count_total,
        rr_pct, source_max_synced_at,
        inbox_count, active_inbox_count
      from public.infra_domain_metrics
    `);

    const metrics: MetricSnapshot[] = metricsResult.rows.map(r => ({
      domain: r.domain,
      provider_group: r.provider_group,
      is_free_mail: r.is_free_mail,
      sent_total: Number(r.sent_total),
      reply_count_total: Number(r.reply_count_total),
      auto_reply_count_total: Number(r.auto_reply_count_total),
      rr_pct: r.rr_pct === null ? null : Number(r.rr_pct),
      source_max_synced_at: r.source_max_synced_at,
      inbox_count: r.inbox_count,
      active_inbox_count: r.active_inbox_count,
    }));

    const priorResult = await client.query<{
      domain: string;
      current_status: RrStatus;
      status_since: Date;
    }>(`select domain, current_status, status_since from public.domain_rr_state`);
    const priorByDomain = new Map<string, { current_status: RrStatus; status_since: Date }>(
      priorResult.rows.map(r => [r.domain, { current_status: r.current_status, status_since: r.status_since }]),
    );

    const upserts: StateUpsertRow[] = [];
    const events: EventInsertRow[] = [];
    const statusDist: Record<string, number> = {};
    let stateInserts = 0;
    let stateUpdates = 0;
    let statusUnchanged = 0;

    for (const m of metrics) {
      const newStatus = computeStatus(m, { now });
      const freshness = computeFreshness(m, newStatus, { now });
      const rr = computeRrPct(m);
      const prior = priorByDomain.get(m.domain);

      let statusSince: Date;
      if (!prior) {
        stateInserts += 1;
        statusSince = now;
      } else if (prior.current_status === newStatus) {
        statusUnchanged += 1;
        statusSince = prior.status_since;
      } else {
        stateUpdates += 1;
        statusSince = now;
      }

      const key = `${m.provider_group}:${newStatus}`;
      statusDist[key] = (statusDist[key] ?? 0) + 1;

      upserts.push({
        domain: m.domain,
        provider_group: m.provider_group,
        current_status: newStatus,
        status_since: statusSince,
        last_sent_total: m.sent_total,
        last_reply_count: m.reply_count_total,
        last_auto_reply_count: m.auto_reply_count_total,
        last_rr_pct: rr,
        inbox_count: m.inbox_count,
        active_inbox_count: m.active_inbox_count,
        source_max_synced_at: m.source_max_synced_at,
        data_freshness_status: freshness,
        last_evaluated_at: now,
      });

      const isTransition = !prior || prior.current_status !== newStatus;
      if (isTransition) {
        const eventType = eventTypeForTransition(newStatus);
        if (eventType) {
          events.push({
            domain: m.domain,
            event_type: eventType,
            from_status: prior?.current_status ?? null,
            to_status: newStatus,
            sent_total: m.sent_total,
            reply_count: m.reply_count_total,
            rr_pct: rr,
            reason: null,
          });
        }
      }
    }

    if (!dryRun) {
      for (let i = 0; i < upserts.length; i += UPSERT_CHUNK_SIZE) {
        await flushStateChunk(client, upserts.slice(i, i + UPSERT_CHUNK_SIZE));
      }
      for (let i = 0; i < events.length; i += EVENT_CHUNK_SIZE) {
        await flushEventChunk(client, events.slice(i, i + EVENT_CHUNK_SIZE));
      }
    }

    return {
      metrics_read: metricsResult.rowCount ?? metrics.length,
      domains_evaluated: metrics.length,
      state_inserts: stateInserts,
      state_updates: stateUpdates,
      status_unchanged: statusUnchanged,
      events_written: dryRun ? 0 : events.length,
      status_distribution: statusDist,
      duration_ms: Date.now() - startedAt,
    };
  } finally {
    if (ownClient) {
      await client.end();
    }
  }
}

async function flushStateChunk(client: Client, rows: StateUpsertRow[]): Promise<void> {
  if (rows.length === 0) return;
  const cols = [
    'domain',
    'provider_group',
    'current_status',
    'status_since',
    'last_sent_total',
    'last_reply_count',
    'last_auto_reply_count',
    'last_rr_pct',
    'inbox_count',
    'active_inbox_count',
    'source_max_synced_at',
    'data_freshness_status',
    'last_evaluated_at',
  ];
  const values: unknown[] = [];
  const placeholders: string[] = [];
  let p = 1;
  for (const r of rows) {
    placeholders.push(
      `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`,
    );
    values.push(
      r.domain,
      r.provider_group,
      r.current_status,
      r.status_since,
      r.last_sent_total,
      r.last_reply_count,
      r.last_auto_reply_count,
      r.last_rr_pct,
      r.inbox_count,
      r.active_inbox_count,
      r.source_max_synced_at,
      r.data_freshness_status,
      r.last_evaluated_at,
    );
  }
  const sql = `
    insert into public.domain_rr_state (${cols.join(',')})
    values ${placeholders.join(',')}
    on conflict (domain) do update set
      provider_group = excluded.provider_group,
      current_status = excluded.current_status,
      status_since = excluded.status_since,
      last_sent_total = excluded.last_sent_total,
      last_reply_count = excluded.last_reply_count,
      last_auto_reply_count = excluded.last_auto_reply_count,
      last_rr_pct = excluded.last_rr_pct,
      inbox_count = excluded.inbox_count,
      active_inbox_count = excluded.active_inbox_count,
      source_max_synced_at = excluded.source_max_synced_at,
      data_freshness_status = excluded.data_freshness_status,
      last_evaluated_at = excluded.last_evaluated_at
  `;
  await client.query(sql, values);
}

async function flushEventChunk(client: Client, rows: EventInsertRow[]): Promise<void> {
  if (rows.length === 0) return;
  const cols = ['domain', 'event_type', 'from_status', 'to_status', 'sent_total', 'reply_count', 'rr_pct', 'reason'];
  const values: unknown[] = [];
  const placeholders: string[] = [];
  let p = 1;
  for (const r of rows) {
    placeholders.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    values.push(
      r.domain,
      r.event_type,
      r.from_status,
      r.to_status,
      r.sent_total,
      r.reply_count,
      r.rr_pct,
      r.reason,
    );
  }
  const sql = `
    insert into public.domain_rr_events (${cols.join(',')})
    values ${placeholders.join(',')}
  `;
  await client.query(sql, values);
}
