import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { Pool } from 'pg';

type ProviderGroup = 'google_otd' | 'outlook' | 'unknown';
type RrStatus =
  | 'great'
  | 'good'
  | 'warmup'
  | 'retire'
  | 'unscored'
  | 'unknown_provider'
  | 'free_mail_excluded'
  | 'stale_data'
  | 'sync_zero';

const STATUS_ORDER: RrStatus[] = [
  'great',
  'good',
  'warmup',
  'retire',
  'unscored',
  'sync_zero',
  'stale_data',
  'unknown_provider',
  'free_mail_excluded',
];

const REVIEW_STATUSES: RrStatus[] = ['retire', 'warmup', 'unknown_provider', 'stale_data'];

const CSV_COLUMNS = [
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
] as const;

const ACCOUNT_CSV_COLUMNS = [
  'domain',
  'account_email',
  'workspace_slug',
  'account_status',
  'provider_code_raw',
  'last_seen_at',
] as const;

export interface ReviewRow {
  domain: string;
  provider_group: ProviderGroup;
  current_status: RrStatus;
  status_since: Date | string;
  last_rr_pct: number | null;
  last_sent_total: number;
  last_reply_count: number;
  sent_7d: number;
  replies_7d: number;
  active_account_count: number;
  source_max_synced_at: Date | string | null;
  data_freshness_status: string;
}

export interface AccountRow {
  domain: string;
  account_email: string;
  workspace_slug: string;
  account_status: string | null;
  provider_code_raw: number | null;
  last_seen_at: Date | string | null;
}

export interface DistributionRow {
  current_status: RrStatus;
  provider_group: ProviderGroup;
  domain_count: number;
  sent_7d: number;
}

export interface FreshnessSummary {
  infra_domain_metrics_last_refresh: Date | string | null;
  domain_rr_state_last_evaluated: Date | string | null;
  state_count: number;
}

export interface ReviewDataset {
  generatedAt: Date;
  reportDate: string;
  freshness: FreshnessSummary;
  distribution: DistributionRow[];
  rowsByStatus: Record<RrStatus, ReviewRow[]>;
  retireAccounts: AccountRow[];
}

interface OutputPaths {
  report: string;
  retireCsv: string;
  warmupCsv: string;
  unknownProviderCsv: string;
  staleDataCsv: string;
  retireAccountsCsv: string;
}

interface BuildResult {
  reportPath: string;
  csvPaths: string[];
  retireDomains: number;
  warmupDomains: number;
  unknownProviderDomains: number;
  staleDataDomains: number;
  retireActiveAccounts: number;
  retireSent7d: number;
  durationMs: number;
}

export async function buildDomainRrReviewSurface(): Promise<BuildResult> {
  const startedAt = Date.now();
  const reportDate = parseReportDate(process.argv) ?? new Date().toISOString().slice(0, 10);
  const renaissanceRoot = resolveRenaissanceRoot(process.cwd());
  const deliverablesDir = path.join(renaissanceRoot, 'deliverables');
  const domainRrDir = path.join(deliverablesDir, 'domain-rr');
  const outputPaths = getOutputPaths(deliverablesDir, domainRrDir, reportDate);

  if (!process.env.PIPELINE_SUPABASE_DB_URL) {
    throw new Error('Missing PIPELINE_SUPABASE_DB_URL');
  }

  const pool = new Pool({
    connectionString: process.env.PIPELINE_SUPABASE_DB_URL,
    max: 4,
    statement_timeout: 120_000,
    query_timeout: 120_000,
  });
  try {
    const dataset = await loadReviewDataset(pool, reportDate);
    enforceStopConditions(dataset);

    await mkdir(domainRrDir, { recursive: true });
    await writeFile(outputPaths.report, buildMarkdownReport(dataset, outputPaths), 'utf8');
    await writeFile(outputPaths.retireCsv, toCsv(dataset.rowsByStatus.retire, CSV_COLUMNS), 'utf8');
    await writeFile(outputPaths.warmupCsv, toCsv(dataset.rowsByStatus.warmup, CSV_COLUMNS), 'utf8');
    await writeFile(outputPaths.unknownProviderCsv, toCsv(dataset.rowsByStatus.unknown_provider, CSV_COLUMNS), 'utf8');
    await writeFile(outputPaths.staleDataCsv, toCsv(dataset.rowsByStatus.stale_data, CSV_COLUMNS), 'utf8');
    await writeFile(outputPaths.retireAccountsCsv, toCsv(dataset.retireAccounts, ACCOUNT_CSV_COLUMNS), 'utf8');

    const durationMs = Date.now() - startedAt;
    if (durationMs > 120_000) {
      throw new Error(`Stop condition: review surface took ${(durationMs / 1000).toFixed(1)}s to generate`);
    }

    return {
      reportPath: outputPaths.report,
      csvPaths: [
        outputPaths.retireCsv,
        outputPaths.warmupCsv,
        outputPaths.unknownProviderCsv,
        outputPaths.staleDataCsv,
        outputPaths.retireAccountsCsv,
      ],
      retireDomains: dataset.rowsByStatus.retire.length,
      warmupDomains: dataset.rowsByStatus.warmup.length,
      unknownProviderDomains: dataset.rowsByStatus.unknown_provider.length,
      staleDataDomains: dataset.rowsByStatus.stale_data.length,
      retireActiveAccounts: dataset.retireAccounts.length,
      retireSent7d: sum(dataset.rowsByStatus.retire, row => row.sent_7d),
      durationMs,
    };
  } finally {
    await pool.end();
  }
}

async function loadReviewDataset(client: Pool, reportDate: string): Promise<ReviewDataset> {
  const generatedAt = new Date();
  const [freshnessResult, distributionResult, reviewRowsResult, accountsResult] = await Promise.all([
    client.query<{
      infra_domain_metrics_last_refresh: Date | null;
      domain_rr_state_last_evaluated: Date | null;
      state_count: string;
    }>(`
      select
        (select max(source_max_synced_at) from public.infra_domain_metrics) as infra_domain_metrics_last_refresh,
        max(last_evaluated_at) as domain_rr_state_last_evaluated,
        count(*)::bigint as state_count
      from public.domain_rr_state
    `),
    client.query<{
      current_status: RrStatus;
      provider_group: ProviderGroup;
      domain_count: string;
      sent_7d: string | null;
    }>(`
      with seven_day as (
        select domain, sum(sent)::bigint as sent_7d
        from public.infra_domain_daily_metrics
        where metric_date >= current_date - interval '7 days'
        group by domain
      )
      select
        s.current_status,
        s.provider_group,
        count(*)::bigint as domain_count,
        coalesce(sum(seven_day.sent_7d), 0)::bigint as sent_7d
      from public.domain_rr_state s
      left join seven_day on seven_day.domain = s.domain
      group by s.current_status, s.provider_group
    `),
    client.query<ReviewRowDb>(`
      with seven_day as (
        select
          domain,
          sum(sent)::bigint as sent_7d,
          sum(replies)::bigint as replies_7d
        from public.infra_domain_daily_metrics
        where metric_date >= current_date - interval '7 days'
        group by domain
      ),
      active_accounts as (
        select
          domain,
          count(*) filter (where lower(coalesce(account_status, '')) = 'active')::bigint as active_account_count
        from public.infra_accounts
        group by domain
      )
      select
        s.domain,
        s.provider_group,
        s.current_status,
        s.status_since,
        s.last_rr_pct,
        s.last_sent_total,
        s.last_reply_count,
        coalesce(seven_day.sent_7d, 0)::bigint as sent_7d,
        coalesce(seven_day.replies_7d, 0)::bigint as replies_7d,
        coalesce(active_accounts.active_account_count, 0)::bigint as active_account_count,
        s.source_max_synced_at,
        s.data_freshness_status
      from public.domain_rr_state s
      left join seven_day on seven_day.domain = s.domain
      left join active_accounts on active_accounts.domain = s.domain
      where s.current_status = any($1::text[])
      order by s.current_status, coalesce(seven_day.sent_7d, 0) desc, s.domain
    `, [REVIEW_STATUSES]),
    client.query<AccountRowDb>(`
      select
        a.domain,
        a.account_email,
        a.workspace_slug,
        a.account_status,
        a.provider_code_raw,
        a.last_seen_at
      from public.infra_accounts a
      join public.domain_rr_state s on s.domain = a.domain
      where s.current_status = 'retire'
        and lower(coalesce(a.account_status, '')) = 'active'
    `),
  ]);

  const freshnessRow = freshnessResult.rows[0];
  const rowsByStatus = emptyRowsByStatus();
  for (const row of reviewRowsResult.rows) {
    rowsByStatus[row.current_status].push(normalizeReviewRow(row));
  }

  return {
    generatedAt,
    reportDate,
    freshness: {
      infra_domain_metrics_last_refresh: freshnessRow?.infra_domain_metrics_last_refresh ?? null,
      domain_rr_state_last_evaluated: freshnessRow?.domain_rr_state_last_evaluated ?? null,
      state_count: Number(freshnessRow?.state_count ?? 0),
    },
    distribution: distributionResult.rows.map(row => ({
      current_status: row.current_status,
      provider_group: row.provider_group,
      domain_count: Number(row.domain_count),
      sent_7d: Number(row.sent_7d ?? 0),
    })),
    rowsByStatus,
    retireAccounts: accountsResult.rows.map(normalizeAccountRow),
  };
}

interface ReviewRowDb extends Omit<ReviewRow, 'last_sent_total' | 'last_reply_count' | 'sent_7d' | 'replies_7d' | 'active_account_count' | 'last_rr_pct'> {
  last_rr_pct: string | null;
  last_sent_total: string;
  last_reply_count: string;
  sent_7d: string;
  replies_7d: string;
  active_account_count: string;
}

interface AccountRowDb extends Omit<AccountRow, 'provider_code_raw'> {
  provider_code_raw: number | null;
}

function normalizeReviewRow(row: ReviewRowDb): ReviewRow {
  return {
    ...row,
    last_rr_pct: row.last_rr_pct === null ? null : Number(row.last_rr_pct),
    last_sent_total: Number(row.last_sent_total),
    last_reply_count: Number(row.last_reply_count),
    sent_7d: Number(row.sent_7d),
    replies_7d: Number(row.replies_7d),
    active_account_count: Number(row.active_account_count),
  };
}

function normalizeAccountRow(row: AccountRowDb): AccountRow {
  return row;
}

function emptyRowsByStatus(): Record<RrStatus, ReviewRow[]> {
  const rowsByStatus = {} as Record<RrStatus, ReviewRow[]>;
  for (const status of STATUS_ORDER) {
    rowsByStatus[status] = [];
  }
  return rowsByStatus;
}

export function buildMarkdownReport(dataset: ReviewDataset, outputPaths: OutputPaths): string {
  const retireRows = dataset.rowsByStatus.retire;
  const warmupRows = dataset.rowsByStatus.warmup;
  const unknownRows = dataset.rowsByStatus.unknown_provider;
  const staleRows = dataset.rowsByStatus.stale_data;
  const sourceAge = ageMs(dataset.generatedAt, dataset.freshness.infra_domain_metrics_last_refresh);
  const stateAge = ageMs(dataset.generatedAt, dataset.freshness.domain_rr_state_last_evaluated);
  const freshnessWarning = buildFreshnessWarning(sourceAge);

  return [
    `# Domain RR Review - ${dataset.reportDate}`,
    '',
    '## Freshness',
    `- Generated at: ${formatDateTime(dataset.generatedAt)}`,
    `- infra_domain_metrics last refresh: ${formatDateTime(dataset.freshness.infra_domain_metrics_last_refresh)} (${formatAgeHours(sourceAge)} ago)`,
    `- domain_rr_state last evaluated: ${formatDateTime(dataset.freshness.domain_rr_state_last_evaluated)} (${formatAgeMinutes(stateAge)} ago)`,
    `- domain_rr_state rows: ${formatInt(dataset.freshness.state_count)}`,
    `- Freshness warning: ${freshnessWarning}`,
    '',
    '## Status distribution',
    '',
    buildDistributionTable(dataset.distribution),
    '',
    '## CSV exports',
    `- Retire domains: ${relativeToRenaissance(outputPaths.retireCsv)}`,
    `- Warmup domains: ${relativeToRenaissance(outputPaths.warmupCsv)}`,
    `- Unknown-provider domains: ${relativeToRenaissance(outputPaths.unknownProviderCsv)}`,
    `- Stale-data domains: ${relativeToRenaissance(outputPaths.staleDataCsv)}`,
    `- Retire active accounts: ${relativeToRenaissance(outputPaths.retireAccountsCsv)}`,
    '',
    '## Top 50 retire domains (by 7d send volume)',
    '',
    buildDomainTable(retireRows.slice(0, 50)),
    '',
    '## Top 50 warmup domains (by 7d send volume)',
    '',
    buildDomainTable(warmupRows.slice(0, 50)),
    '',
    `## ${unknownRows.length} unknown_provider domains (full list - should be tiny edge cases)`,
    '',
    buildDomainTable(unknownRows),
    '',
    '## stale_data audit (sample of 50, sorted by last_metric_date desc)',
    '',
    "These are domains where source data is >48h old. The scorer can't bucket them. Mostly inactive cohorts.",
    '',
    buildDomainTable([...staleRows].sort(compareSourceFreshnessDesc).slice(0, 50)),
    '',
  ].join('\n');
}

function buildDistributionTable(rows: DistributionRow[]): string {
  const byStatusProvider = new Map<string, DistributionRow>();
  for (const row of rows) {
    byStatusProvider.set(`${row.current_status}:${row.provider_group}`, row);
  }

  const lines = ['| Status | google_otd | outlook | total | 7d sends |', '|---|---:|---:|---:|---:|'];
  for (const status of STATUS_ORDER) {
    const google = byStatusProvider.get(`${status}:google_otd`);
    const outlook = byStatusProvider.get(`${status}:outlook`);
    const statusRows = rows.filter(row => row.current_status === status);
    const total = sum(statusRows, row => row.domain_count);
    const sent7d = sum(statusRows, row => row.sent_7d);
    if (total === 0) continue;
    lines.push(
      `| ${status} | ${formatInt(google?.domain_count ?? 0)} | ${formatInt(outlook?.domain_count ?? 0)} | ${formatInt(total)} | ${formatInt(sent7d)} |`,
    );
  }
  return lines.join('\n');
}

function buildDomainTable(rows: ReviewRow[]): string {
  const lines = ['| domain | provider | 7d sent | 7d replies | RR% | active accts |', '|---|---|---:|---:|---:|---:|'];
  for (const row of rows) {
    lines.push(
      `| ${row.domain} | ${row.provider_group} | ${formatInt(row.sent_7d)} | ${formatInt(row.replies_7d)} | ${formatPct(row.last_rr_pct)} | ${formatInt(row.active_account_count)} |`,
    );
  }
  if (rows.length === 0) {
    lines.push('| _(none)_ |  |  |  |  |  |');
  }
  return lines.join('\n');
}

export function toCsv<T, K extends keyof T>(rows: T[], columns: readonly K[]): string {
  const header = columns.join(',');
  const body = rows.map(row => columns.map(column => csvEscape(formatCsvValue(row[column]))).join(','));
  return [header, ...body].join('\n') + '\n';
}

function enforceStopConditions(dataset: ReviewDataset): void {
  const distributionTotal = sum(dataset.distribution, row => row.domain_count);
  if (distributionTotal !== dataset.freshness.state_count) {
    throw new Error(
      `Stop condition: status distribution total ${distributionTotal} disagrees with domain_rr_state count ${dataset.freshness.state_count}`,
    );
  }

  const unknownCount = dataset.rowsByStatus.unknown_provider.length;
  if (unknownCount > 100) {
    throw new Error(`Stop condition: unknown_provider has ${unknownCount} domains, expected <=100`);
  }

  const retireCount = dataset.rowsByStatus.retire.length;
  if (retireCount > 10_000) {
    throw new Error(`Stop condition: retire bucket has ${retireCount} domains, expected <=10,000`);
  }

  const avgActiveAccounts = retireCount === 0 ? 0 : dataset.retireAccounts.length / retireCount;
  if (avgActiveAccounts > 500) {
    throw new Error(
      `Stop condition: retire active-account average is ${avgActiveAccounts.toFixed(1)} accounts/domain, expected <=500`,
    );
  }
}

function getOutputPaths(deliverablesDir: string, domainRrDir: string, reportDate: string): OutputPaths {
  return {
    report: path.join(deliverablesDir, `${reportDate}-domain-rr-review.md`),
    retireCsv: path.join(domainRrDir, `retire-domains-${reportDate}.csv`),
    warmupCsv: path.join(domainRrDir, `warmup-domains-${reportDate}.csv`),
    unknownProviderCsv: path.join(domainRrDir, `unknown-provider-domains-${reportDate}.csv`),
    staleDataCsv: path.join(domainRrDir, `stale-data-domains-${reportDate}.csv`),
    retireAccountsCsv: path.join(domainRrDir, `retire-domain-active-accounts-${reportDate}.csv`),
  };
}

function resolveRenaissanceRoot(startDir: string): string {
  const candidates = [
    process.env.RENAISSANCE_ROOT,
    path.resolve(startDir, '..'),
    path.resolve(startDir, '..', '..'),
    path.resolve(startDir, '..', '..', '..'),
  ].filter((candidate): candidate is string => Boolean(candidate));

  for (const candidate of candidates) {
    if (candidate.endsWith('Renaissance')) {
      return candidate;
    }
  }
  throw new Error('Could not resolve Renaissance root. Set RENAISSANCE_ROOT to /path/to/Renaissance.');
}

function parseReportDate(args: string[]): string | null {
  const index = args.indexOf('--date');
  if (index === -1) return null;
  const value = args[index + 1];
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error('Invalid --date. Expected YYYY-MM-DD.');
  }
  return value;
}

function buildFreshnessWarning(sourceAgeMs: number | null): string {
  if (sourceAgeMs === null) return 'RED - source timestamp missing';
  const hours = sourceAgeMs / 3_600_000;
  if (hours > 24) return 'RED - source is older than 24h';
  if (hours > 12) return 'YELLOW - source is older than 12h';
  return 'OK - source is fresh';
}

function compareSourceFreshnessDesc(a: ReviewRow, b: ReviewRow): number {
  return toTime(b.source_max_synced_at) - toTime(a.source_max_synced_at);
}

function toTime(value: Date | string | null): number {
  if (!value) return 0;
  return value instanceof Date ? value.getTime() : new Date(value).getTime();
}

function ageMs(now: Date, value: Date | string | null): number | null {
  if (!value) return null;
  return now.getTime() - toTime(value);
}

function formatAgeHours(value: number | null): string {
  if (value === null) return 'unknown';
  return `${(value / 3_600_000).toFixed(1)}h`;
}

function formatAgeMinutes(value: number | null): string {
  if (value === null) return 'unknown';
  return `${(value / 60_000).toFixed(1)}m`;
}

function formatDateTime(value: Date | string | null): string {
  if (!value) return 'unknown';
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString();
}

function formatInt(value: number): string {
  return Math.round(value).toLocaleString('en-US');
}

function formatPct(value: number | null): string {
  return value === null ? '' : value.toFixed(4);
}

function formatCsvValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function csvEscape(value: string): string {
  if (!/[",\n\r]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

function relativeToRenaissance(filePath: string): string {
  const marker = `${path.sep}Renaissance${path.sep}`;
  const index = filePath.lastIndexOf(marker);
  return index === -1 ? filePath : filePath.slice(index + marker.length);
}

function sum<T>(rows: T[], mapper: (row: T) => number): number {
  return rows.reduce((total, row) => total + mapper(row), 0);
}

async function main(): Promise<void> {
  const result = await buildDomainRrReviewSurface();
  console.log(`[rr-review] report=${result.reportPath}`);
  for (const csvPath of result.csvPaths) {
    console.log(`[rr-review] csv=${csvPath}`);
  }
  console.log(`[rr-review] retire_domains=${result.retireDomains}`);
  console.log(`[rr-review] warmup_domains=${result.warmupDomains}`);
  console.log(`[rr-review] unknown_provider_domains=${result.unknownProviderDomains}`);
  console.log(`[rr-review] stale_data_domains=${result.staleDataDomains}`);
  console.log(`[rr-review] retire_active_accounts=${result.retireActiveAccounts}`);
  console.log(`[rr-review] retire_sent_7d=${result.retireSent7d}`);
  console.log(`[rr-review] duration_ms=${result.durationMs}`);
}

if (require.main === module) {
  main().catch(err => {
    console.error('[rr-review] Fatal:', err instanceof Error ? err.message : err);
    process.exit(1);
  });
}
