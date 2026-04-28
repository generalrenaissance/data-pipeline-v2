import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { Pool } from 'pg';

import { isFreeMailDomain } from '../src/infra/free-mail';

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

type SkipReason = 'STALE_DOMAIN_DATA' | 'FREE_MAIL_DOMAIN';
type CooldownStatus = 'PASS' | 'SKIP';

const FRESHNESS_GATE_HOURS = 24;
const RECENT_FLIP_COOLDOWN_HOURS = 48;

const ACCOUNT_CSV_COLUMNS = [
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

export interface DomainRow {
  domain: string;
  provider_group: ProviderGroup;
  current_status: RrStatus;
  status_since: Date | string;
  last_rr_pct: number | null;
  last_sent_total: number;
  source_max_synced_at: Date | string | null;
  is_free_mail: boolean;
  sent_7d: number;
  replies_7d: number;
  active_account_count: number;
  status_since_age_hours: number;
  cooldown_status: CooldownStatus;
}

export interface AccountRow {
  domain: string;
  account_email: string;
  workspace_slug: string;
  provider_code_raw: number | null;
  provider_group: ProviderGroup;
  account_status: string;
  daily_limit: number | null;
  last_seen_at: Date | string | null;
  domain_status_since: Date | string;
  domain_status_since_age_hours: number;
  cooldown_status: CooldownStatus;
  domain_last_rr_pct: number | null;
  domain_last_sent_total: number;
}

export interface FreshnessSummary {
  infra_domain_metrics_last_refresh: Date | string | null;
  domain_rr_state_last_evaluated: Date | string | null;
}

export interface AutoPauseDataset {
  generatedAt: Date;
  reportDate: string;
  freshness: FreshnessSummary;
  retireDomains: DomainRow[];
  targetDomains: DomainRow[];
  wouldPauseDomains: DomainRow[];
  skippedDomains: Array<DomainRow & { skip_reason: SkipReason }>;
  wouldPauseAccounts: AccountRow[];
  cooldownSkippedAccounts: AccountRow[];
}

interface OutputPaths {
  report: string;
  accountsCsv: string;
}

interface BuildResult {
  reportPath: string;
  accountsCsvPath: string;
  wouldPauseDomains: number;
  wouldPauseAccounts: number;
  cooldownSkippedDomains: number;
  cooldownSkippedAccounts: number;
  rawTargetAccounts: number;
  freshnessGate: 'PASS' | 'FAIL';
  estimatedColdPerDaySaved: number;
  durationMs: number;
}

interface DomainRowDb extends Omit<DomainRow, 'last_rr_pct' | 'last_sent_total' | 'sent_7d' | 'replies_7d' | 'active_account_count'> {
  last_rr_pct: string | null;
  last_sent_total: string;
  sent_7d: string | null;
  replies_7d: string | null;
  active_account_count: string;
}

interface AccountRowDb extends Omit<AccountRow, 'domain_status_since_age_hours' | 'cooldown_status' | 'domain_last_rr_pct' | 'domain_last_sent_total'> {
  domain_status_since_age_hours: string;
  cooldown_status: CooldownStatus;
  domain_last_rr_pct: string | null;
  domain_last_sent_total: string;
}

export async function buildAutoPauseDryRun(): Promise<BuildResult> {
  const startedAt = Date.now();
  const reportDate = parseReportDate(process.argv) ?? new Date().toISOString().slice(0, 10);
  const renaissanceRoot = resolveRenaissanceRoot(process.cwd());
  const outputDir = path.join(renaissanceRoot, 'deliverables', 'auto-pause');
  const outputPaths = getOutputPaths(outputDir, reportDate);

  if (!process.env.PIPELINE_SUPABASE_DB_URL) {
    throw new Error('Missing PIPELINE_SUPABASE_DB_URL');
  }

  const pool = new Pool({
    connectionString: process.env.PIPELINE_SUPABASE_DB_URL,
    max: 4,
    statement_timeout: 300_000,
    query_timeout: 300_000,
  });

  try {
    const dataset = await loadAutoPauseDataset(pool, reportDate);
    enforceStopConditions(dataset);

    await mkdir(outputDir, { recursive: true });
    await writeFile(outputPaths.report, buildMarkdownReport(dataset), 'utf8');
    await writeFile(
      outputPaths.accountsCsv,
      toCsv([...dataset.wouldPauseAccounts, ...dataset.cooldownSkippedAccounts], ACCOUNT_CSV_COLUMNS),
      'utf8',
    );

    const durationMs = Date.now() - startedAt;
    const sent7d = sum(dataset.wouldPauseDomains, row => row.sent_7d);
    const rawTargetAccounts = dataset.wouldPauseAccounts.length + dataset.cooldownSkippedAccounts.length;

    return {
      reportPath: outputPaths.report,
      accountsCsvPath: outputPaths.accountsCsv,
      wouldPauseDomains: dataset.wouldPauseDomains.length,
      wouldPauseAccounts: dataset.wouldPauseAccounts.length,
      cooldownSkippedDomains: dataset.targetDomains.filter(row => row.cooldown_status === 'SKIP').length,
      cooldownSkippedAccounts: dataset.cooldownSkippedAccounts.length,
      rawTargetAccounts,
      freshnessGate: freshnessGateStatus(dataset),
      estimatedColdPerDaySaved: Math.round(sent7d / 7),
      durationMs,
    };
  } finally {
    await pool.end();
  }
}

async function loadAutoPauseDataset(client: Pool, reportDate: string): Promise<AutoPauseDataset> {
  const generatedAt = new Date();
  const [freshnessResult, domainsResult] = await Promise.all([
    client.query<{
      infra_domain_metrics_last_refresh: Date | null;
      domain_rr_state_last_evaluated: Date | null;
    }>(`
      select
        (select max(source_max_synced_at) from public.infra_domain_metrics) as infra_domain_metrics_last_refresh,
        (select max(last_evaluated_at) from public.domain_rr_state) as domain_rr_state_last_evaluated
    `),
    client.query<DomainRowDb>(`
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
          count(*) filter (where account_status = 'active')::bigint as active_account_count
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
        s.source_max_synced_at,
        coalesce(m.is_free_mail, false) as is_free_mail,
        coalesce(seven_day.sent_7d, 0)::bigint as sent_7d,
        coalesce(seven_day.replies_7d, 0)::bigint as replies_7d,
        coalesce(active_accounts.active_account_count, 0)::bigint as active_account_count
      from public.domain_rr_state s
      left join public.infra_domain_metrics m on m.domain = s.domain
      left join seven_day on seven_day.domain = s.domain
      left join active_accounts on active_accounts.domain = s.domain
      where s.current_status = 'retire'
      order by coalesce(seven_day.sent_7d, 0) desc, s.domain
    `),
  ]);

  const freshnessRow = freshnessResult.rows[0];
  const baseDataset: AutoPauseDataset = {
    generatedAt,
    reportDate,
    freshness: {
      infra_domain_metrics_last_refresh: freshnessRow?.infra_domain_metrics_last_refresh ?? null,
      domain_rr_state_last_evaluated: freshnessRow?.domain_rr_state_last_evaluated ?? null,
    },
    retireDomains: domainsResult.rows.map(row => normalizeDomainRow(row, generatedAt)),
    targetDomains: [],
    wouldPauseDomains: [],
    skippedDomains: [],
    wouldPauseAccounts: [],
    cooldownSkippedAccounts: [],
  };
  const guarded = applyAutoPauseGuards(baseDataset);
  const targetDomains = guarded.targetDomains.map(row => row.domain);
  const accountsResult = targetDomains.length === 0
    ? { rows: [] as AccountRowDb[] }
    : await client.query<AccountRowDb>(`
      select
        a.domain,
        a.account_email,
        a.workspace_slug,
        a.provider_code_raw,
        a.provider_group,
        a.account_status,
        a.daily_limit,
        a.last_seen_at,
        s.status_since as domain_status_since,
        extract(epoch from ($2::timestamptz - s.status_since)) / 3600 as domain_status_since_age_hours,
        case
          when $3::boolean then 'SKIP'
          when s.status_since <= $2::timestamptz - interval '48 hours' then 'PASS'
          else 'SKIP'
        end as cooldown_status,
        s.last_rr_pct as domain_last_rr_pct,
        s.last_sent_total as domain_last_sent_total
      from public.infra_accounts a
      join public.domain_rr_state s on s.domain = a.domain
      where a.domain = any($1::text[])
        and a.account_status = 'active'
    `, [targetDomains, generatedAt.toISOString(), freshnessGateStatus(baseDataset) === 'FAIL']);

  const accounts = accountsResult.rows.map(normalizeAccountRow);
  return applyAutoPauseGuards({
    ...baseDataset,
    wouldPauseAccounts: accounts,
  });
}

export function applyAutoPauseGuards(dataset: AutoPauseDataset): AutoPauseDataset {
  const freshnessFails = freshnessGateStatus(dataset) === 'FAIL';
  const targetDomains: DomainRow[] = [];
  const wouldPauseDomains: DomainRow[] = [];
  const skippedDomains: Array<DomainRow & { skip_reason: SkipReason }> = [];

  for (const domain of dataset.retireDomains) {
    const effectiveDomain: DomainRow = freshnessFails
      ? { ...domain, cooldown_status: 'SKIP' }
      : domain;
    const skipReason = domainSkipReason(effectiveDomain, dataset.generatedAt, freshnessFails);
    if (skipReason) {
      skippedDomains.push({ ...effectiveDomain, skip_reason: skipReason });
    } else if (effectiveDomain.cooldown_status === 'PASS') {
      targetDomains.push(effectiveDomain);
      wouldPauseDomains.push(effectiveDomain);
    } else {
      targetDomains.push(effectiveDomain);
    }
  }

  const targetDomainSet = new Set(targetDomains.map(row => row.domain));
  const hardFilteredAccounts = dataset.wouldPauseAccounts
    .filter(account => targetDomainSet.has(account.domain) && account.account_status === 'active')
    .map(account => (freshnessFails ? { ...account, cooldown_status: 'SKIP' as const } : account));

  return {
    ...dataset,
    targetDomains,
    wouldPauseDomains,
    skippedDomains,
    wouldPauseAccounts: hardFilteredAccounts.filter(account => account.cooldown_status === 'PASS'),
    cooldownSkippedAccounts: hardFilteredAccounts.filter(account => account.cooldown_status === 'SKIP'),
  };
}

function domainSkipReason(domain: DomainRow, now: Date, freshnessFails: boolean): SkipReason | null {
  if (!freshnessFails && isStale(domain.source_max_synced_at, now, FRESHNESS_GATE_HOURS)) return 'STALE_DOMAIN_DATA';
  if (domain.is_free_mail || isFreeMailDomain(domain.domain)) return 'FREE_MAIL_DOMAIN';
  return null;
}

export function buildMarkdownReport(dataset: AutoPauseDataset): string {
  const sourceAge = ageHoursOrNull(dataset.generatedAt, dataset.freshness.infra_domain_metrics_last_refresh);
  const scorerAge = ageMinutesOrNull(dataset.generatedAt, dataset.freshness.domain_rr_state_last_evaluated);
  const gate = freshnessGateStatus(dataset);
  const sent7d = sum(dataset.targetDomains, row => row.sent_7d);
  const passDomains = dataset.wouldPauseDomains;
  const cooldownSkippedDomains = dataset.targetDomains.filter(row => row.cooldown_status === 'SKIP');
  const staleDomains = skippedByReason(dataset, 'STALE_DOMAIN_DATA');
  const zeroActive = dataset.retireDomains.filter(row => row.active_account_count === 0);
  const rawTargetAccounts = dataset.wouldPauseAccounts.length + dataset.cooldownSkippedAccounts.length;

  return [
    `# Auto-Pause Dry-Run - ${dataset.reportDate}`,
    '',
    '## Header',
    `- Generated at: ${formatDateTime(dataset.generatedAt)}`,
    `- Source freshness: infra_domain_metrics last refresh ${formatAgeHours(sourceAge)} ago`,
    `- Scorer last evaluated: ${formatAgeMinutes(scorerAge)} ago`,
    `- Freshness gate: ${gate} (must be <24h to recommend pauses)`,
    '',
    '## Summary',
    `- Retire-bucket domains (total): ${formatInt(dataset.retireDomains.length)}`,
    `- Active accounts on retire domains (raw target pool): ${formatInt(rawTargetAccounts)}`,
    `- Would-pause-NOW (passed cooldown): ${formatInt(dataset.wouldPauseAccounts.length)} accounts across ${formatInt(passDomains.length)} domains`,
    `- Cooldown-SKIPPED (recently flipped, <48h ago): ${formatInt(dataset.cooldownSkippedAccounts.length)} accounts across ${formatInt(cooldownSkippedDomains.length)} domains`,
    `- Estimated 7-day waste prevented if all paused: ${formatInt(sent7d)} cold sends (~${formatInt(Math.round(sent7d / 7))} daily)`,
    '',
    '## Cooldown breakdown',
    '',
    buildCooldownTable(dataset),
    '',
    '## Distribution by provider (cooldown PASS only)',
    '',
    buildProviderDistributionTable(dataset),
    '',
    '## Top 50 would-pause-NOW domains (by 7-day send volume; cooldown=PASS only)',
    '',
    buildDomainTable(dataset.wouldPauseDomains.slice(0, 50)),
    '',
    '## Top 25 cooldown-skipped domains (review for early signals)',
    '',
    buildCooldownSkippedTable(cooldownSkippedDomains.slice(0, 25)),
    '',
    '## Confidence flags',
    `- Domains where source_max_synced_at > 24h ago (already filtered out in Phase B): ${formatInt(staleDomains.length)}`,
    `- Domains with 0 active accounts (would-pause target is empty - already retired manually): ${formatInt(zeroActive.length)}`,
    '',
    '## Hard-filter reason counts',
    '',
    buildSkipReasonTable(dataset),
    '',
  ].join('\n');
}

function buildProviderDistributionTable(dataset: AutoPauseDataset): string {
  const providers: ProviderGroup[] = ['google_otd', 'outlook', 'unknown'];
  const lines = ['| Provider | Retire domains | Active accts |', '|---|---:|---:|'];
  for (const provider of providers) {
    const domains = dataset.wouldPauseDomains.filter(row => row.provider_group === provider);
    if (domains.length === 0) continue;
    const domainSet = new Set(domains.map(row => row.domain));
    const accounts = dataset.wouldPauseAccounts.filter(row => domainSet.has(row.domain)).length;
    lines.push(`| ${provider} | ${formatInt(domains.length)} | ${formatInt(accounts)} |`);
  }
  if (lines.length === 2) lines.push('| _(none)_ | 0 | 0 |');
  return lines.join('\n');
}

function buildCooldownTable(dataset: AutoPauseDataset): string {
  const passDomains = dataset.targetDomains.filter(row => row.cooldown_status === 'PASS').length;
  const skipDomains = dataset.targetDomains.filter(row => row.cooldown_status === 'SKIP').length;
  return [
    '| Reason | Domains | Active accts |',
    '|---|---:|---:|',
    `| Cooldown PASS (status_since >= 48h) | ${formatInt(passDomains)} | ${formatInt(dataset.wouldPauseAccounts.length)} |`,
    `| Cooldown SKIP (status_since < 48h) | ${formatInt(skipDomains)} | ${formatInt(dataset.cooldownSkippedAccounts.length)} |`,
  ].join('\n');
}

function buildDomainTable(rows: DomainRow[]): string {
  const lines = ['| domain | provider | 7d sent | 7d replies | RR% | active accts | status_since |', '|---|---|---:|---:|---:|---:|---|'];
  for (const row of rows) {
    lines.push(
      `| ${row.domain} | ${row.provider_group} | ${formatInt(row.sent_7d)} | ${formatInt(row.replies_7d)} | ${formatPct(row.last_rr_pct)} | ${formatInt(row.active_account_count)} | ${formatDateTime(row.status_since)} |`,
    );
  }
  if (rows.length === 0) {
    lines.push('| _(none)_ |  |  |  |  |  |  |');
  }
  return lines.join('\n');
}

function buildCooldownSkippedTable(rows: DomainRow[]): string {
  const lines = ['| domain | provider | 7d sent | RR% | status_since (hours ago) | active accts |', '|---|---|---:|---:|---:|---:|'];
  for (const row of rows) {
    lines.push(
      `| ${row.domain} | ${row.provider_group} | ${formatInt(row.sent_7d)} | ${formatPct(row.last_rr_pct)} | ${formatAgeHours(row.status_since_age_hours)} | ${formatInt(row.active_account_count)} |`,
    );
  }
  if (rows.length === 0) {
    lines.push('| _(none)_ |  |  |  |  |  |');
  }
  return lines.join('\n');
}

function buildSkipReasonTable(dataset: AutoPauseDataset): string {
  const counts = new Map<SkipReason, number>();
  for (const row of dataset.skippedDomains) {
    counts.set(row.skip_reason, (counts.get(row.skip_reason) ?? 0) + 1);
  }
  const lines = ['| Skip reason | Domains |', '|---|---:|'];
  for (const reason of ['STALE_DOMAIN_DATA', 'FREE_MAIL_DOMAIN'] as const) {
    lines.push(`| ${reason} | ${formatInt(counts.get(reason) ?? 0)} |`);
  }
  return lines.join('\n');
}

export function toCsv<T, K extends keyof T>(rows: T[], columns: readonly K[]): string {
  const header = columns.join(',');
  const body = rows.map(row => columns.map(column => csvEscape(formatCsvValue(row[column]))).join(','));
  return [header, ...body].join('\n') + '\n';
}

function normalizeDomainRow(row: DomainRowDb, now: Date): DomainRow {
  const statusSinceAgeHours = ageHours(now, row.status_since);
  const cooldownStatus: CooldownStatus = statusSinceAgeHours >= RECENT_FLIP_COOLDOWN_HOURS ? 'PASS' : 'SKIP';
  return {
    ...row,
    last_rr_pct: row.last_rr_pct === null ? null : Number(row.last_rr_pct),
    last_sent_total: Number(row.last_sent_total),
    sent_7d: Number(row.sent_7d ?? 0),
    replies_7d: Number(row.replies_7d ?? 0),
    active_account_count: Number(row.active_account_count),
    status_since_age_hours: statusSinceAgeHours,
    cooldown_status: cooldownStatus,
  };
}

function normalizeAccountRow(row: AccountRowDb): AccountRow {
  return {
    ...row,
    domain_status_since_age_hours: Number(row.domain_status_since_age_hours),
    cooldown_status: row.cooldown_status,
    domain_last_rr_pct: row.domain_last_rr_pct === null ? null : Number(row.domain_last_rr_pct),
    domain_last_sent_total: Number(row.domain_last_sent_total),
  };
}

function freshnessGateStatus(dataset: AutoPauseDataset): 'PASS' | 'FAIL' {
  return isStale(dataset.freshness.infra_domain_metrics_last_refresh, dataset.generatedAt, FRESHNESS_GATE_HOURS)
    ? 'FAIL'
    : 'PASS';
}

function enforceStopConditions(dataset: AutoPauseDataset): void {
  if (dataset.wouldPauseAccounts.some(row => !row.account_email.trim())) {
    throw new Error('Stop condition: would-pause CSV contains an empty account_email');
  }
  if (dataset.cooldownSkippedAccounts.some(row => !row.account_email.trim())) {
    throw new Error('Stop condition: would-pause CSV contains an empty account_email');
  }

  if (freshnessGateStatus(dataset) === 'FAIL') return;

  const rawRetireActiveAccounts = dataset.wouldPauseAccounts.length + dataset.cooldownSkippedAccounts.length;
  const currentBaseline = 414_000;
  if (rawRetireActiveAccounts > currentBaseline * 2 || rawRetireActiveAccounts < currentBaseline * 0.3) {
    throw new Error(
      `Stop condition: raw retire active-account count ${rawRetireActiveAccounts} is outside 0.3x-2x of ${currentBaseline}`,
    );
  }
}

function skippedByReason(dataset: AutoPauseDataset, reason: SkipReason): DomainRow[] {
  return dataset.skippedDomains.filter(row => row.skip_reason === reason);
}

function isStale(value: Date | string | null, now: Date, maxAgeHours: number): boolean {
  const age = ageHoursOrNull(now, value);
  return age === null || age > maxAgeHours;
}

function ageHours(now: Date, value: Date | string): number {
  return (now.getTime() - toTime(value)) / 3_600_000;
}

function ageHoursOrNull(now: Date, value: Date | string | null): number | null {
  if (!value) return null;
  return ageHours(now, value);
}

function ageMinutesOrNull(now: Date, value: Date | string | null): number | null {
  if (!value) return null;
  return (now.getTime() - toTime(value)) / 60_000;
}

function toTime(value: Date | string): number {
  return value instanceof Date ? value.getTime() : new Date(value).getTime();
}

function getOutputPaths(outputDir: string, reportDate: string): OutputPaths {
  return {
    report: path.join(outputDir, `${reportDate}-would-pause-summary.md`),
    accountsCsv: path.join(outputDir, `${reportDate}-would-pause-accounts.csv`),
  };
}

function resolveRenaissanceRoot(startDir: string): string {
  if (process.env.RENAISSANCE_ROOT) {
    return path.resolve(process.env.RENAISSANCE_ROOT);
  }

  const candidates = [
    path.resolve(startDir, '..'),
    path.resolve(startDir, '..', '..'),
    path.resolve(startDir, '..', '..', '..'),
  ];

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

function formatDateTime(value: Date | string | null): string {
  if (!value) return 'unknown';
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString();
}

function formatAgeHours(value: number | null): string {
  if (value === null) return 'unknown';
  return `${value.toFixed(1)}h`;
}

function formatAgeMinutes(value: number | null): string {
  if (value === null) return 'unknown';
  return `${value.toFixed(1)}m`;
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

function sum<T>(rows: T[], mapper: (row: T) => number): number {
  return rows.reduce((total, row) => total + mapper(row), 0);
}

async function main(): Promise<void> {
  const result = await buildAutoPauseDryRun();
  console.log(`[auto-pause-dry-run] report=${result.reportPath}`);
  console.log(`[auto-pause-dry-run] accounts_csv=${result.accountsCsvPath}`);
  console.log(`[auto-pause-dry-run] freshness_gate=${result.freshnessGate}`);
  console.log(`[auto-pause-dry-run] raw_target_accounts=${result.rawTargetAccounts}`);
  console.log(`[auto-pause-dry-run] would_pause_now_domains=${result.wouldPauseDomains}`);
  console.log(`[auto-pause-dry-run] would_pause_now_accounts=${result.wouldPauseAccounts}`);
  console.log(`[auto-pause-dry-run] cooldown_skipped_domains=${result.cooldownSkippedDomains}`);
  console.log(`[auto-pause-dry-run] cooldown_skipped_accounts=${result.cooldownSkippedAccounts}`);
  console.log(`[auto-pause-dry-run] estimated_cold_per_day_saved=${result.estimatedColdPerDaySaved}`);
  console.log(`[auto-pause-dry-run] duration_ms=${result.durationMs}`);
}

if (require.main === module) {
  main().catch(err => {
    console.error('[auto-pause-dry-run] Fatal:', err instanceof Error ? err.message : err);
    process.exit(1);
  });
}
