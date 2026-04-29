// Reads from public.infra_sheet_registry (which the existing
// infra-sheet-registry-sync workflow refreshes daily from the live
// Renaissance Google Sheet) and writes a sanitized JSON snapshot to
// data/inbox-hub-latest.json.
//
// THIS REPO IS PUBLIC. The output goes into the public tree.
//
// Two safety layers:
//   1. SELECT only columns in ALLOWED_COLUMNS. Schema additions don't leak.
//   2. Runtime shape validator: throws if any DENIED_COLUMNS key appears,
//      or if any unknown key (not on either list) appears.
//
// Excluded by design:
//   - Employee/contractor PII: campaign_manager, inbox_manager, technical
//   - Sending-infra exposure: brand_name, brand_domain, workspace_name, workspace_slug
//   - Operational/financial: billing_date, domain_purchase_date, warmup_start_date, batch
//   - The raw_row jsonb (contains all of the above)

import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { Pool } from 'pg';

const ALLOWED_COLUMNS = [
  'tag',
  'offer',
  'sheet_status',
  'email_provider',
  'provider_group',
  'group_name',
  'pair',
  'infra_type',
  'accounts_expected',
  'cold_per_account',
  'warmup_per_account',
  'expected_daily_cold',
  'expected_domain_count',
  'accounts_per_domain',
  'tag_value',
  'low_rr',
  'warmup_emails_daily',
  'need_warmup',
  'row_confidence',
  'sheet_synced_at',
] as const;

type AllowedColumn = (typeof ALLOWED_COLUMNS)[number];

const DENIED_COLUMNS: ReadonlySet<string> = new Set([
  'campaign_manager',
  'inbox_manager',
  'technical',
  'brand_name',
  'brand_domain',
  'billing_date',
  'domain_purchase_date',
  'warmup_start_date',
  'batch',
  'raw_row',
  'workspace_name',
  'workspace_slug',
  'source_tab',
  'source_row',
  'row_warnings',
  'created_at',
  'updated_at',
  'deliverability_label',
]);

const ALLOWED_SET: ReadonlySet<string> = new Set(ALLOWED_COLUMNS);

export interface ExportPayload {
  generated_at: string;
  source: string;
  sheet_synced_at: string | null;
  row_count: number;
  allowed_columns: readonly AllowedColumn[];
  rows: Record<AllowedColumn, unknown>[];
}

export function validateRowShape(row: Record<string, unknown>): void {
  const keys = Object.keys(row);
  const denied = keys.filter((k) => DENIED_COLUMNS.has(k));
  if (denied.length > 0) {
    throw new Error(
      `SECURITY: denied column(s) appeared in export output: ${denied.join(', ')}. ` +
        `This must never reach a public-repo commit. Aborting.`,
    );
  }
  const unknown = keys.filter((k) => !ALLOWED_SET.has(k));
  if (unknown.length > 0) {
    throw new Error(
      `SECURITY: unknown column(s) appeared in export output: ${unknown.join(', ')}. ` +
        `Add explicitly to ALLOWED_COLUMNS or DENIED_COLUMNS before re-running.`,
    );
  }
}

function toIsoString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'string') return value;
  return String(value);
}

export function buildPayload(
  inputRows: Record<AllowedColumn, unknown>[],
  generatedAt: Date = new Date(),
): ExportPayload {
  // pg returns timestamptz columns as Date objects; strings after a
  // JSON round-trip. Normalize to ISO strings here so payload-equivalence
  // checks compare like-with-like across both code paths.
  const rows = inputRows.map((row) => ({
    ...row,
    sheet_synced_at: toIsoString(row.sheet_synced_at),
  })) as Record<AllowedColumn, unknown>[];
  for (const row of rows) validateRowShape(row);
  const lastSync = rows.length > 0 ? (rows[0].sheet_synced_at as string | null) ?? null : null;
  return {
    generated_at: generatedAt.toISOString(),
    source: 'public.infra_sheet_registry',
    sheet_synced_at: lastSync,
    row_count: rows.length,
    allowed_columns: ALLOWED_COLUMNS,
    rows,
  };
}

// Compare the data-bearing parts of two payloads. `generated_at` is
// excluded so a re-run with identical underlying data is a no-op
// even though the generation timestamp moved. Without this, the
// daily cron would commit a fresh JSON every run regardless of
// whether the sheet actually changed.
export function payloadsEquivalent(a: ExportPayload, b: ExportPayload): boolean {
  if (a.row_count !== b.row_count) return false;
  if (a.sheet_synced_at !== b.sheet_synced_at) return false;
  if (a.source !== b.source) return false;
  if (JSON.stringify(a.allowed_columns) !== JSON.stringify(b.allowed_columns)) return false;
  return JSON.stringify(a.rows) === JSON.stringify(b.rows);
}

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');

  if (!process.env.PIPELINE_SUPABASE_DB_URL) {
    throw new Error('Missing PIPELINE_SUPABASE_DB_URL');
  }

  const pool = new Pool({
    connectionString: process.env.PIPELINE_SUPABASE_DB_URL,
    statement_timeout: 60_000,
    query_timeout: 60_000,
  });

  try {
    const cols = ALLOWED_COLUMNS.join(', ');
    const result = await pool.query(`
      select ${cols}
      from public.infra_sheet_registry
      order by sheet_synced_at desc, tag asc, offer asc
    `);

    const rows = result.rows as Record<AllowedColumn, unknown>[];
    const payload = buildPayload(rows);

    console.log(`[inbox-hub-export] rows=${payload.row_count}`);
    console.log(`[inbox-hub-export] sheet_synced_at=${payload.sheet_synced_at ?? '(none)'}`);

    if (dryRun) {
      const json = JSON.stringify(payload, null, 2);
      console.log(`[inbox-hub-export] bytes=${json.length}`);
      console.log(`[inbox-hub-export] dry_run=true (no write)`);
      const sample = rows[0];
      if (sample) {
        console.log(`[inbox-hub-export] sample_keys=${Object.keys(sample).sort().join(',')}`);
      }
      return;
    }

    const outPath = path.resolve('data/inbox-hub-latest.json');
    let existing: ExportPayload | null = null;
    try {
      const existingText = await readFile(outPath, 'utf-8');
      existing = JSON.parse(existingText) as ExportPayload;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
    }

    if (existing && payloadsEquivalent(existing, payload)) {
      console.log(`[inbox-hub-export] no-op: rows + sheet_synced_at unchanged since last write`);
      return;
    }

    const json = JSON.stringify(payload, null, 2);
    console.log(`[inbox-hub-export] bytes=${json.length}`);
    await mkdir(path.dirname(outPath), { recursive: true });
    await writeFile(outPath, json + '\n', 'utf-8');
    console.log(`[inbox-hub-export] wrote=${outPath}`);
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error('[inbox-hub-export] Fatal:', err instanceof Error ? err.message : err);
    process.exit(1);
  });
}
