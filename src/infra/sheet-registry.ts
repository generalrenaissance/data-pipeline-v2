import { readFile } from 'node:fs/promises';
import path from 'node:path';

import type { SupabaseClient } from '../supabase';
import type { ProviderGroup } from './provider-routing';

export type RowConfidence = 'high' | 'medium' | 'low' | 'invalid';

export interface SheetRegistryRow {
  source_tab: string;
  source_row: number;
  tag: string | null;
  offer: string | null;
  campaign_manager: string | null;
  workspace_name: string | null;
  workspace_slug: string | null;
  sheet_status: string | null;
  deliverability_label: string | null;
  need_warmup: boolean | null;
  group_name: string | null;
  pair: string | null;
  inbox_manager: string | null;
  billing_date: string | null;
  warmup_start_date: string | null;
  brand_name: string | null;
  brand_domain: string | null;
  infra_type: string | null;
  technical: string | null;
  batch: string | null;
  email_provider: string | null;
  provider_group: ProviderGroup;
  accounts_expected: number | null;
  cold_per_account: number | null;
  warmup_per_account: number | null;
  expected_daily_cold: number | null;
  accounts_per_domain: number | null;
  expected_domain_count: number | null;
  tag_value: number | null;
  domain_purchase_date: string | null;
  low_rr: boolean | null;
  warmup_emails_daily: number | null;
  row_confidence: RowConfidence;
  row_warnings: string[];
  raw_row: unknown[];
  sheet_synced_at: string;
  updated_at: string;
}

export interface BrandRegistryRow {
  source_tab: string;
  source_row: number;
  offer: string;
  brand_name: string;
  brand_domain: string | null;
  group_assigned: string | null;
  date_created: string | null;
  setup_status: string | null;
  raw_row: unknown[];
  sheet_synced_at: string;
}

export interface CancelledRegistryRow {
  source_tab: string;
  source_row: number;
  tag: string | null;
  offer: string | null;
  campaign_manager: string | null;
  inbox_manager: string | null;
  workspace_name: string | null;
  workspace_slug: string | null;
  sheet_status: string | null;
  brand_name: string | null;
  infra_type: string | null;
  technical: string | null;
  batch: string | null;
  warmup_start_date: string | null;
  domain_purchase_date: string | null;
  accounts_expected: number | null;
  cold_per_account: number | null;
  warmup_per_account: number | null;
  row_confidence: RowConfidence;
  row_warnings: string[];
  raw_row: unknown[];
  sheet_synced_at: string;
}

export interface ParsedSheetRegistry {
  sheetRows: SheetRegistryRow[];
  brandRows: BrandRegistryRow[];
  cancelledRows: CancelledRegistryRow[];
  warnings: string[];
}

const ACTIVE_TABS = ['Funding', 'Other', 'Archived_ERC_', 'Archived_Other_'] as const;

function cleanString(value: unknown): string | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length === 0 ? null : trimmed;
  }
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  return null;
}

function normalizeHeader(value: unknown): string {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/[()]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function headerIndex(headers: unknown[], names: string[]): number | null {
  const normalized = headers.map(normalizeHeader);
  for (const name of names) {
    const idx = normalized.indexOf(normalizeHeader(name));
    if (idx >= 0) return idx;
  }
  return null;
}

function getByHeader(row: unknown[], headers: unknown[], names: string[]): unknown {
  const idx = headerIndex(headers, names);
  return idx === null ? undefined : row[idx];
}

function parseNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'string') return null;
  const cleaned = value.replace(/,/g, '').trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseInteger(value: unknown): number | null {
  const n = parseNumber(value);
  return n === null ? null : Math.round(n);
}

function parseBoolean(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value === 1 ? true : value === 0 ? false : null;
  if (typeof value !== 'string') return null;
  const v = value.trim().toLowerCase();
  if (['true', 'yes', 'y', '1'].includes(v)) return true;
  if (['false', 'no', 'n', '0'].includes(v)) return false;
  return null;
}

function parseDate(value: unknown): string | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    const epoch = Date.UTC(1899, 11, 30);
    const d = new Date(epoch + value * 86_400_000);
    return d.toISOString().slice(0, 10);
  }
  if (typeof value !== 'string') return null;
  const raw = value.trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const cleaned = raw.replace(/\b(\d{1,2})(st|nd|rd|th)\b/gi, '$1');
  const parsed = new Date(cleaned);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

export function normalizeWorkspaceSlug(value: string | null): string | null {
  if (!value) return null;
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return slug.length > 0 ? slug : null;
}

function normalizeBrandKey(value: string | null): string | null {
  if (!value) return null;
  const normalized = value
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/[^a-z0-9]+/g, '');
  return normalized.length > 0 ? normalized : null;
}

function normalizeDomain(value: string | null): string | null {
  if (!value) return null;
  const cleaned = value.trim().toLowerCase();
  if (!cleaned) return null;
  try {
    const withScheme = /^https?:\/\//.test(cleaned) ? cleaned : `https://${cleaned}`;
    return new URL(withScheme).hostname.replace(/^www\./, '');
  } catch {
    return cleaned.replace(/^www\./, '').replace(/\/+$/, '') || null;
  }
}

function providerGroup(emailProvider: string | null, infraType: string | null): ProviderGroup {
  const p = (emailProvider ?? '').toLowerCase();
  const t = (infraType ?? '').toLowerCase();
  if (p.includes('outlook') || p.includes('azure')) return 'outlook';
  if (p.includes('google')) return 'google_otd';
  if (t.includes('outreach today') || t.includes('google')) return 'google_otd';
  if (t.includes('outlook') || t.includes('mailin')) return 'outlook';
  return 'unknown';
}

function confidenceForActive(row: {
  tag: string | null;
  campaignManager: string | null;
  workspace: string | null;
  status: string | null;
  emailProvider: string | null;
  offer: string | null;
}): RowConfidence {
  if (!row.tag || /^helper/i.test(row.tag) || /^total$/i.test(row.tag)) return 'invalid';
  if (!row.campaignManager) return 'low';
  if (row.workspace && row.status && row.emailProvider && row.offer) return 'high';
  return 'medium';
}

function hasDateLookingOperationalValue(value: string | null): boolean {
  if (!value) return false;
  return parseDate(value) !== null;
}

function parseActiveRows(tab: string, rows: unknown[][], brandsByName: Map<string, BrandRegistryRow>, syncedAt: string): SheetRegistryRow[] {
  const headers = rows[0] ?? [];
  const output: SheetRegistryRow[] = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i] ?? [];
    const tag = tab === 'Other'
      ? cleanString(row[0])
      : cleanString(getByHeader(row, headers, ['Tag']));
    const brandName = cleanString(getByHeader(row, headers, ['Branding']));
    const brand = brandsByName.get(normalizeBrandKey(brandName) ?? '');
    const campaignManager = cleanString(getByHeader(row, headers, ['Campaign Manager']));
    const workspace = cleanString(getByHeader(row, headers, ['Workspace']));
    const status = cleanString(getByHeader(row, headers, ['Status']));
    const offer = cleanString(getByHeader(row, headers, ['OFFER']));
    const emailProvider = cleanString(getByHeader(row, headers, ['Email Provider']));
    const infraType = cleanString(getByHeader(row, headers, ['Type']));
    const rowConfidence = confidenceForActive({
      tag,
      campaignManager,
      workspace,
      status,
      emailProvider,
      offer,
    });
    const warnings: string[] = [];
    if (tag && output.some(r => r.tag === tag)) warnings.push(`duplicate tag within ${tab}: ${tag}`);
    if (rowConfidence === 'invalid') warnings.push('missing or helper tag');
    output.push({
      source_tab: tab,
      source_row: i,
      tag,
      offer,
      campaign_manager: campaignManager,
      workspace_name: workspace,
      workspace_slug: normalizeWorkspaceSlug(workspace),
      sheet_status: status,
      deliverability_label: cleanString(getByHeader(row, headers, ['Deliverability'])),
      need_warmup: parseBoolean(getByHeader(row, headers, ['Need warmup'])),
      group_name: cleanString(getByHeader(row, headers, ['Group'])),
      pair: cleanString(getByHeader(row, headers, ['Pair'])),
      inbox_manager: cleanString(getByHeader(row, headers, ['Inbox Manager'])),
      billing_date: parseDate(getByHeader(row, headers, ['Billing Date'])),
      warmup_start_date: parseDate(getByHeader(row, headers, ['Warmup Start Date'])),
      brand_name: brandName,
      brand_domain: brand?.brand_domain ?? null,
      infra_type: infraType,
      technical: cleanString(getByHeader(row, headers, ['Technical'])),
      batch: cleanString(getByHeader(row, headers, ['Batch'])),
      email_provider: emailProvider,
      provider_group: providerGroup(emailProvider, infraType),
      accounts_expected: parseInteger(getByHeader(row, headers, ['Accounts'])),
      cold_per_account: parseInteger(getByHeader(row, headers, ['Cold Emails'])),
      warmup_per_account: parseInteger(getByHeader(row, headers, ['Warmup Emails'])),
      expected_daily_cold: parseInteger(getByHeader(row, headers, ['Total Daily Cold emails sent', 'Total Daily emails sent'])),
      accounts_per_domain: parseNumber(getByHeader(row, headers, ['Accounts per Domain'])),
      expected_domain_count: parseNumber(getByHeader(row, headers, ['Domains'])),
      tag_value: parseNumber(getByHeader(row, headers, ['Tag Value'])),
      domain_purchase_date: parseDate(getByHeader(row, headers, ['Domain Purchase Date'])),
      low_rr: parseBoolean(getByHeader(row, headers, ['LOW RR'])),
      warmup_emails_daily: parseInteger(getByHeader(row, headers, ['Warmup Emails Daily', 'Warmup Emails (Daily)'])),
      row_confidence: rowConfidence,
      row_warnings: warnings,
      raw_row: row,
      sheet_synced_at: syncedAt,
      updated_at: syncedAt,
    });
  }
  return output;
}

export function parseBrandRows(rows: unknown[][], syncedAt = new Date().toISOString()): BrandRegistryRow[] {
  const offerRow = rows[1] ?? [];
  const out: BrandRegistryRow[] = [];
  for (let base = 0; base < offerRow.length; base += 5) {
    const offer = cleanString(offerRow[base]);
    if (!offer) continue;
    for (let i = 4; i < rows.length; i++) {
      const row = rows[i] ?? [];
      const brandName = cleanString(row[base]);
      if (!brandName) continue;
      out.push({
        source_tab: 'Brands_allocation',
        source_row: i,
        offer,
        brand_name: brandName,
        brand_domain: normalizeDomain(cleanString(row[base + 1])),
        group_assigned: cleanString(row[base + 2]),
        date_created: parseDate(row[base + 3]),
        setup_status: cleanString(row[base + 4]),
        raw_row: row,
        sheet_synced_at: syncedAt,
      });
    }
  }
  return out;
}

export function parseCancelledRows(rows: unknown[][], syncedAt = new Date().toISOString()): CancelledRegistryRow[] {
  const headers = rows[0] ?? [];
  const out: CancelledRegistryRow[] = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i] ?? [];
    const tag = cleanString(getByHeader(row, headers, ['Tag/Group', 'Tag']));
    const workspace = cleanString(getByHeader(row, headers, ['Workspace']));
    const brandName = cleanString(getByHeader(row, headers, ['Branding']));
    const infraType = cleanString(getByHeader(row, headers, ['Type']));
    const warnings: string[] = [];
    for (const [label, names] of Object.entries({
      brand_name: ['Branding'],
      infra_type: ['Type'],
      workspace: ['Workspace'],
      technical: ['Technical'],
      batch: ['Batch'],
    })) {
      const value = cleanString(getByHeader(row, headers, names));
      if (hasDateLookingOperationalValue(value)) warnings.push(`${label} looks like date: ${value}`);
    }
    let confidence: RowConfidence = 'medium';
    if (!tag) confidence = 'invalid';
    else if (warnings.length > 0) confidence = 'low';
    out.push({
      source_tab: 'Cancelled',
      source_row: i,
      tag,
      offer: cleanString(getByHeader(row, headers, ['OFFER'])),
      campaign_manager: cleanString(getByHeader(row, headers, ['Campaign Manager'])),
      inbox_manager: cleanString(getByHeader(row, headers, ['Inbox Manager'])),
      workspace_name: workspace,
      workspace_slug: normalizeWorkspaceSlug(workspace),
      sheet_status: cleanString(getByHeader(row, headers, ['Status'])),
      brand_name: brandName,
      infra_type: infraType,
      technical: cleanString(getByHeader(row, headers, ['Technical'])),
      batch: cleanString(getByHeader(row, headers, ['Batch'])),
      warmup_start_date: parseDate(getByHeader(row, headers, ['Warmup Start Date'])),
      domain_purchase_date: parseDate(getByHeader(row, headers, ['Domain Purchase Date'])),
      accounts_expected: parseInteger(getByHeader(row, headers, ['Accounts'])),
      cold_per_account: parseInteger(getByHeader(row, headers, ['Cold Emails'])),
      warmup_per_account: parseInteger(getByHeader(row, headers, ['Warmup Emails'])),
      row_confidence: confidence,
      row_warnings: warnings,
      raw_row: row,
      sheet_synced_at: syncedAt,
    });
  }
  return out;
}

async function loadJsonRows(dir: string, file: string): Promise<unknown[][]> {
  const raw = await readFile(path.join(dir, file), 'utf8');
  return JSON.parse(raw) as unknown[][];
}

export async function parseSheetDump(dir: string, now = new Date()): Promise<ParsedSheetRegistry> {
  const syncedAt = now.toISOString();
  const funding = await loadJsonRows(dir, 'Funding.json');
  const warnings: string[] = [];
  if ((funding[0] ?? [])[6] !== 'Campaign Manager') {
    throw new Error(`Funding.json row 0 column G expected Campaign Manager, got ${(funding[0] ?? [])[6]}`);
  }

  const brandRows = parseBrandRows(await loadJsonRows(dir, 'Brands_allocation.json'), syncedAt);
  const brandsByName = new Map<string, BrandRegistryRow>();
  for (const row of brandRows) {
    const key = normalizeBrandKey(row.brand_name);
    if (key) brandsByName.set(key, row);
  }

  const sheetRows: SheetRegistryRow[] = [];
  for (const tab of ACTIVE_TABS) {
    const rows = tab === 'Funding' ? funding : await loadJsonRows(dir, `${tab}.json`);
    sheetRows.push(...parseActiveRows(tab, rows, brandsByName, syncedAt));
  }
  const tagCounts = new Map<string, number>();
  for (const row of sheetRows) {
    if (!row.tag) continue;
    tagCounts.set(row.tag, (tagCounts.get(row.tag) ?? 0) + 1);
  }
  for (const [tag, count] of tagCounts) {
    if (count > 1) warnings.push(`duplicate tag reported: ${tag} (${count})`);
  }

  return {
    sheetRows,
    brandRows,
    cancelledRows: parseCancelledRows(await loadJsonRows(dir, 'Cancelled.json'), syncedAt),
    warnings,
  };
}

export async function syncSheetRegistry(db: SupabaseClient, parsed: ParsedSheetRegistry): Promise<void> {
  await db.upsert('infra_brand_registry', parsed.brandRows, 'source_tab,source_row,offer,brand_name');
  await db.upsert('infra_sheet_registry', parsed.sheetRows, 'source_tab,source_row');
  await db.upsert('infra_cancelled_registry', parsed.cancelledRows, 'source_tab,source_row');
}
