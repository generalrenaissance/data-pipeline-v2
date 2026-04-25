import type { SupabaseClient } from '../supabase';
import type { ProviderGroup } from './provider-routing';

type MappingStatus =
  | 'mapped'
  | 'unmapped'
  | 'no_account_tags'
  | 'sheet_tag_missing'
  | 'mixed_cm'
  | 'cancelled'
  | 'free_mail_excluded'
  | 'invalid';

interface InfraAccountRow {
  account_email: string;
  domain: string;
  workspace_slug: string;
  workspace_name: string | null;
  provider_group: ProviderGroup;
  account_status: string | null;
  is_free_mail: boolean;
}

interface AccountTagMappingRow {
  workspace_slug: string;
  account_email: string;
  resource_id: string;
  domain: string | null;
  tag_id: string;
  tag_label: string;
}

interface SheetRow {
  tag: string | null;
  offer: string | null;
  campaign_manager: string | null;
  workspace_name: string | null;
  workspace_slug: string | null;
  sheet_status: string | null;
  brand_name: string | null;
  brand_domain: string | null;
  infra_type: string | null;
  inbox_manager: string | null;
  group_name: string | null;
  pair: string | null;
  email_provider: string | null;
  batch: string | null;
  accounts_expected: number | null;
  expected_daily_cold: number | null;
  accounts_per_domain: number | null;
  expected_domain_count: number | null;
  domain_purchase_date: string | null;
  low_rr: boolean | null;
  row_confidence: 'high' | 'medium' | 'low' | 'invalid';
}

interface CancelledRow {
  tag: string | null;
  row_confidence: 'high' | 'medium' | 'low' | 'invalid';
}

export interface DomainRegistryRow {
  domain: string;
  provider_group: ProviderGroup;
  primary_campaign_manager: string | null;
  campaign_managers: string[];
  tag_labels: string[];
  sheet_tags: string[];
  workspace_slugs: string[];
  workspace_names: string[];
  offers: string[];
  brand_names: string[];
  brand_domains: string[];
  sheet_statuses: string[];
  infra_types: string[];
  inbox_managers: string[];
  group_names: string[];
  pairs: string[];
  email_providers: string[];
  batches: string[];
  accounts_per_domain_values: number[];
  expected_domain_count_values: number[];
  domain_purchase_dates: string[];
  low_rr_flags: boolean[];
  mapped_account_count: number;
  unmapped_account_count: number;
  total_account_count: number;
  active_account_count: number;
  sheet_accounts_expected_total: number | null;
  expected_daily_cold_total: number | null;
  cancelled_match_count: number;
  mapping_status: MappingStatus;
  confidence_score: number;
  mapping_warnings: string[];
  last_built_at: string;
}

function uniq(values: Array<string | null | undefined>): string[] {
  return Array.from(new Set(values.map(v => v?.trim()).filter((v): v is string => !!v))).sort();
}

function uniqNumbers(values: Array<number | null | undefined>): number[] {
  return Array.from(new Set(values.filter((v): v is number => typeof v === 'number' && Number.isFinite(v)))).sort((a, b) => a - b);
}

function uniqBooleans(values: Array<boolean | null | undefined>): boolean[] {
  return Array.from(new Set(values.filter((v): v is boolean => typeof v === 'boolean'))).sort();
}

function tagKey(tag: string | null | undefined): string | null {
  const v = tag?.trim().toLowerCase();
  return v ? v : null;
}

function mostCommonProvider(values: ProviderGroup[]): ProviderGroup {
  if (values.length === 0) return 'unknown';
  const counts = new Map<ProviderGroup, number>();
  for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);
  let best: ProviderGroup = 'unknown';
  let count = -1;
  for (const [value, n] of counts) {
    if (n > count) {
      best = value;
      count = n;
    }
  }
  return best;
}

function addScorePenalty(status: MappingStatus, sheetRows: SheetRow[], cancelledCount: number): number {
  let score = 100;
  if (status === 'mixed_cm') score -= 40;
  if (status === 'no_account_tags') score -= 35;
  if (status === 'sheet_tag_missing') score -= 30;
  if (status === 'cancelled' || cancelledCount > 0) score -= 20;
  if (sheetRows.some(r => r.row_confidence === 'low')) score -= 10;
  if (status === 'free_mail_excluded' || status === 'invalid') score = 0;
  return Math.max(0, Math.min(100, score));
}

function sumNullable(values: Array<number | null>): number | null {
  let total = 0;
  let seen = false;
  for (const value of values) {
    if (typeof value !== 'number') continue;
    total += value;
    seen = true;
  }
  return seen ? total : null;
}

export function buildDomainRegistryRows(input: {
  accounts: InfraAccountRow[];
  mappings: AccountTagMappingRow[];
  sheetRows: SheetRow[];
  cancelledRows: CancelledRow[];
  builtAt?: string;
}): DomainRegistryRow[] {
  const builtAt = input.builtAt ?? new Date().toISOString();
  const sheetByTag = new Map<string, SheetRow[]>();
  for (const row of input.sheetRows) {
    if (row.row_confidence === 'invalid') continue;
    const key = tagKey(row.tag);
    if (!key) continue;
    const rows = sheetByTag.get(key) ?? [];
    rows.push(row);
    sheetByTag.set(key, rows);
  }
  const cancelledByTag = new Map<string, CancelledRow[]>();
  for (const row of input.cancelledRows) {
    if (row.row_confidence === 'invalid') continue;
    const key = tagKey(row.tag);
    if (!key) continue;
    const rows = cancelledByTag.get(key) ?? [];
    rows.push(row);
    cancelledByTag.set(key, rows);
  }

  const accountsByDomain = new Map<string, InfraAccountRow[]>();
  for (const account of input.accounts) {
    const rows = accountsByDomain.get(account.domain) ?? [];
    rows.push(account);
    accountsByDomain.set(account.domain, rows);
  }
  const mappingsByAccount = new Map<string, AccountTagMappingRow[]>();
  for (const mapping of input.mappings) {
    const key = `${mapping.workspace_slug}\u0001${mapping.account_email.toLowerCase()}`;
    const rows = mappingsByAccount.get(key) ?? [];
    rows.push(mapping);
    mappingsByAccount.set(key, rows);
  }

  const output: DomainRegistryRow[] = [];
  for (const [domain, accounts] of accountsByDomain) {
    const accountMappings = new Map<string, AccountTagMappingRow[]>();
    for (const account of accounts) {
      const key = `${account.workspace_slug}\u0001${account.account_email.toLowerCase()}`;
      const rows = mappingsByAccount.get(key) ?? [];
      if (rows.length > 0) accountMappings.set(account.account_email, rows);
    }
    const allMappings = Array.from(accountMappings.values()).flat();
    const tagLabels = uniq(allMappings.map(r => r.tag_label));
    const joinedSheetRows = tagLabels.flatMap(label => sheetByTag.get(tagKey(label) ?? '') ?? []);
    const highConfidenceSheetRows = joinedSheetRows.filter(r => r.row_confidence === 'high');
    const cancelledMatches = tagLabels.flatMap(label => cancelledByTag.get(tagKey(label) ?? '') ?? []);
    const campaignManagers = uniq(joinedSheetRows.map(r => r.campaign_manager));
    const warnings: string[] = [];
    if (tagLabels.length > 0 && joinedSheetRows.length === 0) warnings.push('account tags did not match sheet registry');
    if (campaignManagers.length > 1) warnings.push(`mixed campaign managers: ${campaignManagers.join(', ')}`);
    if (cancelledMatches.length > 0) warnings.push('one or more tags matched cancelled registry');

    let mappingStatus: MappingStatus;
    if (accounts.some(a => a.is_free_mail)) {
      mappingStatus = 'free_mail_excluded';
    } else if (accounts.length === 0) {
      mappingStatus = 'invalid';
    } else if (allMappings.length === 0) {
      mappingStatus = 'no_account_tags';
    } else if (joinedSheetRows.length === 0) {
      mappingStatus = cancelledMatches.length > 0 ? 'cancelled' : 'sheet_tag_missing';
    } else if (cancelledMatches.length > 0 && highConfidenceSheetRows.length === 0) {
      mappingStatus = 'cancelled';
    } else if (campaignManagers.length > 1) {
      mappingStatus = 'mixed_cm';
    } else if (campaignManagers.length === 1) {
      mappingStatus = 'mapped';
    } else {
      mappingStatus = 'unmapped';
    }

    output.push({
      domain,
      provider_group: mostCommonProvider(accounts.map(a => a.provider_group)),
      primary_campaign_manager: mappingStatus === 'mapped' && campaignManagers.length === 1 ? campaignManagers[0]! : null,
      campaign_managers: campaignManagers,
      tag_labels: tagLabels,
      sheet_tags: uniq(joinedSheetRows.map(r => r.tag)),
      workspace_slugs: uniq(accounts.map(r => r.workspace_slug)),
      workspace_names: uniq(accounts.map(r => r.workspace_name).concat(joinedSheetRows.map(r => r.workspace_name))),
      offers: uniq(joinedSheetRows.map(r => r.offer)),
      brand_names: uniq(joinedSheetRows.map(r => r.brand_name)),
      brand_domains: uniq(joinedSheetRows.map(r => r.brand_domain)),
      sheet_statuses: uniq(joinedSheetRows.map(r => r.sheet_status)),
      infra_types: uniq(joinedSheetRows.map(r => r.infra_type)),
      inbox_managers: uniq(joinedSheetRows.map(r => r.inbox_manager)),
      group_names: uniq(joinedSheetRows.map(r => r.group_name)),
      pairs: uniq(joinedSheetRows.map(r => r.pair)),
      email_providers: uniq(joinedSheetRows.map(r => r.email_provider)),
      batches: uniq(joinedSheetRows.map(r => r.batch)),
      accounts_per_domain_values: uniqNumbers(joinedSheetRows.map(r => r.accounts_per_domain)),
      expected_domain_count_values: uniqNumbers(joinedSheetRows.map(r => r.expected_domain_count)),
      domain_purchase_dates: uniq(joinedSheetRows.map(r => r.domain_purchase_date)),
      low_rr_flags: uniqBooleans(joinedSheetRows.map(r => r.low_rr)),
      mapped_account_count: accountMappings.size,
      unmapped_account_count: Math.max(0, accounts.length - accountMappings.size),
      total_account_count: accounts.length,
      active_account_count: accounts.filter(a => a.account_status === 'active').length,
      sheet_accounts_expected_total: sumNullable(joinedSheetRows.map(r => r.accounts_expected)),
      expected_daily_cold_total: sumNullable(joinedSheetRows.map(r => r.expected_daily_cold)),
      cancelled_match_count: cancelledMatches.length,
      mapping_status: mappingStatus,
      confidence_score: addScorePenalty(mappingStatus, joinedSheetRows, cancelledMatches.length),
      mapping_warnings: warnings,
      last_built_at: builtAt,
    });
  }
  return output.sort((a, b) => a.domain.localeCompare(b.domain));
}

export async function rebuildDomainRegistry(db: SupabaseClient): Promise<{ rowsWritten: number; statusCounts: Record<string, number> }> {
  const [accounts, mappings, sheetRows, cancelledRows] = await Promise.all([
    db.selectAll(
      'infra_accounts',
      'select=account_email,domain,workspace_slug,workspace_name,provider_group,account_status,is_free_mail',
    ) as Promise<InfraAccountRow[]>,
    db.selectAll(
      'infra_account_tag_mappings',
      'select=workspace_slug,account_email,resource_id,domain,tag_id,tag_label',
    ) as Promise<AccountTagMappingRow[]>,
    db.selectAll(
      'infra_sheet_registry',
      'select=tag,offer,campaign_manager,workspace_name,workspace_slug,sheet_status,brand_name,brand_domain,infra_type,inbox_manager,group_name,pair,email_provider,batch,accounts_expected,expected_daily_cold,accounts_per_domain,expected_domain_count,domain_purchase_date,low_rr,row_confidence',
    ) as Promise<SheetRow[]>,
    db.selectAll('infra_cancelled_registry', 'select=tag,row_confidence') as Promise<CancelledRow[]>,
  ]);
  const rows = buildDomainRegistryRows({ accounts, mappings, sheetRows, cancelledRows });
  await db.upsert('infra_domain_registry', rows, 'domain');
  const statusCounts: Record<string, number> = {};
  for (const row of rows) statusCounts[row.mapping_status] = (statusCounts[row.mapping_status] ?? 0) + 1;
  return { rowsWritten: rows.length, statusCounts };
}
