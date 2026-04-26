import type { InstantlyClient } from '../instantly';
import type { SupabaseClient } from '../supabase';
import type { Account, TagMapping } from '../types';

import { emailToDomain } from './domain-utils';
import { EXCLUDED_SLUGS } from './provider-routing';

export interface AccountTagProbeResult {
  workspace: string;
  accountsSeen: number;
  mappingsSeen: number;
  resourceTypeCounts: Record<string, number>;
  accountMatchingResourceTypes: Record<string, number>;
  sampleResourceIdsByType: Record<string, string[]>;
  sampleAccountEmails: string[];
  sampleMatches: Array<{
    account_email: string;
    resource_id: string;
    tag_label: string | null;
    resource_type: number;
    mapping_id: string | null;
  }>;
  verdict: 'ACCOUNT_TAGS_AVAILABLE' | 'ACCOUNT_TAGS_NOT_FOUND';
  chosenResourceType: number | null;
}

export interface AccountTagSyncStats {
  workspaceCount: number;
  accountsSeen: number;
  mappingsSeen: number;
  rowsWritten: number;
  apiCalls: number;
  rateLimitEvents: number;
  errors: string[];
  durationMs: number;
}

interface AccountTagRow {
  workspace_slug: string;
  account_email: string;
  resource_id: string;
  domain: string | null;
  tag_id: string;
  tag_label: string;
  resource_type: number;
  mapping_id: string | null;
  api_synced_at: string;
  raw_mapping: TagMapping;
}

export const RESOURCE_TYPE_ACCOUNTS = 1;

function pickString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function accountEmail(account: Account): string | null {
  const email = pickString(account.email);
  return email ? email.toLowerCase() : null;
}

function chooseResourceType(matches: Record<string, number>): number | null {
  let bestType: number | null = null;
  let bestCount = 0;
  for (const [rawType, count] of Object.entries(matches)) {
    const n = Number(rawType);
    if (!Number.isFinite(n)) continue;
    if (count > bestCount) {
      bestType = n;
      bestCount = count;
    }
  }
  return bestType;
}

export async function probeAccountTagMappings(
  workspaceSlug: string,
  client: InstantlyClient,
  options: { maxMappingPages?: number } = {},
): Promise<AccountTagProbeResult> {
  const [accounts, tagMap, mappings] = await Promise.all([
    client.getAccountsRaw(),
    client.getTagMap(),
    client.getAllCustomTagMappings(options.maxMappingPages),
  ]);
  const accountsByEmail = new Map<string, Account>();
  for (const account of accounts) {
    const email = accountEmail(account);
    if (!email) continue;
    accountsByEmail.set(email, account);
  }
  const resourceTypeCounts: Record<string, number> = {};
  const accountMatchingResourceTypes: Record<string, number> = {};
  const sampleResourceIdsByType: Record<string, string[]> = {};
  const sampleMatches: AccountTagProbeResult['sampleMatches'] = [];

  for (const mapping of mappings) {
    const typeKey = String(mapping.resource_type);
    resourceTypeCounts[typeKey] = (resourceTypeCounts[typeKey] ?? 0) + 1;
    const samples = sampleResourceIdsByType[typeKey] ?? [];
    if (samples.length < 5 && mapping.resource_id) {
      samples.push(mapping.resource_id);
      sampleResourceIdsByType[typeKey] = samples;
    }
    const resourceId = mapping.resource_id.trim().toLowerCase();
    const matched = accountsByEmail.get(resourceId);
    if (!matched) continue;
    accountMatchingResourceTypes[typeKey] = (accountMatchingResourceTypes[typeKey] ?? 0) + 1;
    if (sampleMatches.length < 10) {
      sampleMatches.push({
        account_email: resourceId,
        resource_id: mapping.resource_id,
        tag_label: tagMap.get(mapping.tag_id) ?? null,
        resource_type: mapping.resource_type,
        mapping_id: pickString(mapping.id),
      });
    }
  }

  const sampleAccountEmails: string[] = [];
  for (const email of accountsByEmail.keys()) {
    if (sampleAccountEmails.length >= 5) break;
    sampleAccountEmails.push(email);
  }

  const chosenResourceType = chooseResourceType(accountMatchingResourceTypes);
  return {
    workspace: workspaceSlug,
    accountsSeen: accounts.length,
    mappingsSeen: mappings.length,
    resourceTypeCounts,
    accountMatchingResourceTypes,
    sampleResourceIdsByType,
    sampleAccountEmails,
    sampleMatches,
    verdict: chosenResourceType === null ? 'ACCOUNT_TAGS_NOT_FOUND' : 'ACCOUNT_TAGS_AVAILABLE',
    chosenResourceType,
  };
}

function inScopeWorkspaces(keyMap: Record<string, string>, workspaceFilter?: string): Array<[string, string]> {
  return Object.entries(keyMap).filter(([slug]) => {
    if (EXCLUDED_SLUGS.includes(slug)) return false;
    return !workspaceFilter || workspaceFilter === slug;
  });
}

export async function buildAccountTagRows(
  workspaceSlug: string,
  client: InstantlyClient,
  resourceType = RESOURCE_TYPE_ACCOUNTS,
  syncedAt = new Date().toISOString(),
): Promise<{ rows: AccountTagRow[]; accountsSeen: number; mappingsSeen: number }> {
  const [accounts, tagMap, mappings] = await Promise.all([
    client.getAccountsRaw(),
    client.getTagMap(),
    client.getAllCustomTagMappings(),
  ]);
  const accountsByEmail = new Map<string, Account>();
  for (const account of accounts) {
    const email = accountEmail(account);
    if (!email) continue;
    accountsByEmail.set(email, account);
  }
  const rows: AccountTagRow[] = [];
  for (const mapping of mappings) {
    if (mapping.resource_type !== resourceType) continue;
    const accountEmailKey = mapping.resource_id.trim().toLowerCase();
    const account = accountsByEmail.get(accountEmailKey);
    if (!account) continue;
    const label = tagMap.get(mapping.tag_id);
    if (!label) continue;
    rows.push({
      workspace_slug: workspaceSlug,
      account_email: accountEmailKey,
      resource_id: mapping.resource_id,
      domain: emailToDomain(accountEmailKey),
      tag_id: mapping.tag_id,
      tag_label: label,
      resource_type: mapping.resource_type,
      mapping_id: pickString(mapping.id),
      api_synced_at: syncedAt,
      raw_mapping: mapping,
    });
  }
  return { rows, accountsSeen: accounts.length, mappingsSeen: mappings.length };
}

export async function syncAccountTags(deps: {
  keyMap: Record<string, string>;
  supabase: SupabaseClient;
  makeClient: (apiKey: string) => InstantlyClient;
  resourceType?: number;
  workspaceFilter?: string;
}): Promise<AccountTagSyncStats> {
  const start = Date.now();
  const stats: AccountTagSyncStats = {
    workspaceCount: 0,
    accountsSeen: 0,
    mappingsSeen: 0,
    rowsWritten: 0,
    apiCalls: 0,
    rateLimitEvents: 0,
    errors: [],
    durationMs: 0,
  };
  for (const [slug, key] of inScopeWorkspaces(deps.keyMap, deps.workspaceFilter)) {
    stats.workspaceCount++;
    const client = deps.makeClient(key);
    try {
      const built = await buildAccountTagRows(slug, client, deps.resourceType ?? RESOURCE_TYPE_ACCOUNTS);
      stats.accountsSeen += built.accountsSeen;
      stats.mappingsSeen += built.mappingsSeen;
      if (built.rows.length > 0) {
        await deps.supabase.upsert('infra_account_tag_mappings', built.rows, 'workspace_slug,account_email,tag_id');
      }
      stats.rowsWritten += built.rows.length;
    } catch (err) {
      stats.errors.push(`${slug}: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      stats.apiCalls += client.apiCallCount;
      stats.rateLimitEvents += client.rateLimitEvents;
    }
  }
  stats.durationMs = Date.now() - start;
  return stats;
}
