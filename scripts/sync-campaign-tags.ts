import { loadCampaignTagCache, upsertCampaignTagCache, deleteStaleCampaignTagCache, type CampaignTagCacheRow } from '../src/campaign-tag-cache';
import { InstantlyClient } from '../src/instantly';
import { SupabaseClient } from '../src/supabase';
import type { Campaign } from '../src/types';

const {
  PIPELINE_SUPABASE_URL,
  PIPELINE_SUPABASE_KEY,
  INSTANTLY_API_KEYS,
  WORKSPACE_FILTER,
} = process.env;

function toSlug(ws: string): string {
  return ws.toLowerCase().replace(/\s+/g, '-');
}

async function fetchCampaignTagRows(
  workspaceSlug: string,
  apiKey: string,
): Promise<{ rows: CampaignTagCacheRow[]; campaigns: Campaign[]; totalMappings: number; taggedCampaigns: number }> {
  const client = new InstantlyClient(apiKey);
  const [campaigns, tagMap, allMappings] = await Promise.all([
    client.getCampaigns(),
    client.getTagMap(),
    client.getAllCustomTagMappings(),
  ]);

  const tagsByCampaign = new Map<string, string[]>();
  for (const mapping of allMappings) {
    if (mapping.resource_type !== 2) continue;
    const label = tagMap.get(mapping.tag_id);
    if (!label) continue;
    const existing = tagsByCampaign.get(mapping.resource_id) ?? [];
    existing.push(label);
    tagsByCampaign.set(mapping.resource_id, existing);
  }

  const refreshedAt = new Date().toISOString();
  const rows = campaigns.map((campaign) => ({
    workspace_id: workspaceSlug,
    campaign_id: campaign.id,
    tags: Array.from(new Set(tagsByCampaign.get(campaign.id) ?? [])).sort(),
    refreshed_at: refreshedAt,
  }));

  return {
    rows,
    campaigns,
    totalMappings: allMappings.length,
    taggedCampaigns: rows.filter((row) => row.tags.length > 0).length,
  };
}

async function syncWorkspaceTagCache(
  workspaceSlug: string,
  apiKey: string,
  db: SupabaseClient,
): Promise<void> {
  const existing = await loadCampaignTagCache(db, workspaceSlug).catch(() => new Map<string, string[]>());
  const { rows, campaigns, totalMappings, taggedCampaigns } = await fetchCampaignTagRows(workspaceSlug, apiKey);
  await upsertCampaignTagCache(db, rows);
  await deleteStaleCampaignTagCache(db, workspaceSlug, campaigns.map((campaign) => campaign.id));
  const activeCampaignIds = new Set(campaigns.map((campaign) => campaign.id));
  const staleDeleted = [...existing.keys()].filter((campaignId) => !activeCampaignIds.has(campaignId)).length;
  console.log(
    `[tag-cache] ${workspaceSlug}: ${campaigns.length} campaigns, ` +
    `${totalMappings} mappings fetched, ${taggedCampaigns} tagged, ${staleDeleted} stale deleted`
  );
}

async function main(): Promise<void> {
  const required: Record<string, string | undefined> = {
    PIPELINE_SUPABASE_URL,
    PIPELINE_SUPABASE_KEY,
    INSTANTLY_API_KEYS,
  };
  const missing = Object.entries(required).filter(([, value]) => !value).map(([key]) => key);
  if (missing.length > 0) {
    throw new Error(`Missing env vars: ${missing.join(', ')}`);
  }

  const parsed = JSON.parse(INSTANTLY_API_KEYS!) as Record<string, string>;
  const workspaceFilter = new Set(
    (WORKSPACE_FILTER ?? '').split(',').map((entry) => toSlug(entry.trim())).filter(Boolean),
  );
  const filtered = workspaceFilter.size > 0
    ? Object.fromEntries(Object.entries(parsed).filter(([slug]) => workspaceFilter.has(slug)))
    : parsed;

  const db = new SupabaseClient(PIPELINE_SUPABASE_URL!, PIPELINE_SUPABASE_KEY!);
  let errors = 0;
  console.log(`[tag-cache] Starting sync for ${Object.keys(filtered).length} workspaces`);

  for (const [workspaceSlug, apiKey] of Object.entries(filtered)) {
    try {
      await syncWorkspaceTagCache(workspaceSlug, apiKey, db);
    } catch (err) {
      errors++;
      console.error(`[tag-cache] ${workspaceSlug}: ERROR`, err);
    }
  }

  console.log(`[tag-cache] Done with ${errors} errors`);
  if (errors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('[tag-cache] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
