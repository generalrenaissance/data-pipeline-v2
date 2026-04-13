import { SupabaseClient } from './supabase';

export interface CampaignTagCacheRow {
  workspace_id: string;
  campaign_id: string;
  tags: string[];
  refreshed_at: string;
}

export async function loadCampaignTagCache(
  db: SupabaseClient,
  workspaceSlug: string,
): Promise<Map<string, string[]>> {
  const rows = await db.select(
    'campaign_tag_cache',
    `select=campaign_id,tags&workspace_id=eq.${encodeURIComponent(workspaceSlug)}&limit=1000`,
  );
  const cache = new Map<string, string[]>();
  for (const row of rows as Array<{ campaign_id: string; tags: string[] | null }>) {
    cache.set(row.campaign_id, row.tags ?? []);
  }
  return cache;
}

export async function upsertCampaignTagCache(
  db: SupabaseClient,
  rows: CampaignTagCacheRow[],
): Promise<void> {
  await db.upsert('campaign_tag_cache', rows, 'workspace_id,campaign_id');
}

export async function deleteStaleCampaignTagCache(
  db: SupabaseClient,
  workspaceSlug: string,
  activeCampaignIds: string[],
): Promise<void> {
  if (activeCampaignIds.length === 0) {
    await db.delete('campaign_tag_cache', `workspace_id=eq.${encodeURIComponent(workspaceSlug)}`);
    return;
  }

  const encodedIds = encodeURIComponent(`(${activeCampaignIds.map((campaignId) => `"${campaignId}"`).join(',')})`);
  await db.delete(
    'campaign_tag_cache',
    `workspace_id=eq.${encodeURIComponent(workspaceSlug)}&campaign_id=not.in.${encodedIds}`,
  );
}
