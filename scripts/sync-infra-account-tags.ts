import { InstantlyClient } from '../src/instantly';
import { parseInstantlyKeyMap } from '../src/instantly-key-map';
import { RESOURCE_TYPE_ACCOUNTS, syncAccountTags } from '../src/infra/account-tags';
import { SupabaseClient } from '../src/supabase';

// Mirrors campaign-tag-sync pacing: weekly GitHub Action, 360-minute timeout,
// full pagination, no resume/checkpoint until the first scheduled audit proves it is needed.
async function main(): Promise<void> {
  const keysJson = process.env.INSTANTLY_API_KEYS;
  const url = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const key = process.env.PIPELINE_SUPABASE_KEY ?? process.env.PIPELINE_SUPABASE_SERVICE_ROLE_KEY ?? process.env.SUPABASE_KEY;
  const resourceTypeRaw = process.env.ACCOUNT_TAG_RESOURCE_TYPE;
  const missing: string[] = [];
  if (!keysJson) missing.push('INSTANTLY_API_KEYS');
  if (!url) missing.push('PIPELINE_SUPABASE_URL');
  if (!key) missing.push('PIPELINE_SUPABASE_KEY');
  if (missing.length > 0) throw new Error(`Missing env vars: ${missing.join(', ')}`);
  const resourceType = resourceTypeRaw ? Number(resourceTypeRaw) : RESOURCE_TYPE_ACCOUNTS;
  if (!Number.isInteger(resourceType)) throw new Error(`Invalid ACCOUNT_TAG_RESOURCE_TYPE: ${resourceTypeRaw}`);

  const startedAt = new Date().toISOString();
  const db = new SupabaseClient(url!, key!);
  const stats = await syncAccountTags({
    keyMap: parseInstantlyKeyMap(keysJson!),
    supabase: db,
    makeClient: apiKey => new InstantlyClient(apiKey),
    resourceType,
    workspaceFilter: process.env.WORKSPACE_FILTER,
  });
  await db.insert('infra_sync_runs', [{
    run_type: 'account_tags',
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    status: stats.errors.length > 0 ? 'partial' : 'completed',
    workspace_count: stats.workspaceCount,
    accounts_seen: stats.accountsSeen,
    account_metric_rows: 0,
    domains_written: stats.rowsWritten,
    api_calls_made: stats.apiCalls,
    rate_limit_events: stats.rateLimitEvents,
    errors: stats.errors,
    duration_ms: stats.durationMs,
  }]);
  console.log(`[account-tags] workspaces=${stats.workspaceCount}`);
  console.log(`[account-tags] accounts_seen=${stats.accountsSeen}`);
  console.log(`[account-tags] mappings_seen=${stats.mappingsSeen}`);
  console.log(`[account-tags] rows_written=${stats.rowsWritten}`);
  console.log(`[account-tags] errors=${stats.errors.length}`);
  if (stats.errors.length > 0) process.exit(1);
}

main().catch((err) => {
  console.error('[account-tags] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
