/**
 * runner.ts - GitHub Actions entry point for the data pipeline sync.
 *
 * Called by .github/workflows/data-pipeline-sync.yml
 * Reads secrets from environment variables (set as GitHub Actions secrets).
 *
 * Usage:
 *   npx tsx src/runner.ts
 *   npx tsx src/runner.ts --inbox
 *
 * Env vars required:
 *   INSTANTLY_API_KEYS   - JSON string: { "workspace-slug": "api-key", ... }
 *   SUPABASE_URL         - https://nmkaydqcnkjsehyqokgg.supabase.co
 *   SUPABASE_KEY         - service role key
 *
 * Optional env vars (set by workflow_dispatch inputs):
 *   WORKSPACE_FILTER     - comma-separated workspace slugs to sync (default: all)
 *   RUN_TYPE             - "full" or "inbox" (default: "full")
 */

import { syncAllWorkspaces } from './sync';

async function main() {
  const keysRaw = process.env.INSTANTLY_API_KEYS;
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_KEY;

  if (!keysRaw) {
    console.error('[runner] Missing INSTANTLY_API_KEYS');
    process.exit(1);
  }
  if (!supabaseUrl || !supabaseKey) {
    console.error('[runner] Missing SUPABASE_URL or SUPABASE_KEY');
    process.exit(1);
  }

  let keyMap: Record<string, string>;
  try {
    keyMap = JSON.parse(keysRaw);
  } catch (e) {
    console.error('[runner] Failed to parse INSTANTLY_API_KEYS as JSON:', e);
    process.exit(1);
  }

  // Filter workspaces if WORKSPACE_FILTER is set (from workflow_dispatch input)
  const wsFilter = process.env.WORKSPACE_FILTER?.trim();
  if (wsFilter) {
    const slugs = new Set(wsFilter.split(',').map(s => s.trim()).filter(Boolean));
    const before = Object.keys(keyMap).length;
    keyMap = Object.fromEntries(Object.entries(keyMap).filter(([slug]) => slugs.has(slug)));
    console.log(`[runner] Workspace filter applied: ${Object.keys(keyMap).length}/${before} workspaces`);
  }

  // Determine run type (--inbox flag or RUN_TYPE env var)
  const runType = process.env.RUN_TYPE ?? 'full';
  const isInboxRun = runType === 'inbox' || process.argv.includes('--inbox');

  console.log(`[runner] Starting ${isInboxRun ? 'inbox' : 'full'} sync for ${Object.keys(keyMap).length} workspaces`);
  const start = Date.now();

  await syncAllWorkspaces(keyMap, supabaseUrl, supabaseKey, isInboxRun);

  const elapsed = Math.round((Date.now() - start) / 1000);
  console.log(`[runner] Completed in ${elapsed}s`);
}

main().catch((e) => {
  console.error('[runner] Fatal:', e instanceof Error ? e.message : e);
  process.exit(1);
});
