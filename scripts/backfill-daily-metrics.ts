/**
 * One-off historical backfill for campaign_daily_metrics.
 *
 * For each campaign across all workspaces, calls Instantly
 * /campaigns/analytics/daily once for [--start, --end] and upserts every
 * returned day row. Designed to populate 2026-03-26 -> today after the new
 * table ships, bypassing the usual trailing-window refresh.
 *
 * Usage:
 *   cd data-pipeline-v2
 *   INSTANTLY_API_KEYS='{"renaissance-1":"...","..."}' \
 *   PIPELINE_SUPABASE_URL=... \
 *   PIPELINE_SUPABASE_KEY=... \
 *   npx tsx scripts/backfill-daily-metrics.ts --start 2026-03-26 --end 2026-04-18
 *
 * Flags:
 *   --start YYYY-MM-DD  (required)
 *   --end   YYYY-MM-DD  (default: today UTC)
 *   --workspaces slug1,slug2  (optional filter)
 *   --dry-run                  (skip writes)
 */

import { InstantlyClient } from '../src/instantly';
import { parseInstantlyKeyMap } from '../src/instantly-key-map';
import { SupabaseClient } from '../src/supabase';
import { runWithConcurrency } from '../src/sync';

const WORKSPACE_CONCURRENCY = 2;
const CAMPAIGN_CONCURRENCY = 4;

function flag(name: string): string | undefined {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return undefined;
  return process.argv[idx + 1];
}

async function main() {
  const startDate = flag('--start');
  const endDate = flag('--end') ?? new Date().toISOString().split('T')[0];
  const dryRun = process.argv.includes('--dry-run');
  const workspacesFilter = flag('--workspaces');

  if (!startDate || !/^\d{4}-\d{2}-\d{2}$/.test(startDate)) {
    console.error('[backfill] --start YYYY-MM-DD is required');
    process.exit(1);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
    console.error(`[backfill] invalid --end: ${endDate}`);
    process.exit(1);
  }

  const keysRaw = process.env.INSTANTLY_API_KEYS;
  const supabaseUrl = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const supabaseKey =
    process.env.PIPELINE_SUPABASE_KEY ??
    process.env.PIPELINE_SUPABASE_SERVICE_ROLE_KEY ??
    process.env.SUPABASE_KEY;

  if (!keysRaw) {
    console.error('[backfill] Missing INSTANTLY_API_KEYS');
    process.exit(1);
  }
  if (!supabaseUrl || !supabaseKey) {
    console.error('[backfill] Missing PIPELINE_SUPABASE_URL / PIPELINE_SUPABASE_KEY');
    process.exit(1);
  }

  let keyMap = parseInstantlyKeyMap(keysRaw);
  if (workspacesFilter) {
    const slugs = new Set(workspacesFilter.split(',').map(s => s.trim()).filter(Boolean));
    const before = Object.keys(keyMap).length;
    keyMap = Object.fromEntries(Object.entries(keyMap).filter(([slug]) => slugs.has(slug)));
    console.log(`[backfill] Workspace filter: ${Object.keys(keyMap).length}/${before}`);
  }

  console.log(
    `[backfill] window=${startDate}..${endDate} workspaces=${Object.keys(keyMap).length} dryRun=${dryRun}`,
  );

  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const now = new Date().toISOString();
  const workspaces = Object.entries(keyMap);

  let totalCampaigns = 0;
  let totalRows = 0;

  await runWithConcurrency(workspaces, WORKSPACE_CONCURRENCY, async ([slug, apiKey]) => {
    const client = new InstantlyClient(apiKey);
    let campaigns: Awaited<ReturnType<typeof client.getCampaigns>>;
    try {
      campaigns = await client.getCampaigns();
    } catch (err) {
      console.warn(`[backfill] ${slug}: getCampaigns failed, retrying...`, err);
      campaigns = await client.getCampaigns();
    }
    totalCampaigns += campaigns.length;

    const rows: unknown[] = [];
    await runWithConcurrency(campaigns, CAMPAIGN_CONCURRENCY, async (campaign) => {
      try {
        const daily = await client.getCampaignDailyAnalytics(campaign.id, startDate, endDate);
        for (const d of daily) {
          rows.push({
            campaign_id: campaign.id,
            date: d.date,
            sent: d.sent,
            contacted: d.contacted,
            new_leads_contacted: d.new_leads_contacted,
            opened: d.opened,
            unique_opened: d.unique_opened,
            replies: d.replies,
            unique_replies: d.unique_replies,
            replies_automatic: d.replies_automatic,
            unique_replies_automatic: d.unique_replies_automatic,
            clicks: d.clicks,
            unique_clicks: d.unique_clicks,
            opportunities: d.opportunities,
            unique_opportunities: d.unique_opportunities,
            synced_at: now,
          });
        }
      } catch (err) {
        console.error(`[backfill] ${slug} ${campaign.id} (${campaign.name}):`, err);
      }
    });

    totalRows += rows.length;
    if (rows.length === 0) {
      console.log(`[backfill] ${slug}: 0 rows`);
      return;
    }

    if (dryRun) {
      console.log(`[backfill] ${slug}: ${rows.length} rows (dry-run, skipped write)`);
      return;
    }

    await db.upsert('campaign_daily_metrics', rows, 'campaign_id,date');
    console.log(`[backfill] ${slug}: ${campaigns.length} campaigns -> ${rows.length} daily rows upserted`);
  });

  console.log(`[backfill] Done. ${totalCampaigns} campaigns, ${totalRows} rows.`);
}

main().catch((e) => {
  console.error('[backfill] Fatal:', e instanceof Error ? e.message : e);
  process.exit(1);
});
