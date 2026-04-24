import { InstantlyClient } from './instantly';
import { loadCampaignTagCache } from './campaign-tag-cache';
import { SupabaseClient } from './supabase';
import {
  classifyProduct,
  deriveInfraType,
  extractSegmentFromName,
  extractSignature,
  resolveCampaignManager,
  resolveSpintax,
  stripHtml,
  workspaceDisplayName,
} from './transforms';

export const CAMPAIGN_CONCURRENCY = 5;
export const WORKSPACE_CONCURRENCY = 3;
export const GHOST_CLEANUP_MAX_PER_WORKSPACE = 20;
export const GHOST_STATUS = 'deleted';

export interface WorkspaceCampaignRollup {
  campaign_id: string;
  campaign_name: string;
  workspace_id: string;
  status: string;
  synced_at: string;
}

export interface GhostCleanupPlan {
  missing: WorkspaceCampaignRollup[];
  skipReason: 'empty_fetch' | 'over_cap' | null;
}

function quotePostgrest(value: string): string {
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function buildInFilter(values: string[]): string {
  return `(${values.map(quotePostgrest).join(',')})`;
}

export function buildGhostCleanupPlan(
  fetchedCampaignIds: Set<string>,
  activeRollups: WorkspaceCampaignRollup[],
  maxPerWorkspace: number = GHOST_CLEANUP_MAX_PER_WORKSPACE,
): GhostCleanupPlan {
  if (fetchedCampaignIds.size === 0) {
    return { missing: [], skipReason: 'empty_fetch' };
  }

  const missing = activeRollups.filter(row => !fetchedCampaignIds.has(row.campaign_id));
  if (missing.length > maxPerWorkspace) {
    return { missing, skipReason: 'over_cap' };
  }

  return { missing, skipReason: null };
}

export function buildStoredCampaignTags(cachedTags: string[] | undefined): string[] | null {
  const tags = Array.from(
    new Set(
      (cachedTags ?? [])
        .map(tag => tag.trim())
        .filter(Boolean),
    ),
  ).sort();

  return tags.length > 0 ? tags : null;
}

async function cleanupMissingCampaigns(
  workspaceSlug: string,
  db: SupabaseClient,
  fetchedCampaignIds: Set<string>,
  now: string,
): Promise<void> {
  const workspaceIds = [workspaceSlug, workspaceDisplayName(workspaceSlug)];
  const activeStatuses = ['1', '2', 'Active'];
  const params = [
    'select=campaign_id,campaign_name,workspace_id,status,synced_at',
    `workspace_id=in.${buildInFilter(workspaceIds)}`,
    'step=eq.__ALL__',
    'variant=eq.__ALL__',
    `status=in.${buildInFilter(activeStatuses)}`,
  ].join('&');

  const activeRollups = await db.select('campaign_data', params) as WorkspaceCampaignRollup[];
  const plan = buildGhostCleanupPlan(fetchedCampaignIds, activeRollups);

  if (plan.skipReason === 'empty_fetch') {
    console.warn(`[ghost-cleanup] ${workspaceSlug}: skipped because campaign fetch returned 0 ids`);
    return;
  }

  if (plan.skipReason === 'over_cap') {
    console.error(
      `[ghost-cleanup] ${workspaceSlug}: refusing to mark ${plan.missing.length} campaigns ${GHOST_STATUS} ` +
      `(cap=${GHOST_CLEANUP_MAX_PER_WORKSPACE})`
    );
    return;
  }

  if (plan.missing.length === 0) {
    return;
  }

  const dryRun = process.env.GHOST_ARCHIVE_DRY_RUN === 'true';
  const missingLabels = plan.missing.map(row => `${row.campaign_name} (${row.campaign_id})`);

  if (dryRun) {
    console.warn(
      `[ghost-cleanup] ${workspaceSlug}: dry run - would mark ${plan.missing.length} campaigns ${GHOST_STATUS}: ` +
      missingLabels.join('; ')
    );
    return;
  }

  for (const row of plan.missing) {
    const updateParams = [
      `campaign_id=eq.${row.campaign_id}`,
      `status=in.${buildInFilter(activeStatuses)}`,
    ].join('&');
    await db.update('campaign_data', updateParams, {
      status: GHOST_STATUS,
      synced_at: now,
    });
  }

  console.warn(
    `[ghost-cleanup] ${workspaceSlug}: marked ${plan.missing.length} campaigns ${GHOST_STATUS}: ` +
    missingLabels.join('; ')
  );
}

export type RunType = 'full' | 'inbox' | 'daily_metrics' | 'today_metrics';

/**
 * Trailing-window length for daily_metrics run. Re-reads this many prior days
 * on each run so Instantly's late updates (replies/opps settling) are captured
 * in the table without a separate correction job.
 */
export const DAILY_METRICS_WINDOW_DAYS = 7;

export async function runWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const settled = await Promise.allSettled(batch.map(fn));
    for (const r of settled) {
      if (r.status === 'fulfilled') results.push(r.value);
      else console.error('[concurrency] error:', r.reason);
    }
  }
  return results;
}

export async function syncWorkspace(
  workspaceSlug: string,
  apiKey: string,
  db: SupabaseClient,
  runType: RunType,
): Promise<void> {
  const client = new InstantlyClient(apiKey);
  const today = new Date().toISOString().split('T')[0];
  const now = new Date().toISOString();

  // Daily metrics run: pull /campaigns/analytics/daily per campaign for the
  // trailing window. Does not touch campaign_metrics_daily or campaign_data.
  if (runType === 'daily_metrics') {
    await syncWorkspaceDailyMetrics(workspaceSlug, client, db, now);
    return;
  }

  // Intraday metrics run: active campaigns only, today's date only.
  if (runType === 'today_metrics') {
    await syncWorkspaceTodayMetrics(workspaceSlug, client, db, now);
    return;
  }

  // Inbox-only run (10:30 UTC cron)
  if (runType === 'inbox') {
    const accounts = await client.getAccounts();
    await db.upsert('sender_inboxes', accounts.map(a => ({
      email: a.email,
      workspace_id: workspaceSlug,
      workspace_name: workspaceDisplayName(workspaceSlug),
      status: String(a.status ?? ''),
      provider: a.provider_code !== undefined ? String(a.provider_code) : null,
      warmup_status: a.warmup_status ?? null,
      daily_sent: a.daily_sent_count ?? (a as any).daily_sent ?? null,
      weekly_sent: a.weekly_sent_count ?? (a as any).weekly_sent ?? null,
      monthly_sent: a.monthly_sent_count ?? (a as any).monthly_sent ?? null,
      health_score: a.health_score ?? null,
      synced_at: now,
    })), 'email,workspace_id');
    console.log(`[inbox] ${workspaceSlug}: ${accounts.length} accounts`);
    return;
  }

  // List all campaigns — retry once on timeout
  let campaigns: Awaited<ReturnType<typeof client.getCampaigns>>;
  try {
    campaigns = await client.getCampaigns();
  } catch (err) {
    console.warn(`[sync] ${workspaceSlug}: getCampaigns failed, retrying once...`);
    campaigns = await client.getCampaigns();
  }
  const fetchedCampaignIds = new Set(campaigns.map(campaign => campaign.id));

  let campaignTagsFromCache = new Map<string, string[]>();
  try {
    campaignTagsFromCache = await loadCampaignTagCache(db, workspaceSlug);
    console.log(`[tags] ${workspaceSlug}: ${campaignTagsFromCache.size} cached rows loaded for safekeeping`);
    if (campaignTagsFromCache.size === 0 && campaigns.length > 0) {
      console.warn(
        `[tags] ${workspaceSlug}: cache empty, leaving tag-derived fields null in hourly sync ` +
        `until the dedicated refresh workflow catches up`
      );
    }
  } catch (err) {
    console.warn(
      `[sync] ${workspaceSlug}: campaign_tag_cache unavailable, continuing without stored tags:`,
      err,
    );
    campaignTagsFromCache = new Map<string, string[]>();
  }

  // metricsRows feeds campaign_metrics_daily — the time-series archive,
  // retained as the only source of dated snapshots for trend analysis.
  const metricsRows: unknown[] = [];

  // V3: campaign_data rows (one per variant + __ALL__ rollup)
  const campaignDataRows: unknown[] = [];
  const campaignsWithStoredTags = new Set<string>();

  await runWithConcurrency(campaigns, CAMPAIGN_CONCURRENCY, async (campaign) => {
    try {
      const [detail, analytics, steps] = await Promise.all([
        client.getCampaignDetail(campaign.id),
        client.getCampaignAnalytics(campaign.id),
        client.getStepAnalytics(campaign.id),
      ]);

      const storedTags = buildStoredCampaignTags(campaignTagsFromCache.get(campaign.id));
      if (storedTags) {
        campaignsWithStoredTags.add(campaign.id);
      }
      const cmName = resolveCampaignManager(workspaceSlug, campaign.name, []);
      const leadSource = null;
      const campaignStatus = String(detail.status ?? campaign.status ?? '');

      // V3 derived fields
      const product = classifyProduct(campaign.name, []);
      const excludedFromAnalysis = product === 'ERC' || product === 'S125';
      const exclusionReason = excludedFromAnalysis ? `product=${product}` : null;
      const infraType = deriveInfraType(workspaceSlug);
      // Segment: derived from campaign name keywords only (never from tags)
      const segment = extractSegmentFromName(campaign.name);
      const rg_batch_tags: string[] = [];
      const pair_tag: string | null = null;
      const sender_tags: string[] = [];
      const other_tags: string[] = [];

      // Build a metrics lookup keyed by step+variant for this campaign
      const metricsLookup = new Map<string, { sent: number; replied: number; opportunities: number }>();
      for (const s of steps) {
        if (s.step == null || s.variant == null) continue;
        const stepNum = parseInt(String(s.step), 10) + 1; // 0-indexed → 1-indexed
        const variantLetter = String.fromCharCode(65 + parseInt(String(s.variant), 10));
        const key = `${stepNum}|${variantLetter}`;
        metricsLookup.set(key, {
          sent: s.sent ?? 0,
          replied: s.replies ?? 0,
          opportunities: s.opportunities ?? 0,
        });
        metricsRows.push({
          campaign_id: campaign.id,
          step: stepNum,
          variant: variantLetter,
          date: today,
          sent: s.sent ?? 0,
          replied: s.replies ?? 0,
          unique_replies: s.unique_replies ?? 0,
          replies_automatic: s.replies_automatic ?? 0,
          unique_replies_automatic: s.unique_replies_automatic ?? 0,
          opportunities: s.opportunities ?? 0,
          unique_opportunities: s.unique_opportunities ?? 0,
          synced_at: now,
        });
      }

      // Variant copy from sequences
      const sequence = detail.sequences?.[0];
      if (sequence) {
        sequence.steps.forEach((step, stepIndex) => {
          const stepNum = stepIndex + 1; // 1-indexed
          step.variants.forEach((variant, variantIndex) => {
            const variantLetter = String.fromCharCode(65 + variantIndex); // 0→A, 1→B

            // Raw copy: HTML stripped, spintax preserved
            const rawBody = stripHtml(variant.body ?? '');
            const rawSubject = stripHtml(variant.subject ?? '');

            // Extract signature from raw (pre-spintax) body
            const { body: bodyNoSig, signature } = extractSignature(rawBody);

            // Resolved previews: spintax resolved on body WITHOUT signature
            const bodyPreview = resolveSpintax(bodyNoSig);
            const subjectPreview = resolveSpintax(rawSubject);

            // V3: build campaign_data row for this variant
            const metricsKey = `${stepNum}|${variantLetter}`;
            const m = metricsLookup.get(metricsKey) ?? { sent: 0, replied: 0, opportunities: 0 };
            const { sent, replied, opportunities } = m;

            campaignDataRows.push({
              campaign_id: campaign.id,
              campaign_name: campaign.name,
              workspace_id: workspaceSlug,
              workspace_name: workspaceDisplayName(workspaceSlug),
              cm_name: cmName,
              segment,
              product,
              infra_type: infraType,
              status: campaignStatus,
              date_launched: detail.timestamp_created ?? null,
              daily_limit: (detail.daily_limit as number) ?? null,
              lead_source: leadSource,
              tags: storedTags,
              rg_batch_tags: rg_batch_tags.length > 0 ? rg_batch_tags : null,
              pair_tag,
              sender_tags: sender_tags.length > 0 ? sender_tags : null,
              other_tags: other_tags.length > 0 ? other_tags : null,
              excluded_from_analysis: excludedFromAnalysis,
              exclusion_reason: exclusionReason,
              step: String(stepNum),
              variant: variantLetter,
              subject: rawSubject || null,
              body: bodyNoSig || null,
              subject_preview: subjectPreview || null,
              body_preview: bodyPreview || null,
              signature: signature || null,
              v_disabled: variant.v_disabled ?? false,
              emails_sent: sent,
              replies: replied,
              opportunities,
              analytics_sequence_started: null, // campaign-level only
              total_leads: null,
              leads_completed: null,
              leads_bounced: null,
              leads_unsubscribed: null,
              e_op: opportunities > 0 ? Math.round((sent / opportunities) * 100) / 100 : null,
              reply_rate: sent > 0 ? Math.round((replied / sent) * 1000000) / 1000000 : null,
              synced_at: now,
            });
          });
        });
      }

      // V3: campaign-level __ALL__ row — aggregate across all variants
      const totalSent = steps.reduce((sum, s) => sum + (s.sent ?? 0), 0);
      const totalReplied = steps.reduce((sum, s) => sum + (s.replies ?? 0), 0);
      const totalOpps = steps.reduce((sum, s) => sum + (s.opportunities ?? 0), 0);

      campaignDataRows.push({
        campaign_id: campaign.id,
        campaign_name: campaign.name,
        workspace_id: workspaceSlug,
        workspace_name: workspaceDisplayName(workspaceSlug),
        cm_name: cmName,
        segment,
        product,
        infra_type: infraType,
        status: campaignStatus,
        date_launched: detail.timestamp_created ?? null,
        daily_limit: (detail.daily_limit as number) ?? null,
        lead_source: leadSource,
        tags: storedTags,
        rg_batch_tags: rg_batch_tags.length > 0 ? rg_batch_tags : null,
        pair_tag,
        sender_tags: sender_tags.length > 0 ? sender_tags : null,
        other_tags: other_tags.length > 0 ? other_tags : null,
        excluded_from_analysis: excludedFromAnalysis,
        exclusion_reason: exclusionReason,
        step: '__ALL__',
        variant: '__ALL__',
        subject: null,
        body: null,
        subject_preview: null,
        body_preview: null,
        signature: null,
        v_disabled: false,
        emails_sent: totalSent,
        replies: totalReplied,
        opportunities: totalOpps,
        analytics_sequence_started: analytics.contacted_count ?? null,
        total_leads: analytics.leads_count ?? null,
        leads_completed: analytics.completed_count ?? null,
        leads_bounced: analytics.bounced_count ?? null,
        leads_unsubscribed: analytics.unsubscribed_count ?? null,
        e_op: totalOpps > 0 ? Math.round((totalSent / totalOpps) * 100) / 100 : null,
        reply_rate: totalSent > 0 ? Math.round((totalReplied / totalSent) * 1000000) / 1000000 : null,
        synced_at: now,
      });
    } catch (err) {
      console.error(`[sync] Error on campaign ${campaign.id} (${campaign.name}):`, err);
    }
  });

  // Write to time-series archive (campaign_metrics_daily retained as the
  // historical snapshot source for trend queries; campaigns + variant_copy
  // were dropped in V3 cleanup 2026-04-10).
  if (metricsRows.length > 0) {
    await db.upsert('campaign_metrics_daily', metricsRows, 'campaign_id,step,variant,date');
  }

  // Write to V3 campaign_data table (primary read target)
  if (campaignDataRows.length > 0) {
    try {
      await db.upsert('campaign_data', campaignDataRows, 'campaign_id,step,variant');
      console.log(`[v3] ${workspaceSlug}: ${campaignDataRows.length} campaign_data rows upserted`);
    } catch (err) {
      console.error(`[v3] ${workspaceSlug}: campaign_data write failed:`, err);
    }
  }

  try {
    await cleanupMissingCampaigns(workspaceSlug, db, fetchedCampaignIds, now);
  } catch (err) {
    console.error(`[ghost-cleanup] ${workspaceSlug}: failed:`, err);
  }

  console.log(
    `[sync] ${workspaceSlug}: ${campaigns.length} campaigns, ` +
    `${campaignsWithStoredTags.size} campaigns with stored tags, ` +
    `${campaignDataRows.length} campaign_data rows, ${metricsRows.length} metric rows`
  );
}

/**
 * Per-campaign /daily fetch with trailing window. Writes to
 * campaign_daily_metrics. Does not touch campaign_metrics_daily or
 * campaign_data.
 */
export async function syncWorkspaceDailyMetrics(
  workspaceSlug: string,
  client: InstantlyClient,
  db: SupabaseClient,
  now: string,
  windowDays: number = DAILY_METRICS_WINDOW_DAYS,
): Promise<void> {
  const endDate = now.split('T')[0];
  const start = new Date(endDate);
  start.setUTCDate(start.getUTCDate() - windowDays);
  const startDate = start.toISOString().split('T')[0];

  let campaigns: Awaited<ReturnType<typeof client.getCampaigns>>;
  try {
    campaigns = await client.getCampaigns();
  } catch (err) {
    console.warn(`[daily] ${workspaceSlug}: getCampaigns failed, retrying once...`);
    campaigns = await client.getCampaigns();
  }

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
      console.error(`[daily] ${workspaceSlug}: campaign ${campaign.id} (${campaign.name}):`, err);
    }
  });

  if (rows.length > 0) {
    await db.upsert('campaign_daily_metrics', rows, 'campaign_id,date');
  }
  console.log(
    `[daily] ${workspaceSlug}: ${campaigns.length} campaigns, ` +
    `${rows.length} daily rows upserted (window ${startDate}..${endDate})`
  );
}

/**
 * Intraday today-only fetch: active campaigns only, today's date.
 * Designed for hourly runs to keep campaign_daily_metrics current during
 * the sending day without the cost of a full 7-day trailing window.
 */
export async function syncWorkspaceTodayMetrics(
  workspaceSlug: string,
  client: InstantlyClient,
  db: SupabaseClient,
  now: string,
): Promise<void> {
  const today = now.split('T')[0];

  let allCampaigns: Awaited<ReturnType<typeof client.getCampaigns>>;
  try {
    allCampaigns = await client.getCampaigns();
  } catch (err) {
    console.warn(`[today] ${workspaceSlug}: getCampaigns failed, retrying once...`);
    allCampaigns = await client.getCampaigns();
  }
  // Status 1 = active in Instantly v2. Filter before making per-campaign calls.
  const campaigns = allCampaigns.filter(c => String(c.status) === '1');

  const rows: unknown[] = [];
  await runWithConcurrency(campaigns, CAMPAIGN_CONCURRENCY, async (campaign) => {
    try {
      const daily = await client.getCampaignDailyAnalytics(campaign.id, today, today);
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
      console.error(`[today] ${workspaceSlug}: campaign ${campaign.id} (${campaign.name}):`, err);
    }
  });

  if (rows.length > 0) {
    await db.upsert('campaign_daily_metrics', rows, 'campaign_id,date');
  }
  console.log(
    `[today] ${workspaceSlug}: ${campaigns.length} active campaigns, ` +
    `${rows.length} rows upserted for ${today}`
  );
}

/**
 * Syncs all workspaces in keyMap. Called by runner.ts (GitHub Actions) and
 * can be imported by index.ts for /trigger endpoint if needed.
 */
export async function syncAllWorkspaces(
  keyMap: Record<string, string>,
  supabaseUrl: string,
  supabaseKey: string,
  runType: RunType,
): Promise<void> {
  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const workspaces = Object.entries(keyMap);
  console.log(
    `[syncAllWorkspaces] runType=${runType} workspaces=${workspaces.length}`
  );

  await runWithConcurrency(workspaces, WORKSPACE_CONCURRENCY, async ([slug, apiKey]) => {
    try {
      await syncWorkspace(slug, apiKey, db, runType);
    } catch (err) {
      console.error(`[syncAllWorkspaces] Error on workspace ${slug}:`, err);
    }
  });

  // Refresh step rollups once after all workspaces are written — full runs only.
  // Campaign-level __ALL__ rows are written directly in Node.
  if (runType === 'full') {
    try {
      await db.rpc('refresh_campaign_rollups', {});
      console.log('[syncAllWorkspaces] Step rollups refreshed.');
    } catch (err) {
      console.error('[syncAllWorkspaces] Step rollup refresh failed:', err);
    }
  }

  console.log('[syncAllWorkspaces] Done.');
}
