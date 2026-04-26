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

/**
 * Cold-start cap for new-campaign daily-metrics backfill. When a campaign
 * appears in the workspace sweep with zero existing rows in
 * campaign_daily_metrics, fetch up to this many days of history instead of
 * the trailing-window default. Caps absolute API cost on rare runaway cases.
 */
export const DAILY_METRICS_COLD_START_DAYS = 90;

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

/**
 * Minimal row shape used by the campaign_data winner-pick coalesce step.
 * Both newly-built sync rows and rows fetched from campaign_data are reduced
 * to this shape before ranking, so the pure planner does not depend on the
 * full V3 column set.
 */
export interface CampaignDataDedupeRow {
  campaign_id: string;
  campaign_name: string;
  workspace_id: string;
  step: string;
  variant: string;
  emails_sent: number | null;
  synced_at: string | null;
}

export interface CampaignDataWinnerPickPlan {
  /** campaign_ids that already exist in campaign_data and should be deleted
   *  (plus FK-reassigned) because a different campaign_id with the same
   *  (workspace_id, campaign_name, step, variant) ranks higher under the
   *  winner-pick rule.
   */
  loserCampaignIds: string[];
  /** Diagnostic detail for each (workspace_id, campaign_name) collision the
   *  planner resolved. One entry per surviving winner; loserCampaignIds for
   *  that group is non-empty.
   */
  resolutions: Array<{
    workspace_id: string;
    campaign_name: string;
    step: string;
    variant: string;
    winner_id: string;
    losers: string[];
  }>;
}

/**
 * Winner-pick rule (matches the SQL dedupe migrations 2026-04-26):
 *   1. Highest emails_sent (NULLS treated as 0)
 *   2. Tiebreak: most recent synced_at (DESC, NULLS LAST)
 *
 * The rule is applied per (workspace_id, campaign_name, step, variant)
 * group. Multi-workspace duplicates (same campaign_name across different
 * workspaces) are NEVER considered colliding — they are legitimately distinct
 * campaigns and stay separate. Because the sync runs per-workspace, the
 * incoming `newRows` always belong to a single workspace, but we still scope
 * the rule by workspace_id defensively in case the helper is reused.
 */
function rankRow(a: CampaignDataDedupeRow, b: CampaignDataDedupeRow): number {
  const aSent = a.emails_sent ?? 0;
  const bSent = b.emails_sent ?? 0;
  if (aSent !== bSent) return bSent - aSent; // DESC
  const aSync = a.synced_at ?? '';
  const bSync = b.synced_at ?? '';
  if (aSync === bSync) return 0;
  // Most recent first; missing synced_at sorts last.
  if (!aSync) return 1;
  if (!bSync) return -1;
  return aSync < bSync ? 1 : -1;
}

/**
 * Pure planner for the campaign_data winner-pick coalesce. Given the rows
 * about to be upserted by this sync run and the rows already present in
 * campaign_data for the same (workspace_id, campaign_name, step, variant)
 * keys, return the campaign_ids whose existing rows should be deleted (and
 * their FK references reassigned to the winner).
 *
 * Multi-workspace name collisions are excluded — the function only considers
 * groups where every observed row shares a single workspace_id.
 */
export function buildCampaignDataWinnerPickPlan(
  newRows: CampaignDataDedupeRow[],
  existingRows: CampaignDataDedupeRow[],
): CampaignDataWinnerPickPlan {
  // Group rows by (workspace_id, campaign_name, step, variant) using a JSON
  // tuple key so arbitrary characters in campaign_name (commas, spaces, etc.)
  // can never collide with a delimiter.
  const groupKey = (r: CampaignDataDedupeRow): string =>
    JSON.stringify([r.workspace_id, r.campaign_name, r.step, r.variant]);
  const xKeyOf = (r: CampaignDataDedupeRow): string =>
    JSON.stringify([r.campaign_name, r.step, r.variant]);

  const candidates = new Map<string, CampaignDataDedupeRow[]>();
  const observedWorkspaces = new Map<string, Set<string>>();
  const groupTuple = new Map<string, { workspace_id: string; campaign_name: string; step: string; variant: string }>();

  const note = (rows: CampaignDataDedupeRow[]): void => {
    for (const r of rows) {
      const xKey = xKeyOf(r);
      let ws = observedWorkspaces.get(xKey);
      if (!ws) {
        ws = new Set<string>();
        observedWorkspaces.set(xKey, ws);
      }
      ws.add(r.workspace_id);

      const k = groupKey(r);
      let arr = candidates.get(k);
      if (!arr) {
        arr = [];
        candidates.set(k, arr);
      }
      arr.push(r);
      if (!groupTuple.has(k)) {
        groupTuple.set(k, {
          workspace_id: r.workspace_id,
          campaign_name: r.campaign_name,
          step: r.step,
          variant: r.variant,
        });
      }
    }
  };

  note(newRows);
  note(existingRows);

  const newCampaignIdsByKey = new Map<string, Set<string>>();
  for (const r of newRows) {
    const k = groupKey(r);
    let s = newCampaignIdsByKey.get(k);
    if (!s) {
      s = new Set<string>();
      newCampaignIdsByKey.set(k, s);
    }
    s.add(r.campaign_id);
  }

  const resolutions: CampaignDataWinnerPickPlan['resolutions'] = [];
  const loserSet = new Set<string>();

  for (const [key, rows] of candidates) {
    if (rows.length < 2) continue;
    const tuple = groupTuple.get(key);
    if (!tuple) continue;
    const xKey = JSON.stringify([tuple.campaign_name, tuple.step, tuple.variant]);
    const ws = observedWorkspaces.get(xKey);
    if (ws && ws.size > 1) continue; // multi-workspace — never coalesce

    // Deduplicate by campaign_id (a row may appear in both newRows and
    // existingRows if the sync re-touches the same campaign). Prefer the
    // newRows copy because its emails_sent/synced_at are fresher.
    const byId = new Map<string, CampaignDataDedupeRow>();
    for (const r of rows) byId.set(r.campaign_id, r);
    const distinct = Array.from(byId.values());
    if (distinct.length < 2) continue;

    distinct.sort(rankRow);
    const winner = distinct[0];
    const losers = distinct.slice(1).map(r => r.campaign_id);

    // Only flag losers if at least one new-row campaign_id is in this group.
    // Otherwise the collision is among pre-existing rows untouched by this
    // sync — leaving cleanup to the migration / a later sync that does touch
    // them. This keeps the per-sync delete blast radius bounded.
    const newIdsHere = newCampaignIdsByKey.get(key);
    if (!newIdsHere || newIdsHere.size === 0) continue;

    for (const id of losers) loserSet.add(id);
    resolutions.push({
      workspace_id: tuple.workspace_id,
      campaign_name: tuple.campaign_name,
      step: tuple.step,
      variant: tuple.variant,
      winner_id: winner.campaign_id,
      losers,
    });
  }

  return {
    loserCampaignIds: Array.from(loserSet),
    resolutions,
  };
}

/**
 * Async wrapper: read existing campaign_data rows in the same workspace that
 * could collide on (workspace_id, campaign_name, step='__ALL__',
 * variant='__ALL__') with rows we're about to upsert; pick winners; reassign
 * meetings_booked_raw / meetings_unmatched_queue FKs from loser → winner;
 * delete loser rows from campaign_data.
 *
 * Scoped to the rollup row (step='__ALL__', variant='__ALL__') because the
 * dedupe-detection key is the rollup. Per-step/variant rows for a loser
 * campaign_id share that same campaign_id as PK, so deleting all of them
 * here would over-delete legitimate sibling rows. Sibling rows for a loser
 * campaign_id will simply stop being refreshed by future syncs (campaign no
 * longer surfaced) and naturally age out via existing ghost-cleanup logic.
 */
export async function coalesceCampaignDataDupes(
  workspaceSlug: string,
  db: SupabaseClient,
  newRows: CampaignDataDedupeRow[],
): Promise<CampaignDataWinnerPickPlan> {
  // Only the __ALL__ rollup is the dedupe-detection key. The per-step/variant
  // rows belong to the same campaign_id, so the winner-pick decision at the
  // rollup row dictates them.
  const newRollupRows = newRows.filter(
    r => r.step === '__ALL__' && r.variant === '__ALL__',
  );
  if (newRollupRows.length === 0) {
    return { loserCampaignIds: [], resolutions: [] };
  }

  const candidateNames = Array.from(
    new Set(newRollupRows.map(r => r.campaign_name)),
  );
  if (candidateNames.length === 0) {
    return { loserCampaignIds: [], resolutions: [] };
  }

  // Fetch existing __ALL__/__ALL__ rows that share campaign_name with this
  // sync's new rows AND live in this workspace. Workspace_id can show up as
  // either the slug or the display name in legacy rows; check both.
  const workspaceIds = [workspaceSlug, workspaceDisplayName(workspaceSlug)];
  const existing: CampaignDataDedupeRow[] = [];

  // Chunk by campaign_name to keep PostgREST URL length bounded.
  const chunkSize = 50;
  for (let i = 0; i < candidateNames.length; i += chunkSize) {
    const chunk = candidateNames.slice(i, i + chunkSize);
    const params = [
      'select=campaign_id,campaign_name,workspace_id,step,variant,emails_sent,synced_at',
      `workspace_id=in.${buildInFilter(workspaceIds)}`,
      `campaign_name=in.${buildInFilter(chunk)}`,
      'step=eq.__ALL__',
      'variant=eq.__ALL__',
    ].join('&');
    const rows = (await db.selectAll('campaign_data', params)) as CampaignDataDedupeRow[];
    existing.push(...rows);
  }

  // Normalize workspace_id on existing rows to the slug we're syncing under,
  // so the planner's per-workspace partition matches new + existing.
  const normalized = existing.map(r => ({ ...r, workspace_id: workspaceSlug }));

  const plan = buildCampaignDataWinnerPickPlan(newRollupRows, normalized);
  if (plan.loserCampaignIds.length === 0) {
    return plan;
  }

  // Reassign meetings_booked_raw FKs from each loser to its winner.
  // We can only PATCH per-winner because each loser has a different winner,
  // and PostgREST doesn't expose CASE WHEN inside a PATCH body.
  for (const r of plan.resolutions) {
    if (r.losers.length === 0) continue;
    const losersFilter = `campaign_id=in.${buildInFilter(r.losers)}`;
    try {
      await db.update('meetings_booked_raw', losersFilter, {
        campaign_id: r.winner_id,
      });
    } catch (err) {
      // FK reassignment must succeed before delete to preserve attribution
      // invariants. If it fails, abort this resolution and leave the loser
      // rows untouched. Next sync will retry.
      console.error(
        `[v3-coalesce] ${workspaceSlug}: meetings_booked_raw FK reassign failed for "${r.campaign_name}" (${r.losers.join(',')} → ${r.winner_id}):`,
        err,
      );
      // Drop these losers from the deletion set.
      for (const id of r.losers) {
        const idx = plan.loserCampaignIds.indexOf(id);
        if (idx >= 0) plan.loserCampaignIds.splice(idx, 1);
      }
      continue;
    }

    try {
      await db.update(
        'meetings_unmatched_queue',
        `resolved_campaign_id=in.${buildInFilter(r.losers)}`,
        { resolved_campaign_id: r.winner_id },
      );
    } catch (err) {
      // Defensive — usually 0 rows. Log and continue.
      console.warn(
        `[v3-coalesce] ${workspaceSlug}: meetings_unmatched_queue reassign warning for "${r.campaign_name}":`,
        err,
      );
    }
  }

  if (plan.loserCampaignIds.length === 0) {
    return plan;
  }

  // Delete loser __ALL__/__ALL__ rows. Per-step/variant siblings of the
  // loser campaign_id are intentionally left in place (see docblock).
  try {
    const params = [
      `campaign_id=in.${buildInFilter(plan.loserCampaignIds)}`,
      'step=eq.__ALL__',
      'variant=eq.__ALL__',
    ].join('&');
    await db.delete('campaign_data', params);
    console.log(
      `[v3-coalesce] ${workspaceSlug}: deleted ${plan.loserCampaignIds.length} loser rollup rows ` +
        `across ${plan.resolutions.length} (workspace,campaign_name) groups`,
    );
  } catch (err) {
    console.error(
      `[v3-coalesce] ${workspaceSlug}: campaign_data delete failed:`,
      err,
    );
  }

  return plan;
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

/**
 * For a set of campaign_ids, return the subset whose campaign_data rows are
 * ALL marked status='deleted'. Mirrors the writer-side gate that prevents
 * phantom-campaign rows in campaign_daily_metrics: Instantly's daily-analytics
 * endpoint keeps returning data for ids long after the campaign is deleted,
 * so we filter by canonical status from campaign_data. A campaign with NO
 * rows in campaign_data (new/uncached) is NOT considered deleted — letting
 * brand-new campaigns through is required for the cold-start backfill.
 */
async function fetchFullyDeletedCampaignIds(
  db: SupabaseClient,
  campaignIds: string[],
): Promise<Set<string>> {
  if (campaignIds.length === 0) return new Set();
  // Chunk to keep PostgREST `in.()` filters under the URL length cap.
  const chunkSize = 200;
  const perCampaignStatuses = new Map<string, string[]>();
  for (let i = 0; i < campaignIds.length; i += chunkSize) {
    const chunk = campaignIds.slice(i, i + chunkSize);
    const params = [
      'select=campaign_id,status',
      `campaign_id=in.${buildInFilter(chunk)}`,
    ].join('&');
    const rows = (await db.selectAll('campaign_data', params)) as {
      campaign_id: string;
      status: string;
    }[];
    for (const r of rows) {
      const list = perCampaignStatuses.get(r.campaign_id) ?? [];
      list.push(String(r.status ?? ''));
      perCampaignStatuses.set(r.campaign_id, list);
    }
  }
  const deleted = new Set<string>();
  for (const [campaignId, statuses] of perCampaignStatuses) {
    if (statuses.length > 0 && statuses.every(s => s === GHOST_STATUS)) {
      deleted.add(campaignId);
    }
  }
  return deleted;
}

/**
 * Returns the subset of campaign_ids that already have at least one row in
 * campaign_daily_metrics. Used by the daily-metrics sync to detect new
 * campaigns and trigger a cold-start full-history backfill instead of the
 * trailing-window pull (which would silently skip the campaign's first sends
 * if those days had already aged out of the window before sync caught up).
 */
async function fetchCampaignIdsWithDailyHistory(
  db: SupabaseClient,
  campaignIds: string[],
): Promise<Set<string>> {
  if (campaignIds.length === 0) return new Set();
  const chunkSize = 200;
  const known = new Set<string>();
  for (let i = 0; i < campaignIds.length; i += chunkSize) {
    const chunk = campaignIds.slice(i, i + chunkSize);
    const params = [
      'select=campaign_id',
      `campaign_id=in.${buildInFilter(chunk)}`,
    ].join('&');
    const rows = (await db.selectAll('campaign_daily_metrics', params)) as {
      campaign_id: string;
    }[];
    for (const r of rows) known.add(r.campaign_id);
  }
  return known;
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

  // Write to V3 campaign_data table (primary read target).
  //
  // Pre-upsert dedupe coalesce: campaign_data PK is (campaign_id, step,
  // variant), but the dedupe-detection key downstream consumers (matcher,
  // analytics) rely on is (workspace_id, campaign_name, step, variant).
  // When Instantly returns multiple campaign_id UUIDs sharing the same
  // campaign_name in the same workspace (recreate-after-delete pattern),
  // a plain upsert produces structural duplicates that re-emerge within
  // minutes of any Supabase-layer dedupe (see today's audit-trail SQL:
  // sql/2026-04-26-campaign-data-dedupe-EXECUTED.sql and -c-EXECUTED.sql).
  //
  // Apply the same winner-pick rule used in the dedupe migrations
  // (highest emails_sent, tiebreak most-recent synced_at) at write-time-zero,
  // scoped to this workspace.
  if (campaignDataRows.length > 0) {
    try {
      const dedupeInputs = (campaignDataRows as Array<Record<string, unknown>>).map(r => ({
        campaign_id: String(r.campaign_id),
        campaign_name: String(r.campaign_name),
        workspace_id: String(r.workspace_id),
        step: String(r.step),
        variant: String(r.variant),
        emails_sent: typeof r.emails_sent === 'number' ? r.emails_sent : null,
        synced_at: typeof r.synced_at === 'string' ? r.synced_at : null,
      }));
      await coalesceCampaignDataDupes(workspaceSlug, db, dedupeInputs);
    } catch (err) {
      // Coalesce is best-effort: if it fails, fall through to the upsert so
      // we don't block the primary write path. Next sync retries.
      console.error(`[v3-coalesce] ${workspaceSlug}: failed (continuing to upsert):`, err);
    }

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
  coldStartDays: number = DAILY_METRICS_COLD_START_DAYS,
): Promise<void> {
  const endDate = now.split('T')[0];
  const trailingStart = new Date(endDate);
  trailingStart.setUTCDate(trailingStart.getUTCDate() - windowDays);
  const trailingStartDate = trailingStart.toISOString().split('T')[0];

  const coldStart = new Date(endDate);
  coldStart.setUTCDate(coldStart.getUTCDate() - coldStartDays);
  const coldStartDate = coldStart.toISOString().split('T')[0];

  let campaigns: Awaited<ReturnType<typeof client.getCampaigns>>;
  try {
    campaigns = await client.getCampaigns();
  } catch (err) {
    console.warn(`[daily] ${workspaceSlug}: getCampaigns failed, retrying once...`);
    campaigns = await client.getCampaigns();
  }

  const campaignIds = campaigns.map(c => c.id);
  const [deletedIds, knownIds] = await Promise.all([
    fetchFullyDeletedCampaignIds(db, campaignIds),
    fetchCampaignIdsWithDailyHistory(db, campaignIds),
  ]);

  const rows: unknown[] = [];
  let skippedDeleted = 0;
  let coldStartCount = 0;
  await runWithConcurrency(campaigns, CAMPAIGN_CONCURRENCY, async (campaign) => {
    if (deletedIds.has(campaign.id)) {
      skippedDeleted += 1;
      return;
    }
    const isColdStart = !knownIds.has(campaign.id);
    const startDate = isColdStart ? coldStartDate : trailingStartDate;
    try {
      const daily = await client.getCampaignDailyAnalytics(campaign.id, startDate, endDate);
      if (isColdStart) {
        coldStartCount += 1;
        console.log(
          `[daily] ${workspaceSlug}: cold-start backfill for ${campaign.id} ` +
          `(${campaign.name}) window ${startDate}..${endDate} -> ${daily.length} rows`
        );
      }
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
    `[daily] ${workspaceSlug}: ${campaigns.length} campaigns ` +
    `(${skippedDeleted} skipped as deleted, ${coldStartCount} cold-start), ` +
    `${rows.length} daily rows upserted (trailing ${trailingStartDate}..${endDate})`
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

  // Defense-in-depth: drop any campaign whose campaign_data rollup is fully
  // status='deleted'. Instantly occasionally keeps returning daily rows for
  // ids long after the campaign is deleted in the workspace; the canonical
  // status lives in campaign_data.
  const deletedIds = await fetchFullyDeletedCampaignIds(db, campaigns.map(c => c.id));

  const rows: unknown[] = [];
  let skippedDeleted = 0;
  await runWithConcurrency(campaigns, CAMPAIGN_CONCURRENCY, async (campaign) => {
    if (deletedIds.has(campaign.id)) {
      skippedDeleted += 1;
      return;
    }
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
    `[today] ${workspaceSlug}: ${campaigns.length} active campaigns ` +
    `(${skippedDeleted} skipped as deleted), ${rows.length} rows upserted for ${today}`
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
