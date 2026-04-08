import { InstantlyClient } from './instantly';
import { SupabaseClient } from './supabase';
import {
  classifyLeadSource,
  classifyProduct,
  classifySegmentFromName,
  deriveInfraType,
  extractSignature,
  LEAD_SOURCE_TAGS,
  normalizeSegment,
  parseCmName,
  parseRgBatchIds,
  resolveBody,
  resolveSpintax,
  resolveSubject,
  stripHtml,
  WORKSPACE_CM_DEFAULTS,
} from './transforms';

export const CAMPAIGN_CONCURRENCY = 5;
export const WORKSPACE_CONCURRENCY = 3;

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
  isInboxRun: boolean,
): Promise<void> {
  const client = new InstantlyClient(apiKey);
  const today = new Date().toISOString().split('T')[0];
  const now = new Date().toISOString();

  // Inbox-only run (10:30 UTC cron)
  if (isInboxRun) {
    const accounts = await client.getAccounts();
    await db.upsert('sender_inboxes', accounts.map(a => ({
      email: a.email,
      workspace_id: workspaceSlug,
      workspace_name: workspaceSlug,
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

  // Build tag map (UUID → label) for resolving per-campaign tags
  const tagMap = await client.getTagMap();

  // List all campaigns
  const campaigns = await client.getCampaigns();

  const campaignRows: unknown[] = [];
  const variantCopyRows: unknown[] = [];
  const metricsRows: unknown[] = [];

  // V3: campaign_data rows (one per variant)
  const campaignDataRows: unknown[] = [];

  await runWithConcurrency(campaigns, CAMPAIGN_CONCURRENCY, async (campaign) => {
    try {
      const [detail, analytics, steps] = await Promise.all([
        client.getCampaignDetail(campaign.id),
        client.getCampaignAnalytics(campaign.id),
        client.getStepAnalytics(campaign.id),
      ]);

      const cmName =
        parseCmName(campaign.name) ?? WORKSPACE_CM_DEFAULTS[workspaceSlug] ?? null;
      const resolvedTags = (detail.email_tag_list ?? [])
        .map(id => tagMap.get(id))
        .filter(Boolean) as string[];
      const rgBatchIds = parseRgBatchIds(campaign.name);
      const leadSource = classifyLeadSource(resolvedTags);
      const campaignStatus = String(detail.status ?? campaign.status ?? '');

      // V3 derived fields
      const product = classifyProduct(campaign.name, resolvedTags);
      const excludedFromAnalysis = product === 'ERC' || product === 'S125';
      const exclusionReason = excludedFromAnalysis ? `product=${product}` : null;
      const infraType = deriveInfraType(workspaceSlug);
      // Segment: try custom tags first, fall back to campaign name parsing
      const segmentTag = resolvedTags.find(t => !LEAD_SOURCE_TAGS.has(t)) ?? null;
      const rawSegment = segmentTag ?? classifySegmentFromName(campaign.name);
      const segment = normalizeSegment(rawSegment);

      campaignRows.push({
        campaign_id: campaign.id,
        workspace_id: workspaceSlug,
        workspace_name: workspaceSlug,
        name: campaign.name,
        status: campaignStatus,
        cm_name: cmName,
        tags: resolvedTags.length > 0 ? resolvedTags : null,
        lead_source: leadSource,
        rg_batch_ids: rgBatchIds.length > 0 ? rgBatchIds : null,
        leads_count: analytics.leads_count,
        contacted_count: analytics.contacted_count,
        completed_count: analytics.completed_count,
        bounced_count: analytics.bounced_count,
        unsubscribed_count: analytics.unsubscribed_count,
        instantly_created_at: detail.timestamp_created ?? null,
        timestamp_updated: detail.timestamp_updated ?? null,
        daily_limit: (detail.daily_limit as number) ?? null,
        synced_at: now,
      });

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

            variantCopyRows.push({
              campaign_id: campaign.id,
              step: stepNum,
              variant: variantLetter,
              subject: rawSubject,
              body: rawBody,
              subject_resolved: subjectPreview,
              body_resolved: bodyPreview,
              v_disabled: variant.v_disabled ?? false,
              synced_at: now,
            });

            // V3: build campaign_data row for this variant
            const metricsKey = `${stepNum}|${variantLetter}`;
            const m = metricsLookup.get(metricsKey) ?? { sent: 0, replied: 0, opportunities: 0 };
            const { sent, replied, opportunities } = m;

            campaignDataRows.push({
              campaign_id: campaign.id,
              campaign_name: campaign.name,
              workspace_id: workspaceSlug,
              workspace_name: workspaceSlug,
              cm_name: cmName,
              segment,
              product,
              infra_type: infraType,
              status: campaignStatus,
              date_launched: detail.timestamp_created ?? null,
              daily_limit: (detail.daily_limit as number) ?? null,
              lead_source: leadSource,
              tags: resolvedTags.length > 0 ? resolvedTags : null,
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
              leads_contacted: null, // campaign-level only
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
        workspace_name: workspaceSlug,
        cm_name: cmName,
        segment,
        product,
        infra_type: infraType,
        status: campaignStatus,
        date_launched: detail.timestamp_created ?? null,
        daily_limit: (detail.daily_limit as number) ?? null,
        lead_source: leadSource,
        tags: resolvedTags.length > 0 ? resolvedTags : null,
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
        leads_contacted: analytics.contacted_count ?? null,
        e_op: totalOpps > 0 ? Math.round((totalSent / totalOpps) * 100) / 100 : null,
        reply_rate: totalSent > 0 ? Math.round((totalReplied / totalSent) * 1000000) / 1000000 : null,
        synced_at: now,
      });
    } catch (err) {
      console.error(`[sync] Error on campaign ${campaign.id} (${campaign.name}):`, err);
    }
  });

  // Write to old tables (dual-write during transition period)
  await Promise.all([
    db.upsert('campaigns', campaignRows, 'campaign_id'),
    db.upsert('variant_copy', variantCopyRows, 'campaign_id,step,variant'),
    db.upsert('campaign_metrics_daily', metricsRows, 'campaign_id,step,variant,date'),
  ]);

  // Write to V3 campaign_data table
  if (campaignDataRows.length > 0) {
    try {
      await db.upsert('campaign_data', campaignDataRows, 'campaign_id,step,variant');
      console.log(`[v3] ${workspaceSlug}: ${campaignDataRows.length} campaign_data rows upserted`);
    } catch (err) {
      console.error(`[v3] ${workspaceSlug}: campaign_data write failed:`, err);
      // Failure here must NOT break the sync - old tables already written above
    }
  }

  console.log(
    `[sync] ${workspaceSlug}: ${campaigns.length} campaigns, ` +
    `${variantCopyRows.length} variants, ${metricsRows.length} metric rows`
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
  isInboxRun: boolean,
): Promise<void> {
  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const workspaces = Object.entries(keyMap);
  console.log(
    `[syncAllWorkspaces] isInboxRun=${isInboxRun} workspaces=${workspaces.length}`
  );

  await runWithConcurrency(workspaces, WORKSPACE_CONCURRENCY, async ([slug, apiKey]) => {
    try {
      await syncWorkspace(slug, apiKey, db, isInboxRun);
    } catch (err) {
      console.error(`[syncAllWorkspaces] Error on workspace ${slug}:`, err);
    }
  });

  // Refresh rollups once after all workspaces are written
  if (!isInboxRun) {
    try {
      await db.rpc('refresh_campaign_rollups', {});
      console.log('[syncAllWorkspaces] Rollups refreshed.');
    } catch (err) {
      console.error('[syncAllWorkspaces] Rollup refresh failed:', err);
    }
  }

  console.log('[syncAllWorkspaces] Done.');
}
