/**
 * sync-sequence-started.ts
 *
 * Every 6h script. Paginates POST /api/v2/leads/list with
 * filter=FILTER_VAL_CONTACTED per campaign to get the Leads tab
 * "Sequence started" count (excludes deleted leads).
 * Upserts to campaign_data.sequence_started in Pipeline Supabase.
 *
 * Run: npx tsx scripts/sync-sequence-started.ts
 */

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const {
  PIPELINE_SUPABASE_URL,
  PIPELINE_SUPABASE_KEY,
  INSTANTLY_API_KEYS,
} = process.env;

const CONCURRENCY = 10; // Low — each campaign paginates many pages
const UPSERT_BATCH_SIZE = 50;
const PAGE_SIZE = 100;
const FETCH_TIMEOUT_MS = 30_000; // 30s per request — don't hang forever

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface CampaignRow {
  workspace_id: string;
  campaign_id: string;
}

/** Normalize "Renaissance 6" → "renaissance-6" to match API key map */
function toSlug(ws: string): string {
  return ws.toLowerCase().replace(/\s+/g, '-');
}

interface SequenceStartedRow {
  campaign_id: string;
  sequence_started: number;
}

// ---------------------------------------------------------------------------
// Instantly leads/list pagination — count contacted leads
// ---------------------------------------------------------------------------

async function countContactedLeads(
  campaignId: string,
  apiKey: string,
): Promise<number> {
  let count = 0;
  let cursor: string | undefined;

  while (true) {
    const body: Record<string, unknown> = {
      campaign: campaignId,
      filter: 'FILTER_VAL_CONTACTED',
      limit: PAGE_SIZE,
    };
    if (cursor) body.starting_after = cursor;

    const res = await fetch('https://api.instantly.ai/api/v2/leads/list', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });

    if (!res.ok) {
      throw new Error(`Instantly API ${res.status}: ${await res.text()}`);
    }

    const data = (await res.json()) as {
      items: { id: string }[];
      next_starting_after?: string;
    };

    count += data.items.length;

    if (!data.next_starting_after || data.items.length === 0) break;
    cursor = data.next_starting_after;
  }

  return count;
}

// ---------------------------------------------------------------------------
// Supabase helpers
// ---------------------------------------------------------------------------

async function pipelineSelect<T>(table: string, query: string): Promise<T[]> {
  const res = await fetch(`${PIPELINE_SUPABASE_URL}/rest/v1/${table}?${query}`, {
    headers: {
      Authorization: `Bearer ${PIPELINE_SUPABASE_KEY}`,
      apikey: PIPELINE_SUPABASE_KEY!,
    },
  });
  if (!res.ok) {
    throw new Error(`Pipeline Supabase select failed ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T[]>;
}

async function pipelineUpsertBatch(rows: SequenceStartedRow[]): Promise<void> {
  for (let i = 0; i < rows.length; i += UPSERT_BATCH_SIZE) {
    const chunk = rows.slice(i, i + UPSERT_BATCH_SIZE);
    const payload = chunk.map((r) => ({
      campaign_id: r.campaign_id,
      step: '__ALL__',
      variant: '__ALL__',
      sequence_started: r.sequence_started,
    }));
    const res = await fetch(
      `${PIPELINE_SUPABASE_URL}/rest/v1/campaign_data?on_conflict=campaign_id,step,variant`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${PIPELINE_SUPABASE_KEY}`,
          apikey: PIPELINE_SUPABASE_KEY!,
          Prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(payload),
      },
    );
    if (!res.ok) {
      throw new Error(`Pipeline Supabase upsert failed ${res.status}: ${await res.text()}`);
    }
    console.log(`[seq-started] Upserted batch of ${chunk.length} rows`);
  }
}

// ---------------------------------------------------------------------------
// Concurrency helper
// ---------------------------------------------------------------------------

async function withConcurrency<T>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<void>,
): Promise<void> {
  const queue = [...items];
  let completed = 0;
  const total = items.length;
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      while (queue.length > 0) {
        const item = queue.shift()!;
        await fn(item);
        completed++;
        if (completed % 25 === 0) {
          console.log(`[seq-started] Progress: ${completed}/${total}`);
        }
      }
    },
  );
  await Promise.all(workers);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const startMs = Date.now();
  console.log(`[seq-started] sync-sequence-started starting at ${new Date().toISOString()}`);

  const required: Record<string, string | undefined> = {
    PIPELINE_SUPABASE_URL,
    PIPELINE_SUPABASE_KEY,
    INSTANTLY_API_KEYS,
  };
  const missing = Object.entries(required).filter(([, v]) => !v).map(([k]) => k);
  if (missing.length) throw new Error(`Missing env vars: ${missing.join(', ')}`);

  let keyMap: Record<string, string>;
  try {
    keyMap = JSON.parse(INSTANTLY_API_KEYS!) as Record<string, string>;
  } catch {
    throw new Error('INSTANTLY_API_KEYS is not valid JSON');
  }
  console.log(`[seq-started] Loaded API keys for ${Object.keys(keyMap).length} workspaces`);

  const campaigns = await pipelineSelect<CampaignRow>(
    'campaigns',
    'select=workspace_id,campaign_id&status=in.(1,2)',
  );
  console.log(`[seq-started] ${campaigns.length} active campaigns to process`);
  if (campaigns.length === 0) {
    console.log('[seq-started] Nothing to do.');
    return;
  }

  const noKey = [...new Set(campaigns.map((c) => c.workspace_id))].filter((ws) => !keyMap[toSlug(ws)]);
  if (noKey.length > 0) {
    console.warn(`[seq-started] WARNING: No API key for workspace(s): ${noKey.join(', ')}`);
  }

  const results: SequenceStartedRow[] = [];
  const failed: string[] = [];
  const skipped: string[] = [];
  let totalPages = 0;

  await withConcurrency(campaigns, CONCURRENCY, async (campaign) => {
    const apiKey = keyMap[toSlug(campaign.workspace_id)];
    if (!apiKey) {
      skipped.push(campaign.campaign_id);
      return;
    }

    try {
      const count = await countContactedLeads(campaign.campaign_id, apiKey);
      results.push({
        campaign_id: campaign.campaign_id,
        sequence_started: count,
      });
      totalPages += Math.max(1, Math.ceil(count / PAGE_SIZE));
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[seq-started] FAIL ${campaign.workspace_id}/${campaign.campaign_id}: ${msg}`);
      failed.push(campaign.campaign_id);
    }
  });

  // Single flush after all pagination is done — no race condition
  console.log(`[seq-started] Pagination complete. Upserting ${results.length} rows...`);
  await pipelineUpsertBatch(results);

  const elapsed = Math.round((Date.now() - startMs) / 1000);
  console.log(`[seq-started] Results: ${results.length} ok, ${failed.length} failed, ${skipped.length} skipped (no key)`);
  console.log(`[seq-started] Total pages fetched: ~${totalPages}`);
  console.log(`[seq-started] Done in ${elapsed}s`);

  const attempted = results.length + failed.length;
  const errorRate = attempted > 0 ? failed.length / attempted : 0;
  if (errorRate > 0.1) {
    console.error(`[seq-started] Error rate ${(errorRate * 100).toFixed(1)}% exceeds 10% threshold (${failed.length}/${attempted} attempted)`);
    process.exit(1);
  }
}

main().catch((e) => {
  console.error('[seq-started] Fatal:', e instanceof Error ? e.message : e);
  process.exit(1);
});
