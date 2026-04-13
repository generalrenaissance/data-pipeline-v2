/**
 * Conversation Messages Sync
 *
 * Pulls emails from Instantly v2 API (GET /emails) for all workspaces
 * and upserts into Pipeline Supabase `conversation_messages` table.
 *
 * Incremental: paginates newest-first, stops when hitting already-synced data.
 * Full: paginates all emails (used for initial backfill).
 */

import { SupabaseClient } from './supabase';

interface InstantlyEmail {
  id: string;
  thread_id: string;
  campaign_id?: string;
  lead?: string;
  eaccount?: string;
  from_address_email?: string;
  from_address_json?: Array<{ name?: string; address?: string }>;
  to_address_email_list?: string;
  to_address_json?: Array<{ name?: string; address?: string }>;
  subject?: string;
  body?: { text?: string; html?: string };
  ue_type: number;
  step?: string;
  is_unread?: number;
  i_status?: number;
  ai_interest_value?: number;
  content_preview?: string;
  subsequence_id?: string;
  timestamp_email?: string;
  timestamp_created?: string;
  organization_id?: string;
}

interface SyncOptions {
  full?: boolean; // Full backfill (don't stop at already-synced)
  maxPages?: number; // Safety cap per workspace
  workspaceFilter?: string; // Sync only this workspace slug
}

interface ConversationSyncCheckpoint {
  workspace_id: string;
  committed_message_timestamp: string | null;
  last_started_at: string | null;
  last_completed_at: string | null;
  last_status: 'success' | 'failed' | null;
  last_error: string | null;
  updated_at: string | null;
}

const WORKSPACE_KEYS: Record<string, string> = {};
const EMAIL_PAGE_SIZE = 100;
const REQUEST_INTERVAL_MS = 3_500;
const RATE_LIMIT_RECOVERY_MS = 60_000;
const TIMEOUT_BACKOFF_MS = 15_000;

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseTimestamp(ts: string | null | undefined): number | null {
  if (!ts) return null;
  const parsed = Date.parse(ts);
  return Number.isNaN(parsed) ? null : parsed;
}

function maxTimestamp(a: string | null, b: string | null): string | null {
  const aMs = parseTimestamp(a);
  const bMs = parseTimestamp(b);
  if (aMs === null) return b;
  if (bMs === null) return a;
  return bMs > aMs ? b : a;
}

function isOlderThanCutoff(candidate: string | null | undefined, cutoff: string | null): boolean {
  const candidateMs = parseTimestamp(candidate);
  const cutoffMs = parseTimestamp(cutoff);
  if (candidateMs === null || cutoffMs === null) return false;
  return candidateMs < cutoffMs;
}

class WorkspaceRequestLimiter {
  private nextRequestAt = 0;

  async waitForSlot(): Promise<void> {
    const waitMs = this.nextRequestAt - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }
  }

  markRequestComplete(): void {
    this.nextRequestAt = Date.now() + REQUEST_INTERVAL_MS;
  }
}

export function loadWorkspaceKeys(envPrefix: string = 'INSTANTLY_KEY_'): void {
  for (const [key, val] of Object.entries(process.env)) {
    if (key.startsWith(envPrefix) && val) {
      const slug = key
        .replace(envPrefix, '')
        .toLowerCase()
        .replace(/_/g, '-');
      if (slug !== 'personal') {
        WORKSPACE_KEYS[slug] = val;
      }
    }
  }
}

export function loadWorkspaceKeysFromJson(json: string): void {
  const parsed = JSON.parse(json);
  for (const [name, key] of Object.entries(parsed)) {
    const slug = name.toLowerCase().replace(/\s+/g, '-');
    WORKSPACE_KEYS[slug] = key as string;
  }
}

function parseStep(stepRaw: string | undefined | null): { step: number | null; variant: string | null } {
  if (!stepRaw) return { step: null, variant: null };
  const parts = stepRaw.split('_');
  if (parts.length < 3) return { step: null, variant: null };
  const stepIndex = parseInt(parts[1], 10);
  const variantIndex = parseInt(parts[2], 10);
  if (isNaN(stepIndex) || isNaN(variantIndex)) return { step: null, variant: null };
  return {
    step: stepIndex + 1,
    variant: String.fromCharCode(65 + variantIndex), // 0=A, 1=B, 2=C...
  };
}

function mapEmailToRow(email: InstantlyEmail, workspaceSlug: string): Record<string, unknown> {
  const direction = email.ue_type === 2 ? 'inbound' : email.ue_type === 3 ? 'outbound_manual' : `ue_${email.ue_type}`;
  const { step, variant } = parseStep(email.step);

  // For inbound: from = lead, to = our sender (eaccount)
  // For outbound_manual: from = our sender (eaccount), to = lead
  let senderEmail: string;
  let senderName: string | null = null;
  let recipientEmail: string;
  let recipientName: string | null = null;

  if (email.ue_type === 2) {
    // Inbound: lead sent it
    senderEmail = email.from_address_email ?? email.lead ?? '';
    senderName = email.from_address_json?.[0]?.name ?? null;
    recipientEmail = email.eaccount ?? email.to_address_email_list ?? '';
    recipientName = email.to_address_json?.[0]?.name ?? null;
  } else {
    // Outbound manual: our sender sent it
    senderEmail = email.eaccount ?? email.from_address_email ?? '';
    senderName = email.from_address_json?.[0]?.name ?? null;
    recipientEmail = email.lead ?? email.to_address_email_list ?? '';
    recipientName = email.to_address_json?.[0]?.name ?? null;
  }

  return {
    id: email.id,
    thread_id: email.thread_id,
    campaign_id: email.campaign_id ?? null,
    workspace_id: workspaceSlug,
    lead_email: email.lead ?? '',
    sender_email: senderEmail,
    sender_name: senderName,
    recipient_email: recipientEmail,
    recipient_name: recipientName,
    direction,
    ue_type: email.ue_type,
    body_text: email.body?.text ?? null,
    body_html: email.body?.html ?? null,
    subject: email.subject ?? null,
    message_timestamp: email.timestamp_email ?? email.timestamp_created ?? new Date().toISOString(),
    step_raw: email.step ?? null,
    step,
    variant,
    is_unread: email.is_unread === 1,
    interest_status: email.i_status ?? null,
    ai_interest_value: email.ai_interest_value ?? null,
    content_preview: email.content_preview ?? null,
    eaccount: email.eaccount ?? null,
    subsequence_id: email.subsequence_id ?? null,
    synced_at: new Date().toISOString(),
  };
}

async function fetchEmails(
  apiKey: string,
  limiter: WorkspaceRequestLimiter,
  cursor?: string,
): Promise<{ items: InstantlyEmail[]; nextCursor: string | undefined }> {
  const url = new URL('https://api.instantly.ai/api/v2/emails');
  url.searchParams.set('limit', String(EMAIL_PAGE_SIZE));
  if (cursor) url.searchParams.set('starting_after', cursor);

  const maxRetries = 3;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    await limiter.waitForSlot();
    try {
      const start = Date.now();
      const res = await fetch(url.toString(), {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'User-Agent': 'Renaissance-DataPipeline/2.0',
        },
        signal: AbortSignal.timeout(60_000),
      });
      limiter.markRequestComplete();
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);

      // Instantly returns 403 (not 429) for rate limits
      if ((res.status === 429 || res.status === 403) && attempt < maxRetries) {
        console.log(
          `  Rate limited (${res.status}, ${elapsed}s), waiting ${RATE_LIMIT_RECOVERY_MS / 1000}s ` +
          `(attempt ${attempt + 1}/${maxRetries})...`
        );
        await sleep(RATE_LIMIT_RECOVERY_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Instantly ${res.status}: ${body}`);
      }

      const data: any = await res.json();
      return { items: data.items ?? [], nextCursor: data.next_starting_after ?? undefined };
    } catch (err: any) {
      const isTimeout = err.name === 'TimeoutError' || err.name === 'AbortError' || err.message?.includes('timeout');
      if (isTimeout && attempt < maxRetries) {
        limiter.markRequestComplete();
        const wait = TIMEOUT_BACKOFF_MS * Math.pow(2, attempt);
        console.log(`  Timeout on attempt ${attempt + 1}, waiting ${wait / 1000}s before retry...`);
        await sleep(wait);
        continue;
      }
      throw err;
    }
  }

  throw new Error('Instantly: max retries exceeded');
}

async function getCheckpoint(db: SupabaseClient, workspaceSlug: string): Promise<ConversationSyncCheckpoint | null> {
  const rows = await db.select(
    'conversation_sync_checkpoints',
    `select=workspace_id,committed_message_timestamp,last_started_at,last_completed_at,last_status,last_error,updated_at&workspace_id=eq.${workspaceSlug}&limit=1`,
  );
  return rows.length === 0 ? null : (rows[0] as ConversationSyncCheckpoint);
}

async function upsertCheckpoint(
  db: SupabaseClient,
  checkpoint: ConversationSyncCheckpoint,
): Promise<void> {
  await db.upsert('conversation_sync_checkpoints', [checkpoint], 'workspace_id');
}

export async function syncWorkspace(
  workspaceSlug: string,
  apiKey: string,
  db: SupabaseClient,
  options: SyncOptions = {},
): Promise<{ rows: number; pages: number; stopped: string }> {
  const maxPages = options.maxPages ?? 2000;
  const limiter = new WorkspaceRequestLimiter();
  const workspaceStartedAt = new Date().toISOString();
  const existingCheckpoint = await getCheckpoint(db, workspaceSlug);
  const committedCheckpoint = options.full ? null : (existingCheckpoint?.committed_message_timestamp ?? null);
  await upsertCheckpoint(db, {
    workspace_id: workspaceSlug,
    committed_message_timestamp: existingCheckpoint?.committed_message_timestamp ?? null,
    last_started_at: workspaceStartedAt,
    last_completed_at: existingCheckpoint?.last_completed_at ?? null,
    last_status: existingCheckpoint?.last_status ?? null,
    last_error: null,
    updated_at: workspaceStartedAt,
  });

  console.log(`  [${workspaceSlug}] Fetching committed checkpoint...`);
  console.log(`  [${workspaceSlug}] Committed checkpoint: ${committedCheckpoint ?? 'none (first sync)'}`);
  console.log(`  [${workspaceSlug}] Starting pagination...`);

  let cursor: string | undefined;
  let totalRows = 0;
  let pages = 0;
  let stopped = 'exhausted';
  let consecutiveEmpty = 0;
  let maxSeenMessageTs: string | null = null;

  try {
    while (pages < maxPages) {
      const { items, nextCursor } = await fetchEmails(apiKey, limiter, cursor);
      pages++;

      if (items.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 2) {
          stopped = 'empty';
          break;
        }
        if (!nextCursor) {
          stopped = 'exhausted';
          break;
        }
        cursor = nextCursor;
        continue;
      }

      consecutiveEmpty = 0;

      for (const item of items) {
        const itemTs = item.timestamp_email ?? item.timestamp_created ?? null;
        maxSeenMessageTs = maxTimestamp(maxSeenMessageTs, itemTs);
      }

      const rows = items.map(email => mapEmailToRow(email, workspaceSlug));
      await db.upsert('conversation_messages', rows, 'id');
      totalRows += rows.length;

      if (committedCheckpoint && !options.full) {
        const oldestInPage = items[items.length - 1];
        const oldestTs = oldestInPage.timestamp_email ?? oldestInPage.timestamp_created ?? null;
        if (isOlderThanCutoff(oldestTs, committedCheckpoint)) {
          stopped = 'incremental_cutoff';
          break;
        }
      }

      if (!nextCursor) {
        stopped = 'exhausted';
        break;
      }

      cursor = nextCursor;

      if (pages % 10 === 0) {
        console.log(`  [${workspaceSlug}] page ${pages}: ${totalRows} rows so far...`);
      }
    }

    if (pages >= maxPages) stopped = 'max_pages';

    const completedAt = new Date().toISOString();
    await upsertCheckpoint(db, {
      workspace_id: workspaceSlug,
      committed_message_timestamp: maxTimestamp(existingCheckpoint?.committed_message_timestamp ?? null, maxSeenMessageTs),
      last_started_at: workspaceStartedAt,
      last_completed_at: completedAt,
      last_status: 'success',
      last_error: null,
      updated_at: completedAt,
    });

    return { rows: totalRows, pages, stopped };
  } catch (err) {
    const completedAt = new Date().toISOString();
    const message = err instanceof Error ? err.message : String(err);
    await upsertCheckpoint(db, {
      workspace_id: workspaceSlug,
      committed_message_timestamp: existingCheckpoint?.committed_message_timestamp ?? null,
      last_started_at: workspaceStartedAt,
      last_completed_at: completedAt,
      last_status: 'failed',
      last_error: message,
      updated_at: completedAt,
    });
    throw err;
  }
}

export async function syncAllWorkspaces(
  supabaseUrl: string,
  supabaseKey: string,
  options: SyncOptions = {},
): Promise<void> {
  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const startTime = Date.now();
  let totalRows = 0;
  let totalPages = 0;
  let errors = 0;
  const results: Array<{ workspace: string; rows: number; pages: number; stopped: string; error?: string }> = [];

  if (options.workspaceFilter && !WORKSPACE_KEYS[options.workspaceFilter]) {
    throw new Error(`Workspace '${options.workspaceFilter}' not found in loaded keys. Available: ${Object.keys(WORKSPACE_KEYS).join(', ')}`);
  }

  const workspaces = options.workspaceFilter
    ? { [options.workspaceFilter]: WORKSPACE_KEYS[options.workspaceFilter] }
    : WORKSPACE_KEYS;

  console.log(`[conversation-sync] Starting sync for ${Object.keys(workspaces).length} workspaces (mode: ${options.full ? 'full' : 'incremental'})`);

  for (const [slug, apiKey] of Object.entries(workspaces)) {
    console.log(`[conversation-sync] ${slug}...`);
    try {
      const result = await syncWorkspace(slug, apiKey!, db, options);
      totalRows += result.rows;
      totalPages += result.pages;
      results.push({ workspace: slug, ...result });
      console.log(`[conversation-sync] ${slug}: ${result.rows} rows, ${result.pages} pages (${result.stopped})`);
    } catch (err: any) {
      errors++;
      const msg = err.message ?? String(err);
      results.push({ workspace: slug, rows: 0, pages: 0, stopped: 'error', error: msg });
      console.error(`[conversation-sync] ${slug}: ERROR - ${msg}`);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n[conversation-sync] DONE: ${totalRows} rows, ${totalPages} pages, ${elapsed}s, ${errors} errors`);

  // Print summary table
  console.log('\nWorkspace Summary:');
  for (const r of results) {
    const status = r.error ? `ERROR: ${r.error.slice(0, 80)}` : `${r.rows} rows / ${r.pages} pages (${r.stopped})`;
    console.log(`  ${r.workspace.padEnd(25)} ${status}`);
  }

  if (errors > 0) {
    process.exit(1);
  }
}
