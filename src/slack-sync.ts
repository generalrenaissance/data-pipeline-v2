import {
  SEEDED_CAMPAIGN_ALIASES,
  type AliasRecord,
  type CampaignRecord,
  type MeetingNameStats,
  type QueueRecord,
  buildQueueUpsert,
  buildResolvedQueuePatch,
  buildResolverContext,
  resolveCampaignName,
} from './meetings-matcher';
import { SupabaseClient } from './supabase';

const CHANNELS = [
  { id: 'C0AFLH79SP6', name: 'llama-success', partner: 'Llama' },
  { id: 'C08KYHBKLG1', name: 'greenbridge-success', partner: 'GreenBridge' },
  { id: 'C08JMH4TE12', name: 'btc-success', partner: 'BTC' },
  { id: 'C06SNCHPV6J', name: 'qualifi-success', partner: 'Qualifi' },
];

const FALLBACK_LOOKBACK_HOURS = 36;
const CC_SAM_CHANNEL_ID = 'C0AR0EA21C1';
const CC_SAM_MENTION = '<@U0AM2CQHW9E>';

interface SlackSyncRow {
  channel_id: string;
  channel_name: string;
  partner: string;
  message_ts: string;
  line_index: number;
  posted_at: string;
  posted_by_slack_id: string | null;
  posted_by: string | null;
  raw_text: string | null;
  raw_line: string;
  booking_number: number | null;
  campaign_name_raw: string | null;
  synced_at: string;
}

interface QueueDigestEntry {
  rawName: string;
  reason: string;
  candidates: { campaign_name: string; score: number }[];
}

export async function syncMeetingsBooked(
  slackToken: string,
  supabaseUrl: string,
  supabaseKey: string,
  slackCookie?: string,
  ccSlackBotToken?: string,
): Promise<void> {
  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const rowsByName = new Map<string, MeetingNameStats>();
  let totalRows = 0;

  for (const channel of CHANNELS) {
    try {
      const oldest = await determineOldest(db, channel.id);
      const rows = await fetchChannel(slackToken, channel, oldest, slackCookie);
      if (rows.length === 0) {
        console.log(`[slack-sync] ${channel.name}: 0 rows`);
        continue;
      }

      await db.upsert('meetings_booked_raw', rows, 'channel_id,message_ts,line_index');
      totalRows += rows.length;
      trackRows(rowsByName, rows);
      console.log(`[slack-sync] ${channel.name}: ${rows.length} rows upserted`);
    } catch (err) {
      console.error(`[slack-sync] ${channel.name}: error`, err);
    }
  }

  if (rowsByName.size === 0) {
    console.log('[slack-sync] No candidate names to match');
    return;
  }

  const digestEntries = await matchRecentNames(db, rowsByName, ccSlackBotToken);
  console.log(`[slack-sync] Processed ${rowsByName.size} unique names across ${totalRows} rows`);

  if (digestEntries.length > 0 && ccSlackBotToken) {
    await postCcSamDigest(ccSlackBotToken, digestEntries);
  } else if (digestEntries.length > 0) {
    console.warn('[slack-sync] Missing CC_SLACK_BOT_TOKEN - digest skipped');
  }

  const rolled = await db.rpc('rollup_meetings_booked', {});
  console.log(`[slack-sync] Rollup: ${rolled} campaign_data rows updated`);
}

async function determineOldest(db: SupabaseClient, channelId: string): Promise<string> {
  const fallback = Math.floor((Date.now() - FALLBACK_LOOKBACK_HOURS * 3600_000) / 1000);
  const params =
    `select=posted_at&channel_id=eq.${encodeURIComponent(channelId)}` +
    '&order=posted_at.desc.nullslast&limit=1';
  const rows = await db.select('meetings_booked_raw', params) as Array<{ posted_at?: string | null }>;
  const latest = rows[0]?.posted_at ? Date.parse(rows[0].posted_at) : Number.NaN;

  if (!Number.isFinite(latest)) {
    return String(fallback);
  }

  return String(Math.floor((latest - 6 * 3600_000) / 1000));
}

async function fetchChannel(
  token: string,
  channel: typeof CHANNELS[number],
  oldest: string,
  cookie?: string,
): Promise<SlackSyncRow[]> {
  const rows: SlackSyncRow[] = [];
  let cursor: string | undefined;

  do {
    const params = new URLSearchParams({ channel: channel.id, oldest, limit: '200', inclusive: 'true' });
    if (cursor) params.set('cursor', cursor);

    const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
    if (cookie) headers.Cookie = `d=${cookie}`;

    const res = await fetch(`https://slack.com/api/conversations.history?${params}`, { headers });
    const data: any = await res.json();
    if (!data.ok) throw new Error(`Slack API error: ${data.error}`);

    for (const msg of data.messages ?? []) {
      const text = typeof msg.text === 'string' ? msg.text : '';
      const lines = text.split('\n');

      lines.forEach((line: string, lineIndex: number) => {
        const parsed = parseMeetingBookedLine(line.trim());
        if (!parsed) return;

        rows.push({
          channel_id: channel.id,
          channel_name: channel.name,
          partner: channel.partner,
          message_ts: msg.ts,
          line_index: lineIndex,
          posted_at: new Date(parseFloat(msg.ts) * 1000).toISOString(),
          posted_by_slack_id: msg.user ?? null,
          posted_by: msg.username ?? msg.bot_profile?.name ?? null,
          raw_text: text || null,
          raw_line: line.trim(),
          ...parsed,
          synced_at: new Date().toISOString(),
        });
      });
    }

    cursor = data.response_metadata?.next_cursor || undefined;
  } while (cursor);

  return rows;
}

function parseMeetingBookedLine(line: string): {
  booking_number: number | null;
  campaign_name_raw: string | null;
} | null {
  const sanitized = line.replace(/^[•*]+?\s*/, '').trim();
  if (!/meeting\s+booke?d?\s+\d+/i.test(sanitized)) return null;

  const seqMatch = sanitized.match(/meeting\s+booke?d?\s+(\d+)/i);
  const booking_number = seqMatch ? parseInt(seqMatch[1], 10) : null;
  const campaign_name_raw = sanitized
    .replace(/.*meeting\s+booke?d?\s+\d+\s*[-\u2012-\u2015\u2212]?\s*/i, '')
    .trim() || null;

  return { booking_number, campaign_name_raw };
}

function trackRows(rowsByName: Map<string, MeetingNameStats>, rows: SlackSyncRow[]): void {
  for (const row of rows) {
    if (!row.campaign_name_raw) continue;

    const current = rowsByName.get(row.campaign_name_raw) ?? {
      count: 0,
      channelNames: new Set<string>(),
      firstSeenAt: row.posted_at,
      lastSeenAt: row.posted_at,
    };

    current.count += 1;
    current.channelNames.add(row.channel_name);
    if (!current.firstSeenAt || row.posted_at < current.firstSeenAt) current.firstSeenAt = row.posted_at;
    if (!current.lastSeenAt || row.posted_at > current.lastSeenAt) current.lastSeenAt = row.posted_at;

    rowsByName.set(row.campaign_name_raw, current);
  }
}

async function loadCampaigns(db: SupabaseClient): Promise<CampaignRecord[]> {
  const rows = await db.selectAll(
    'campaigns',
    'select=campaign_id,name&name=not.is.null',
  ) as CampaignRecord[];
  return rows.filter(row => row.campaign_id && row.name);
}

async function loadAliases(db: SupabaseClient): Promise<AliasRecord[]> {
  try {
    const rows = await db.selectAll(
      'campaign_aliases',
      'select=alias,campaign_id,note,created_by',
    ) as AliasRecord[];
    return [...rows, ...SEEDED_CAMPAIGN_ALIASES];
  } catch (error) {
    console.warn('[slack-sync] campaign_aliases missing or unreadable - using seeded aliases only');
    return [...SEEDED_CAMPAIGN_ALIASES];
  }
}

async function loadQueue(db: SupabaseClient): Promise<Map<string, QueueRecord>> {
  try {
    const rows = await db.selectAll(
      'meetings_unmatched_queue',
      'select=campaign_name_raw,candidate_hash,review_status,queue_reason,top_candidates,occurrence_count,source_channels,first_seen_at,last_seen_at,last_digest_at',
    ) as QueueRecord[];
    return new Map(rows.map(row => [row.campaign_name_raw, row]));
  } catch (error) {
    console.warn('[slack-sync] meetings_unmatched_queue missing or unreadable - digest cache disabled');
    return new Map();
  }
}

async function matchRecentNames(
  db: SupabaseClient,
  rowsByName: Map<string, MeetingNameStats>,
  ccSlackBotToken?: string,
): Promise<QueueDigestEntry[]> {
  const [campaigns, aliases, queueByName] = await Promise.all([
    loadCampaigns(db),
    loadAliases(db),
    loadQueue(db),
  ]);
  const context = buildResolverContext(campaigns, aliases);
  const digestEntries: QueueDigestEntry[] = [];

  for (const [rawName, stats] of rowsByName) {
    const existingQueue = queueByName.get(rawName);
    const resolution = resolveCampaignName(rawName, context, existingQueue);
    const filter =
      `campaign_name_raw=eq.${encodeURIComponent(rawName)}` +
      '&campaign_id=is.null';

    if (resolution.kind === 'match') {
      await db.update('meetings_booked_raw', filter, {
        campaign_id: resolution.campaignId,
        match_method: resolution.matchMethod,
        match_confidence: resolution.matchConfidence,
      });

      if (existingQueue) {
        await db.update(
          'meetings_unmatched_queue',
          `campaign_name_raw=eq.${encodeURIComponent(rawName)}`,
          buildResolvedQueuePatch(resolution.campaignId),
        );
      }

      console.log(
        `[slack-sync] ${resolution.matchMethod}: ${rawName} -> ${resolution.campaignName}`,
      );
      continue;
    }

    if (resolution.kind === 'ignore') {
      await db.update('meetings_booked_raw', filter, {
        match_method: 'ignored_linkedin',
        match_confidence: 0,
      });
      continue;
    }

    await db.upsert(
      'meetings_unmatched_queue',
      [buildQueueUpsert(resolution, stats, existingQueue)],
      'campaign_name_raw',
    );

    if (!existingQueue || !existingQueue.last_digest_at) {
      digestEntries.push({
        rawName,
        reason: resolution.queueReason,
        candidates: resolution.topCandidates.map(candidate => ({
          campaign_name: candidate.campaign_name,
          score: candidate.score,
        })),
      });
    }
  }

  if (digestEntries.length > 0 && ccSlackBotToken) {
    const digestedAt = new Date().toISOString();
    for (const entry of digestEntries) {
      await db.update(
        'meetings_unmatched_queue',
        `campaign_name_raw=eq.${encodeURIComponent(entry.rawName)}`,
        { last_digest_at: digestedAt, updated_at: digestedAt },
      );
    }
  }

  return digestEntries;
}

async function postCcSamDigest(botToken: string, entries: QueueDigestEntry[]): Promise<void> {
  const lines = [
    `${CC_SAM_MENTION} ${entries.length} new manual-review meeting name${entries.length === 1 ? '' : 's'} queued`,
    ...entries.slice(0, 20).flatMap(entry => {
      const candidateText =
        entry.candidates.length > 0
          ? entry.candidates.map(candidate => `${candidate.campaign_name} (${candidate.score})`).join(' | ')
          : 'no viable candidates';

      return [`- ${entry.rawName}`, `  ${entry.reason}: ${candidateText}`];
    }),
    entries.length > 20 ? `_plus ${entries.length - 20} more in meetings_unmatched_queue_` : '',
  ].filter(Boolean);

  const res = await fetch('https://slack.com/api/chat.postMessage', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${botToken}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify({
      channel: CC_SAM_CHANNEL_ID,
      text: lines.join('\n'),
    }),
  });

  const payload: any = await res.json().catch(() => ({}));
  if (!res.ok || !payload.ok) {
    throw new Error(`Slack postMessage failed: ${payload.error ?? res.status}`);
  }
}
