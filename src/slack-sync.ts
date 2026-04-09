import { SupabaseClient } from './supabase';

const CHANNELS = [
  { id: 'C0AFLH79SP6', name: 'llama-success' },
  { id: 'C08KYHBKLG1', name: 'greenbridge-success' },
  { id: 'C08JMH4TE12', name: 'btc-success' },
  { id: 'C06SNCHPV6J', name: 'qualifi-success' },
];

const LOOKBACK_HOURS = 8;

export async function syncMeetingsBooked(
  slackToken: string,
  supabaseUrl: string,
  supabaseKey: string,
  slackCookie?: string,
): Promise<void> {
  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const oldest = Math.floor((Date.now() - LOOKBACK_HOURS * 3600_000) / 1000).toString();

  for (const channel of CHANNELS) {
    try {
      const rows = await fetchChannel(slackToken, channel.id, channel.name, oldest, slackCookie);
      if (rows.length === 0) {
        console.log(`[slack-sync] ${channel.name}: 0 rows`);
        continue;
      }
      await db.upsert('meetings_booked_raw', rows, 'channel_id,message_ts,line_index');
      console.log(`[slack-sync] ${channel.name}: ${rows.length} rows upserted`);
    } catch (err) {
      console.error(`[slack-sync] ${channel.name}: error`, err);
    }
  }

  // Match newly inserted rows to campaigns
  await matchUnmatchedRows(db);
}

async function matchUnmatchedRows(db: SupabaseClient): Promise<void> {
  const pass1 = await db.rpc('match_meetings_exact', {});
  const pass2 = await db.rpc('match_meetings_normalized', {});
  console.log(`[slack-sync] Matching: ${pass1} exact, ${pass2} normalized`);
}

async function fetchChannel(
  token: string,
  channelId: string,
  channelName: string,
  oldest: string,
  cookie?: string,
): Promise<unknown[]> {
  const rows: unknown[] = [];
  let cursor: string | undefined;

  do {
    const params = new URLSearchParams({ channel: channelId, oldest, limit: '200', inclusive: 'true' });
    if (cursor) params.set('cursor', cursor);

    const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
    if (cookie) headers['Cookie'] = `d=${cookie}`;
    const res = await fetch(`https://slack.com/api/conversations.history?${params}`, { headers });
    const data: any = await res.json();

    if (!data.ok) throw new Error(`Slack API error: ${data.error}`);

    for (const msg of data.messages ?? []) {
      const lines = (msg.text ?? '').split('\n');
      lines.forEach((line: string, lineIndex: number) => {
        const parsed = parseMeetingBookedLine(line.trim());
        if (!parsed) return;
        rows.push({
          channel_id: channelId,
          channel_name: channelName,
          message_ts: msg.ts,
          line_index: lineIndex,
          posted_at: new Date(parseFloat(msg.ts) * 1000).toISOString(),
          posted_by_slack_id: msg.user ?? null,
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
  if (!/meeting\s+booke?d?\s+\d+/i.test(line)) return null;

  const seqMatch = line.match(/meeting\s+booke?d?\s+(\d+)/i);
  const booking_number = seqMatch ? parseInt(seqMatch[1], 10) : null;

  // Strip "Meeting Booked 123 - " prefix, keep everything else intact
  const campaign_name_raw = line
    .replace(/.*meeting\s+booke?d?\s+\d+\s*[-–]?\s*/i, '')
    .trim() || null;

  return { booking_number, campaign_name_raw };
}
