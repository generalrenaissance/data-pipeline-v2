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
): Promise<void> {
  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const oldest = Math.floor((Date.now() - LOOKBACK_HOURS * 3600_000) / 1000).toString();

  for (const channel of CHANNELS) {
    try {
      const rows = await fetchChannel(slackToken, channel.id, channel.name, oldest);
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
}

async function fetchChannel(
  token: string,
  channelId: string,
  channelName: string,
  oldest: string,
): Promise<unknown[]> {
  const rows: unknown[] = [];
  let cursor: string | undefined;

  do {
    const params = new URLSearchParams({ channel: channelId, oldest, limit: '200', inclusive: 'true' });
    if (cursor) params.set('cursor', cursor);

    const res = await fetch(`https://slack.com/api/conversations.history?${params}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
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
  seq_num: number | null; qualified: 'ON' | 'OFF' | null;
  campaign_ref: string | null; cm_name: string | null;
} | null {
  if (!/meeting\s+booke?d?\s+\d+/i.test(line)) return null;

  const seqMatch = line.match(/meeting\s+booke?d?\s+(\d+)/i);
  const seq_num = seqMatch ? parseInt(seqMatch[1], 10) : null;

  let rest = line.replace(/.*meeting\s+booke?d?\s+\d+\s*[-–]?\s*/i, '').trim();

  let qualified: 'ON' | 'OFF' | null = null;
  if (/^ON\b/i.test(rest)) { qualified = 'ON'; rest = rest.replace(/^ON\s*[-–]?\s*/i, '').trim(); }
  else if (/^OFF\b/i.test(rest)) { qualified = 'OFF'; rest = rest.replace(/^OFF\s*[-–]?\s*/i, '').trim(); }

  const parenMatches = [...rest.matchAll(/\(([^)]+)\)/g)];
  const validParens = parenMatches.filter(m => !/^copy$/i.test(m[1].trim()));
  const cm_name = validParens.length > 0 ? validParens[validParens.length - 1][1].trim() : null;

  const lastParenIdx = cm_name ? rest.lastIndexOf(`(${cm_name}`) : rest.length;
  const campaign_ref = rest.slice(0, lastParenIdx).replace(/[-\s–]+$/, '').trim() || null;

  return { seq_num, qualified, campaign_ref, cm_name };
}
