/**
 * LLM Meetings Matcher — silent-failure watchdog.
 *
 * The matcher runs as a Claude Code scheduled routine (trig_01WNisJzJBbrb2Ja7Bhc9oSg)
 * at 06:00 UTC daily. On 2026-04-24 it hit a Claude API stream-idle timeout and
 * died without posting a summary to #cc-sam. On 2026-04-26 the same silent failure
 * recurred. This watchdog runs ~30 min after the scheduled start time and posts a
 * fallback alert if no summary post is found in the channel for the run's date.
 *
 * Detection signature: a message in C0AR0EA21C1 within the last `LOOKBACK_MINUTES`
 * minutes whose text starts with "LLM meetings pass". If found → silent (do nothing,
 * exit 0). If missing → post the fallback alert.
 *
 * Auth: reuses CC_SLACK_BOT_TOKEN (same xoxb that the matcher itself uses to post).
 *
 * Manual run:
 *   CC_SLACK_BOT_TOKEN=xoxb-... npx tsx scripts/llm-matcher-watchdog.ts
 *
 * Optional env:
 *   WATCHDOG_LOOKBACK_MINUTES (default 60) — how far back to scan for the summary.
 *   WATCHDOG_DRY_RUN=1                       — log decision without posting.
 */

const CC_SAM_CHANNEL_ID = 'C0AR0EA21C1';
const SUMMARY_PREFIX = 'LLM meetings pass';

interface SlackMessage {
  text?: string;
  ts?: string;
  user?: string;
  bot_id?: string;
}

async function fetchRecentMessages(
  botToken: string,
  channel: string,
  oldestEpochSec: number,
): Promise<SlackMessage[]> {
  const params = new URLSearchParams({
    channel,
    limit: '50',
    oldest: oldestEpochSec.toString(),
  });

  const res = await fetch(`https://slack.com/api/conversations.history?${params}`, {
    headers: { Authorization: `Bearer ${botToken}` },
  });
  const payload: any = await res.json().catch(() => ({}));
  if (!res.ok || !payload.ok) {
    throw new Error(`conversations.history failed: ${payload.error ?? res.status}`);
  }
  return (payload.messages ?? []) as SlackMessage[];
}

async function postFallback(botToken: string, channel: string, text: string): Promise<void> {
  const res = await fetch('https://slack.com/api/chat.postMessage', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${botToken}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify({ channel, text }),
  });
  const payload: any = await res.json().catch(() => ({}));
  if (!res.ok || !payload.ok) {
    throw new Error(`chat.postMessage failed: ${payload.error ?? res.status}`);
  }
}

async function main(): Promise<void> {
  const botToken = process.env.CC_SLACK_BOT_TOKEN;
  if (!botToken) {
    console.error('[matcher-watchdog] Missing CC_SLACK_BOT_TOKEN — cannot run.');
    process.exit(1);
  }

  const lookbackMinutes = Number(process.env.WATCHDOG_LOOKBACK_MINUTES ?? 60);
  const dryRun = process.env.WATCHDOG_DRY_RUN === '1';
  const oldest = Math.floor((Date.now() - lookbackMinutes * 60 * 1000) / 1000);

  const messages = await fetchRecentMessages(botToken, CC_SAM_CHANNEL_ID, oldest);
  const summaryFound = messages.some(msg => (msg.text ?? '').trimStart().startsWith(SUMMARY_PREFIX));

  if (summaryFound) {
    console.log(
      `[matcher-watchdog] Summary found in #cc-sam within last ${lookbackMinutes}min — routine OK.`,
    );
    return;
  }

  const nowIso = new Date().toISOString();
  const fallback =
    `:x: LLM Meetings Matcher routine ended without summary` +
    ` (no "${SUMMARY_PREFIX}" message in #cc-sam within the last ${lookbackMinutes} min as of ${nowIso}).` +
    ` Routine: trig_01WNisJzJBbrb2Ja7Bhc9oSg.` +
    ` Investigate via https://claude.ai/code/routines/trig_01WNisJzJBbrb2Ja7Bhc9oSg.`;

  if (dryRun) {
    console.log(`[matcher-watchdog] DRY RUN — would post:\n${fallback}`);
    return;
  }

  await postFallback(botToken, CC_SAM_CHANNEL_ID, fallback);
  console.log('[matcher-watchdog] Fallback alert posted to #cc-sam.');
}

main().catch(err => {
  console.error(
    '[matcher-watchdog] fatal:',
    err instanceof Error ? err.message : err,
  );
  process.exit(1);
});
