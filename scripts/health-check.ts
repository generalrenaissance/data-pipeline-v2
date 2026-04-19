/**
 * Pipeline health monitor.
 *
 * Runs hourly from .github/workflows/pipeline-health.yml. Posts a Slack alert
 * when the pipeline looks unhealthy. Designed to catch the 2026-04-10 silent
 * outage class: workflows fail in ~3s with no code execution, no table is
 * updated, nobody notices for 3+ days.
 *
 * Two checks:
 *   A. Table freshness — SELECT MAX(synced_at) per table, compare to per-table SLA
 *   B. Workflow failures — gh api for last 3 runs of each monitored workflow
 *
 * v1 dedup strategy: post every hourly run when unhealthy. Accept repetition
 * in exchange for zero state. Swap for a dedup table if noise becomes a problem.
 *
 * Usage:
 *   PIPELINE_SUPABASE_URL=... PIPELINE_SUPABASE_KEY=... \
 *   PIPELINE_HEALTH_SLACK_WEBHOOK=... GITHUB_TOKEN=... GITHUB_REPOSITORY=owner/repo \
 *   npx tsx scripts/health-check.ts
 *
 * Flags:
 *   --dry-run    Print alerts to stdout instead of posting to Slack
 */
import { SupabaseClient } from '../src/supabase';

interface TableSla {
  table: string;
  maxAgeHours: number;
  note: string;
}

const MONITORED_TABLES: TableSla[] = [
  { table: 'campaign_metrics_daily', maxAgeHours: 3, note: 'hourly sync, 1h buffer' },
  { table: 'campaign_data', maxAgeHours: 3, note: 'hourly sync' },
  { table: 'campaign_daily_metrics', maxAgeHours: 30, note: 'daily 05:15 UTC cron' },
  { table: 'sender_inboxes', maxAgeHours: 26, note: 'daily inbox run 10:30 UTC' },
  // variant_copy retired in V3 cleanup 2026-04-10 (no writer remains in sync.ts) — do NOT monitor.
];

const MONITORED_WORKFLOWS = [
  'data-pipeline-sync.yml',
  'sequence-started-sync-gh.yml',
  'meetings-booked-sync.yml',
  'conversation-messages-sync.yml',
  'campaign-tag-sync.yml',
];

interface Alert {
  kind: 'stale' | 'failures' | 'empty' | 'error';
  key: string;
  message: string;
}

async function checkTableFreshness(
  db: SupabaseClient,
  sla: TableSla,
): Promise<Alert | null> {
  try {
    const rows = (await db.select(
      sla.table,
      'select=synced_at&order=synced_at.desc.nullslast&limit=1',
    )) as Array<{ synced_at: string | null }>;
    if (rows.length === 0 || !rows[0].synced_at) {
      return {
        kind: 'empty',
        key: `${sla.table}:empty`,
        message: `\`${sla.table}\` is empty — pipeline may have never written`,
      };
    }
    const latest = new Date(rows[0].synced_at).getTime();
    const ageHours = (Date.now() - latest) / 3_600_000;
    if (ageHours > sla.maxAgeHours) {
      return {
        kind: 'stale',
        key: `${sla.table}:stale`,
        message: `\`${sla.table}\` last synced ${ageHours.toFixed(1)}h ago (SLA ${sla.maxAgeHours}h — ${sla.note})`,
      };
    }
    return null;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return {
      kind: 'error',
      key: `${sla.table}:error`,
      message: `freshness check for \`${sla.table}\` errored: ${msg}`,
    };
  }
}

async function checkWorkflowFailures(
  repo: string,
  token: string,
  workflow: string,
): Promise<Alert | null> {
  const url = `https://api.github.com/repos/${repo}/actions/workflows/${workflow}/runs?per_page=3&exclude_pull_requests=true`;
  try {
    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
      },
    });
    if (!res.ok) {
      return {
        kind: 'error',
        key: `${workflow}:error`,
        message: `GH API ${res.status} for ${workflow}: ${await res.text().catch(() => '')}`,
      };
    }
    const body = (await res.json()) as {
      workflow_runs: Array<{ status: string; conclusion: string | null; html_url: string }>;
    };
    const completed = body.workflow_runs.filter((r) => r.status === 'completed');
    if (completed.length < 2) return null; // not enough signal
    const recent = completed.slice(0, 2);
    const allFailed = recent.every((r) => r.conclusion === 'failure');
    if (allFailed) {
      return {
        kind: 'failures',
        key: `${workflow}:failures`,
        message: `\`${workflow}\` last 2 runs failed — ${recent[0].html_url}`,
      };
    }
    return null;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return {
      kind: 'error',
      key: `${workflow}:error`,
      message: `workflow check for ${workflow} errored: ${msg}`,
    };
  }
}

async function postSlack(webhook: string, text: string): Promise<void> {
  const res = await fetch(webhook, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });
  if (!res.ok) {
    throw new Error(`Slack webhook ${res.status}: ${await res.text().catch(() => '')}`);
  }
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  const supabaseUrl = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const supabaseKey =
    process.env.PIPELINE_SUPABASE_KEY ??
    process.env.PIPELINE_SUPABASE_SERVICE_ROLE_KEY ??
    process.env.SUPABASE_KEY;
  const webhook = process.env.PIPELINE_HEALTH_SLACK_WEBHOOK;
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPOSITORY; // owner/name

  if (!supabaseUrl || !supabaseKey) {
    console.error('[health] missing PIPELINE_SUPABASE_URL / PIPELINE_SUPABASE_KEY');
    process.exit(1);
  }
  if (!dryRun && !webhook) {
    console.error('[health] missing PIPELINE_HEALTH_SLACK_WEBHOOK (or pass --dry-run)');
    process.exit(1);
  }

  const db = new SupabaseClient(supabaseUrl, supabaseKey);

  const [freshness, workflows] = await Promise.all([
    Promise.all(MONITORED_TABLES.map((sla) => checkTableFreshness(db, sla))),
    token && repo
      ? Promise.all(MONITORED_WORKFLOWS.map((wf) => checkWorkflowFailures(repo, token, wf)))
      : Promise.resolve([] as (Alert | null)[]),
  ]);
  if (!token || !repo) {
    console.warn('[health] no GITHUB_TOKEN / GITHUB_REPOSITORY — skipping workflow-failure check');
  }

  const alerts = [...freshness, ...workflows].filter((a): a is Alert => a !== null);

  if (alerts.length === 0) {
    console.log(`[health] ok — ${MONITORED_TABLES.length} tables fresh, ${MONITORED_WORKFLOWS.length} workflows healthy`);
    return;
  }

  const lines = [
    `:rotating_light: *Pipeline health check — ${alerts.length} alert${alerts.length === 1 ? '' : 's'}*`,
    ...alerts.map((a) => `• ${a.message}`),
    `_checked at ${new Date().toISOString()}_`,
  ];
  const text = lines.join('\n');

  if (dryRun) {
    console.log('[health] dry-run — would post:');
    console.log(text);
    return;
  }
  await postSlack(webhook!, text);
  console.log(`[health] posted ${alerts.length} alerts to Slack`);
}

main().catch((err) => {
  console.error('[health] fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
