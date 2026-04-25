import { InstantlyClient } from '../src/instantly';
import { SupabaseClient } from '../src/supabase';
import { parseInstantlyKeyMap } from '../src/instantly-key-map';
import {
  inventory,
  metricsBackfill,
  metricsIncremental,
  rebuildAggregates,
  type RunStats,
  type SyncDeps,
} from '../src/infra/sync-infra-domains';

/**
 * sync-infra-domains.ts
 *
 * CLI wrapper for the Sam-owned infra metrics sync. Modes:
 *   --mode phase0
 *   --mode inventory [--workspace <slug>]
 *   --mode metrics-incremental [--workspace <slug>] [--days N]
 *   --mode metrics-backfill --start YYYY-MM-DD --end YYYY-MM-DD [--workspace <slug>]
 *   --mode full [--workspace <slug>] [--days N]
 *   --mode aggregate-only [--workspace <slug>]
 */

type Mode =
  | 'phase0'
  | 'inventory'
  | 'metrics-incremental'
  | 'metrics-backfill'
  | 'full'
  | 'aggregate-only';

interface CliArgs {
  mode: Mode;
  workspace?: string;
  start?: string;
  end?: string;
  days?: number;
}

function parseArgs(argv: string[]): CliArgs {
  const args: Partial<CliArgs> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = argv[i + 1];
    switch (a) {
      case '--mode':
        args.mode = next as Mode;
        i++;
        break;
      case '--workspace':
        args.workspace = next;
        i++;
        break;
      case '--start':
        args.start = next;
        i++;
        break;
      case '--end':
        args.end = next;
        i++;
        break;
      case '--days':
        args.days = parseInt(next ?? '', 10);
        i++;
        break;
      default:
        break;
    }
  }
  if (!args.mode) throw new Error('Missing required --mode');
  return args as CliArgs;
}

function emptyStats(): RunStats {
  return {
    workspaceCount: 0,
    accountsSeen: 0,
    accountMetricRows: 0,
    domainsWritten: 0,
    apiCalls: 0,
    rateLimitEvents: 0,
    durationMs: 0,
    errors: [],
  };
}

interface RunRow {
  id: string;
}

async function insertRunRow(
  supabase: SupabaseClient,
  url: string,
  key: string,
  runType: string,
): Promise<string | null> {
  const startedAt = new Date().toISOString();
  const res = await fetch(`${url}/rest/v1/infra_sync_runs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${key}`,
      apikey: key,
      Prefer: 'return=representation',
    },
    body: JSON.stringify([{ run_type: runType, started_at: startedAt, status: 'running' }]),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    console.error(`[infra-sync] Failed to insert run row (${res.status}): ${body}`);
    return null;
  }
  const rows = (await res.json().catch(() => [])) as RunRow[];
  return rows[0]?.id ?? null;
}

async function finalizeRunRow(
  supabase: SupabaseClient,
  runId: string,
  status: 'completed' | 'partial' | 'failed',
  stats: RunStats,
): Promise<void> {
  try {
    await supabase.update(
      'infra_sync_runs',
      `id=eq.${runId}`,
      {
        completed_at: new Date().toISOString(),
        status,
        workspace_count: stats.workspaceCount,
        accounts_seen: stats.accountsSeen,
        account_metric_rows: stats.accountMetricRows,
        domains_written: stats.domainsWritten,
        api_calls_made: stats.apiCalls,
        rate_limit_events: stats.rateLimitEvents,
        errors: stats.errors,
        duration_ms: stats.durationMs,
      },
    );
  } catch (err) {
    console.error(
      `[infra-sync] Failed to finalize run row ${runId}:`,
      err instanceof Error ? err.message : err,
    );
  }
}

function printSummary(mode: Mode, stats: RunStats, errored: boolean): void {
  console.log('[infra-sync] ----- summary -----');
  console.log(`[infra-sync] mode: ${mode}`);
  console.log(`[infra-sync] workspaces processed: ${stats.workspaceCount}`);
  console.log(`[infra-sync] accounts seen: ${stats.accountsSeen}`);
  console.log(`[infra-sync] account daily rows upserted: ${stats.accountMetricRows}`);
  console.log(`[infra-sync] domains written: ${stats.domainsWritten}`);
  console.log(`[infra-sync] API calls: ${stats.apiCalls}`);
  console.log(`[infra-sync] rate-limit events: ${stats.rateLimitEvents}`);
  console.log(`[infra-sync] runtime: ${(stats.durationMs / 1000).toFixed(1)}s`);
  if (stats.errors.length > 0) {
    console.log(`[infra-sync] errors (${stats.errors.length}):`);
    for (const e of stats.errors) console.log(`[infra-sync]   - ${e}`);
  }
  if (errored) console.log('[infra-sync] result: FAILED');
  else if (stats.errors.length > 0) console.log('[infra-sync] result: PARTIAL');
  else console.log('[infra-sync] result: COMPLETED');
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  if (args.mode === 'phase0') {
    console.log(
      '[infra-sync] Phase 0 (API Economics Gate) is COMPLETE. See handoffs/2026-04-24-domain-infra-sync-phase0-blocker.md for evidence.',
    );
    return;
  }

  const apiKeysJson = process.env.INSTANTLY_API_KEYS;
  const supaUrl = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const supaKey = process.env.PIPELINE_SUPABASE_KEY ?? process.env.SUPABASE_KEY;
  const missing: string[] = [];
  if (!apiKeysJson) missing.push('INSTANTLY_API_KEYS');
  if (!supaUrl) missing.push('PIPELINE_SUPABASE_URL (or SUPABASE_URL)');
  if (!supaKey) missing.push('PIPELINE_SUPABASE_KEY (or SUPABASE_KEY)');
  if (missing.length > 0) {
    throw new Error(`Missing required env vars: ${missing.join(', ')}`);
  }

  const keyMap = parseInstantlyKeyMap(apiKeysJson!);
  const supabase = new SupabaseClient(supaUrl!, supaKey!);

  console.log(
    `[infra-sync] mode=${args.mode} workspaces_in_keymap=${Object.keys(keyMap).length}` +
      (args.workspace ? ` filter=${args.workspace}` : ''),
  );

  const runId = await insertRunRow(supabase, supaUrl!, supaKey!, args.mode);

  const deps: SyncDeps = {
    keyMap,
    supabase,
    runId: runId ?? undefined,
    makeClient: (apiKey: string) => new InstantlyClient(apiKey),
  };

  let stats: RunStats = emptyStats();
  let errored = false;

  try {
    switch (args.mode) {
      case 'inventory': {
        stats = await inventory(deps, { workspaceFilter: args.workspace });
        break;
      }
      case 'metrics-incremental': {
        stats = await metricsIncremental(deps, {
          workspaceFilter: args.workspace,
          days: args.days ?? 7,
        });
        break;
      }
      case 'metrics-backfill': {
        if (!args.start || !args.end) {
          throw new Error('metrics-backfill requires --start and --end');
        }
        stats = await metricsBackfill(deps, {
          workspaceFilter: args.workspace,
          startDate: args.start,
          endDate: args.end,
        });
        break;
      }
      case 'full': {
        const invStats = await inventory(deps, { workspaceFilter: args.workspace });
        const metStats = await metricsIncremental(deps, {
          workspaceFilter: args.workspace,
          days: args.days ?? 7,
        });
        stats = {
          workspaceCount: invStats.workspaceCount + metStats.workspaceCount,
          accountsSeen: invStats.accountsSeen + metStats.accountsSeen,
          accountMetricRows: invStats.accountMetricRows + metStats.accountMetricRows,
          domainsWritten: metStats.domainsWritten,
          apiCalls: invStats.apiCalls + metStats.apiCalls,
          rateLimitEvents: invStats.rateLimitEvents + metStats.rateLimitEvents,
          durationMs: invStats.durationMs + metStats.durationMs,
          errors: [...invStats.errors, ...metStats.errors],
        };
        break;
      }
      case 'aggregate-only': {
        const start = Date.now();
        const r = await rebuildAggregates(deps, { workspaceFilter: args.workspace });
        stats = emptyStats();
        stats.domainsWritten = r.domainsWritten;
        stats.durationMs = Date.now() - start;
        break;
      }
      default:
        throw new Error(`Unknown mode: ${args.mode}`);
    }
  } catch (err) {
    errored = true;
    stats.errors.push(`fatal: ${err instanceof Error ? err.message : String(err)}`);
  }

  if (runId) {
    const status: 'completed' | 'partial' | 'failed' = errored
      ? 'failed'
      : stats.errors.length > 0
        ? 'partial'
        : 'completed';
    await finalizeRunRow(supabase, runId, status, stats);
  }

  printSummary(args.mode, stats, errored);

  if (errored) process.exit(1);
}

main().catch(err => {
  console.error('[infra-sync] fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
