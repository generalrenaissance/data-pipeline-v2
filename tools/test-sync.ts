/**
 * test-sync.ts - Local test harness for the data pipeline sync.
 *
 * Reads credentials from .env in data-pipeline-v2/ root.
 * No deploy required. Full console output. Feedback loop: under 2 minutes.
 *
 * Usage:
 *   npx tsx tools/test-sync.ts                    # all workspaces
 *   npx tsx tools/test-sync.ts equinox            # single workspace
 *   npx tsx tools/test-sync.ts equinox,funding-1  # multiple workspaces
 *   npx tsx tools/test-sync.ts --inbox            # inbox run (all workspaces)
 *
 * Requires .env in data-pipeline-v2/ with:
 *   INSTANTLY_API_KEYS={"workspace-slug":"api-key",...}
 *   SUPABASE_URL=https://nmkaydqcnkjsehyqokgg.supabase.co
 *   SUPABASE_KEY=<service role key>
 */

import * as fs from 'fs';
import * as path from 'path';
import { parseInstantlyKeyMap } from '../src/instantly-key-map';
import { syncAllWorkspaces } from '../src/sync';

// Load .env manually from repo root (no dotenv dependency needed)
function loadDotenv(filePath: string): void {
  if (!fs.existsSync(filePath)) return;
  const content = fs.readFileSync(filePath, 'utf8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (key && !(key in process.env)) {
      process.env[key] = val;
    }
  }
}

const repoRoot = path.resolve(__dirname, '..');
const dotenvPath = process.env.DOTENV_PATH ?? path.join(repoRoot, '.env');
loadDotenv(dotenvPath);

async function main() {
  const keysRaw = process.env.INSTANTLY_API_KEYS;
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_KEY;

  if (!keysRaw) {
    console.error('[test-sync] Missing INSTANTLY_API_KEYS — set in .env');
    console.error(`  Tried: ${dotenvPath}`);
    process.exit(1);
  }
  if (!supabaseUrl || !supabaseKey) {
    console.error('[test-sync] Missing SUPABASE_URL or SUPABASE_KEY');
    process.exit(1);
  }

  let keyMap: Record<string, string>;
  try {
    keyMap = parseInstantlyKeyMap(keysRaw);
  } catch (e) {
    console.error('[test-sync] Failed to parse INSTANTLY_API_KEYS as JSON:', e);
    process.exit(1);
  }

  // Parse workspace filter from positional args (skip flags like --inbox)
  const args = process.argv.slice(2).filter(a => !a.startsWith('--'));
  if (args.length > 0) {
    const slugs = new Set(args.flatMap(a => a.split(',').map(s => s.trim())).filter(Boolean));
    const before = Object.keys(keyMap).length;
    keyMap = Object.fromEntries(Object.entries(keyMap).filter(([slug]) => slugs.has(slug)));
    if (Object.keys(keyMap).length === 0) {
      console.error(`[test-sync] No matching workspaces for: ${[...slugs].join(', ')}`);
      console.error(`[test-sync] Available: ${Object.keys(parseInstantlyKeyMap(keysRaw)).join(', ')}`);
      process.exit(1);
    }
    console.log(`[test-sync] Workspace filter: ${Object.keys(keyMap).length}/${before} workspaces`);
  }

  const runType: 'full' | 'inbox' | 'daily_metrics' = process.argv.includes('--daily-metrics')
    ? 'daily_metrics'
    : process.argv.includes('--inbox')
      ? 'inbox'
      : 'full';
  console.log(`[test-sync] Starting ${runType} sync for ${Object.keys(keyMap).length} workspaces`);
  console.log(`[test-sync] Supabase: ${supabaseUrl}`);
  const start = Date.now();

  await syncAllWorkspaces(keyMap, supabaseUrl, supabaseKey, runType);

  const elapsed = Math.round((Date.now() - start) / 1000);
  console.log(`[test-sync] Done in ${elapsed}s`);
}

main().catch((e) => {
  console.error('[test-sync] Fatal:', e instanceof Error ? e.message : e);
  process.exit(1);
});
