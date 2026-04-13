/**
 * CLI runner for conversation messages sync.
 *
 * Usage:
 *   npx tsx tools/conversation-messages-sync.ts                     # incremental, all workspaces
 *   npx tsx tools/conversation-messages-sync.ts --full              # full backfill
 *   npx tsx tools/conversation-messages-sync.ts --ws renaissance-6  # single workspace
 *   npx tsx tools/conversation-messages-sync.ts --full --ws renaissance-6
 */

import * as fs from 'fs';
import * as path from 'path';
import { loadWorkspaceKeys, loadWorkspaceKeysFromJson, syncAllWorkspaces } from '../src/conversation-messages-sync';

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
// Load repo .env for Supabase first. For local key files, support both a repo-
// local .env.instantly and the older parent-directory layout used inside the
// Renaissance mono-root.
loadDotenv(process.env.DOTENV_PATH ?? path.join(repoRoot, '.env'));
loadDotenv(path.join(repoRoot, '.env.instantly'));
loadDotenv(path.join(repoRoot, '..', '.env.instantly'));

// Also load from INSTANTLY_API_KEYS JSON env var (GitHub Actions uses this)
if (process.env.INSTANTLY_API_KEYS) {
  loadWorkspaceKeysFromJson(process.env.INSTANTLY_API_KEYS);
} else {
  loadWorkspaceKeys('INSTANTLY_KEY_');
}

async function main() {
  const supabaseUrl = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const supabaseKey = process.env.PIPELINE_SUPABASE_KEY ?? process.env.SUPABASE_KEY;

  if (!supabaseUrl || !supabaseKey) {
    console.error('[conversation-sync] Missing PIPELINE_SUPABASE_URL/KEY or SUPABASE_URL/KEY');
    process.exit(1);
  }

  const args = process.argv.slice(2);
  const full = args.includes('--full');
  const wsIdx = args.indexOf('--ws');
  const workspaceFilter = wsIdx !== -1 ? args[wsIdx + 1] : undefined;

  await syncAllWorkspaces(supabaseUrl, supabaseKey, {
    full,
    workspaceFilter,
  });
}

main().catch((err) => {
  console.error('[conversation-sync] Fatal error:', err);
  process.exit(1);
});
