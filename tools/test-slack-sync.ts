import * as fs from 'fs';
import * as path from 'path';
import { syncMeetingsBooked } from '../src/slack-sync';

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
loadDotenv(process.env.DOTENV_PATH ?? path.join(repoRoot, '.env'));

async function main() {
  const slackToken = process.env.SLACK_TOKEN;
  const slackCookie = process.env.SLACK_COOKIE;
  const supabaseUrl = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const supabaseKey = process.env.PIPELINE_SUPABASE_KEY ?? process.env.SUPABASE_KEY;

  if (!slackToken) {
    console.error('[test-slack-sync] Missing SLACK_TOKEN');
    process.exit(1);
  }
  if (!supabaseUrl || !supabaseKey) {
    console.error('[test-slack-sync] Missing PIPELINE_SUPABASE_URL/KEY or SUPABASE_URL/KEY');
    process.exit(1);
  }

  await syncMeetingsBooked(slackToken, supabaseUrl, supabaseKey, slackCookie);
  console.log('Done.');
}

main().catch(console.error);
