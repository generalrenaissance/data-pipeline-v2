import * as fs from 'fs';
import * as path from 'path';

import {
  SEEDED_CAMPAIGN_ALIASES,
  buildResolverContext,
  resolveCampaignName,
  sqlLiteral,
  type CampaignRecord,
} from '../src/meetings-matcher';
import { SupabaseClient } from '../src/supabase';

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
loadDotenv(process.env.DOTENV_PATH ?? '/Users/sam/Documents/Claude Code/Renaissance/.env');

interface MeetingRow {
  campaign_name_raw: string | null;
  posted_at: string | null;
}

interface Counts {
  total: number;
  last60d: number;
  last24h: number;
}

function summarizeRows(rows: MeetingRow[]): Map<string, Counts> {
  const now = Date.now();
  const sixtyDaysAgo = now - 60 * 24 * 3600_000;
  const twentyFourHoursAgo = now - 24 * 3600_000;
  const counts = new Map<string, Counts>();

  for (const row of rows) {
    if (!row.campaign_name_raw) continue;
    const current = counts.get(row.campaign_name_raw) ?? { total: 0, last60d: 0, last24h: 0 };
    current.total += 1;

    if (row.posted_at) {
      const postedAt = Date.parse(row.posted_at);
      if (Number.isFinite(postedAt) && postedAt >= sixtyDaysAgo) current.last60d += 1;
      if (Number.isFinite(postedAt) && postedAt >= twentyFourHoursAgo) current.last24h += 1;
    }

    counts.set(row.campaign_name_raw, current);
  }

  return counts;
}

async function main() {
  const outputPath =
    process.argv.find(arg => arg.startsWith('--output='))?.split('=', 2)[1] ??
    path.join(repoRoot, 'sql', '2026-04-22-meetings-backfill-preview.sql');

  const supabaseUrl = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const supabaseKey =
    process.env.PIPELINE_SUPABASE_KEY ??
    process.env.PIPELINE_SUPABASE_SERVICE_ROLE_KEY ??
    process.env.SUPABASE_KEY;

  if (!supabaseUrl || !supabaseKey) {
    console.error('[meetings-preview] Missing PIPELINE_SUPABASE_URL / PIPELINE_SUPABASE_KEY');
    process.exit(1);
  }

  const db = new SupabaseClient(supabaseUrl, supabaseKey);
  const [campaigns, rows] = await Promise.all([
    db.selectAll('campaigns', 'select=campaign_id,name&name=not.is.null') as Promise<CampaignRecord[]>,
    db.selectAll(
      'meetings_booked_raw',
      'select=campaign_name_raw,posted_at&campaign_id=is.null',
    ) as Promise<MeetingRow[]>,
  ]);

  const context = buildResolverContext(campaigns, SEEDED_CAMPAIGN_ALIASES);
  const countsByName = summarizeRows(rows);

  let matchedTotal = 0;
  let matched60d = 0;
  let matched24h = 0;
  let matchedNames = 0;
  const unresolved: Array<{ rawName: string; total: number; resolution: ReturnType<typeof resolveCampaignName> }> = [];
  const statements: string[] = [];

  for (const [rawName, counts] of countsByName) {
    const resolution = resolveCampaignName(rawName, context);
    if (resolution.kind === 'match') {
      matchedNames += 1;
      matchedTotal += counts.total;
      matched60d += counts.last60d;
      matched24h += counts.last24h;
      statements.push(
        `update public.meetings_booked_raw set campaign_id = ${sqlLiteral(resolution.campaignId)}, match_method = ${sqlLiteral(
          resolution.matchMethod,
        )}, match_confidence = ${resolution.matchConfidence} where campaign_id is null and campaign_name_raw = ${sqlLiteral(
          rawName,
        )};`,
      );
      continue;
    }

    unresolved.push({ rawName, total: counts.total, resolution });
  }

  unresolved.sort((left, right) => right.total - left.total || left.rawName.localeCompare(right.rawName));

  const lines = [
    '-- Meetings matcher backfill preview [2026-04-22]',
    `-- Generated at ${new Date().toISOString()}`,
    `-- Unmatched rows in scope: ${rows.length}`,
    `-- Unique unmatched names in scope: ${countsByName.size}`,
    `-- Deterministic matches recover ${matchedTotal} rows across ${matchedNames} names`,
    `-- Last 60d deterministic recoveries: ${matched60d}`,
    `-- Last 24h deterministic recoveries: ${matched24h}`,
    '-- Review before applying. This file is intentionally not auto-run.',
    '',
    'begin;',
    ...statements,
    'commit;',
    '',
    '-- Top unresolved names for manual review',
    ...unresolved.slice(0, 40).map(entry => {
      if (entry.resolution.kind === 'queue') {
        const candidates = entry.resolution.topCandidates
          .map(candidate => `${candidate.campaign_name} (${candidate.score})`)
          .join(' | ') || 'no viable candidates';
        return `-- ${entry.rawName} [${entry.resolution.queueReason}] x${entry.total}: ${candidates}`;
      }
      return `-- ${entry.rawName} [ignored_linkedin] x${entry.total}`;
    }),
    '',
  ];

  fs.writeFileSync(outputPath, lines.join('\n'));

  console.log(`[meetings-preview] wrote ${outputPath}`);
  console.log(
    `[meetings-preview] deterministic recovery: ${matchedTotal}/${rows.length} rows, ${matchedNames}/${countsByName.size} names`,
  );
  console.log(`[meetings-preview] last 60d recoveries: ${matched60d}`);
  console.log(`[meetings-preview] last 24h recoveries: ${matched24h}`);
}

main().catch(err => {
  console.error('[meetings-preview] fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
