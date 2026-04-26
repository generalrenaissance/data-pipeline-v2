import { parseSheetDump, syncSheetRegistry } from '../src/infra/sheet-registry';
import { SupabaseClient } from '../src/supabase';

function countBy<T extends string>(values: T[]): Record<T, number> {
  const out = {} as Record<T, number>;
  for (const v of values) out[v] = (out[v] ?? 0) + 1;
  return out;
}

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');
  const dumpDir = process.env.SHEET_DUMP_DIR ?? '/tmp/renaissance_sheet';
  const parsed = await parseSheetDump(dumpDir);

  console.log(`[sheet-registry] dump_dir=${dumpDir}`);
  console.log('[sheet-registry] Funding.json row 0 column G: Campaign Manager');
  console.log(`[sheet-registry] sheet_rows=${parsed.sheetRows.length}`);
  console.log(`[sheet-registry] brand_rows=${parsed.brandRows.length}`);
  console.log(`[sheet-registry] cancelled_rows=${parsed.cancelledRows.length}`);
  console.log('[sheet-registry] sheet_confidence=', countBy(parsed.sheetRows.map(r => r.row_confidence)));
  console.log('[sheet-registry] cancelled_confidence=', countBy(parsed.cancelledRows.map(r => r.row_confidence)));
  for (const warning of parsed.warnings.slice(0, 20)) {
    console.log(`[sheet-registry] warning=${warning}`);
  }

  if (dryRun) return;

  const url = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const key = process.env.PIPELINE_SUPABASE_KEY ?? process.env.PIPELINE_SUPABASE_SERVICE_ROLE_KEY ?? process.env.SUPABASE_KEY;
  if (!url || !key) {
    throw new Error('Missing PIPELINE_SUPABASE_URL and PIPELINE_SUPABASE_KEY');
  }
  await syncSheetRegistry(new SupabaseClient(url, key), parsed);
  console.log('[sheet-registry] synced=true');
}

main().catch((err) => {
  console.error('[sheet-registry] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
