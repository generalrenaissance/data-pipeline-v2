import { rebuildDomainRegistry } from '../src/infra/domain-registry';
import { SupabaseClient } from '../src/supabase';

async function main(): Promise<void> {
  const url = process.env.PIPELINE_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const key = process.env.PIPELINE_SUPABASE_KEY ?? process.env.PIPELINE_SUPABASE_SERVICE_ROLE_KEY ?? process.env.SUPABASE_KEY;
  if (!url || !key) throw new Error('Missing PIPELINE_SUPABASE_URL and PIPELINE_SUPABASE_KEY');
  const result = await rebuildDomainRegistry(new SupabaseClient(url, key));
  console.log(`[domain-registry] rows_written=${result.rowsWritten}`);
  console.log(`[domain-registry] mapping_status_counts=${JSON.stringify(result.statusCounts)}`);
}

main().catch((err) => {
  console.error('[domain-registry] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
