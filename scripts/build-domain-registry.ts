import { rebuildDomainRegistry } from '../src/infra/domain-registry';

// rebuildDomainRegistry now calls the server-side SQL function via direct
// postgres connection (PIPELINE_SUPABASE_DB_URL). PostgREST URL+KEY are no
// longer needed for this script; they remain available for other scripts.
async function main(): Promise<void> {
  if (!process.env.PIPELINE_SUPABASE_DB_URL) {
    throw new Error('Missing PIPELINE_SUPABASE_DB_URL');
  }
  const result = await rebuildDomainRegistry();
  console.log(`[domain-registry] rows_written=${result.rowsWritten}`);
  console.log(`[domain-registry] mapping_status_counts=${JSON.stringify(result.statusCounts)}`);
}

main().catch((err) => {
  console.error('[domain-registry] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
