// CLI entry point for the domain RR scorer. The library is in
// src/infra/score-domain-rr.ts. Runs hourly via
// .github/workflows/domain-rr-score.yml.

import { scoreDomainRr } from '../src/infra/score-domain-rr';

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');
  const result = await scoreDomainRr({ dryRun });

  console.log(`[rr-scorer] metrics_read=${result.metrics_read}`);
  console.log(`[rr-scorer] domains_evaluated=${result.domains_evaluated}`);
  console.log(`[rr-scorer] state_inserts=${result.state_inserts}`);
  console.log(`[rr-scorer] state_updates=${result.state_updates}`);
  console.log(`[rr-scorer] status_unchanged=${result.status_unchanged}`);
  console.log(`[rr-scorer] events_written=${result.events_written}`);
  console.log(`[rr-scorer] duration_ms=${result.duration_ms}`);

  const sortedDist = Object.entries(result.status_distribution).sort(([a], [b]) => a.localeCompare(b));
  for (const [key, count] of sortedDist) {
    console.log(`[rr-scorer] dist ${key}=${count}`);
  }

  if (dryRun) {
    console.log(`[rr-scorer] dry-run, no writes performed`);
  }
}

main().catch(err => {
  console.error('[rr-scorer] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
