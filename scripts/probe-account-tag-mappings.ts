import { InstantlyClient } from '../src/instantly';
import { parseInstantlyKeyMap } from '../src/instantly-key-map';
import { probeAccountTagMappings } from '../src/infra/account-tags';

function toSlug(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, '-');
}

async function main(): Promise<void> {
  const keysJson = process.env.INSTANTLY_API_KEYS;
  if (!keysJson) throw new Error('Missing INSTANTLY_API_KEYS');
  const keyMap = parseInstantlyKeyMap(keysJson);
  const filter = process.env.WORKSPACE_FILTER ? toSlug(process.env.WORKSPACE_FILTER) : undefined;
  const selected = Object.entries(keyMap).filter(([slug]) => !filter || slug === filter).slice(0, filter ? 1 : 2);
  if (selected.length === 0) throw new Error(`No workspace matched WORKSPACE_FILTER=${filter ?? ''}`);

  const maxPagesRaw = process.env.MAX_MAPPING_PAGES;
  const maxMappingPages = maxPagesRaw ? Number(maxPagesRaw) : undefined;
  if (maxPagesRaw && (!Number.isFinite(maxMappingPages) || (maxMappingPages as number) <= 0)) {
    throw new Error(`Invalid MAX_MAPPING_PAGES=${maxPagesRaw}`);
  }

  let available = false;
  for (const [slug, key] of selected) {
    const client = new InstantlyClient(key);
    const result = await probeAccountTagMappings(slug, client, { maxMappingPages });
    if (result.verdict === 'ACCOUNT_TAGS_AVAILABLE') available = true;
    console.log(`workspace=${result.workspace}`);
    console.log(`accounts_seen=${result.accountsSeen}`);
    console.log(`mappings_seen=${result.mappingsSeen}`);
    console.log(`resource_type_counts=${JSON.stringify(result.resourceTypeCounts)}`);
    console.log(`account_matching_resource_types=${JSON.stringify(result.accountMatchingResourceTypes)}`);
    console.log(`sample_resource_ids_by_type=${JSON.stringify(result.sampleResourceIdsByType)}`);
    console.log(`sample_account_emails=${JSON.stringify(result.sampleAccountEmails)}`);
    console.log(`sample_matches=${JSON.stringify(result.sampleMatches)}`);
    console.log(`chosen_account_resource_type=${result.chosenResourceType ?? ''}`);
    console.log(`verdict=${result.verdict}`);
  }
  if (!available) process.exit(2);
}

main().catch((err) => {
  console.error('[account-tag-probe] Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
