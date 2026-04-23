export interface CampaignTagResolution {
  cachedTags: string[];
  legacyTags: string[];
  resolvedTags: string[];
  shouldBackfillCache: boolean;
}

function normalizeTags(tags: string[]): string[] {
  return Array.from(
    new Set(
      tags
        .map(tag => tag.trim())
        .filter(Boolean),
    ),
  ).sort();
}

export function resolveCampaignTagSources(
  cachedTags: string[] | undefined,
  legacyTags: string[],
): CampaignTagResolution {
  const normalizedCached = normalizeTags(cachedTags ?? []);
  const normalizedLegacy = normalizeTags(legacyTags);

  return {
    cachedTags: normalizedCached,
    legacyTags: normalizedLegacy,
    resolvedTags: normalizeTags([...normalizedCached, ...normalizedLegacy]),
    shouldBackfillCache: normalizedLegacy.length > 0 && normalizedCached.length === 0,
  };
}

export function selectWorkspacesForRefresh(
  keyMap: Record<string, string>,
  workspaceFilter: Set<string>,
  shardIndex?: number,
  shardCount?: number,
): Record<string, string> {
  let entries = Object.entries(keyMap).sort(([left], [right]) => left.localeCompare(right));

  if (workspaceFilter.size > 0) {
    entries = entries.filter(([slug]) => workspaceFilter.has(slug));
    return Object.fromEntries(entries);
  }

  if (shardCount === undefined || shardCount <= 1) {
    return Object.fromEntries(entries);
  }

  if (shardIndex === undefined || shardIndex < 0 || shardIndex >= shardCount) {
    throw new Error(`Invalid workspace shard: index=${shardIndex ?? 'unset'} count=${shardCount}`);
  }

  return Object.fromEntries(
    entries.filter((_, index) => index % shardCount === shardIndex),
  );
}
