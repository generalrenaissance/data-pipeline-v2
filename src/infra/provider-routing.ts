export type ProviderGroup = 'google_otd' | 'outlook' | 'unknown';

export const EXCLUDED_SLUGS: readonly string[] = ['personal', 'sam-test'];

const KNOWN_SLUG_TO_PROVIDER_GROUP: Readonly<Record<string, ProviderGroup>> = {
  'outlook-1': 'outlook',
  'outlook-2': 'outlook',
  'outlook-3': 'outlook',
  'renaissance-1': 'google_otd',
  'renaissance-2': 'google_otd',
  'renaissance-3': 'google_otd',
  'renaissance-4': 'google_otd',
  'renaissance-5': 'google_otd',
  'renaissance-6': 'google_otd',
  'renaissance-7': 'google_otd',
  'renaissance-8': 'google_otd',
};

export function slugToProviderGroup(slug: string): ProviderGroup {
  return KNOWN_SLUG_TO_PROVIDER_GROUP[slug] ?? 'unknown';
}
