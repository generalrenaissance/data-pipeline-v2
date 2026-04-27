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

/**
 * Map an Instantly account-level provider_code to Renaissance's internal
 * ProviderGroup taxonomy.
 *
 * Source: Instantly OpenAPI components.schemas["def-0"].provider_code enum
 *   1 = Custom IMAP/SMTP   -> Renaissance OTD (per Sam 2026-04-26)
 *   2 = Google             -> google
 *   3 = Microsoft          -> outlook
 *   4 = AWS                -> unknown (zero accounts in estate; Sam decision)
 *   8 = AirMail            -> unknown (zero accounts in estate; Sam decision)
 *   null/other             -> unknown
 *
 * Codes 1 + 2 are bundled into a single `google_otd` group because they share
 * the same retire threshold (0.5%; see score-domain-rr.ts).
 */
export function accountProviderCodeToGroup(
  code: number | null | undefined,
): ProviderGroup {
  if (code === 1 || code === 2) return 'google_otd';
  if (code === 3) return 'outlook';
  return 'unknown';
}

/**
 * @deprecated Slug-based provider attribution is unreliable. Workspaces
 * routinely contain accounts from multiple providers. Use
 * accountProviderCodeToGroup() with raw_account.provider_code instead.
 * Retained for backwards compatibility with tests; do not call from new code
 * paths.
 */
export function slugToProviderGroup(slug: string): ProviderGroup {
  return KNOWN_SLUG_TO_PROVIDER_GROUP[slug] ?? 'unknown';
}
