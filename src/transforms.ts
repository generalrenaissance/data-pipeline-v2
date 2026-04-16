/**
 * Strip HTML tags from email body, normalize whitespace.
 */
export function stripHtml(html: string): string {
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')
    .replace(/&nbsp;/g, ' ').replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * Resolve spintax to first option.
 * Handles {{RANDOM|a|b|c}} (Instantly) and {a|b|c} (standard).
 * Nested spintax resolved inside-out.
 */
export function resolveSpintax(text: string): string {
  let resolved = text;
  let prev = '';
  while (resolved !== prev) {
    prev = resolved;
    // {{RANDOM|a|b|c}} — Instantly double-brace format
    resolved = resolved.replace(/\{\{RANDOM\|([^}]+)\}\}/g, (_, options) =>
      options.split('|')[0].trim()
    );
    // {a|b|c} — standard spintax with pipes
    resolved = resolved.replace(/\{([^{}]+\|[^{}]+)\}/g, (_, options) =>
      options.split('|')[0].trim()
    );
  }
  // {RANDOM...} — catch-all for any remaining {RANDOM} blocks
  // Handles: {RANDOM text}, {RANDOMtext}, {RANDOM\nline1\nline2}, {RANDOM, text}, {RANDOM?|text}
  // Takes first meaningful chunk after the RANDOM keyword
  resolved = resolved.replace(/\{RANDOM([^}]*)\}/gs, (_, content) => {
    if (!content) return '';
    // Split on newlines, pipes, or commas and take the first non-empty chunk
    const firstChunk = content.split(/[\n|,]/)[0]
      .replace(/^[?\s]+/, '') // strip leading ? or whitespace
      .trim();
    return firstChunk;
  });
  return resolved;
}

/**
 * Plain-text resolved version of email body.
 * Strips HTML, resolves spintax to first option.
 */
export function resolveBody(raw: string): string {
  return resolveSpintax(stripHtml(raw));
}

export function resolveSubject(raw: string): string {
  return resolveSpintax(raw.replace(/<[^>]+>/g, '').trim());
}

/**
 * Whitelist of known CM names. Used to validate regex matches and avoid
 * false positives like "(copy)" or "(X)" being treated as CM names.
 */
const KNOWN_CMS = new Set([
  'EYVER', 'TOMI', 'CARLOS', 'BRENDAN', 'LEO', 'IDO',
  'MARCOS', 'SHAAN', 'ANDRES', 'LAUTARO', 'ALEX', 'SAMUEL',
  'DENVER', 'GRACE',
]);

/**
 * Parse CM name from campaign name using multiple pattern strategies.
 *
 * Pattern 1: (NAME) or (NAME) RB/X at end — most common
 *   "ON - Pair 5 - British (CARLOS)" → "CARLOS"
 *   "ON - PAIR 10 - REAL ESTATE (ANDRES)" → "ANDRES"
 *
 * Pattern 2: - NAME or - NAME N at end — used in The Eagles workspace
 *   "ON - A - CEO - LAUTARO 1" → "LAUTARO"
 *   "ON - A - CLEANING - LAUTARO" → "LAUTARO"
 *
 * Returns null if no known CM name found.
 */
export function parseCmName(campaignName: string): string | null {
  const upper = campaignName.toUpperCase();

  // Pattern 1: (NAME) optionally followed by RB, X, or whitespace at end
  const parenMatch = upper.match(/\(([A-Z]+)\)\s*(?:RB|X)?\s*$/);
  if (parenMatch && KNOWN_CMS.has(parenMatch[1])) {
    return parenMatch[1];
  }

  // Pattern 2: dash/space then NAME optionally followed by number at end
  const dashMatch = upper.match(/[-\s]([A-Z]{3,})\s*\d*\s*$/);
  if (dashMatch && KNOWN_CMS.has(dashMatch[1])) {
    return dashMatch[1];
  }

  return null;
}

/**
 * Parse RG batch IDs from campaign name.
 * Example: "RG49/RG50/RG51" or "RG1934 RG1935 RG1936"
 */
export function parseRgBatchIds(campaignName: string): string[] {
  return campaignName.match(/RG\d+/g) ?? [];
}

/**
 * Known lead source tag labels from Instantly custom tags.
 * Used to classify which custom tag represents the lead source.
 */
export const LEAD_SOURCE_TAGS = new Set([
  'Apollo', 'Apollo Not Found', 'ZoomInfo', 'Google Maps', 'Sales Nav',
  'Claygent', 'Leadrock', 'Leads From Anwar', 'Leads From Ben',
  'Leads From Tufan', 'Leads From Britshi', 'AudienceLab', 'Allforce',
  'A-Leads', 'Ben Leads',
]);

/**
 * Classify lead source from a campaign's custom tags.
 * Returns the first matching lead source tag label, or null.
 */
export function classifyLeadSource(customTags: string[]): string | null {
  for (const tag of customTags) {
    if (LEAD_SOURCE_TAGS.has(tag)) return tag;
  }
  return null;
}

// ── Signature Extractor ────────────────────────────────────────────────────
// Ported from tools/backfill-campaign-data.mjs

const CLOSING_PHRASES = new Set([
  'best', 'best regards', 'warm regards', 'kind regards', 'thanks',
  'thanks again', 'talk soon', 'appreciate your time', 'all the best',
  'take care', 'sincerely', 'regards',
]);

const CLOSING_RE = new RegExp(
  '^(' + [...CLOSING_PHRASES].map(p => p.replace(/\s+/g, '\\s+')).join('|') + ')\\s*,?\\s*$',
  'im',
);

function splitAtDepth(text: string): string[] {
  const options: string[] = [];
  let current = '';
  let depth = 0;
  let i = 0;
  while (i < text.length) {
    if (text[i] === '{' && text[i + 1] === '{') {
      depth++; current += '{{'; i += 2;
    } else if (text[i] === '}' && text[i + 1] === '}') {
      depth--; current += '}}'; i += 2;
    } else if (text[i] === '|' && depth === 0) {
      options.push(current); current = ''; i++;
    } else {
      current += text[i]; i++;
    }
  }
  options.push(current);
  return options;
}

function isClosingLine(line: string): boolean {
  const trimmed = line.trim();
  if (!trimmed) return false;
  if (CLOSING_RE.test(trimmed)) return true;
  const cleaned = trimmed.replace(/,?\s*$/, '');
  const m = cleaned.match(/^\{\{RANDOM\|(.+)\}\}$/);
  if (m) {
    const options = splitAtDepth(m[1]);
    const firstResolved = resolveSpintax(options[0]).trim().replace(/,?\s*$/, '').toLowerCase();
    if (CLOSING_PHRASES.has(firstResolved)) return true;
  }
  return false;
}

export function extractSignature(body: string): { body: string; signature: string } {
  if (!body) return { body: '', signature: '' };
  const lines = body.split('\n');
  const searchStart = Math.max(0, lines.length - 10);
  for (let i = searchStart; i < lines.length; i++) {
    if (isClosingLine(lines[i])) {
      return {
        body: lines.slice(0, i).join('\n').trim(),
        signature: lines.slice(i).join('\n').trim(),
      };
    }
  }
  return { body: body.trim(), signature: '' };
}

// ── Segment Extraction from Campaign Name ─────────────────────────────────
// Keyword-based extraction. First match wins. NULL if no match.
// Do NOT derive segment from Instantly tags — they are batch/infra identifiers.

const SEGMENT_KEYWORDS: Array<[string[], string]> = [
  [['home services', 'home service', 'home improvement', 'home imp', 'property maintenance', 'prop maintenance', 'prop mainten', 'roofing', 'contractors', 'electrician'], 'home_services'],
  [['finance', 'accounting', 'controller', 'controllers', 'cfo', 'vp finance', 'finance exec'], 'finance_accounting'],
  [['manufacturing', 'manufacturer', 'manufactur', 'manu'], 'manufacturing'],
  [['consulting', 'consultants', 'consultant', 'professional services', 'professional service'], 'consulting'],
  [['nonprofit', 'non-profit', 'non profit', 'nonprofits', 'associations', 'association'], 'nonprofit'],
  [['e-commerce', 'ecommerce', 'ecom'], 'ecommerce'],
  [['spanish', 'latino', 'hispanic'], 'spanish_speaking'],
  [['restaurant', 'bar', 'bars', 'catering', 'dining', 'hospitality', 'hotel', 'hotels', 'bakery'], 'restaurant'],
  [['construction'], 'construction'],
  [['cleaning'], 'cleaning'],
  [['trucking', 'logistics', 'courier'], 'trucking'],
  [['hvac'], 'hvac'],
  [['landscaping', 'lawn'], 'landscaping'],
  [['retail', 'clothing', 'beauty', 'salon', 'department store', 'store', 'shops', 'shop'], 'retail'],
  [['advertising'], 'advertising'],
  [['insurance'], 'insurance'],
  [['law firm', 'law firms', 'legal', 'patent attorney'], 'legal'],
  [['auto', 'automotive'], 'automotive'],
  [['healthcare', 'medical', 'mental health', 'psychiatrist', 'therapy', 'physical therapy', 'chiropractor', 'acupuncture', 'accupunture', 'medspa'], 'healthcare'],
  [['presidents', 'president'], 'presidents'],
  [['ceos', 'ceo'], 'ceos'],
  [['general', ' gen ', 'gen ', 'smb owners', 'smb', 'owners', 'business owner'], 'general'],
];

export function extractSegmentFromName(campaignName: string): string | null {
  const lower = campaignName.toLowerCase();
  for (const [keywords, segment] of SEGMENT_KEYWORDS) {
    for (const kw of keywords) {
      if (lower.includes(kw)) return segment;
    }
  }
  return null;
}

// ── Tag Classification ─────────────────────────────────────────────────────
// Splits the flat tags[] array into 4 typed buckets.

const RG_TAG_RE = /^RG\d[\d-]*$/i;
const PAIR_TAG_RE = /^(pair\s+\d+(\s+\w+)*|adonis\s+pair)$/i;
const KD_RE = /^KD[\s\d]/i;
const B42_RE = /^B42\./i;

function toTitleCase(str: string): string {
  return str.replace(/\w+/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
}

export interface TagClassification {
  rg_batch_tags: string[];
  pair_tag: string | null;
  sender_tags: string[];
  other_tags: string[];
}

export function classifyTags(tags: string[]): TagClassification {
  const rg_batch_tags: string[] = [];
  let pair_tag: string | null = null;
  const sender_tags: string[] = [];
  const other_tags: string[] = [];

  for (const raw of tags) {
    const tag = raw.replace(/\t/g, ' ').trim();
    if (!tag) continue;

    if (RG_TAG_RE.test(tag)) {
      rg_batch_tags.push(tag.toUpperCase());
      continue;
    }

    if (PAIR_TAG_RE.test(tag)) {
      if (!pair_tag) pair_tag = toTitleCase(tag);
      continue;
    }

    // Sender: exactly two Title-cased words, not a known non-name pattern
    const parts = tag.split(/\s+/);
    if (
      parts.length === 2 &&
      /^[A-Z][a-z]+$/.test(parts[0]) &&
      /^[A-Z][a-z]+$/.test(parts[1]) &&
      !KD_RE.test(tag) &&
      !B42_RE.test(tag) &&
      !LEAD_SOURCE_TAGS.has(tag)
    ) {
      sender_tags.push(tag);
      continue;
    }

    other_tags.push(tag);
  }

  return { rg_batch_tags, pair_tag, sender_tags, other_tags };
}

// ── Product Classification ─────────────────────────────────────────────────

export function classifyProduct(campaignName: string, _tags: string[]): string {
  const upper = campaignName.toUpperCase();
  if (upper.includes('ERC')) return 'ERC';
  if (upper.includes('SECTION 125') || upper.includes('S125')) return 'S125';
  return 'FUNDING';
}

// ── Infra Type ─────────────────────────────────────────────────────────────

export function deriveInfraType(workspaceSlug: string): 'outlook' | 'google' {
  return workspaceSlug.startsWith('outlook') ? 'outlook' : 'google';
}

// ── Workspace CM Defaults ──────────────────────────────────────────────────

/**
 * Workspace slug → default CM name fallback.
 * Used only when parseCmName returns null.
 * Derived from live campaign data — single-CM workspaces only.
 * Mixed-CM workspaces (renaissance-4, renaissance-5) are left null
 * because parseCmName handles them per-campaign.
 */
export const WORKSPACE_CM_DEFAULTS: Record<string, string | null> = {
  'renaissance-1': 'IDO',
  'renaissance-2': 'EYVER',
  'renaissance-3': null,
  'renaissance-4': null,   // mixed: EYVER, ANDRES, CARLOS, ALEX, LEO
  'renaissance-5': null,   // mixed: ALEX, EYVER, MARCOS
  'renaissance-6': null,
  'renaissance-7': null,
  'koi-and-destroy': 'TOMI',
  'the-dyad': 'CARLOS',
  'the-gatekeepers': 'BRENDAN',
  'equinox': 'LEO',
  'outlook-1': 'IDO',
  'outlook-2': 'MARCOS',
  'outlook-3': 'LEO',
  'prospects-power': 'SHAAN',
  'automated-applications': 'EYVER',
  'warm-leads': null,
  'section-125-1': 'IDO',
  'section-125-2': null,
  'erc-1': null,
  'erc-2': null,
  'the-eagles': 'LAUTARO',  // all campaigns are LAUTARO, parseCmName catches via pattern 2
};

/**
 * Workspace slug → human display name.
 * Written to campaign_data.workspace_name (and sender_inboxes.workspace_name).
 * workspace_id always remains the slug — this map only affects display.
 *
 * When an Instantly workspace is renamed, update the value here. The slug
 * (INSTANTLY_KEY_MAP secret key) stays stable so downstream joins don't break.
 * Unknown slugs fall back to the slug itself.
 */
export const WORKSPACE_DISPLAY_NAMES: Record<string, string> = {
  'renaissance-1': 'Renaissance 1',
  'renaissance-2': 'Renaissance 2',
  'renaissance-3': 'Renaissance 3',
  'renaissance-4': 'Renaissance 4',
  'renaissance-5': 'Renaissance 5',
  'renaissance-6': 'Renaissance 6',
  'renaissance-7': 'Renaissance 7',
  'koi-and-destroy': 'Koi and Destroy',
  'the-dyad': 'The Dyad',
  'the-gatekeepers': 'The Gatekeepers',
  'the-eagles': 'The Eagles',
  'equinox': 'Equinox',
  'outlook-1': 'Outlook 1',
  'outlook-2': 'Outlook 2',
  'outlook-3': 'Outlook 3',
  'prospects-power': 'Prospects Power',
  'automated-applications': 'Automated Applications',
  'warm-leads': 'Warm Leads',
  'section-125-1': 'Section 125 1',
  'section-125-2': 'Section 125 2',
  'erc-1': 'Tariffs + Funding',
  'erc-2': 'ERC 2',
};

export function workspaceDisplayName(slug: string): string {
  return WORKSPACE_DISPLAY_NAMES[slug] ?? slug;
}
