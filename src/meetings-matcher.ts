type QueueReason = 'ambiguous_strict' | 'hard_reject' | 'no_match' | 'ignored_linkedin';

export type MatchMethod = 'alias' | 'strict_auto';

export interface CampaignAliasSeed {
  alias: string;
  campaign_id: string;
  note: string;
  created_by: string;
}

export interface CampaignRecord {
  campaign_id: string;
  name: string;
}

export interface AliasRecord {
  alias: string;
  campaign_id: string;
  note?: string | null;
  created_by?: string | null;
}

export interface QueueRecord {
  campaign_name_raw: string;
  candidate_hash?: string | null;
  review_status?: string | null;
  queue_reason?: string | null;
  top_candidates?: CandidateSuggestion[] | string | null;
  occurrence_count?: number | null;
  source_channels?: string[] | null;
  first_seen_at?: string | null;
  last_seen_at?: string | null;
  last_digest_at?: string | null;
}

export interface MeetingNameStats {
  count: number;
  channelNames: Set<string>;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
}

export interface CandidateSuggestion {
  campaign_id: string;
  campaign_name: string;
  score: number;
}

interface NameMetadata {
  normalized: string;
  strictKey: string;
  pairNumbers: string[];
  cmTag: string | null;
  personCodes: string[];
  rgCodes: string[];
  hasLinkedIn: boolean;
  indexTokens: string[];
}

interface CampaignRuntime extends CampaignRecord {
  metadata: NameMetadata;
}

export interface MatchResolverContext {
  campaigns: CampaignRuntime[];
  aliasMap: Map<string, AliasRecord>;
  campaignMap: Map<string, CampaignRuntime>;
  strictMap: Map<string, CampaignRuntime[]>;
  tokenMap: Map<string, CampaignRuntime[]>;
}

export type MatchResolution =
  | {
      kind: 'match';
      rawName: string;
      campaignId: string;
      campaignName: string;
      matchMethod: MatchMethod;
      matchConfidence: number;
    }
  | {
      kind: 'queue';
      rawName: string;
      queueReason: QueueReason;
      topCandidates: CandidateSuggestion[];
      candidateHash: string;
    }
  | {
      kind: 'ignore';
      rawName: string;
      queueReason: 'ignored_linkedin';
    };

const STATE_PREFIX_PATTERNS = [
  /^\[(?:old|off)\]\s*/i,
  /^old\s*✅\s*/i,
  /^(?:old|on|off)\s*-\s*/i,
  /^(?:old|on|off)\s+/i,
  /^:date:\s*/i,
  /^\p{Extended_Pictographic}+\s*/u,
];

const PERSON_CODE_PATTERNS = ['np', 'rb', 'nm', 'x', 'jessica', 'william', 'kenneth', 'z'];
const NOISE_TOKENS = new Set([
  'on',
  'off',
  'old',
  'copy',
  'with',
  'and',
  'the',
  'meeting',
  'booked',
]);

export const SEEDED_CAMPAIGN_ALIASES: CampaignAliasSeed[] = [
  { alias: 'Advertising - Google + Others', campaign_id: '66df1ad8-7f3a-4636-8c9a-a2b28c5f0cad', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'Auto - Google + others', campaign_id: 'd416aa46-bb44-4cdb-b255-b3190baa929d', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'Cleaning - Google + Others', campaign_id: '34f25f61-3901-468b-abad-92dfdef14589', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'Construction - Google + Others', campaign_id: '32793ed7-48d7-4f1f-8f3c-522b3bbc75e3', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'Construction (General) - Pair 9 - RG2268/RG2269/RG2270/RG2271/RG2272 (BRENDAN)', campaign_id: '54a3f910-37f3-484b-902b-6b8b3d85f5b4', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'Construction 2 - Outlook', campaign_id: '94f7d20c-5774-4da2-be7f-a84c076edb41', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'General - Pair 8 - Quickcred - SHAAN', campaign_id: '23b3e4bf-6967-4a8d-ab0a-766344e4040a', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'KD2 - BrightFunds - Home Improvement(TOMI)', campaign_id: '26d44e26-5fdc-49f1-a034-2cd0d589462a', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'KD5 - Fundora - CEOs(TOMI)', campaign_id: '59d7e965-f313-4281-9255-f201d4f349ee', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'KD6 - Summit Bridge - Founders(TOMI)', campaign_id: '33a8b0f4-87d0-4c4d-9a26-8f7c2f5bef09', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'OFF - RG961+RG2280+RG2281+RG2282 - Flex Group - HEALTHCARE GMAPS - (EYVER) RB', campaign_id: 'ab0e3dd3-049e-42f7-8920-f6a4b9405f79', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - Health Pair 5 (ANDRES) X', campaign_id: '385f1746-ae78-4f07-b188-132778bd4d73', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - Pair - Beauty (Alex', campaign_id: 'd8cc548e-1d3b-4af6-aa50-7000f250d174', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - Pair - Cleaning (Alex)', campaign_id: 'b85ec44c-9e37-46f7-a32b-8b41b861156a', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - Pair 1 - Restaurants (Alex) X', campaign_id: 'ffbc8ad7-acd4-4aac-b818-47b8e94e23ed', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - PAIR 2 - Advertising (MARCOS)', campaign_id: '514dedeb-12be-4c4c-b7ec-8f9175e02ec3', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - PAIR 4 - HVAC (MARCOS)', campaign_id: '8078e9da-272d-4a13-bd46-b419171ea7e3', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - PAIR 5 - GENERAL SN (MARCOS)', campaign_id: '2450c6a9-829a-4124-ad22-067b9ce9e9c6', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - PAIR 6 - Real State (MARCOS)', campaign_id: '68435798-de80-4f9a-9639-8c10542c3dbf', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'ON - PAIR 7 - Prop Mainten (MARCOS)', campaign_id: '5b0dd2ef-dac8-44d3-b386-5ee798aebffa', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'RG3580 - General GMAPS (1298-1301) - Southern Edge Funds - (SHAAN)', campaign_id: '320f9bb1-ae1b-4cda-bb73-9e364686bd04', note: 'Seed 2026-04-22', created_by: 'sam' },
  { alias: 'RG49/RG50/RG51 - Qualify - Construction (CARLOS)', campaign_id: 'fd0414d9-845c-4e09-a6f5-e0692a90f2ea', note: 'Seed 2026-04-22', created_by: 'sam' },
];

export function decodeHtmlEntities(input: string): string {
  return input.replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (entity, body) => {
    const lower = String(body).toLowerCase();

    if (lower === 'amp') return '&';
    if (lower === 'apos' || lower === '#39') return "'";
    if (lower === 'quot') return '"';
    if (lower === 'lt') return '<';
    if (lower === 'gt') return '>';
    if (lower.startsWith('#x')) {
      return String.fromCodePoint(parseInt(lower.slice(2), 16));
    }
    if (lower.startsWith('#')) {
      return String.fromCodePoint(parseInt(lower.slice(1), 10));
    }

    return entity;
  });
}

export function normalizeMeetingName(input: string): string {
  return decodeHtmlEntities(input)
    .replace(/[\u2012-\u2015\u2212]/g, '-')
    .replace(/\s*&\s*/g, ' & ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function stripStatePrefix(input: string): string {
  let value = normalizeMeetingName(input);

  while (true) {
    const next = STATE_PREFIX_PATTERNS.reduce((current, pattern) => {
      if (current !== value) return current;
      return value.replace(pattern, '').trim();
    }, value);

    if (next === value) return value;
    value = next;
  }
}

export function canonicalMatchKey(input: string): string {
  return stripStatePrefix(input).toLowerCase();
}

export function aliasLookupKey(input: string): string {
  return normalizeMeetingName(input).toLowerCase();
}

function extractPairNumbers(input: string): string[] {
  const match = normalizeMeetingName(input).match(/\bpair\b\s*([0-9,\sand]+)/i);
  if (!match) return [];

  return Array.from(
    new Set((match[1].match(/\d+/g) ?? []).map(token => token.trim()).filter(Boolean)),
  ).sort();
}

function extractCmTag(input: string): string | null {
  const matches = [...normalizeMeetingName(input).matchAll(/\(([A-Za-z]+)\)/g)];
  for (let index = matches.length - 1; index >= 0; index -= 1) {
    const value = matches[index][1].trim();
    if (value.toLowerCase() !== 'copy') {
      return value.toLowerCase();
    }
  }
  return null;
}

function extractPersonCodes(input: string): string[] {
  const normalized = normalizeMeetingName(input);
  const found = new Set<string>();

  for (const code of PERSON_CODE_PATTERNS) {
    const pattern = new RegExp(`(?:\\)|_|\\s)${code}(?:\\b|\\s|$)`, 'i');
    if (pattern.test(normalized)) {
      found.add(code);
    }
  }

  return Array.from(found).sort();
}

function extractRgCodes(input: string): string[] {
  return Array.from(
    new Set(
      [...normalizeMeetingName(input).matchAll(/\bRG\s*([0-9]+)\b/gi)].map(match => match[1]),
    ),
  ).sort();
}

function extractIndexTokens(normalized: string, pairNumbers: string[], rgCodes: string[], cmTag: string | null, personCodes: string[]): string[] {
  const tokens = new Set<string>();

  for (const token of normalized.toLowerCase().split(/[^a-z0-9]+/).filter(Boolean)) {
    if (token.length < 2 || NOISE_TOKENS.has(token)) continue;
    tokens.add(token);
  }

  for (const pair of pairNumbers) tokens.add(`pair:${pair}`);
  for (const rg of rgCodes) tokens.add(`rg:${rg}`);
  if (cmTag) tokens.add(`cm:${cmTag}`);
  for (const code of personCodes) tokens.add(`pc:${code}`);

  return Array.from(tokens).sort();
}

function buildMetadata(input: string): NameMetadata {
  const normalized = normalizeMeetingName(input);
  const pairNumbers = extractPairNumbers(normalized);
  const cmTag = extractCmTag(normalized);
  const personCodes = extractPersonCodes(normalized);
  const rgCodes = extractRgCodes(normalized);

  return {
    normalized,
    strictKey: canonicalMatchKey(normalized),
    pairNumbers,
    cmTag,
    personCodes,
    rgCodes,
    hasLinkedIn: /\blinkedin\b/i.test(normalized),
    indexTokens: extractIndexTokens(normalized, pairNumbers, rgCodes, cmTag, personCodes),
  };
}

function sameSet(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function hardRejectReason(source: NameMetadata, candidate: NameMetadata): QueueReason | null {
  if (source.hasLinkedIn) return 'ignored_linkedin';
  if (source.pairNumbers.length > 0 && candidate.pairNumbers.length > 0 && !sameSet(source.pairNumbers, candidate.pairNumbers)) {
    return 'hard_reject';
  }
  if (source.cmTag && candidate.cmTag && source.cmTag !== candidate.cmTag) {
    return 'hard_reject';
  }
  if (source.personCodes.length > 0 && candidate.personCodes.length > 0 && !sameSet(source.personCodes, candidate.personCodes)) {
    return 'hard_reject';
  }
  if (
    source.rgCodes.length > 0 &&
    candidate.rgCodes.length > 0 &&
    sameSet(source.rgCodes, candidate.rgCodes) &&
    source.strictKey !== candidate.strictKey
  ) {
    return 'hard_reject';
  }
  return null;
}

function levenshteinDistance(left: string, right: string): number {
  if (left === right) return 0;
  if (left.length === 0) return right.length;
  if (right.length === 0) return left.length;

  const previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  const current = new Array<number>(right.length + 1).fill(0);

  for (let row = 1; row <= left.length; row += 1) {
    current[0] = row;

    for (let col = 1; col <= right.length; col += 1) {
      const cost = left[row - 1] === right[col - 1] ? 0 : 1;
      current[col] = Math.min(
        current[col - 1] + 1,
        previous[col] + 1,
        previous[col - 1] + cost,
      );
    }

    for (let col = 0; col < current.length; col += 1) {
      previous[col] = current[col];
    }
  }

  return previous[right.length];
}

function similarityRatio(left: string, right: string): number {
  if (!left && !right) return 100;
  const maxLength = Math.max(left.length, right.length);
  if (maxLength === 0) return 100;
  const distance = levenshteinDistance(left, right);
  return Math.round(((maxLength - distance) / maxLength) * 100);
}

function tokenSetRatio(left: string, right: string): number {
  const leftTokens = Array.from(new Set(normalizeMeetingName(left).toLowerCase().split(/\s+/).filter(Boolean))).sort();
  const rightTokens = Array.from(new Set(normalizeMeetingName(right).toLowerCase().split(/\s+/).filter(Boolean))).sort();

  const shared = leftTokens.filter(token => rightTokens.includes(token));
  const leftOnly = leftTokens.filter(token => !shared.includes(token));
  const rightOnly = rightTokens.filter(token => !shared.includes(token));

  const base = shared.join(' ');
  const leftCombined = [...shared, ...leftOnly].join(' ');
  const rightCombined = [...shared, ...rightOnly].join(' ');

  return Math.max(
    similarityRatio(base, leftCombined),
    similarityRatio(base, rightCombined),
    similarityRatio(leftCombined, rightCombined),
  );
}

function buildTokenMap(campaigns: CampaignRuntime[]): Map<string, CampaignRuntime[]> {
  const tokenMap = new Map<string, CampaignRuntime[]>();

  for (const campaign of campaigns) {
    for (const token of campaign.metadata.indexTokens) {
      const bucket = tokenMap.get(token);
      if (bucket) bucket.push(campaign);
      else tokenMap.set(token, [campaign]);
    }
  }

  return tokenMap;
}

export function buildResolverContext(campaigns: CampaignRecord[], aliases: AliasRecord[]): MatchResolverContext {
  const runtimeCampaigns = campaigns
    .filter(campaign => campaign.name?.trim())
    .map(campaign => ({
      ...campaign,
      metadata: buildMetadata(campaign.name),
    }));

  const campaignMap = new Map(runtimeCampaigns.map(campaign => [campaign.campaign_id, campaign]));
  const strictMap = new Map<string, CampaignRuntime[]>();

  for (const campaign of runtimeCampaigns) {
    const bucket = strictMap.get(campaign.metadata.strictKey);
    if (bucket) bucket.push(campaign);
    else strictMap.set(campaign.metadata.strictKey, [campaign]);
  }

  const aliasMap = new Map<string, AliasRecord>();
  for (const alias of aliases) {
    aliasMap.set(aliasLookupKey(alias.alias), alias);
  }

  return {
    campaigns: runtimeCampaigns,
    aliasMap,
    campaignMap,
    strictMap,
    tokenMap: buildTokenMap(runtimeCampaigns),
  };
}

function findCandidatePool(metadata: NameMetadata, context: MatchResolverContext): CampaignRuntime[] {
  const seen = new Map<string, CampaignRuntime>();

  for (const token of metadata.indexTokens) {
    for (const campaign of context.tokenMap.get(token) ?? []) {
      seen.set(campaign.campaign_id, campaign);
    }
  }

  return seen.size > 0 ? Array.from(seen.values()) : context.campaigns;
}

export function rankCandidates(rawName: string, context: MatchResolverContext, limit = 5): {
  topCandidates: CandidateSuggestion[];
  sawHardReject: boolean;
  bestRejectedScore: number;
  bestKeptScore: number;
} {
  const metadata = buildMetadata(rawName);
  const candidates: CandidateSuggestion[] = [];
  let sawHardReject = false;
  let bestRejectedScore = -1;

  for (const campaign of findCandidatePool(metadata, context)) {
    const score = tokenSetRatio(metadata.normalized, campaign.metadata.normalized);

    if (hardRejectReason(metadata, campaign.metadata)) {
      sawHardReject = true;
      bestRejectedScore = Math.max(bestRejectedScore, score);
      continue;
    }

    candidates.push({
      campaign_id: campaign.campaign_id,
      campaign_name: campaign.name,
      score,
    });
  }

  candidates.sort((left, right) => {
    if (right.score !== left.score) return right.score - left.score;
    return left.campaign_name.localeCompare(right.campaign_name);
  });

  return {
    topCandidates: candidates.slice(0, limit),
    sawHardReject,
    bestRejectedScore,
    bestKeptScore: candidates[0]?.score ?? -1,
  };
}

function buildCandidateHash(candidates: CandidateSuggestion[]): string {
  return candidates.map(candidate => `${candidate.campaign_id}:${candidate.score}`).join('|');
}

export function resolveCampaignName(
  rawName: string,
  context: MatchResolverContext,
  existingQueue?: QueueRecord | null,
): MatchResolution {
  const metadata = buildMetadata(rawName);

  if (metadata.hasLinkedIn) {
    return {
      kind: 'ignore',
      rawName,
      queueReason: 'ignored_linkedin',
    };
  }

  const alias = context.aliasMap.get(aliasLookupKey(rawName));
  if (alias) {
    const campaign = context.campaignMap.get(alias.campaign_id);
    if (campaign) {
      return {
        kind: 'match',
        rawName,
        campaignId: campaign.campaign_id,
        campaignName: campaign.name,
        matchMethod: 'alias',
        matchConfidence: 1,
      };
    }
  }

  const strictCandidates = context.strictMap.get(metadata.strictKey) ?? [];
  if (strictCandidates.length === 1) {
    return {
      kind: 'match',
      rawName,
      campaignId: strictCandidates[0].campaign_id,
      campaignName: strictCandidates[0].name,
      matchMethod: 'strict_auto',
      matchConfidence: 0.98,
    };
  }
  if (strictCandidates.length > 1) {
    const topCandidates = strictCandidates.slice(0, 5).map(candidate => ({
      campaign_id: candidate.campaign_id,
      campaign_name: candidate.name,
      score: 100,
    }));
    return {
      kind: 'queue',
      rawName,
      queueReason: 'ambiguous_strict',
      candidateHash: buildCandidateHash(topCandidates),
      topCandidates,
    };
  }

  const { topCandidates, sawHardReject, bestRejectedScore, bestKeptScore } = rankCandidates(rawName, context);
  const candidateHash = buildCandidateHash(topCandidates);

  if (
    existingQueue &&
    existingQueue.review_status !== 'resolved' &&
    existingQueue.candidate_hash === candidateHash &&
    Array.isArray(existingQueue.top_candidates)
  ) {
    return {
      kind: 'queue',
      rawName,
      queueReason: (existingQueue.queue_reason as QueueReason | undefined) ?? (sawHardReject ? 'hard_reject' : 'no_match'),
      candidateHash,
      topCandidates: existingQueue.top_candidates,
    };
  }

  return {
    kind: 'queue',
    rawName,
    queueReason:
      sawHardReject && ((topCandidates.length === 0) || bestRejectedScore >= bestKeptScore)
        ? 'hard_reject'
        : 'no_match',
    candidateHash,
    topCandidates,
  };
}

export function buildQueueUpsert(
  resolution: Extract<MatchResolution, { kind: 'queue' }>,
  stats: MeetingNameStats | undefined,
  existingQueue?: QueueRecord | null,
): Record<string, unknown> {
  const now = new Date().toISOString();
  const existingChannels = new Set(existingQueue?.source_channels ?? []);
  for (const channel of stats?.channelNames ?? []) existingChannels.add(channel);

  return {
    campaign_name_raw: resolution.rawName,
    queue_reason: resolution.queueReason,
    top_candidates: resolution.topCandidates,
    candidate_hash: resolution.candidateHash,
    occurrence_count: (existingQueue?.occurrence_count ?? 0) + (stats?.count ?? 0),
    source_channels: Array.from(existingChannels).sort(),
    first_seen_at: existingQueue?.first_seen_at ?? stats?.firstSeenAt ?? now,
    last_seen_at: stats?.lastSeenAt ?? existingQueue?.last_seen_at ?? now,
    review_status: existingQueue?.review_status && existingQueue.review_status !== 'resolved'
      ? existingQueue.review_status
      : 'pending',
    updated_at: now,
    created_at: existingQueue?.first_seen_at ? undefined : now,
  };
}

export function buildResolvedQueuePatch(campaignId: string): Record<string, unknown> {
  const now = new Date().toISOString();
  return {
    review_status: 'resolved',
    resolved_campaign_id: campaignId,
    resolved_at: now,
    updated_at: now,
  };
}

export function sqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}
