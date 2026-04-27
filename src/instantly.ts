import type { Campaign, StepAnalytics, CampaignAnalytics, CampaignDailyAnalytics, Account, Tag, TagMapping } from './types';
import type { AccountDailyMetric } from './infra/types';

const TRANSIENT_STATUS_CODES = new Set([429, 500, 502, 503, 504]);
const SPLITTABLE_STATUS_CODES = new Set([500, 502, 503, 504]);

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function retryDelayMs(attempt: number): number {
  const base = 500 * Math.pow(2, attempt);
  const jitter = Math.floor(Math.random() * 250);
  return base + jitter;
}

function isTransientFetchError(err: unknown): boolean {
  if (err instanceof DOMException) {
    return err.name === 'TimeoutError' || err.name === 'AbortError';
  }
  return err instanceof TypeError;
}

function instantlyStatusFromError(err: unknown): number | null {
  if (!(err instanceof Error)) return null;
  const match = err.message.match(/^Instantly (\d{3}) on GET /);
  if (!match) return null;
  const status = Number(match[1]);
  return Number.isFinite(status) ? status : null;
}

function isSplittableAccountAnalyticsError(err: unknown): boolean {
  const status = instantlyStatusFromError(err);
  if (status !== null) return SPLITTABLE_STATUS_CODES.has(status);
  return isTransientFetchError(err);
}

function shiftDateIso(dateIso: string, deltaDays: number): string {
  const [y, m, d] = dateIso.split('-').map(n => parseInt(n, 10));
  const dt = new Date(Date.UTC(y!, (m! - 1), d!));
  dt.setUTCDate(dt.getUTCDate() + deltaDays);
  return dt.toISOString().slice(0, 10);
}

function daysInclusive(startDate: string, endDate: string): number {
  const start = Date.parse(`${startDate}T00:00:00.000Z`);
  const end = Date.parse(`${endDate}T00:00:00.000Z`);
  return Math.floor((end - start) / 86_400_000) + 1;
}

function splitDateRange(startDate: string, endDate: string): {
  left: { startDate: string; endDate: string };
  right: { startDate: string; endDate: string };
} {
  const days = daysInclusive(startDate, endDate);
  const leftDays = Math.floor(days / 2);
  const leftEnd = shiftDateIso(startDate, leftDays - 1);
  const rightStart = shiftDateIso(leftEnd, 1);
  return {
    left: { startDate, endDate: leftEnd },
    right: { startDate: rightStart, endDate },
  };
}

function dedupeAccountDailyMetrics(rows: AccountDailyMetric[]): AccountDailyMetric[] {
  const seen = new Set<string>();
  const out: AccountDailyMetric[] = [];
  for (const row of rows) {
    const key = `${String(row.email_account ?? '').trim().toLowerCase()}\u0000${String(row.date ?? '').trim()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function compactBody(body: string): string {
  return body.replace(/\s+/g, ' ').trim().slice(0, 500);
}

export class InstantlyClient {
  private baseUrl = 'https://api.instantly.ai/api/v2';

  apiCallCount = 0;
  rateLimitEvents = 0;

  constructor(private apiKey: string) {}

  private async get<T>(path: string, params?: Record<string, string>): Promise<T> {
    return this.getWithRetry<T>(path, params);
  }

  private async getWithRetry<T>(path: string, params?: Record<string, string>): Promise<T> {
    const url = new URL(`${this.baseUrl}${path}`);
    if (params) {
      for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    }
    const maxAttempts = 5;
    let lastBody = '';
    let lastStatus = 0;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      this.apiCallCount++;
      let res: Response;
      try {
        res = await fetch(url.toString(), {
          headers: { Authorization: `Bearer ${this.apiKey}` },
          signal: AbortSignal.timeout(30_000),
        });
      } catch (err) {
        if (attempt < maxAttempts - 1) {
          if (isTransientFetchError(err)) {
            await sleep(retryDelayMs(attempt));
            continue;
          }
        }
        throw err;
      }
      if (TRANSIENT_STATUS_CODES.has(res.status)) {
        if (res.status === 429) this.rateLimitEvents++;
        lastStatus = res.status;
        lastBody = await res.text().catch(() => '');
        if (attempt < maxAttempts - 1) {
          await sleep(retryDelayMs(attempt));
          continue;
        }
        break;
      }
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Instantly ${res.status} on GET ${path}: ${compactBody(body)}`);
      }
      return res.json() as Promise<T>;
    }
    throw new Error(`Instantly ${lastStatus} on GET ${path} after ${maxAttempts} attempts: ${compactBody(lastBody)}`);
  }

  private async getAll<T>(path: string, params?: Record<string, string>): Promise<T[]> {
    const results: T[] = [];
    let cursor: string | undefined;
    do {
      const p = { limit: '100', ...params, ...(cursor ? { starting_after: cursor } : {}) };
      const raw = await this.get<{ items?: T[]; next_starting_after?: string } | T[]>(path, p);
      const items: T[] = Array.isArray(raw) ? raw : (raw.items ?? []);
      results.push(...items);
      cursor = Array.isArray(raw) ? undefined : (raw.next_starting_after ?? undefined);
    } while (cursor);
    return results;
  }

  /** All campaigns regardless of status */
  async getCampaigns(): Promise<Campaign[]> {
    return this.getAll<Campaign>('/campaigns');
  }

  /** Full campaign detail including sequences (copy) */
  async getCampaignDetail(campaignId: string): Promise<Campaign> {
    return this.get<Campaign>(`/campaigns/${campaignId}`);
  }

  /** Campaign-level aggregate stats */
  async getCampaignAnalytics(campaignId: string): Promise<CampaignAnalytics> {
    const raw = await this.get<unknown[]>('/campaigns/analytics', {
      id: campaignId,
    });
    const c: any = Array.isArray(raw) ? (raw[0] ?? {}) : raw;
    return {
      leads_count: (c.leads_count as number) ?? 0,
      contacted_count: (c.new_leads_contacted_count as number) ?? 0,
      completed_count: (c.completed_count as number) ?? 0,
      bounced_count: (c.bounced_count as number) ?? 0,
      unsubscribed_count: (c.unsubscribed_count as number) ?? 0,
    };
  }

  /**
   * Per-calendar-day campaign metrics from Instantly /campaigns/analytics/daily.
   * Returns one row per day in [start_date, end_date] that had activity. Empty
   * array if no activity. Both dates inclusive, format YYYY-MM-DD.
   *
   * Unlike /campaigns/analytics/steps (cumulative, eventually consistent), this
   * endpoint returns UI-aligned day values with retroactive updates applied.
   */
  async getCampaignDailyAnalytics(
    campaignId: string,
    startDate: string,
    endDate: string,
  ): Promise<CampaignDailyAnalytics[]> {
    const raw = await this.get<unknown>('/campaigns/analytics/daily', {
      campaign_id: campaignId,
      start_date: startDate,
      end_date: endDate,
    });
    const items = Array.isArray(raw)
      ? raw
      : ((raw as any).items ?? (raw as any).daily ?? []);
    return (items as any[]).map(r => ({
      date: String(r.date),
      sent: r.sent ?? 0,
      contacted: r.contacted ?? 0,
      new_leads_contacted: r.new_leads_contacted ?? 0,
      opened: r.opened ?? 0,
      unique_opened: r.unique_opened ?? 0,
      replies: r.replies ?? 0,
      unique_replies: r.unique_replies ?? 0,
      replies_automatic: r.replies_automatic ?? 0,
      unique_replies_automatic: r.unique_replies_automatic ?? 0,
      clicks: r.clicks ?? 0,
      unique_clicks: r.unique_clicks ?? 0,
      opportunities: r.opportunities ?? 0,
      unique_opportunities: r.unique_opportunities ?? 0,
    }));
  }

  /** Per-step/variant cumulative metrics */
  async getStepAnalytics(campaignId: string): Promise<StepAnalytics[]> {
    const raw = await this.get<unknown>('/campaigns/analytics/steps', {
      campaign_id: campaignId,
      include_opportunities_count: 'true',
    });
    const items = Array.isArray(raw) ? raw : ((raw as any).items ?? []);
    return (items as StepAnalytics[]).filter(
      r => r.step !== null && r.step !== 'null' && r.variant !== null
    );
  }

  /** All sender accounts */
  async getAccounts(): Promise<Account[]> {
    return this.getAll<Account>('/accounts');
  }

  /** Tag UUID → label map for this workspace */
  async getTagMap(): Promise<Map<string, string>> {
    const tags = await this.getAll<Tag>('/custom-tags');
    return new Map(tags.map(t => [t.id, t.label]));
  }

  /**
   * Fetch ALL custom-tag-mappings for this workspace (unfiltered).
   * IMPORTANT: Instantly's resource_type/resource_id/tag_id filters on this
   * endpoint are broken — they're ignored or partially honored (verified
   * empirically 2026-04-10). Always fetch unfiltered, then filter client-side
   * on resource_type === 2 (= campaigns, determined empirically).
   *
   * Page size is capped at 100 server-side: Instantly returns HTTP 400
   * "querystring/limit must be <= 100" for any higher value. Large
   * workspaces (koi-and-destroy has ~72k mappings) paginate to hundreds of
   * sequential subrequests — budget accordingly at the caller.
   */
  async getAllCustomTagMappings(maxPages?: number): Promise<TagMapping[]> {
    if (maxPages === undefined) return this.getAll<TagMapping>('/custom-tag-mappings');
    const results: TagMapping[] = [];
    let cursor: string | undefined;
    let pages = 0;
    do {
      const p: Record<string, string> = { limit: '100' };
      if (cursor) p.starting_after = cursor;
      const raw = await this.get<{ items?: TagMapping[]; next_starting_after?: string } | TagMapping[]>(
        '/custom-tag-mappings',
        p,
      );
      const items: TagMapping[] = Array.isArray(raw) ? raw : (raw.items ?? []);
      results.push(...items);
      cursor = Array.isArray(raw) ? undefined : (raw.next_starting_after ?? undefined);
      pages++;
      if (pages >= maxPages) break;
    } while (cursor);
    return results;
  }

  /**
   * Raw account inventory with all server-side fields preserved (e.g.
   * `provider_code`, `workspace`). Paginates `/accounts` via `starting_after`.
   * Optional `search` narrows by domain or partial email.
   */
  async getAccountsRaw(params?: { search?: string }): Promise<Account[]> {
    const results: Account[] = [];
    let cursor: string | undefined;
    do {
      const p: Record<string, string> = { limit: '100' };
      if (params?.search) p.search = params.search;
      if (cursor) p.starting_after = cursor;
      const raw = await this.getWithRetry<{ items?: Account[]; next_starting_after?: string } | Account[]>(
        '/accounts',
        p,
      );
      const items: Account[] = Array.isArray(raw) ? raw : (raw.items ?? []);
      results.push(...items);
      cursor = Array.isArray(raw) ? undefined : (raw.next_starting_after ?? undefined);
    } while (cursor);
    return results;
  }

  /**
   * Workspace-wide daily account analytics for a date window.
   *
   * Phase 0 (2026-04-24) verified the v2 API ignores all email-filter param
   * variants (`emails`, `email`, `email_account`, `account`, `accounts`,
   * `emails[]`) and always returns the full workspace dump. POST is unsupported
   * (404). One GET per workspace per window is the only viable shape.
   * Filter to specific accounts client-side after the call.
   */
  async getWorkspaceAccountDailyAnalytics(params: {
    startDate: string;
    endDate: string;
  }): Promise<AccountDailyMetric[]> {
    const raw = await this.getWithRetry<unknown>('/accounts/analytics/daily', {
      start_date: params.startDate,
      end_date: params.endDate,
    });
    const items: any[] = Array.isArray(raw) ? raw : ((raw as any)?.items ?? []);
    return items
      .filter(r => r && r.email_account !== '')
      .map(r => ({
        date: String(r.date ?? ''),
        email_account: String(r.email_account ?? ''),
        sent: r.sent ?? 0,
        bounced: r.bounced ?? 0,
        contacted: r.contacted ?? 0,
        new_leads_contacted: r.new_leads_contacted ?? 0,
        opened: r.opened ?? 0,
        unique_opened: r.unique_opened ?? 0,
        replies: r.replies ?? 0,
        unique_replies: r.unique_replies ?? 0,
        replies_automatic: r.replies_automatic ?? 0,
        unique_replies_automatic: r.unique_replies_automatic ?? 0,
        clicks: r.clicks ?? 0,
        unique_clicks: r.unique_clicks ?? 0,
      }));
  }

  async getWorkspaceAccountDailyAnalyticsAdaptive(params: {
    startDate: string;
    endDate: string;
    logLabel?: string;
  }): Promise<AccountDailyMetric[]> {
    const fetchRange = async (
      startDate: string,
      endDate: string,
    ): Promise<AccountDailyMetric[]> => {
      try {
        return await this.getWorkspaceAccountDailyAnalytics({ startDate, endDate });
      } catch (err) {
        if (startDate === endDate || !isSplittableAccountAnalyticsError(err)) throw err;
        const label = params.logLabel ? `${params.logLabel}: ` : '';
        const status = instantlyStatusFromError(err);
        const reason = status !== null ? String(status) : err instanceof Error ? err.name : 'fetch error';
        console.log(
          `[metrics] ${label}window ${startDate}..${endDate} failed with ${reason}, splitting`,
        );
        const { left, right } = splitDateRange(startDate, endDate);
        const leftRows = await fetchRange(left.startDate, left.endDate);
        console.log(
          `[metrics] ${label}fetched ${left.startDate}..${left.endDate} rows=${leftRows.length}`,
        );
        const rightRows = await fetchRange(right.startDate, right.endDate);
        console.log(
          `[metrics] ${label}fetched ${right.startDate}..${right.endDate} rows=${rightRows.length}`,
        );
        return dedupeAccountDailyMetrics([...leftRows, ...rightRows]);
      }
    };

    return fetchRange(params.startDate, params.endDate);
  }
}
