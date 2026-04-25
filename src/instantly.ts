import type { Campaign, StepAnalytics, CampaignAnalytics, CampaignDailyAnalytics, Account, Tag, TagMapping } from './types';
import type { AccountDailyMetric } from './infra/types';

export class InstantlyClient {
  private baseUrl = 'https://api.instantly.ai/api/v2';

  apiCallCount = 0;
  rateLimitEvents = 0;

  constructor(private apiKey: string) {}

  private async get<T>(path: string, params?: Record<string, string>): Promise<T> {
    const url = new URL(`${this.baseUrl}${path}`);
    if (params) {
      for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    }
    this.apiCallCount++;
    let res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${this.apiKey}` },
      signal: AbortSignal.timeout(30_000),
    });
    if (res.status === 429) {
      this.rateLimitEvents++;
      await new Promise(r => setTimeout(r, 2000));
      this.apiCallCount++;
      res = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${this.apiKey}` },
        signal: AbortSignal.timeout(30_000),
      });
      if (res.status === 429) this.rateLimitEvents++;
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Instantly ${res.status} on GET ${path}: ${body}`);
    }
    return res.json() as Promise<T>;
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
      const res = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${this.apiKey}` },
        signal: AbortSignal.timeout(30_000),
      });
      if (res.status === 429) {
        this.rateLimitEvents++;
        lastStatus = 429;
        if (attempt < maxAttempts - 1) {
          const base = 500 * Math.pow(2, attempt);
          const jitter = Math.floor(Math.random() * 250);
          await new Promise(r => setTimeout(r, base + jitter));
          continue;
        }
        lastBody = await res.text().catch(() => '');
        break;
      }
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Instantly ${res.status} on GET ${path}: ${body}`);
      }
      return res.json() as Promise<T>;
    }
    throw new Error(`Instantly ${lastStatus} on GET ${path} after ${maxAttempts} attempts: ${lastBody}`);
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
  async getAllCustomTagMappings(): Promise<TagMapping[]> {
    return this.getAll<TagMapping>('/custom-tag-mappings');
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
}
