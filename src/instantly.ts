import type { Campaign, StepAnalytics, CampaignAnalytics, CampaignDailyAnalytics, Account, Tag, TagMapping } from './types';

export class InstantlyClient {
  private baseUrl = 'https://api.instantly.ai/api/v2';

  constructor(private apiKey: string) {}

  private async get<T>(path: string, params?: Record<string, string>): Promise<T> {
    const url = new URL(`${this.baseUrl}${path}`);
    if (params) {
      for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    }
    let res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${this.apiKey}` },
      signal: AbortSignal.timeout(30_000),
    });
    if (res.status === 429) {
      await new Promise(r => setTimeout(r, 2000));
      res = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${this.apiKey}` },
        signal: AbortSignal.timeout(30_000),
      });
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Instantly ${res.status} on GET ${path}: ${body}`);
    }
    return res.json() as Promise<T>;
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

  /** All campaigns, or filtered by status (e.g. 'active') */
  async getCampaigns(options?: { status?: string }): Promise<Campaign[]> {
    const params: Record<string, string> = {};
    if (options?.status) params.status = options.status;
    return this.getAll<Campaign>('/campaigns', params);
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
}
