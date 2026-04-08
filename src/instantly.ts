import type { Campaign, StepAnalytics, CampaignAnalytics, Account, Tag } from './types';

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
    });
    if (res.status === 429) {
      await new Promise(r => setTimeout(r, 2000));
      res = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${this.apiKey}` },
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
    const raw = await this.get<Record<string, unknown>>('/campaigns/analytics', {
      campaign_id: campaignId,
    });
    const c = Array.isArray((raw as any).campaigns)
      ? ((raw as any).campaigns[0] ?? {})
      : raw;
    return {
      leads_count: (c.leads_count as number) ?? 0,
      contacted_count: (c.contacted_count as number) ?? 0,
      completed_count: (c.completed_count as number) ?? 0,
      bounced_count: (c.bounced_count as number) ?? 0,
      unsubscribed_count: (c.unsubscribed_count as number) ?? 0,
    };
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

}
