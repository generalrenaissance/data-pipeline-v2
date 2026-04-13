export class SupabaseClient {
  constructor(private url: string, private key: string) {}

  async upsert(table: string, rows: unknown[], onConflict: string): Promise<void> {
    if (rows.length === 0) return;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const res = await fetch(`${this.url}/rest/v1/${table}?on_conflict=${onConflict}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.key}`,
          'apikey': this.key,
          'Prefer': 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(chunk),
      });
      if (!res.ok) {
        const err = await res.text().catch(() => '');
        throw new Error(`Supabase upsert ${res.status} on ${table}: ${err}`);
      }
    }
  }

  async insert(table: string, rows: unknown[]): Promise<void> {
    if (rows.length === 0) return;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const res = await fetch(`${this.url}/rest/v1/${table}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.key}`,
          'apikey': this.key,
          'Prefer': 'return=minimal',
        },
        body: JSON.stringify(chunk),
      });
      if (!res.ok) {
        const err = await res.text().catch(() => '');
        throw new Error(`Supabase insert ${res.status} on ${table}: ${err}`);
      }
    }
  }

  async insertIgnore(table: string, rows: unknown[], onConflict: string): Promise<void> {
    if (rows.length === 0) return;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const res = await fetch(`${this.url}/rest/v1/${table}?on_conflict=${onConflict}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.key}`,
          'apikey': this.key,
          'Prefer': 'resolution=ignore-duplicates,return=minimal',
        },
        body: JSON.stringify(chunk),
      });
      if (!res.ok) {
        const err = await res.text().catch(() => '');
        throw new Error(`Supabase insertIgnore ${res.status} on ${table}: ${err}`);
      }
    }
  }

  async select(table: string, params: string): Promise<unknown[]> {
    const res = await fetch(`${this.url}/rest/v1/${table}?${params}`, {
      headers: {
        'Authorization': `Bearer ${this.key}`,
        'apikey': this.key,
      },
    });
    if (!res.ok) {
      const err = await res.text().catch(() => '');
      throw new Error(`Supabase select ${res.status} on ${table}: ${err}`);
    }
    return res.json() as Promise<unknown[]>;
  }

  async delete(table: string, params: string): Promise<void> {
    const res = await fetch(`${this.url}/rest/v1/${table}?${params}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${this.key}`,
        'apikey': this.key,
        'Prefer': 'return=minimal',
      },
    });
    if (!res.ok) {
      const err = await res.text().catch(() => '');
      throw new Error(`Supabase delete ${res.status} on ${table}: ${err}`);
    }
  }

  async rpc(fn: string, params: Record<string, unknown>): Promise<unknown> {
    const res = await fetch(`${this.url}/rest/v1/rpc/${fn}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.key}`,
        'apikey': this.key,
      },
      body: JSON.stringify(params),
    });
    if (!res.ok) {
      const err = await res.text().catch(() => '');
      throw new Error(`Supabase rpc ${res.status} on ${fn}: ${err}`);
    }
    const text = await res.text();
    if (!text || text.trim() === '') return null;
    return JSON.parse(text);
  }
}
