-- Domain Infra Sync + RR Automation — schema migration [2026-04-24]
--
-- Spec: specs/2026-04-24-domain-infra-sync-and-rr-implementation.md sections 5A-5G
-- Build plan: specs/2026-04-24-domain-infra-sync-build-plan.md Phase 1
-- Phase 0 evidence: handoffs/2026-04-24-domain-infra-sync-phase0-blocker.md
--
-- Creates 7 tables in Pipeline Supabase public schema:
--   infra_accounts                  (5A) — account inventory
--   infra_account_daily_metrics     (5B) — per-account per-day metrics
--   infra_domain_daily_metrics      (5C) — per-domain per-day rollup
--   infra_domain_metrics            (5D) — per-domain lifetime aggregate (scorer input)
--   infra_sync_runs                 (5E) — sync run audit log
--   domain_rr_state                 (5F) — current RR scorer state
--   domain_rr_events                (5G) — RR state-change events (warmup/retire/etc.)
--
-- All tables use `create table if not exists` for idempotency. Re-running this
-- migration is safe; it adds nothing and changes nothing if the tables already
-- exist with the same schema.
--
-- Rollback: see 2026-04-24-domain-infra-sync-rollback.sql.

-- =========================================================================
-- 5A — infra_accounts: latest known account inventory
-- =========================================================================

create table if not exists public.infra_accounts (
  account_email text primary key,
  domain text not null,
  workspace_slug text not null,
  workspace_name text,
  provider_code_raw integer,
  provider_group text not null check (provider_group in ('google_otd', 'outlook', 'unknown')),
  account_status text,
  warmup_status text,
  daily_limit integer,
  sending_gap_seconds integer,
  first_name text,
  last_name text,
  is_free_mail boolean not null default false,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  api_synced_at timestamptz not null,
  raw_account jsonb
);

create index if not exists idx_infra_accounts_domain on public.infra_accounts(domain);
create index if not exists idx_infra_accounts_workspace on public.infra_accounts(workspace_slug);
create index if not exists idx_infra_accounts_provider_group on public.infra_accounts(provider_group);
create index if not exists idx_infra_accounts_free_mail on public.infra_accounts(is_free_mail);

-- =========================================================================
-- 5B — infra_account_daily_metrics: one row per account per day
-- =========================================================================

create table if not exists public.infra_account_daily_metrics (
  account_email text not null,
  metric_date date not null,
  domain text not null,
  workspace_slug text not null,
  provider_group text not null check (provider_group in ('google_otd', 'outlook', 'unknown')),
  sent integer not null default 0,
  bounced integer not null default 0,
  contacted integer not null default 0,
  new_leads_contacted integer not null default 0,
  opened integer not null default 0,
  unique_opened integer not null default 0,
  replies integer not null default 0,
  unique_replies integer not null default 0,
  replies_automatic integer not null default 0,
  unique_replies_automatic integer not null default 0,
  clicks integer not null default 0,
  unique_clicks integer not null default 0,
  api_synced_at timestamptz not null,
  primary key (account_email, metric_date)
);

create index if not exists idx_infra_account_daily_domain_date on public.infra_account_daily_metrics(domain, metric_date);
create index if not exists idx_infra_account_daily_workspace_date on public.infra_account_daily_metrics(workspace_slug, metric_date);
create index if not exists idx_infra_account_daily_provider_date on public.infra_account_daily_metrics(provider_group, metric_date);

-- =========================================================================
-- 5C — infra_domain_daily_metrics: per-domain per-day rollup
-- =========================================================================

create table if not exists public.infra_domain_daily_metrics (
  domain text not null,
  metric_date date not null,
  provider_group text not null check (provider_group in ('google_otd', 'outlook', 'unknown')),
  workspace_count integer not null default 0,
  inbox_count integer not null default 0,
  active_inbox_count integer not null default 0,
  sent integer not null default 0,
  replies integer not null default 0,
  replies_automatic integer not null default 0,
  rr_pct numeric(8,4),
  api_synced_at timestamptz not null,
  primary key (domain, metric_date)
);

create index if not exists idx_infra_domain_daily_date on public.infra_domain_daily_metrics(metric_date);
create index if not exists idx_infra_domain_daily_provider on public.infra_domain_daily_metrics(provider_group);

-- =========================================================================
-- 5D — infra_domain_metrics: per-domain lifetime aggregate (scorer input)
-- =========================================================================

create table if not exists public.infra_domain_metrics (
  domain text primary key,
  provider_group text not null check (provider_group in ('google_otd', 'outlook', 'unknown')),
  dominant_provider_raw integer,
  workspace_count integer not null default 0,
  inbox_count integer not null default 0,
  active_inbox_count integer not null default 0,
  sent_total bigint not null default 0,
  reply_count_total bigint not null default 0,
  auto_reply_count_total bigint not null default 0,
  rr_pct numeric(8,4),
  first_metric_date date,
  last_metric_date date,
  source_coverage_status text not null default 'unknown'
    check (source_coverage_status in ('full', 'partial', 'not_found', 'unknown')),
  source_max_synced_at timestamptz,
  is_free_mail boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_infra_domain_metrics_provider on public.infra_domain_metrics(provider_group);
create index if not exists idx_infra_domain_metrics_sent on public.infra_domain_metrics(sent_total desc);
create index if not exists idx_infra_domain_metrics_rr on public.infra_domain_metrics(rr_pct);
create index if not exists idx_infra_domain_metrics_fresh on public.infra_domain_metrics(source_max_synced_at desc);

-- =========================================================================
-- 5E — infra_sync_runs: sync run audit log
-- =========================================================================

create table if not exists public.infra_sync_runs (
  id uuid primary key default gen_random_uuid(),
  run_type text not null,
  started_at timestamptz not null,
  completed_at timestamptz,
  status text not null check (status in ('running', 'completed', 'partial', 'failed')),
  workspace_count integer not null default 0,
  accounts_seen integer not null default 0,
  account_metric_rows integer not null default 0,
  domains_written integer not null default 0,
  api_calls_made integer not null default 0,
  rate_limit_events integer not null default 0,
  errors text[] not null default '{}',
  duration_ms bigint
);

-- =========================================================================
-- 5F — domain_rr_state: current RR scorer state per domain
-- =========================================================================

create table if not exists public.domain_rr_state (
  domain text primary key,
  provider_group text not null check (provider_group in ('google_otd', 'outlook', 'unknown')),
  current_status text not null check (
    current_status in (
      'great', 'good', 'warmup', 'retire', 'unscored',
      'unknown_provider', 'free_mail_excluded', 'stale_data', 'sync_zero'
    )
  ),
  status_since timestamptz not null,
  phase_baseline_sent bigint not null default 0,
  phase_baseline_replies bigint not null default 0,
  phase_baseline_at timestamptz,
  phase_baseline_reason text,
  last_sent_total bigint not null default 0,
  last_reply_count bigint not null default 0,
  last_auto_reply_count bigint not null default 0,
  last_rr_pct numeric(8,4),
  last_phase_sent bigint not null default 0,
  last_phase_replies bigint not null default 0,
  last_phase_rr_pct numeric(8,4),
  inbox_count integer not null default 0,
  active_inbox_count integer not null default 0,
  source_max_synced_at timestamptz,
  data_freshness_status text not null default 'unknown'
    check (data_freshness_status in ('fresh', 'stale', 'unknown', 'sync_zero')),
  last_evaluated_at timestamptz not null
);

create index if not exists idx_domain_rr_state_status on public.domain_rr_state(current_status);
create index if not exists idx_domain_rr_state_provider on public.domain_rr_state(provider_group);
create index if not exists idx_domain_rr_state_fresh on public.domain_rr_state(source_max_synced_at desc);

-- =========================================================================
-- 5G — domain_rr_events: RR state-change events (warmup/retire/baseline-reset/etc.)
-- =========================================================================

create table if not exists public.domain_rr_events (
  id uuid primary key default gen_random_uuid(),
  domain text not null,
  event_type text not null,
  from_status text,
  to_status text,
  sent_total bigint,
  reply_count bigint,
  rr_pct numeric(8,4),
  phase_sent bigint,
  phase_replies bigint,
  phase_rr_pct numeric(8,4),
  reason text,
  notified_at timestamptz,
  notification_channel text,
  created_at timestamptz not null default now()
);

create index if not exists idx_domain_rr_events_domain_created on public.domain_rr_events(domain, created_at desc);
create index if not exists idx_domain_rr_events_type_created on public.domain_rr_events(event_type, created_at desc);
create index if not exists idx_domain_rr_events_notify on public.domain_rr_events(notified_at) where notified_at is null;
