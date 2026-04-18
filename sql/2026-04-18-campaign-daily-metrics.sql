-- Per-campaign per-calendar-day metrics from Instantly /campaigns/analytics/daily.
-- Created to replace LAG-over-/steps derivation for campaign-day truth (see
-- specs/2026-04-18-daily-sends-sync-proposal.md). /steps is cumulative and
-- eventually consistent; /daily returns UI-aligned day values.

create table if not exists public.campaign_daily_metrics (
  campaign_id                 text not null,
  date                        date not null,
  sent                        integer not null default 0,
  contacted                   integer not null default 0,
  new_leads_contacted         integer not null default 0,
  opened                      integer not null default 0,
  unique_opened               integer not null default 0,
  replies                     integer not null default 0,
  unique_replies              integer not null default 0,
  replies_automatic           integer not null default 0,
  unique_replies_automatic    integer not null default 0,
  clicks                      integer not null default 0,
  unique_clicks               integer not null default 0,
  opportunities               integer not null default 0,
  unique_opportunities        integer not null default 0,
  synced_at                   timestamptz not null default now(),
  primary key (campaign_id, date)
);

create index if not exists campaign_daily_metrics_date_idx
  on public.campaign_daily_metrics (date);

create index if not exists campaign_daily_metrics_campaign_id_idx
  on public.campaign_daily_metrics (campaign_id);

comment on table public.campaign_daily_metrics is
  'RAW - Instantly /campaigns/analytics/daily campaign-level daily metrics aligned to Instantly UI day semantics. Upserted by data-pipeline-v2 via trailing-window refresh; use for campaign-level daily sent/replies/opportunities.';
