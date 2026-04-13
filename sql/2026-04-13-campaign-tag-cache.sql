create table if not exists public.campaign_tag_cache (
  workspace_id text not null,
  campaign_id text not null,
  tags text[] not null default '{}',
  refreshed_at timestamptz not null default now(),
  primary key (workspace_id, campaign_id)
);

comment on table public.campaign_tag_cache is
  'Resolved campaign tag labels for hourly data pipeline reads. Refreshed by the dedicated Campaign Tag Sync workflow.';
