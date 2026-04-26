-- Domain Infra Sheet Registry + Account Tag Bridge [2026-04-25]
--
-- Idempotent migration for the trusted sending-domain ownership registry.

create table if not exists public.infra_sheet_registry (
  source_tab text not null,
  source_row integer not null,
  tag text,
  offer text,
  campaign_manager text,
  workspace_name text,
  workspace_slug text,
  sheet_status text,
  deliverability_label text,
  need_warmup boolean,
  group_name text,
  pair text,
  inbox_manager text,
  billing_date date,
  warmup_start_date date,
  brand_name text,
  brand_domain text,
  infra_type text,
  technical text,
  batch text,
  email_provider text,
  provider_group text check (provider_group in ('google_otd', 'outlook', 'unknown')),
  accounts_expected integer,
  cold_per_account integer,
  warmup_per_account integer,
  expected_daily_cold integer,
  accounts_per_domain numeric,
  expected_domain_count numeric,
  tag_value numeric,
  domain_purchase_date date,
  low_rr boolean,
  warmup_emails_daily integer,
  row_confidence text not null check (row_confidence in ('high', 'medium', 'low', 'invalid')),
  row_warnings text[] not null default '{}',
  raw_row jsonb not null,
  sheet_synced_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (source_tab, source_row)
);

comment on table public.infra_sheet_registry is
  'Normalized Renaissance Sheet tag/group rows. Refreshed by scripts/sync-infra-sheet-registry.ts from the local sheet JSON dump.';

create index if not exists idx_infra_sheet_registry_tag on public.infra_sheet_registry(tag);
create index if not exists idx_infra_sheet_registry_campaign_manager on public.infra_sheet_registry(campaign_manager);
create index if not exists idx_infra_sheet_registry_workspace_slug on public.infra_sheet_registry(workspace_slug);
create index if not exists idx_infra_sheet_registry_provider_group on public.infra_sheet_registry(provider_group);
create index if not exists idx_infra_sheet_registry_sheet_status on public.infra_sheet_registry(sheet_status);

create table if not exists public.infra_brand_registry (
  source_tab text not null,
  source_row integer not null,
  offer text not null,
  brand_name text not null,
  brand_domain text,
  group_assigned text,
  date_created date,
  setup_status text,
  raw_row jsonb not null,
  sheet_synced_at timestamptz not null default now(),
  primary key (source_tab, source_row, offer, brand_name)
);

comment on table public.infra_brand_registry is
  'Normalized brand allocation sheet rows. Refreshed by scripts/sync-infra-sheet-registry.ts from the local sheet JSON dump.';

create index if not exists idx_infra_brand_registry_brand_name on public.infra_brand_registry(brand_name);
create index if not exists idx_infra_brand_registry_brand_domain on public.infra_brand_registry(brand_domain);
create index if not exists idx_infra_brand_registry_offer on public.infra_brand_registry(offer);

create table if not exists public.infra_cancelled_registry (
  source_tab text not null,
  source_row integer not null,
  tag text,
  offer text,
  campaign_manager text,
  inbox_manager text,
  workspace_name text,
  workspace_slug text,
  sheet_status text,
  brand_name text,
  infra_type text,
  technical text,
  batch text,
  warmup_start_date date,
  domain_purchase_date date,
  accounts_expected integer,
  cold_per_account integer,
  warmup_per_account integer,
  row_confidence text not null check (row_confidence in ('high', 'medium', 'low', 'invalid')),
  row_warnings text[] not null default '{}',
  raw_row jsonb not null,
  sheet_synced_at timestamptz not null default now(),
  primary key (source_tab, source_row)
);

comment on table public.infra_cancelled_registry is
  'Normalized cancelled/retired tag registry. Refreshed by scripts/sync-infra-sheet-registry.ts from Cancelled.json.';

create index if not exists idx_infra_cancelled_registry_tag on public.infra_cancelled_registry(tag);
create index if not exists idx_infra_cancelled_registry_campaign_manager on public.infra_cancelled_registry(campaign_manager);
create index if not exists idx_infra_cancelled_registry_sheet_status on public.infra_cancelled_registry(sheet_status);

create table if not exists public.infra_account_tag_mappings (
  workspace_slug text not null,
  account_email text not null,
  resource_id text not null,
  domain text,
  tag_id text not null,
  tag_label text not null,
  resource_type integer not null,
  mapping_id text,
  api_synced_at timestamptz not null,
  raw_mapping jsonb not null,
  primary key (workspace_slug, account_email, tag_id)
);

comment on table public.infra_account_tag_mappings is
  'Instantly account-to-custom-tag mappings. Refreshed by scripts/sync-infra-account-tags.ts after probing account resource_type.';

create index if not exists idx_infra_account_tag_mappings_domain on public.infra_account_tag_mappings(domain);
create index if not exists idx_infra_account_tag_mappings_tag_label on public.infra_account_tag_mappings(tag_label);
create index if not exists idx_infra_account_tag_mappings_workspace_tag on public.infra_account_tag_mappings(workspace_slug, tag_label);

create table if not exists public.infra_domain_registry (
  domain text primary key,
  provider_group text not null check (provider_group in ('google_otd', 'outlook', 'unknown')),
  primary_campaign_manager text,
  campaign_managers text[] not null default '{}',
  tag_labels text[] not null default '{}',
  sheet_tags text[] not null default '{}',
  workspace_slugs text[] not null default '{}',
  workspace_names text[] not null default '{}',
  offers text[] not null default '{}',
  brand_names text[] not null default '{}',
  brand_domains text[] not null default '{}',
  sheet_statuses text[] not null default '{}',
  infra_types text[] not null default '{}',
  inbox_managers text[] not null default '{}',
  group_names text[] not null default '{}',
  pairs text[] not null default '{}',
  email_providers text[] not null default '{}',
  batches text[] not null default '{}',
  accounts_per_domain_values numeric[] not null default '{}',
  expected_domain_count_values numeric[] not null default '{}',
  domain_purchase_dates date[] not null default '{}',
  low_rr_flags boolean[] not null default '{}',
  mapped_account_count integer not null default 0,
  unmapped_account_count integer not null default 0,
  total_account_count integer not null default 0,
  active_account_count integer not null default 0,
  sheet_accounts_expected_total integer,
  expected_daily_cold_total integer,
  cancelled_match_count integer not null default 0,
  mapping_status text not null check (
    mapping_status in (
      'mapped',
      'unmapped',
      'no_account_tags',
      'sheet_tag_missing',
      'mixed_cm',
      'cancelled',
      'free_mail_excluded',
      'invalid'
    )
  ),
  confidence_score integer not null default 0,
  mapping_warnings text[] not null default '{}',
  last_built_at timestamptz not null default now()
);

comment on table public.infra_domain_registry is
  'Per-sending-domain ownership rollup from infra_accounts, account tags, sheet registry, and cancelled registry. Rebuilt by scripts/build-domain-registry.ts.';

create index if not exists idx_infra_domain_registry_primary_cm on public.infra_domain_registry(primary_campaign_manager);
create index if not exists idx_infra_domain_registry_mapping_status on public.infra_domain_registry(mapping_status);
create index if not exists idx_infra_domain_registry_provider_group on public.infra_domain_registry(provider_group);
create index if not exists idx_infra_domain_registry_campaign_managers_gin on public.infra_domain_registry using gin(campaign_managers);
create index if not exists idx_infra_domain_registry_tag_labels_gin on public.infra_domain_registry using gin(tag_labels);
