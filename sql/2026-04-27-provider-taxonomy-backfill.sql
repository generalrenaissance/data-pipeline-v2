-- [2026-04-27] Provider taxonomy backfill
--
-- Replaces workspace-slug-derived provider attribution with account-level
-- Instantly provider_code attribution.
--
-- Mapping, per Sam decision 2026-04-26:
--   1 Custom IMAP/SMTP -> google_otd
--   2 Google           -> google_otd
--   3 Microsoft        -> outlook
--   4 AWS              -> unknown
--   8 AirMail          -> unknown
--   null/other         -> unknown

set statement_timeout = '15min';

begin;

-- 1. Account inventory: provider_group from provider_code_raw.
update public.infra_accounts
set provider_group = case
  when provider_code_raw in (1, 2) then 'google_otd'
  when provider_code_raw = 3 then 'outlook'
  else 'unknown'
end
where provider_group is distinct from case
  when provider_code_raw in (1, 2) then 'google_otd'
  when provider_code_raw = 3 then 'outlook'
  else 'unknown'
end;

-- 2. Account daily metrics: inherit account-level provider_group.
update public.infra_account_daily_metrics m
set provider_group = a.provider_group
from public.infra_accounts a
where m.account_email = a.account_email
  and m.provider_group is distinct from a.provider_group;

create temp table tmp_domain_inventory on commit drop as
select
  domain,
  count(*)::integer as inbox_count,
  count(*) filter (where account_status = 'active')::integer as active_inbox_count,
  count(distinct workspace_slug)::integer as workspace_count,
  bool_or(is_free_mail)::boolean as is_free_mail
from public.infra_accounts
group by domain;

create temp table tmp_domain_provider_7d on commit drop as
select
  m.domain,
  m.provider_group,
  sum(m.sent)::bigint as sent_7d,
  count(distinct a.account_email) filter (where a.account_status = 'active')::integer as active_account_count
from public.infra_account_daily_metrics m
left join public.infra_accounts a on a.account_email = m.account_email
where m.metric_date >= current_date - interval '7 days'
group by m.domain, m.provider_group;

create temp table tmp_domain_dominant_7d on commit drop as
select distinct on (domain)
  domain,
  provider_group
from tmp_domain_provider_7d
order by domain, sent_7d desc, active_account_count desc, provider_group asc;

create temp table tmp_domain_provider_lifetime on commit drop as
select
  m.domain,
  m.provider_group,
  sum(m.sent)::bigint as sent_total,
  count(distinct a.account_email) filter (where a.account_status = 'active')::integer as active_account_count
from public.infra_account_daily_metrics m
left join public.infra_accounts a on a.account_email = m.account_email
group by m.domain, m.provider_group;

create temp table tmp_domain_dominant_lifetime on commit drop as
select distinct on (domain)
  domain,
  provider_group
from tmp_domain_provider_lifetime
order by domain, sent_total desc, active_account_count desc, provider_group asc;

create temp table tmp_domain_provider_code_lifetime on commit drop as
select
  m.domain,
  a.provider_code_raw,
  sum(m.sent)::bigint as sent_total,
  count(distinct a.account_email) filter (where a.account_status = 'active')::integer as active_account_count
from public.infra_account_daily_metrics m
join public.infra_accounts a on a.account_email = m.account_email
where a.provider_code_raw is not null
group by m.domain, a.provider_code_raw;

create temp table tmp_domain_dominant_code on commit drop as
select distinct on (domain)
  domain,
  provider_code_raw
from tmp_domain_provider_code_lifetime
order by domain, sent_total desc, active_account_count desc, provider_code_raw asc;

-- 3. Domain daily metrics: one row per domain/day, provider_group from the
-- domain's dominant 7-day sent-volume provider.
truncate table public.infra_domain_daily_metrics;

insert into public.infra_domain_daily_metrics (
  domain,
  metric_date,
  provider_group,
  workspace_count,
  inbox_count,
  active_inbox_count,
  sent,
  replies,
  replies_automatic,
  rr_pct,
  api_synced_at
)
select
  m.domain,
  m.metric_date,
  coalesce(d7.provider_group, dl.provider_group, 'unknown') as provider_group,
  coalesce(i.workspace_count, 0) as workspace_count,
  coalesce(i.inbox_count, 0) as inbox_count,
  coalesce(i.active_inbox_count, 0) as active_inbox_count,
  sum(m.sent)::integer as sent,
  sum(m.replies)::integer as replies,
  sum(m.replies_automatic)::integer as replies_automatic,
  case
    when sum(m.sent) > 0 then (sum(m.replies)::numeric / sum(m.sent)::numeric) * 100
    else null
  end as rr_pct,
  max(m.api_synced_at) as api_synced_at
from public.infra_account_daily_metrics m
left join tmp_domain_inventory i on i.domain = m.domain
left join tmp_domain_dominant_7d d7 on d7.domain = m.domain
left join tmp_domain_dominant_lifetime dl on dl.domain = m.domain
group by
  m.domain,
  m.metric_date,
  coalesce(d7.provider_group, dl.provider_group, 'unknown'),
  i.workspace_count,
  i.inbox_count,
  i.active_inbox_count;

-- 4. Domain lifetime metrics: totals from account-level daily metrics,
-- provider_group from lifetime sent-volume dominance.
truncate table public.infra_domain_metrics;

insert into public.infra_domain_metrics (
  domain,
  provider_group,
  dominant_provider_raw,
  workspace_count,
  inbox_count,
  active_inbox_count,
  sent_total,
  reply_count_total,
  auto_reply_count_total,
  rr_pct,
  first_metric_date,
  last_metric_date,
  source_coverage_status,
  source_max_synced_at,
  is_free_mail,
  updated_at
)
select
  m.domain,
  coalesce(dl.provider_group, 'unknown') as provider_group,
  dc.provider_code_raw as dominant_provider_raw,
  coalesce(i.workspace_count, 0) as workspace_count,
  coalesce(i.inbox_count, 0) as inbox_count,
  coalesce(i.active_inbox_count, 0) as active_inbox_count,
  sum(m.sent)::bigint as sent_total,
  sum(m.replies)::bigint as reply_count_total,
  sum(m.replies_automatic)::bigint as auto_reply_count_total,
  case
    when sum(m.sent) > 0 then (sum(m.replies)::numeric / sum(m.sent)::numeric) * 100
    else null
  end as rr_pct,
  min(m.metric_date) as first_metric_date,
  max(m.metric_date) as last_metric_date,
  case
    when coalesce(i.inbox_count, 0) = 0 then 'unknown'
    when sum(m.sent) > 0 then 'full'
    else 'partial'
  end as source_coverage_status,
  max(m.api_synced_at) as source_max_synced_at,
  coalesce(i.is_free_mail, false) as is_free_mail,
  now() as updated_at
from public.infra_account_daily_metrics m
left join tmp_domain_inventory i on i.domain = m.domain
left join tmp_domain_dominant_lifetime dl on dl.domain = m.domain
left join tmp_domain_dominant_code dc on dc.domain = m.domain
group by
  m.domain,
  coalesce(dl.provider_group, 'unknown'),
  dc.provider_code_raw,
  i.workspace_count,
  i.inbox_count,
  i.active_inbox_count,
  i.is_free_mail;

commit;
