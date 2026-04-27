-- [2026-04-27] Rebuild infra aggregate rollups server-side.
--
-- Replaces the JS rebuildAggregates() path that paginated
-- infra_account_daily_metrics through PostgREST and hit Supabase's role-level
-- statement_timeout on large windows.
--
-- Usage:
--   set statement_timeout = '15min';
--   select * from public.rebuild_infra_aggregates('renaissance-4', current_date - 2, current_date);
--
-- Returns: (provider_group text, domains_written bigint) for summary logging.
--
-- Provider rollup mirrors sql/2026-04-27-provider-taxonomy-backfill.sql:
-- highest sent volume wins; ties break by active account count, then lexical
-- provider_group / raw provider code.

create or replace function public.rebuild_infra_aggregates(
  p_workspace_filter text default null,
  p_start_date       date default null,
  p_end_date         date default null
) returns table(provider_group text, domains_written bigint)
language plpgsql
as $$
declare
  v_end_date date := coalesce(p_end_date, current_date);
  v_updated_at timestamptz := now();
begin
  -- Caller must SET session-level statement_timeout before invoking this
  -- function. Function-level SET does not reliably override Supabase's
  -- role-level timeout for this workload.
  if to_regclass('pg_temp.tmp_rebuild_domain_scope') is not null then drop table tmp_rebuild_domain_scope; end if;
  if to_regclass('pg_temp.tmp_rebuild_inventory') is not null then drop table tmp_rebuild_inventory; end if;
  if to_regclass('pg_temp.tmp_rebuild_lifetime_source') is not null then drop table tmp_rebuild_lifetime_source; end if;
  if to_regclass('pg_temp.tmp_rebuild_provider_7d') is not null then drop table tmp_rebuild_provider_7d; end if;
  if to_regclass('pg_temp.tmp_rebuild_dominant_7d') is not null then drop table tmp_rebuild_dominant_7d; end if;
  if to_regclass('pg_temp.tmp_rebuild_provider_lifetime') is not null then drop table tmp_rebuild_provider_lifetime; end if;
  if to_regclass('pg_temp.tmp_rebuild_dominant_lifetime') is not null then drop table tmp_rebuild_dominant_lifetime; end if;
  if to_regclass('pg_temp.tmp_rebuild_provider_code_lifetime') is not null then drop table tmp_rebuild_provider_code_lifetime; end if;
  if to_regclass('pg_temp.tmp_rebuild_dominant_code') is not null then drop table tmp_rebuild_dominant_code; end if;
  if to_regclass('pg_temp.tmp_rebuild_daily_source') is not null then drop table tmp_rebuild_daily_source; end if;
  if to_regclass('pg_temp.tmp_rebuild_daily_rows') is not null then drop table tmp_rebuild_daily_rows; end if;
  if to_regclass('pg_temp.tmp_rebuild_lifetime_rows') is not null then drop table tmp_rebuild_lifetime_rows; end if;

  create temp table tmp_rebuild_domain_scope on commit drop as
  select distinct m.domain
  from public.infra_account_daily_metrics m
  where (p_workspace_filter is null or m.workspace_slug = p_workspace_filter)
    and (p_start_date is null or m.metric_date >= p_start_date)
    and m.metric_date <= v_end_date;

  if not exists (select 1 from tmp_rebuild_domain_scope) then
    return;
  end if;

  create temp table tmp_rebuild_inventory on commit drop as
  select
    a.domain,
    count(*)::integer as inbox_count,
    count(*) filter (where a.account_status = 'active')::integer as active_inbox_count,
    count(distinct a.workspace_slug)::integer as workspace_count,
    bool_or(a.is_free_mail)::boolean as is_free_mail
  from public.infra_accounts a
  join tmp_rebuild_domain_scope s on s.domain = a.domain
  group by a.domain;

  create temp table tmp_rebuild_lifetime_source on commit drop as
  select
    m.account_email,
    m.metric_date,
    m.domain,
    m.workspace_slug,
    m.provider_group,
    m.sent,
    m.replies,
    m.replies_automatic,
    m.api_synced_at
  from public.infra_account_daily_metrics m
  join tmp_rebuild_domain_scope s on s.domain = m.domain;

  create index on tmp_rebuild_lifetime_source(domain, metric_date);
  create index on tmp_rebuild_lifetime_source(account_email);

  create temp table tmp_rebuild_provider_7d on commit drop as
  select
    m.domain,
    coalesce(m.provider_group, 'unknown') as provider_group,
    sum(m.sent)::bigint as sent_7d,
    count(distinct a.account_email) filter (where a.account_status = 'active')::integer as active_account_count
  from tmp_rebuild_lifetime_source m
  left join public.infra_accounts a on a.account_email = m.account_email
  where m.metric_date >= current_date - interval '7 days'
  group by m.domain, coalesce(m.provider_group, 'unknown');

  create temp table tmp_rebuild_dominant_7d on commit drop as
  select distinct on (p.domain)
    p.domain,
    p.provider_group
  from tmp_rebuild_provider_7d p
  order by p.domain, p.sent_7d desc, p.active_account_count desc, p.provider_group asc;

  create temp table tmp_rebuild_provider_lifetime on commit drop as
  select
    m.domain,
    coalesce(m.provider_group, 'unknown') as provider_group,
    sum(m.sent)::bigint as sent_total,
    count(distinct a.account_email) filter (where a.account_status = 'active')::integer as active_account_count
  from tmp_rebuild_lifetime_source m
  left join public.infra_accounts a on a.account_email = m.account_email
  group by m.domain, coalesce(m.provider_group, 'unknown');

  create temp table tmp_rebuild_dominant_lifetime on commit drop as
  select distinct on (p.domain)
    p.domain,
    p.provider_group
  from tmp_rebuild_provider_lifetime p
  order by p.domain, p.sent_total desc, p.active_account_count desc, p.provider_group asc;

  create temp table tmp_rebuild_provider_code_lifetime on commit drop as
  select
    m.domain,
    a.provider_code_raw,
    sum(m.sent)::bigint as sent_total,
    count(distinct a.account_email) filter (where a.account_status = 'active')::integer as active_account_count
  from tmp_rebuild_lifetime_source m
  join public.infra_accounts a on a.account_email = m.account_email
  where a.provider_code_raw is not null
  group by m.domain, a.provider_code_raw;

  create temp table tmp_rebuild_dominant_code on commit drop as
  select distinct on (p.domain)
    p.domain,
    p.provider_code_raw
  from tmp_rebuild_provider_code_lifetime p
  order by p.domain, p.sent_total desc, p.active_account_count desc, p.provider_code_raw asc;

  create temp table tmp_rebuild_daily_source on commit drop as
  select
    m.domain,
    m.metric_date,
    sum(m.sent)::integer as sent,
    sum(m.replies)::integer as replies,
    sum(m.replies_automatic)::integer as replies_automatic,
    max(m.api_synced_at) as api_synced_at
  from tmp_rebuild_lifetime_source m
  where (p_start_date is null or m.metric_date >= p_start_date)
    and m.metric_date <= v_end_date
  group by m.domain, m.metric_date;

  create temp table tmp_rebuild_daily_rows on commit drop as
  select
    s.domain,
    d.metric_date,
    coalesce(d7.provider_group, dl.provider_group, 'unknown') as provider_group,
    coalesce(i.workspace_count, 0) as workspace_count,
    coalesce(i.inbox_count, 0) as inbox_count,
    coalesce(i.active_inbox_count, 0) as active_inbox_count,
    coalesce(ds.sent, 0) as sent,
    coalesce(ds.replies, 0) as replies,
    coalesce(ds.replies_automatic, 0) as replies_automatic,
    case
      when coalesce(ds.sent, 0) > 0 then (ds.replies::numeric / ds.sent::numeric) * 100
      else null
    end as rr_pct,
    coalesce(ds.api_synced_at, v_updated_at) as api_synced_at
  from tmp_rebuild_domain_scope s
  cross join lateral (
    select gs::date as metric_date
    from generate_series(p_start_date, v_end_date, interval '1 day') gs
    where p_start_date is not null
    union all
    select distinct ds2.metric_date
    from tmp_rebuild_daily_source ds2
    where p_start_date is null
      and ds2.domain = s.domain
  ) d
  left join tmp_rebuild_daily_source ds
    on ds.domain = s.domain
   and ds.metric_date = d.metric_date
  left join tmp_rebuild_inventory i on i.domain = s.domain
  left join tmp_rebuild_dominant_7d d7 on d7.domain = s.domain
  left join tmp_rebuild_dominant_lifetime dl on dl.domain = s.domain;

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
    r.domain,
    r.metric_date,
    r.provider_group,
    r.workspace_count,
    r.inbox_count,
    r.active_inbox_count,
    r.sent,
    r.replies,
    r.replies_automatic,
    r.rr_pct,
    r.api_synced_at
  from tmp_rebuild_daily_rows r
  on conflict (domain, metric_date) do update set
    provider_group = excluded.provider_group,
    workspace_count = excluded.workspace_count,
    inbox_count = excluded.inbox_count,
    active_inbox_count = excluded.active_inbox_count,
    sent = excluded.sent,
    replies = excluded.replies,
    replies_automatic = excluded.replies_automatic,
    rr_pct = excluded.rr_pct,
    api_synced_at = excluded.api_synced_at;

  create temp table tmp_rebuild_lifetime_rows on commit drop as
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
    v_updated_at as updated_at
  from tmp_rebuild_lifetime_source m
  left join tmp_rebuild_inventory i on i.domain = m.domain
  left join tmp_rebuild_dominant_lifetime dl on dl.domain = m.domain
  left join tmp_rebuild_dominant_code dc on dc.domain = m.domain
  group by
    m.domain,
    coalesce(dl.provider_group, 'unknown'),
    dc.provider_code_raw,
    i.workspace_count,
    i.inbox_count,
    i.active_inbox_count,
    i.is_free_mail;

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
    r.domain,
    r.provider_group,
    r.dominant_provider_raw,
    r.workspace_count,
    r.inbox_count,
    r.active_inbox_count,
    r.sent_total,
    r.reply_count_total,
    r.auto_reply_count_total,
    r.rr_pct,
    r.first_metric_date,
    r.last_metric_date,
    r.source_coverage_status,
    r.source_max_synced_at,
    r.is_free_mail,
    r.updated_at
  from tmp_rebuild_lifetime_rows r
  on conflict (domain) do update set
    provider_group = excluded.provider_group,
    dominant_provider_raw = excluded.dominant_provider_raw,
    workspace_count = excluded.workspace_count,
    inbox_count = excluded.inbox_count,
    active_inbox_count = excluded.active_inbox_count,
    sent_total = excluded.sent_total,
    reply_count_total = excluded.reply_count_total,
    auto_reply_count_total = excluded.auto_reply_count_total,
    rr_pct = excluded.rr_pct,
    first_metric_date = excluded.first_metric_date,
    last_metric_date = excluded.last_metric_date,
    source_coverage_status = excluded.source_coverage_status,
    source_max_synced_at = excluded.source_max_synced_at,
    is_free_mail = excluded.is_free_mail,
    updated_at = excluded.updated_at;

  return query
    select r.provider_group, count(*)::bigint as domains_written
    from tmp_rebuild_lifetime_rows r
    group by r.provider_group
    order by r.provider_group;
end;
$$;

comment on function public.rebuild_infra_aggregates(text, date, date) is
  'Rebuilds infra_domain_daily_metrics and infra_domain_metrics for affected domains from infra_account_daily_metrics. Scope is selected by workspace/date filters; heavy rollups run server-side and require caller to SET session-level statement_timeout.';
