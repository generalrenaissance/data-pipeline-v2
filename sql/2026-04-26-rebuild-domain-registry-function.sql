-- Domain Infra Registry — server-side rebuild function [2026-04-26]
--
-- Replaces the JS rebuildDomainRegistry that paginated 10M+ rows through PostgREST
-- (~2h runtime, hits 8s statement_timeout on deep offsets even with workspace
-- partitioning). Server-side rebuild runs in seconds because the join + aggregate
-- happens where the data lives.
--
-- Usage:
--   select * from public.rebuild_domain_registry();
-- Returns: (mapping_status text, count bigint) — distribution of statuses written.
--
-- Logic ported verbatim from src/infra/domain-registry.ts:buildDomainRegistryRows.
-- The TypeScript version remains for unit-test coverage (test/domain-infra-registry.test.ts).
-- Production path (scripts/build-domain-registry.ts + cron) calls this function instead.

create or replace function public.rebuild_domain_registry()
returns table (mapping_status text, "count" bigint)
language plpgsql
as $$
declare
  v_built_at timestamptz := now();
begin
  truncate table public.infra_domain_registry;

  insert into public.infra_domain_registry (
    domain, provider_group, primary_campaign_manager, campaign_managers,
    tag_labels, sheet_tags, workspace_slugs, workspace_names,
    offers, brand_names, brand_domains, sheet_statuses,
    infra_types, inbox_managers, group_names, pairs,
    email_providers, batches, accounts_per_domain_values,
    expected_domain_count_values, domain_purchase_dates, low_rr_flags,
    mapped_account_count, unmapped_account_count, total_account_count,
    active_account_count, sheet_accounts_expected_total, expected_daily_cold_total,
    cancelled_match_count, mapping_status, confidence_score, mapping_warnings,
    last_built_at
  )
  with
  -- Step 1: account-tag join. Emails are already lowercased on both tables
  -- (verified 2026-04-26: zero rows mismatch lower(account_email)), so we
  -- omit lower() on the join key to let the (workspace_slug, account_email, tag_id)
  -- primary key index drive the lookup.
  account_tags as (
    select
      a.domain,
      a.workspace_slug,
      a.account_email,
      m.tag_label,
      lower(trim(m.tag_label)) as tag_key
    from public.infra_accounts a
    join public.infra_account_tag_mappings m
      on m.workspace_slug = a.workspace_slug
     and m.account_email = a.account_email
  ),
  -- Step 2: per-domain account aggregates from infra_accounts.
  domain_accounts as (
    select
      a.domain,
      array_agg(distinct a.workspace_slug order by a.workspace_slug)
        filter (where a.workspace_slug is not null and trim(a.workspace_slug) <> '') as workspace_slugs,
      array_agg(distinct a.workspace_name order by a.workspace_name)
        filter (where a.workspace_name is not null and trim(a.workspace_name) <> '') as workspace_names_acc,
      count(*) as total_account_count,
      count(*) filter (where a.account_status = 'active') as active_account_count,
      bool_or(a.is_free_mail) as has_free_mail
    from public.infra_accounts a
    group by a.domain
  ),
  -- Step 3: count of distinct accounts that have at least one tag mapping.
  domain_mapped_accounts as (
    select domain, count(distinct account_email) as mapped_account_count
    from account_tags
    group by domain
  ),
  -- Step 4: per-domain most-common provider_group (mode), with deterministic tiebreaker.
  -- Matches JS: mostCommonProvider() — picks max count, ties broken by first-seen.
  -- SQL version ties broken alphabetically (acceptable: still deterministic).
  domain_provider as (
    select distinct on (domain) domain, provider_group
    from (
      select domain, provider_group, count(*) as n
      from public.infra_accounts
      group by domain, provider_group
    ) p
    order by domain, n desc, provider_group asc
  ),
  -- Step 5: per-domain unique tag labels (deduped, case-preserving for output).
  domain_tags as (
    select
      domain,
      array_agg(distinct tag_label order by tag_label)
        filter (where tag_label is not null and trim(tag_label) <> '') as tag_labels
    from account_tags
    group by domain
  ),
  -- Step 6: domain × sheet rows joined via tag_label = sheet.tag (case-insensitive,
  -- trimmed). Excludes invalid sheet rows. DISTINCT to dedup the same sheet row
  -- joining via multiple account-tag pairs.
  domain_sheet_joins as (
    select distinct
      at.domain,
      s.tag,
      s.offer,
      s.campaign_manager,
      s.workspace_name as sheet_workspace_name,
      s.sheet_status,
      s.brand_name,
      s.brand_domain,
      s.infra_type,
      s.inbox_manager,
      s.group_name,
      s.pair,
      s.email_provider,
      s.batch,
      s.accounts_expected,
      s.expected_daily_cold,
      s.accounts_per_domain,
      s.expected_domain_count,
      s.domain_purchase_date,
      s.low_rr,
      s.row_confidence
    from account_tags at
    join public.infra_sheet_registry s
      on lower(trim(s.tag)) = at.tag_key
     and s.row_confidence <> 'invalid'
     and s.tag is not null
     and trim(s.tag) <> ''
  ),
  -- Step 7: domain × cancelled rows joined the same way.
  domain_cancelled as (
    select
      at.domain,
      count(distinct c.tag) as cancelled_match_count
    from account_tags at
    join public.infra_cancelled_registry c
      on lower(trim(c.tag)) = at.tag_key
     and c.row_confidence <> 'invalid'
     and c.tag is not null
     and trim(c.tag) <> ''
    group by at.domain
  ),
  -- Step 8: per-domain aggregates over the joined sheet rows.
  domain_sheet_agg as (
    select
      domain,
      count(*) as joined_sheet_count,
      count(*) filter (where row_confidence = 'high') as high_confidence_count,
      count(*) filter (where row_confidence = 'low') as low_confidence_count,
      array_agg(distinct campaign_manager order by campaign_manager)
        filter (where campaign_manager is not null and trim(campaign_manager) <> '') as campaign_managers,
      array_agg(distinct tag order by tag)
        filter (where tag is not null and trim(tag) <> '') as sheet_tags,
      array_agg(distinct sheet_workspace_name order by sheet_workspace_name)
        filter (where sheet_workspace_name is not null and trim(sheet_workspace_name) <> '') as workspace_names_sheet,
      array_agg(distinct offer order by offer)
        filter (where offer is not null and trim(offer) <> '') as offers,
      array_agg(distinct brand_name order by brand_name)
        filter (where brand_name is not null and trim(brand_name) <> '') as brand_names,
      array_agg(distinct brand_domain order by brand_domain)
        filter (where brand_domain is not null and trim(brand_domain) <> '') as brand_domains,
      array_agg(distinct sheet_status order by sheet_status)
        filter (where sheet_status is not null and trim(sheet_status) <> '') as sheet_statuses,
      array_agg(distinct infra_type order by infra_type)
        filter (where infra_type is not null and trim(infra_type) <> '') as infra_types,
      array_agg(distinct inbox_manager order by inbox_manager)
        filter (where inbox_manager is not null and trim(inbox_manager) <> '') as inbox_managers,
      array_agg(distinct group_name order by group_name)
        filter (where group_name is not null and trim(group_name) <> '') as group_names,
      array_agg(distinct pair order by pair)
        filter (where pair is not null and trim(pair) <> '') as pairs,
      array_agg(distinct email_provider order by email_provider)
        filter (where email_provider is not null and trim(email_provider) <> '') as email_providers,
      array_agg(distinct batch order by batch)
        filter (where batch is not null and trim(batch) <> '') as batches,
      sum(accounts_expected) as sheet_accounts_expected_total,
      sum(expected_daily_cold) as expected_daily_cold_total,
      array_agg(distinct accounts_per_domain order by accounts_per_domain)
        filter (where accounts_per_domain is not null) as accounts_per_domain_values,
      array_agg(distinct expected_domain_count order by expected_domain_count)
        filter (where expected_domain_count is not null) as expected_domain_count_values,
      array_agg(distinct domain_purchase_date order by domain_purchase_date)
        filter (where domain_purchase_date is not null) as domain_purchase_dates,
      array_agg(distinct low_rr order by low_rr)
        filter (where low_rr is not null) as low_rr_flags
    from domain_sheet_joins
    group by domain
  )
  -- Final assembly. LATERAL subquery computes mapping_status once, reused
  -- in primary_campaign_manager, confidence_score, and mapping_warnings.
  select
    da.domain,
    coalesce(dp.provider_group, 'unknown') as provider_group,
    case
      when status_calc.mapping_status = 'mapped'
       and array_length(coalesce(dsa.campaign_managers, array[]::text[]), 1) = 1
      then dsa.campaign_managers[1]
      else null
    end as primary_campaign_manager,
    coalesce(dsa.campaign_managers, array[]::text[]) as campaign_managers,
    coalesce(dt.tag_labels, array[]::text[]) as tag_labels,
    coalesce(dsa.sheet_tags, array[]::text[]) as sheet_tags,
    coalesce(da.workspace_slugs, array[]::text[]) as workspace_slugs,
    -- workspace_names: union of accounts.workspace_name AND sheet.workspace_name (matches JS).
    coalesce(
      (select array_agg(distinct n order by n)
       from unnest(coalesce(da.workspace_names_acc, array[]::text[]) || coalesce(dsa.workspace_names_sheet, array[]::text[])) as n),
      array[]::text[]
    ) as workspace_names,
    coalesce(dsa.offers, array[]::text[]) as offers,
    coalesce(dsa.brand_names, array[]::text[]) as brand_names,
    coalesce(dsa.brand_domains, array[]::text[]) as brand_domains,
    coalesce(dsa.sheet_statuses, array[]::text[]) as sheet_statuses,
    coalesce(dsa.infra_types, array[]::text[]) as infra_types,
    coalesce(dsa.inbox_managers, array[]::text[]) as inbox_managers,
    coalesce(dsa.group_names, array[]::text[]) as group_names,
    coalesce(dsa.pairs, array[]::text[]) as pairs,
    coalesce(dsa.email_providers, array[]::text[]) as email_providers,
    coalesce(dsa.batches, array[]::text[]) as batches,
    coalesce(dsa.accounts_per_domain_values, array[]::numeric[]) as accounts_per_domain_values,
    coalesce(dsa.expected_domain_count_values, array[]::numeric[]) as expected_domain_count_values,
    coalesce(dsa.domain_purchase_dates, array[]::date[]) as domain_purchase_dates,
    coalesce(dsa.low_rr_flags, array[]::boolean[]) as low_rr_flags,
    coalesce(dma.mapped_account_count, 0)::int as mapped_account_count,
    greatest(0, da.total_account_count - coalesce(dma.mapped_account_count, 0))::int as unmapped_account_count,
    da.total_account_count::int,
    da.active_account_count::int,
    dsa.sheet_accounts_expected_total::int,
    dsa.expected_daily_cold_total::int,
    coalesce(dc.cancelled_match_count, 0)::int as cancelled_match_count,
    status_calc.mapping_status,
    -- confidence_score: mirrors addScorePenalty() exactly.
    greatest(0, least(100,
      case
        when status_calc.mapping_status in ('free_mail_excluded', 'invalid') then 0
        else 100
          - case when status_calc.mapping_status = 'mixed_cm' then 40 else 0 end
          - case when status_calc.mapping_status = 'no_account_tags' then 35 else 0 end
          - case when status_calc.mapping_status = 'sheet_tag_missing' then 30 else 0 end
          - case when status_calc.mapping_status = 'cancelled'
                  or coalesce(dc.cancelled_match_count, 0) > 0 then 20 else 0 end
          - case when coalesce(dsa.low_confidence_count, 0) > 0 then 10 else 0 end
      end
    ))::int as confidence_score,
    -- mapping_warnings: ordered list of strings, mirrors JS warnings list.
    (
      array[]::text[]
      || case
           when array_length(coalesce(dt.tag_labels, array[]::text[]), 1) is not null
            and coalesce(dsa.joined_sheet_count, 0) = 0
           then array['account tags did not match sheet registry']
           else array[]::text[]
         end
      || case
           when array_length(coalesce(dsa.campaign_managers, array[]::text[]), 1) > 1
           then array['mixed campaign managers: ' || array_to_string(dsa.campaign_managers, ', ')]
           else array[]::text[]
         end
      || case
           when coalesce(dc.cancelled_match_count, 0) > 0
           then array['one or more tags matched cancelled registry']
           else array[]::text[]
         end
    ) as mapping_warnings,
    v_built_at as last_built_at
  from domain_accounts da
  left join domain_provider dp on dp.domain = da.domain
  left join domain_mapped_accounts dma on dma.domain = da.domain
  left join domain_tags dt on dt.domain = da.domain
  left join domain_sheet_agg dsa on dsa.domain = da.domain
  left join domain_cancelled dc on dc.domain = da.domain
  cross join lateral (
    select
      case
        when da.has_free_mail then 'free_mail_excluded'
        when da.total_account_count = 0 then 'invalid'
        when coalesce(array_length(dt.tag_labels, 1), 0) = 0 then 'no_account_tags'
        when coalesce(dsa.joined_sheet_count, 0) = 0
         and coalesce(dc.cancelled_match_count, 0) > 0 then 'cancelled'
        when coalesce(dsa.joined_sheet_count, 0) = 0 then 'sheet_tag_missing'
        when coalesce(dc.cancelled_match_count, 0) > 0
         and coalesce(dsa.high_confidence_count, 0) = 0 then 'cancelled'
        when array_length(coalesce(dsa.campaign_managers, array[]::text[]), 1) > 1 then 'mixed_cm'
        when array_length(coalesce(dsa.campaign_managers, array[]::text[]), 1) = 1 then 'mapped'
        else 'unmapped'
      end as mapping_status
  ) status_calc
  order by da.domain;

  return query
    select r.mapping_status, count(*) as "count"
    from public.infra_domain_registry r
    group by r.mapping_status
    order by count(*) desc;
end;
$$;

comment on function public.rebuild_domain_registry() is
  'Rebuilds infra_domain_registry from infra_accounts × infra_account_tag_mappings × infra_sheet_registry × infra_cancelled_registry. Logic mirrors src/infra/domain-registry.ts:buildDomainRegistryRows. Atomic via TRUNCATE+INSERT inside a single function call. Returns mapping_status distribution.';
