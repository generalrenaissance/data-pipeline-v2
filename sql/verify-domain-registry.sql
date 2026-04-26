-- Domain Infra Registry — Phase 5/6 Verification Query Pack [2026-04-25]
--
-- Run after `npm run build:domain-registry` completes. Single-file pack so
-- any picker-upper can `psql -f` it and get the full check sheet in one shot.
--
-- Source of truth for stop conditions:
--   specs/2026-04-25-domain-infra-registry-bridge-fix.md  (Phase 5)
--   specs/2026-04-25-observability-dashboard-infra-tab.md (coverage gate ≥70%)
--   handoffs/2026-04-24-domain-infra-foundation-deploy.md (foundation stop conditions)

\timing on
\pset border 2

\echo
\echo === 1. Mapping status distribution ===
\echo Foundation stop condition #3: <30% of high-volume sending domains may be no_account_tags or sheet_tag_missing.
select mapping_status, count(*) as domains
from infra_domain_registry
group by 1
order by 2 desc;

\echo
\echo === 2. CM distribution among mapped domains ===
\echo Sanity check: expect concentration on Leo / Andres / Frank (current owners). No CM should be 100%.
select primary_campaign_manager, count(*) as domains
from infra_domain_registry
where mapping_status = 'mapped'
group by 1
order by 2 desc;

\echo
\echo === 3. Mixed-CM rate ===
\echo Foundation stop condition #4: <5% of mapped domains may be mixed_cm.
with totals as (
  select
    count(*) filter (where mapping_status = 'mapped')   as mapped,
    count(*) filter (where mapping_status = 'mixed_cm') as mixed
  from infra_domain_registry
)
select
  mapped,
  mixed,
  round(100.0 * mixed / nullif(mapped + mixed, 0), 2) as mixed_pct
from totals;

\echo
\echo === 4. Spot-check known good domain (tryunsecuredhq.co) ===
\echo Should resolve to a CM with non-empty tag_labels and confidence >= 80.
select domain, provider_group, primary_campaign_manager, tag_labels, mapping_status, confidence_score
from infra_domain_registry
where domain = 'tryunsecuredhq.co';

\echo
\echo === 5. Sending-domain coverage (Phase 5 of fix-spec) ===
\echo Of domains that actually sent in the last 7 days, what mapping status do they land in?
select r.mapping_status, count(*) as sending_domains, sum(m.sent_total) as cumulative_sent
from infra_domain_registry r
join infra_domain_metrics m using (domain)
where m.sent_total > 0
group by 1
order by 2 desc;

\echo
\echo === 6. Coverage gate (dashboard hard prereq #3) ===
\echo Goal: pct_mapped_high_conf >= 70 over the last 7 days of sends.
\echo If <70, ship Coverage Diagnostics tile in dashboard before the rollup tile.
with recent_sends as (
  select a.domain, coalesce(sum(m.sent), 0) as sent_7d
  from infra_accounts a
  left join infra_account_daily_metrics m
    on m.account_email = a.account_email
   and m.metric_date >= current_date - interval '7 days'
  group by 1
),
domain_status as (
  select r.domain, r.mapping_status, r.confidence_score, coalesce(s.sent_7d, 0) as sent_7d
  from infra_domain_registry r
  left join recent_sends s using (domain)
)
select
  sum(sent_7d)                                                                                  as total_sent_7d,
  sum(case when mapping_status = 'mapped' and confidence_score >= 80 then sent_7d end)          as mapped_high_conf_sent,
  round(100.0
        * sum(case when mapping_status = 'mapped' and confidence_score >= 80 then sent_7d end)
        / nullif(sum(sent_7d), 0), 1)                                                           as pct_mapped_high_conf
from domain_status;

\echo
\echo === 7. Top 30 high-volume domains still NOT mapped ===
\echo Surface unblockers for the next handoff. Investigate top-N if pct_mapped_high_conf is below threshold.
select m.domain, m.sent_total, r.mapping_status, r.primary_campaign_manager, r.tag_labels
from infra_domain_metrics m
left join infra_domain_registry r using (domain)
where m.sent_total > 0
  and (r.mapping_status is null or r.mapping_status <> 'mapped')
order by m.sent_total desc
limit 30;

\echo
\echo === 8. Tag-label distribution per pilot workspace ===
\echo Is the join surfacing batch codes (B59, B95) or persona-name tags? Affects sheet-tag matching downstream.
select workspace_slug, tag_label, count(*) as accounts
from infra_account_tag_mappings
where workspace_slug in ('renaissance-6', 'erc-1', 'koi-and-destroy')
group by 1, 2
order by 1, 3 desc
limit 60;

\echo
\echo === 9. Schema sanity (post-reconciliation) ===
\echo Confirms the fix-spec schema is live: account_email PK, resource_id present, 6 new fields on registry.
select column_name, data_type, is_nullable
from information_schema.columns
where table_schema = 'public'
  and table_name = 'infra_account_tag_mappings'
order by ordinal_position;

\echo --- new dashboard fields on infra_domain_registry ---
select column_name, data_type, udt_name
from information_schema.columns
where table_schema = 'public'
  and table_name = 'infra_domain_registry'
  and column_name in (
    'email_providers',
    'batches',
    'accounts_per_domain_values',
    'expected_domain_count_values',
    'domain_purchase_dates',
    'low_rr_flags'
  )
order by ordinal_position;

\echo
\echo === 10. Latest sync run audit ===
\echo Most recent infra_sync_runs row. Status should be 'completed' with errors=[].
select run_type, started_at, completed_at, status, workspace_count, accounts_seen, domains_written, api_calls_made, rate_limit_events, errors, duration_ms
from infra_sync_runs
order by started_at desc
limit 5;

\echo
\echo === Done. ===
\echo Stop and surface to Sam if any of:
\echo   - section 3: mixed_pct > 5
\echo   - section 5: no_account_tags|sheet_tag_missing share of sending domains > 30%
\echo   - section 6: pct_mapped_high_conf < 70 (dashboard build flips to diagnostics-only mode)
\echo   - section 9: any column missing or mistyped
\echo   - section 10: status != 'completed' or non-empty errors
