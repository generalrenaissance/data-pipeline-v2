-- Backfill infra_account_daily_metrics.provider_group from
-- infra_accounts.provider_group (the correct, account-code-derived
-- value per PR #20).
--
-- Why: PR #20 fixed inventory-side provider attribution to derive
-- provider_group from infra_accounts.provider_code_raw. The
-- metricsIncremental writer at src/infra/sync-infra-domains.ts (loadAccountProviderGroups)
-- still filtered the lookup by workspace_slug=eq.<slug>, so when
-- Instantly's daily-metrics endpoint returned activity for an account
-- under a workspace other than its inventory home, the lookup missed
-- and the writer fell back to 'unknown'. After PR #24's chunking fix
-- finally refreshed all 22 workspaces cleanly on 2026-04-27, this bug
-- surfaced at scale: domain_rr_state.unknown_provider jumped from 8 to
-- 1,381. Verified diagnostic in
-- specs/2026-04-27-step2-followup-daily-metrics-provider-group.md §3.
--
-- This SQL is idempotent (UPDATE is no-op on already-correct rows via
-- IS DISTINCT FROM). Affects ~22.7k of ~6M rows. Direct pg session;
-- statement_timeout = 15min because PostgREST's 2-min default would
-- trip on a 6M-row UPDATE scan.
--
-- Status: this SQL was applied as a hot-fix at 2026-04-28 ~01:30 UTC
-- before the writer fix landed. Re-running it on subsequent deployments
-- is safe (no-op on rows already matching infra_accounts).

set statement_timeout = '15min';

update public.infra_account_daily_metrics d
set provider_group = a.provider_group
from public.infra_accounts a
where d.account_email = a.account_email
  and d.provider_group is distinct from a.provider_group;

-- Recompute aggregates so domain_rr_state picks up the fix on next
-- scorer fire. Bounded to last 7 days because the unbounded full-rebuild
-- exceeds the 15-min statement_timeout on the daily-row materialization
-- step. The 7-day window matches metricsIncremental's standard cadence.
select * from public.rebuild_infra_aggregates(
  null::text,
  (current_date - 7)::date,
  current_date::date
);
