-- Domain Infra Sync + RR Automation — rollback [2026-04-24]
--
-- Drops the 7 tables created by 2026-04-24-domain-infra-sync.sql.
-- No FKs between these tables, so drop order is hygiene-only (reverse of create).
-- `cascade` defends against any dependent objects added later.
--
-- USE WITH CAUTION: this destroys all infra sync data. Only run for:
--   1. Phase 1 build-plan validation (apply forward → apply rollback → re-apply forward).
--   2. Schema rebuild during early development.
--   3. Disaster recovery where the schema needs to be fully rebuilt from a
--      known migration state.

drop table if exists public.domain_rr_events cascade;
drop table if exists public.domain_rr_state cascade;
drop table if exists public.infra_sync_runs cascade;
drop table if exists public.infra_domain_metrics cascade;
drop table if exists public.infra_domain_daily_metrics cascade;
drop table if exists public.infra_account_daily_metrics cascade;
drop table if exists public.infra_accounts cascade;
