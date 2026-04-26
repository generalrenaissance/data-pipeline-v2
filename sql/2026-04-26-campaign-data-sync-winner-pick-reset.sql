-- ============================================================================
-- Campaign Data Sync — Winner-Pick Reset (idempotent re-run of dedupe)
-- Date: 2026-04-26
-- Branch: fix/sync-campaign-data-winner-pick
-- Source plan / rule: 2026-04-26-campaign-data-dedupe-plan.sql
-- Predecessors that already ran today (informational):
--   * 2026-04-26-campaign-data-dedupe-EXECUTED.sql      (option b — 4 deletes)
--   * 2026-04-26-campaign-data-dedupe-c-EXECUTED.sql    (option c — 43 deletes)
-- ============================================================================
--
-- WHY THIS MIGRATION EXISTS
-- -------------------------
-- Today's option-b + option-c dedupes deleted 47 single-workspace duplicate
-- (workspace_id, campaign_name, step='__ALL__', variant='__ALL__') rows. ~3
-- minutes after commit, the Instantly→Supabase sync re-inserted ~22 of them
-- because its upsert key is the PK (campaign_id, step, variant), not
-- (workspace_id, campaign_name, step, variant). Two distinct campaign_id
-- UUIDs sharing the same campaign_name (recreate-after-delete pattern) land
-- as two separate rows.
--
-- The companion code change in this PR adds a pre-upsert coalesce step in
-- src/sync.ts that, before each per-workspace upsert into campaign_data,
-- detects existing rows that share (workspace_id, campaign_name, step,
-- variant) with new rows but a DIFFERENT campaign_id, applies the same
-- winner-pick rule used in today's dedupe SQL, and DELETEs the loser
-- campaign_id from campaign_data (and reassigns meetings_booked_raw FKs).
--
-- This migration ships the same dedupe logic at write-time-zero so the live
-- table starts in a clean state when the new sync deploys. Without this
-- reset, the next sync would still see the ~22 freshly-restored dupes and
-- (correctly) coalesce them, but only one workspace at a time, so it would
-- take a full sweep before convergence. Faster + more deterministic to
-- shrink the set first.
--
-- IDEMPOTENCY
-- -----------
-- This migration uses set-based UPDATE/DELETE statements gated on the
-- presence of duplicates. Re-running it on a clean table is a no-op:
--   * The CTE `eligible` returns 0 rows when no single-ws dupes exist.
--   * The UPDATE / DELETE statements affect 0 rows when `_dedupe_map` is empty.
--   * No DDL changes — only DML. No `IF EXISTS` guards required.
--
-- WINNER-PICK RULE (matches sync code helper exactly)
-- ---------------------------------------------------
-- For each (workspace_id, campaign_name, step='__ALL__', variant='__ALL__')
-- group with > 1 row in the SAME workspace, keep the row with:
--   1. Highest emails_sent (NULLS treated as 0)
--   2. Tiebreak: most recent synced_at (DESC, NULLS LAST)
--
-- Multi-workspace dupe groups (same campaign_name across different
-- workspace_ids) are EXCLUDED — those are legitimately distinct campaigns.
--
-- SUM(meetings_booked) INVARIANT
-- ------------------------------
-- meetings_booked_raw FK reassignments (loser → winner) happen BEFORE the
-- DELETE. After the post-commit `rollup_meetings_booked()` call, total
-- sum(campaign_data.meetings_booked) MUST be unchanged.
-- ============================================================================

BEGIN;

LOCK TABLE campaign_data IN SHARE ROW EXCLUSIVE MODE;

-- Step 1: Materialize winner/loser map for all single-workspace dupe groups
-- at the (campaign_name, step='__ALL__', variant='__ALL__') key.
CREATE TEMP TABLE _dedupe_map_sync_reset ON COMMIT DROP AS
WITH dupe_groups AS (
  SELECT campaign_name, MIN(workspace_id) AS workspace_id
  FROM campaign_data
  WHERE step = '__ALL__' AND variant = '__ALL__'
  GROUP BY campaign_name
  HAVING count(*) > 1
     AND count(DISTINCT workspace_id) = 1   -- single-workspace only
), ranked AS (
  SELECT cd.campaign_id,
         cd.campaign_name,
         cd.workspace_id,
         cd.status,
         cd.emails_sent,
         cd.meetings_booked,
         cd.synced_at,
         ROW_NUMBER() OVER (
           PARTITION BY cd.workspace_id, cd.campaign_name
           ORDER BY COALESCE(cd.emails_sent, 0) DESC,
                    cd.synced_at DESC NULLS LAST
         ) AS rn
  FROM campaign_data cd
  JOIN dupe_groups g
    ON cd.campaign_name = g.campaign_name
   AND cd.workspace_id  = g.workspace_id
  WHERE cd.step = '__ALL__' AND cd.variant = '__ALL__'
)
SELECT campaign_id, campaign_name, workspace_id, status, emails_sent,
       meetings_booked, synced_at, rn,
       FIRST_VALUE(campaign_id) OVER (
         PARTITION BY workspace_id, campaign_name ORDER BY rn
       ) AS winner_id
FROM ranked;

-- Sanity-check counts (visible in SQL output; do not block).
SELECT 'reset_sanity_winners' AS k, count(*) AS v
FROM _dedupe_map_sync_reset WHERE rn = 1;
SELECT 'reset_sanity_losers'  AS k, count(*) AS v
FROM _dedupe_map_sync_reset WHERE rn > 1;

-- Step 2: Reassign meetings_booked_raw FKs from losers to winners.
UPDATE meetings_booked_raw mbr
SET campaign_id  = m.winner_id,
    match_method = COALESCE(mbr.match_method, '') || '+sync_winner_pick_reset_2026_04_26'
FROM _dedupe_map_sync_reset m
WHERE mbr.campaign_id = m.campaign_id
  AND m.rn > 1;

-- Step 3: Reassign meetings_unmatched_queue resolutions (defensive).
UPDATE meetings_unmatched_queue uq
SET resolved_campaign_id = m.winner_id
FROM _dedupe_map_sync_reset m
WHERE uq.resolved_campaign_id = m.campaign_id
  AND m.rn > 1;

-- Step 4: Delete loser rows from campaign_data.
-- Constrained to step='__ALL__' AND variant='__ALL__' to match the dedupe-key
-- scope. Per-step/variant rows are downstream of the rollup row and follow
-- the same campaign_id, so they will naturally coalesce on the next sync via
-- the new pre-upsert helper in src/sync.ts.
DELETE FROM campaign_data cd
USING _dedupe_map_sync_reset m
WHERE cd.campaign_id = m.campaign_id
  AND cd.step = '__ALL__' AND cd.variant = '__ALL__'
  AND m.rn > 1;

-- Step 5: Verify post-state (should be 0 within-txn).
SELECT 'reset_remaining_single_ws_dupes' AS k, count(*) AS v
FROM (
  SELECT campaign_name
  FROM campaign_data
  WHERE step = '__ALL__' AND variant = '__ALL__'
  GROUP BY campaign_name
  HAVING count(*) > 1 AND count(DISTINCT workspace_id) = 1
) x;

COMMIT;

-- ============================================================================
-- POST-COMMIT: Refresh meetings_booked rollup (preserves invariant)
-- ============================================================================
SELECT public.rollup_meetings_booked();

-- ============================================================================
-- POST-COMMIT: Reset last_llm_at on ambiguous_strict rows so matcher re-attempts
-- ============================================================================
UPDATE meetings_unmatched_queue
SET last_llm_at = NULL,
    llm_decision = NULL
WHERE review_status = 'pending'
  AND queue_reason = 'ambiguous_strict'
  AND top_candidates -> 0 ->> 'campaign_name'
    = top_candidates -> 1 ->> 'campaign_name';
