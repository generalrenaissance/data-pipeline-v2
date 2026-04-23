# Data Pipeline V2

GitHub Actions-driven data sync repo for Renaissance campaign data.

This repository pulls data from Instantly and Slack, then writes the normalized
results into Pipeline Supabase. It is a GitHub Actions-only sync repo.

## Runtime

- `data-pipeline-sync.yml` -> `src/runner.ts`
- `conversation-messages-sync.yml` -> `tools/conversation-messages-sync.ts`
- `meetings-booked-sync.yml` -> `tools/test-slack-sync.ts`
- `sequence-started-sync-gh.yml` -> `scripts/sync-sequence-started.ts`

## Required secrets

Store secret values in GitHub Actions secrets or local ignored `.env` files
only. Do not commit credentials to the repository.

- `INSTANTLY_API_KEYS`
- `PIPELINE_SUPABASE_URL`
- `PIPELINE_SUPABASE_KEY`
- `SLACK_TOKEN` and `SLACK_COOKIE` for meetings sync
- `CC_SLACK_BOT_TOKEN` for the manual-review digest posted by meetings sync

## Local checks

```bash
npm ci
npm run typecheck
```

## Meetings Sync Matcher

The meetings workflow now resolves Slack campaign names in this order:

1. `campaign_aliases` exact lookup for Sam-approved non-prefix mappings
2. strict deterministic match where the only allowed difference is a leading
   state prefix such as `ON -`, `OFF -`, `OLD -`, `[OLD]`, or leading emoji
3. hard reject if pair number, CM tag, person code, or same-RG-different-product
   rules conflict
4. queue for manual review in `meetings_unmatched_queue` with the top
   deterministic fuzzy candidates

There is no LLM in the automated meetings pipeline.

Manual review is currently SQL-driven. Sam resolves a queued raw name by writing
`resolved_campaign_id` + `review_status='resolved'` on
`meetings_unmatched_queue`, and the next meetings sync run turns that into a
sticky alias in `campaign_aliases`, backfills unresolved `meetings_booked_raw`
rows for the same raw name, and reruns the rollup.

No license is granted by default.
