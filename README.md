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

## Local checks

```bash
npm ci
npm run typecheck
```

No license is granted by default.
