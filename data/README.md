# data/

Bot-written daily exports.

## inbox-hub-latest.json

Sanitized snapshot of `public.infra_sheet_registry` — the table that
`infra-sheet-registry-sync.yml` refreshes daily at 04:00 UTC from the
Renaissance InboxHub Google Sheet.

Written daily at 04:30 UTC by `inbox-hub-export.yml` running
`scripts/export-inbox-hub-json.ts`. The script enforces a column
allowlist at runtime; PII (campaign manager / inbox manager / technical),
sending-infra (brand_name, brand_domain, workspace), and
operational/financial fields (billing_date, domain_purchase_date,
warmup_start_date, batch, raw_row) are stripped before write.

If a future schema change adds a column, the export run fails closed
until the column is explicitly classified as allowed or denied. The
unit tests in `test/export-inbox-hub-json.test.ts` cover the
allowlist + denylist + unknown-column paths.

Downstream consumers should treat this file as read-only and re-pull
on each use; the file is rewritten daily.
