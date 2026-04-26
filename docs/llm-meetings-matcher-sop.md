# LLM Meetings Matcher — Scheduled Routine SOP

**Purpose:** Run daily after the deterministic meetings sync. Resolve the residual tail of `meetings_unmatched_queue` rows that the rule-based matcher couldn't attribute, using LLM judgment constrained by the SOP below.

**Runs as:** Claude Code scheduled routine (not GH Actions, not a Worker).
**Depends on:** Deterministic sync (GH Actions `meetings-booked-sync.yml`, daily 05:00 UTC) — this runs 1h after.
**Schema prereq:** `sql/2026-04-23-meetings-llm-fields.sql` applied to Pipeline Supabase.

---

## Execution Plan

Follow these steps in order. Use the `pipeline-supabase` MCP for all Supabase reads/writes. Use the `slack` MCP for the summary post.

### 1. Load campaign universe

Query the live campaign list:

```sql
SELECT campaign_id, campaign_name AS name
FROM public.campaign_data
WHERE step = '__ALL__'
  AND variant = '__ALL__'
  AND campaign_name IS NOT NULL;
```

Hold the result as your canonical campaign list. **Never match to a `campaign_id` that isn't in this list.**

### 2. Load pending queue rows

```sql
SELECT
  campaign_name_raw,
  queue_reason,
  top_candidates,
  occurrence_count,
  source_channels,
  first_seen_at,
  last_seen_at,
  last_llm_at,
  llm_decision
FROM public.meetings_unmatched_queue
WHERE review_status = 'pending'
  AND (last_llm_at IS NULL OR last_llm_at < now() - interval '7 days')
ORDER BY occurrence_count DESC, last_seen_at DESC
LIMIT 200;
```

If 0 rows returned, skip to the summary post step with a "nothing to do" message. **Do not** proceed to LLM evaluation.

### 3. Apply the matching SOP

For each raw name, output a decision using this SOP. The `top_candidates` already contain the deterministic matcher's best guesses — treat them as hints, not ground truth.

**MATCH (confidence 0.85–1.0):**
- Raw name contains an unambiguous pair number (`P3`, `Pair 3`, `Pair 13,14`, `PAIR 3,4,7,9`) AND enough other tokens (segment/industry, CM name like `(SAM)`/`(TOMI)`/`(MARCOS)`, mode like `OUTLOOK`) to uniquely identify ONE campaign in the list.
- Raw name is a clear typo or formatting variant of exactly one campaign (e.g., `Restaurnat` → `Restaurant`, `P3 Rest OL` → `ON - PAIR 3 - RESTAURANT (OUTLOOK)`).
- Raw name matches an obvious shorthand convention (e.g., `KD5 Fundora CEO TOMI` → `KD5 - Fundora - CEOs(TOMI)`).

**MATCH (confidence 0.70–0.84):**
- One likely candidate but some discriminating tokens missing. Return as `kind='match'` with the lower confidence — the write threshold below will route it to queue instead of auto-applying.

**UNATTRIBUTED:**
- Raw name is ambiguous between 2+ campaigns that all pass the hard bans below.
- Raw name is only a CM name or only a segment with no pair/product signal.
- Raw name references a campaign not present in the campaign list (likely LinkedIn or partner booking).
- You are below 70% certain.

**Hard bans — NEVER match if any of these fail:**
- **Pair number mismatch**: raw `P3` cannot match any campaign whose primary pair is not 3. A campaign named `PAIR 3,4,7,9` counts as pair 3 for this check.
- **CM tag mismatch**: raw `(TOMI)` cannot match a campaign tagged `(MARCOS)` or `(SAM)`. CM tags are in parentheses, often ALL CAPS.
- **Product family mismatch**: raw `Restaurant` cannot match `HVAC`, `Construction`, `CEOs`, etc. Treat product families as disjoint.
- **LinkedIn signal**: if the raw name contains `linkedin` or `LI` as a standalone token, return `kind='unattributed'` with reasoning `"linkedin_source"` — these bookings come from a separate channel.

### 4. Write resolutions

**Chunk the writes.** Compiling all 200 decisions into a single MCP/tool response has triggered the Claude API stream-idle timeout (observed 2026-04-24, again 2026-04-26 06:00 UTC). Process decisions in batches of **20** to keep the stream alive:

1. Sort the decisions list deterministically (by `occurrence_count desc, campaign_name_raw asc` is fine — matches the Step 2 query order).
2. Slice into chunks of 20.
3. For each chunk: emit the SQL writes (one MCP call per statement is fine; or batch them in a single multi-statement payload, but do not exceed one chunk per call).
4. After each chunk, emit a single short heartbeat line in the agent's running output, e.g. `chunk N/M done — wrote X high-conf, Y low-conf/unattributed`. This keeps the model producing tokens between expensive tool calls and prevents stream-idle.
5. If a chunk fails partway, log which `campaign_name_raw` failed and continue with the next chunk. Do not abort the whole run.

The final summary post (Step 6) runs **after** all chunks complete, using the SQL-derived counts described there — not in-memory tallies from the chunking loop.

For each decision:

**High confidence (`kind='match'` AND `confidence >= 0.85`):**

```sql
-- Apply to all raw rows with this name (backfill)
UPDATE public.meetings_booked_raw
SET campaign_id = $campaign_id,
    match_method = 'llm_fuzzy',
    match_confidence = $confidence
WHERE campaign_name_raw = $raw_name
  AND campaign_id IS NULL;

-- Close the queue row
UPDATE public.meetings_unmatched_queue
SET review_status = 'resolved',
    resolved_campaign_id = $campaign_id,
    resolved_at = now(),
    updated_at = now(),
    last_llm_at = now(),
    llm_decision = $decision_jsonb
WHERE campaign_name_raw = $raw_name;
```

Note: the `resolved_campaign_id` FK points to the stale `public.campaigns` table. If the insert fails with an FK violation, log it and **skip that row** — do not update raw rows. Flag it in the summary. (The FK will be addressed in Step 2.)

**Low confidence or unattributed:**

```sql
UPDATE public.meetings_unmatched_queue
SET last_llm_at = now(),
    llm_decision = $decision_jsonb,
    updated_at = now()
WHERE campaign_name_raw = $raw_name;
```

`$decision_jsonb` shape:
```json
{
  "kind": "match" | "unattributed",
  "campaign_id": "uuid-or-null",
  "campaign_name": "string-or-null",
  "confidence": 0.92,
  "reasoning": "Pair 3 + Restaurant + OUTLOOK uniquely matches one campaign",
  "alternatives": [
    {"campaign_id": "...", "campaign_name": "...", "score": 0.42}
  ],
  "decided_at": "2026-04-23T06:00:00Z"
}
```

### 5. Trigger rollup

If any rows were updated in `meetings_booked_raw`, call the rollup RPC via MCP:

```sql
SELECT public.rollup_meetings_booked();
```

This propagates the new matches into `campaign_data.meetings_booked`. Capture the boolean `did_rollup_run` in your local state so the summary post reflects reality (it lied on the 2026-04-24 first run).

### 6. Post summary to #cc-sam — using SQL-derived counts

Channel: `C0AR0EA21C1`.

**Do NOT use in-memory counters.** The 2026-04-24 first run reported `9 llm_fuzzy raw writes` when the actual count in the database was `1`, and reported `Rollup: skipped` when it had run. The summary must be sourced from SQL queries against the database, executed AFTER all chunked writes from Step 4 and the rollup in Step 5 have completed.

Capture `today_start_utc` once at the start of the run as the UTC midnight of the run's calendar date (e.g. `2026-04-26 00:00:00+00`). Use it as the lower bound for "today's" filters below.

Run these SQL queries via the `pipeline-supabase` MCP and use their results in the summary:

```sql
-- (a) Reviewed count: queue rows touched by this run
SELECT count(*) AS reviewed
FROM public.meetings_unmatched_queue
WHERE last_llm_at >= $today_start_utc;

-- (b) Resolved high-confidence count: queue rows newly closed by this run
SELECT count(*) AS resolved_high_conf
FROM public.meetings_unmatched_queue
WHERE last_llm_at >= $today_start_utc
  AND review_status = 'resolved'
  AND llm_decision IS NOT NULL;

-- (c) Left for manual review: queue rows still pending after this pass
SELECT count(*) AS left_for_review
FROM public.meetings_unmatched_queue
WHERE last_llm_at >= $today_start_utc
  AND review_status = 'pending';

-- (d) Distinct llm_fuzzy raw writes for this run (the number that lied last time)
SELECT
  count(*) AS raw_rows_written,
  count(DISTINCT campaign_name_raw) AS distinct_raw_names
FROM public.meetings_booked_raw
WHERE match_method = 'llm_fuzzy'
  AND synced_at >= $today_start_utc;

-- (e) Rollup propagation: campaign_data rows touched after the rollup call
SELECT count(*) AS rollup_campaigns_touched
FROM public.campaign_data
WHERE step = '__ALL__'
  AND variant = '__ALL__'
  AND updated_at >= $today_start_utc;
```

`F` (FK failures) is still tracked in-memory during the chunk loop because failed UPDATEs don't leave a SQL trace — increment a counter when an UPDATE returns an FK violation in Step 4.

Message format (use the SQL results above, not loop counters):

```
LLM meetings pass — {date ET}
- Reviewed: {a.reviewed} pending rows
- Resolved high-confidence: {b.resolved_high_conf} (match_method=llm_fuzzy)
- Raw rows written: {d.raw_rows_written} across {d.distinct_raw_names} distinct names
- Left for manual review: {c.left_for_review} (confidence <0.85 or unattributed)
- FK failures (campaigns table stale): {F}
- Rollup: {triggered, {e.rollup_campaigns_touched} campaigns touched | skipped (no raw writes)}
```

If `b.resolved_high_conf + c.left_for_review + F == 0`, post a single-line "nothing to do" update instead.

**Self-check before posting:** if `b.resolved_high_conf > 0` and the message says `Rollup: skipped`, that's a bug — abort the post and log an error to stdout. Either the rollup didn't run when it should have, or the SQL conditions don't match.

---

## Hard Rules

- **Validate every `campaign_id` you return against the list from Step 1.** Reject hallucinations. If you "remember" a campaign_id from somewhere else, do not use it.
- **Cap writes at 200 rows per run.** The query LIMIT handles this but if the SOP somehow generates more decisions than query rows, that's a bug — stop and post an error to #cc-sam.
- **Never write to `campaign_aliases`.** Aliases are human-curated. LLM matches are per-row only.
- **Never re-resolve rows that already have `campaign_id IS NOT NULL`.** The UPDATE filter already gates this but double-check in your decision loop.
- **Match confidence is a self-reported score.** Be honest — if you're guessing, confidence goes below 0.70 and the row stays pending.

---

## Reference Data

### CM tag conventions (non-exhaustive)

Common CM tags found in campaign names and raw Slack names:
`(SAM)`, `(TOMI)`, `(TOMY)`, `(MARCOS)`, `(ANDRES)`, `(CARLOS)`, `(SHAAN)`, `(EYVER)`, `(IDO)`, `(ALEX)`, `(LEO)`, `(BRENDAN)`, `(JESSICA)`, `(JEHOON)`

Raw names may have them in different cases or without parens. Normalize before comparing.

### Pair normalization

- `P3`, `Pair 3`, `PAIR 3` all refer to pair 3.
- `PAIR 3,4,7,9` is a multi-pair campaign — it matches ANY of pairs 3, 4, 7, 9 on pair-number checks.
- `Pair 13,14` matches pair 13 or 14.

### Known product families (disjoint)

`Restaurant`, `HVAC`, `Construction`, `Cleaning`, `Auto`, `Advertising`, `Healthcare`, `Home Improvement`, `Home Services`, `Roofing`, `Plumbing`, `Real Estate`, `Property Management`, `Retail`, `Beauty`, `CEOs`, `Founders`, `Presidents`, `General`.

Treat `General` as a catch-all that should ONLY match if raw name explicitly says `General`.

### Prefix noise (ignore)

`ON -`, `OFF -`, `OLD -`, `[OLD]`, `✅`, `⚠️`, leading emoji, `x MA`, trailing ` X`. These are state markers, not part of the identity.

---

## Post-Run Observability

The summary post in #cc-sam is the primary signal. For deeper audit:
- `SELECT * FROM meetings_booked_raw WHERE match_method = 'llm_fuzzy' ORDER BY synced_at DESC LIMIT 50` — recent LLM matches
- `SELECT * FROM meetings_unmatched_queue WHERE last_llm_at IS NOT NULL ORDER BY last_llm_at DESC LIMIT 50` — recent LLM decisions (including low-confidence and unattributed)
- Sample 10 random `llm_fuzzy` rows weekly, verify attribution by hand.

---

## Failure Modes & Recovery

| Symptom | Action |
|---------|--------|
| Step 1 returns 0 campaigns | Abort. Something is broken upstream. Post error to #cc-sam. |
| Campaign list has duplicate names | Proceed. Hard bans + SOP still apply; may generate more "unattributed" verdicts. |
| Supabase write fails mid-batch | Log which raw_name failed, continue with the rest. Report count in summary. |
| FK violation on `resolved_campaign_id` | Skip that row (don't update raw). Count in summary. (Fixed once Step 2 addresses the FK.) |
| Slack post fails | Non-fatal. Log to stdout. The data writes are the source of truth. |

---

## Rollback

To undo all LLM matches (in case of a bad run):

```sql
BEGIN;

-- Revert raw rows
UPDATE public.meetings_booked_raw
SET campaign_id = NULL,
    match_method = NULL,
    match_confidence = NULL
WHERE match_method = 'llm_fuzzy';

-- Reopen queue rows that the LLM resolved
UPDATE public.meetings_unmatched_queue
SET review_status = 'pending',
    resolved_campaign_id = NULL,
    resolved_at = NULL,
    updated_at = now()
WHERE review_status = 'resolved'
  AND llm_decision IS NOT NULL;

-- Re-run rollup
SELECT public.rollup_meetings_booked();

COMMIT;
```

To undo only a single run, filter by `llm_decision->>'decided_at'` timestamp.
