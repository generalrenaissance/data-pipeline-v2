-- Step 3 of meetings matcher rebuild: audit + gating columns for the
-- scheduled LLM fuzzy-match pass.
--
-- last_llm_at: timestamp of the most recent LLM decision for this raw name.
--   Used to skip re-processing unresolvable rows more than once per week.
-- llm_decision: full jsonb payload of the LLM's last verdict (kind,
--   campaign_id, confidence, reasoning, alternatives). Stored for audit
--   and for Step 2's human-review UX to surface.

alter table public.meetings_unmatched_queue
  add column if not exists last_llm_at timestamptz,
  add column if not exists llm_decision jsonb;

create index if not exists meetings_unmatched_queue_last_llm_at_idx
  on public.meetings_unmatched_queue (last_llm_at);

comment on column public.meetings_unmatched_queue.last_llm_at is
  'Timestamp of most recent LLM fuzzy-match pass. Used to gate weekly re-processing of unresolved rows.';
comment on column public.meetings_unmatched_queue.llm_decision is
  'Last LLM verdict for this raw name (kind, campaign_id, confidence, reasoning, alternatives). Audit + Step 2 review input.';
