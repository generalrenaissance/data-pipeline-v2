alter table if exists public.campaign_aliases
  drop constraint if exists campaign_aliases_campaign_id_fkey;

alter table if exists public.meetings_unmatched_queue
  drop constraint if exists meetings_unmatched_queue_resolved_campaign_id_fkey;

alter table if exists public.meetings_unmatched_queue
  add column if not exists applied_at timestamptz;

create index if not exists meetings_unmatched_queue_resolution_apply_idx
  on public.meetings_unmatched_queue (review_status, applied_at);

comment on column public.meetings_unmatched_queue.applied_at is
  'Timestamp when a resolved queue row has been consumed into campaign_aliases / meetings_booked_raw by the meetings sync.';
