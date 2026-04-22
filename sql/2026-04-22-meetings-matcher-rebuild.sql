create table if not exists public.campaign_aliases (
  alias text primary key,
  campaign_id text not null references public.campaigns(campaign_id),
  note text,
  created_by text,
  created_at timestamptz not null default now()
);

create index if not exists campaign_aliases_campaign_id_idx
  on public.campaign_aliases (campaign_id);

comment on table public.campaign_aliases is
  'Manual Slack-name aliases for meetings matching. Seeded from the 2026-04-22 recovery pass and extended whenever Sam approves a non-prefix mapping.';

insert into public.campaign_aliases (alias, campaign_id, note, created_by)
values
  ('Advertising - Google + Others', '66df1ad8-7f3a-4636-8c9a-a2b28c5f0cad', 'Seed 2026-04-22', 'sam'),
  ('Auto - Google + others', 'd416aa46-bb44-4cdb-b255-b3190baa929d', 'Seed 2026-04-22', 'sam'),
  ('Cleaning - Google + Others', '34f25f61-3901-468b-abad-92dfdef14589', 'Seed 2026-04-22', 'sam'),
  ('Construction - Google + Others', '32793ed7-48d7-4f1f-8f3c-522b3bbc75e3', 'Seed 2026-04-22', 'sam'),
  ('Construction (General) - Pair 9 - RG2268/RG2269/RG2270/RG2271/RG2272 (BRENDAN)', '54a3f910-37f3-484b-902b-6b8b3d85f5b4', 'Seed 2026-04-22', 'sam'),
  ('Construction 2 - Outlook', '94f7d20c-5774-4da2-be7f-a84c076edb41', 'Seed 2026-04-22', 'sam'),
  ('General - Pair 8 - Quickcred - SHAAN', '23b3e4bf-6967-4a8d-ab0a-766344e4040a', 'Seed 2026-04-22', 'sam'),
  ('KD2 - BrightFunds - Home Improvement(TOMI)', '26d44e26-5fdc-49f1-a034-2cd0d589462a', 'Seed 2026-04-22', 'sam'),
  ('KD5 - Fundora - CEOs(TOMI)', '59d7e965-f313-4281-9255-f201d4f349ee', 'Seed 2026-04-22', 'sam'),
  ('KD6 - Summit Bridge - Founders(TOMI)', '33a8b0f4-87d0-4c4d-9a26-8f7c2f5bef09', 'Seed 2026-04-22', 'sam'),
  ('OFF - RG961+RG2280+RG2281+RG2282 - Flex Group - HEALTHCARE GMAPS - (EYVER) RB', 'ab0e3dd3-049e-42f7-8920-f6a4b9405f79', 'Seed 2026-04-22', 'sam'),
  ('ON - Health Pair 5 (ANDRES) X', '385f1746-ae78-4f07-b188-132778bd4d73', 'Seed 2026-04-22', 'sam'),
  ('ON - Pair - Beauty (Alex', 'd8cc548e-1d3b-4af6-aa50-7000f250d174', 'Seed 2026-04-22', 'sam'),
  ('ON - Pair - Cleaning (Alex)', 'b85ec44c-9e37-46f7-a32b-8b41b861156a', 'Seed 2026-04-22', 'sam'),
  ('ON - Pair 1 - Restaurants (Alex) X', 'ffbc8ad7-acd4-4aac-b818-47b8e94e23ed', 'Seed 2026-04-22', 'sam'),
  ('ON - PAIR 2 - Advertising (MARCOS)', '514dedeb-12be-4c4c-b7ec-8f9175e02ec3', 'Seed 2026-04-22', 'sam'),
  ('ON - PAIR 4 - HVAC (MARCOS)', '8078e9da-272d-4a13-bd46-b419171ea7e3', 'Seed 2026-04-22', 'sam'),
  ('ON - PAIR 5 - GENERAL SN (MARCOS)', '2450c6a9-829a-4124-ad22-067b9ce9e9c6', 'Seed 2026-04-22', 'sam'),
  ('ON - PAIR 6 - Real State (MARCOS)', '68435798-de80-4f9a-9639-8c10542c3dbf', 'Seed 2026-04-22', 'sam'),
  ('ON - PAIR 7 - Prop Mainten (MARCOS)', '5b0dd2ef-dac8-44d3-b386-5ee798aebffa', 'Seed 2026-04-22', 'sam'),
  ('RG3580 - General GMAPS (1298-1301) - Southern Edge Funds - (SHAAN)', '320f9bb1-ae1b-4cda-bb73-9e364686bd04', 'Seed 2026-04-22', 'sam'),
  ('RG49/RG50/RG51 - Qualify - Construction (CARLOS)', 'fd0414d9-845c-4e09-a6f5-e0692a90f2ea', 'Seed 2026-04-22', 'sam')
on conflict (alias) do update
set campaign_id = excluded.campaign_id,
    note = excluded.note,
    created_by = excluded.created_by;

create table if not exists public.meetings_unmatched_queue (
  campaign_name_raw text primary key,
  queue_reason text not null,
  top_candidates jsonb not null default '[]'::jsonb,
  candidate_hash text,
  occurrence_count integer not null default 0,
  source_channels text[] not null default '{}'::text[],
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  review_status text not null default 'pending',
  resolved_campaign_id text references public.campaigns(campaign_id),
  resolved_at timestamptz,
  last_digest_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists meetings_unmatched_queue_review_status_idx
  on public.meetings_unmatched_queue (review_status, last_digest_at);

comment on table public.meetings_unmatched_queue is
  'Manual-review queue for Slack meeting names that fail deterministic alias/strict matching. The sync stores the top deterministic candidates for Sam to review.';
