-- Postgres/RDS schema for scripts/slack_analyst_bot.py.
-- Run this in the audit database before setting ANALYST_AUDIT_DATABASE_URL.

create extension if not exists pgcrypto;

create schema if not exists analyst_audit;

create table if not exists analyst_audit.threads (
  thread_id bigserial primary key,
  team_id text not null,
  channel_id text not null,
  thread_ts text not null,
  session_id uuid not null,
  first_seen_at timestamptz not null,
  last_seen_at timestamptz not null,
  unique (team_id, channel_id, thread_ts)
);

create table if not exists analyst_audit.turns (
  audit_id uuid primary key,
  thread_id bigint not null references analyst_audit.threads(thread_id) on delete cascade,
  status text not null check (
    status in ('success', 'error', 'cancelled', 'delivery_error')
  ),
  requester text not null,
  question text not null,
  answer text,
  slack_message_ts text,
  started_at timestamptz not null,
  completed_at timestamptz,
  duration_ms integer,
  resume boolean not null default false,
  session_id uuid not null,
  model text,
  repo_git_sha text,
  error text,
  raw_record jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists analyst_audit.turn_usage (
  audit_id uuid primary key references analyst_audit.turns(audit_id) on delete cascade,
  claude_turns integer,
  claude_duration_ms integer,
  claude_cost_usd_equivalent numeric,
  bq_jobs integer,
  bq_billed_bytes bigint,
  bq_gib numeric,
  bq_usd numeric,
  bq_error text,
  permission_denials jsonb not null default '[]'::jsonb,
  bad_json_lines integer not null default 0
);

create table if not exists analyst_audit.trace_events (
  audit_id uuid not null references analyst_audit.turns(audit_id) on delete cascade,
  seq integer not null,
  event_ts timestamptz not null,
  event_type text not null,
  tool_name text,
  text text,
  payload jsonb,
  primary key (audit_id, seq)
);

create table if not exists analyst_audit.feedback (
  id bigserial primary key,
  audit_id uuid not null references analyst_audit.turns(audit_id) on delete cascade,
  feedback_type text not null check (
    feedback_type in ('helpful', 'inaccurate', 'needs_more_investigation')
  ),
  user_id text not null,
  channel_id text not null,
  thread_ts text not null,
  created_at timestamptz not null default now()
);

create table if not exists analyst_audit.eval_cases (
  eval_case_id text primary key,
  title text not null,
  rubric_version text not null,
  prompt text not null,
  expected jsonb not null default '{}'::jsonb,
  tags text[] not null default array[]::text[],
  active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists analyst_audit.eval_runs (
  eval_run_id uuid primary key default gen_random_uuid(),
  eval_case_id text not null references analyst_audit.eval_cases(eval_case_id),
  audit_id uuid references analyst_audit.turns(audit_id) on delete set null,
  run_at timestamptz not null default now(),
  runner text,
  notes text
);

create table if not exists analyst_audit.eval_scores (
  eval_run_id uuid primary key references analyst_audit.eval_runs(eval_run_id) on delete cascade,
  pass boolean,
  score numeric,
  correctness numeric,
  evidence_quality numeric,
  cost_control numeric,
  caveat_quality numeric,
  notes text,
  scored_by text,
  scored_at timestamptz not null default now(),
  details jsonb not null default '{}'::jsonb
);

create index if not exists idx_analyst_audit_turns_started_at
  on analyst_audit.turns (started_at desc);

create index if not exists idx_analyst_audit_turns_thread_started
  on analyst_audit.turns (thread_id, started_at desc);

create index if not exists idx_analyst_audit_turns_status
  on analyst_audit.turns (status);

create index if not exists idx_analyst_audit_trace_events_type
  on analyst_audit.trace_events (event_type);

create index if not exists idx_analyst_audit_feedback_audit_id
  on analyst_audit.feedback (audit_id);

create or replace view analyst_audit.v_turns_for_scoring as
select
  t.audit_id,
  th.team_id,
  th.channel_id,
  th.thread_ts,
  t.status,
  t.requester,
  t.question,
  t.answer,
  t.started_at,
  t.completed_at,
  t.duration_ms,
  t.resume,
  t.error,
  u.claude_turns,
  u.claude_duration_ms,
  u.claude_cost_usd_equivalent,
  u.bq_jobs,
  u.bq_billed_bytes,
  u.bq_gib,
  u.bq_usd,
  u.bq_error,
  u.permission_denials,
  u.bad_json_lines,
  coalesce(
    jsonb_agg(
      jsonb_build_object(
        'feedback_type', f.feedback_type,
        'user_id', f.user_id,
        'created_at', f.created_at
      )
    ) filter (where f.id is not null),
    '[]'::jsonb
  ) as feedback
from analyst_audit.turns t
join analyst_audit.threads th on th.thread_id = t.thread_id
left join analyst_audit.turn_usage u on u.audit_id = t.audit_id
left join analyst_audit.feedback f on f.audit_id = t.audit_id
group by
  t.audit_id,
  th.team_id,
  th.channel_id,
  th.thread_ts,
  u.audit_id;
