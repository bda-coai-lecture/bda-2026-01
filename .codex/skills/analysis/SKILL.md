---
name: analysis
description: Perform cost-bounded, reproducible BigQuery and dbt analysis for the BDA 2 GitHub Archive repository. Use when Codex is asked to analyze GitHub activity, actors, repositories, events, DAU/WAU/MAU, retention, lifecycle, segments, metric changes, raw-versus-mart discrepancies, or to turn a repeated analysis into a dbt model, test, or documented mart in /Users/kakao/bda-2.
---

# BDA BigQuery Analysis

Run GitHub Archive analysis from the smallest trustworthy scope and expand only after validating semantics, quality, and BigQuery scan size.

## Establish repository context

1. Find the repository root. Expect `/Users/kakao/bda-2`; if invoked elsewhere, locate the nearest root containing `dbt/gharchive_metrics/dbt_project.yml`.
2. Read the root `AGENTS.md`.
3. Read `docs/bigquery_dbt_analysis_workflow.md` completely before taking analysis actions. Treat it as the detailed execution contract and do not duplicate it from memory.
4. Inspect `git status --short`. Preserve unrelated user changes.
5. Use `uv run` for Python and repository tooling.

## Translate the request

State or infer these fields before querying:

- question and decision the result supports
- observation grain
- time basis and timezone
- validation range and intended analysis range
- segments and exclusions
- metric definition and expected output grain

If an unresolved choice would materially alter the result, ask one concise question. Otherwise record the assumption and proceed.

## Discover the cheapest trustworthy source

1. Search dbt lineage with `dbt ls` and inspect matching SQL/YAML.
2. Prefer existing mart, then staging/fact, then bounded raw.
3. Confirm grain, keys, time column, partition or shard boundary, incremental strategy, and relevant tests.
4. Compile only selected models or analyses and inspect rendered SQL.

Use the repository commands from the workflow document. Do not run broad builds or scans merely to discover data.

## Validate before expanding

1. Start with the most recent completed UTC day; use seven days only when needed for distributions or late arrival.
2. Check row count, distinct keys, nulls, grain duplicates, event counts, date boundaries, event-type distribution, and incomplete recent data.
3. Dry-run every new or materially changed BigQuery query.
4. Report bytes processed. If the scan is unexpectedly large, reduce columns, dates, event types, entities, or pre-aggregate before joining.
5. Reconcile mart/fact against raw only when necessary. Compare raw `count(*)` with fact `sum(event_count)`, not fact row count.
6. Expand to the requested period only after the small-range checks are coherent.

For `githubarchive.day.20*`, always include a bounded `_TABLE_SUFFIX` predicate. A `created_at` date predicate alone is not a shard-cost boundary.

## Execute according to requested scope

- For an explanation or diagnosis, perform read-only inspection and querying; do not create models, modify dashboards, or write reports unless requested.
- When reproducible SQL is requested, place temporary analysis SQL in `dbt/gharchive_metrics/analyses/` with the metadata header defined in the workflow document.
- When promotion is requested or clearly part of a build request, add the narrowest dbt model plus grain documentation and appropriate `unique`, `not_null`, `relationships`, or reconciliation tests.
- Build only the selected model and necessary graph neighbors. Do not run a full historical backfill without explicit authorization.

## Report results

Lead with the conclusion, then provide:

1. core numbers and exact date range
2. tables and dbt models used
3. filters, exclusions, metric definition, timezone, and output grain
4. quality checks, dry-run bytes, and reconciliation result
5. caveats and the distinction between association and causation
6. reproducible SQL or dbt model path, when an artifact was requested or created

Never expose credentials or secret-file contents. Clearly distinguish observations from inferences.
