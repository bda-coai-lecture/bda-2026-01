# BDA 2 Agent Guide

이 저장소에서 분석 작업을 수행할 때는 [BigQuery + dbt 분석 워크플로우](docs/bigquery_dbt_analysis_workflow.md)를 따른다.

## 기본 원칙

- 질문을 `분석 대상`, `관측 grain`, `기준 시간`, `기간`, `세그먼트`, `제외 조건`, `성공 지표`로 먼저 명세한다.
- 기존 dbt mart/fact로 답할 수 있는지 lineage부터 확인한다. GitHub Archive raw를 바로 조회하지 않는다.
- raw 조회가 필요하면 `githubarchive.day.20*`에 `_TABLE_SUFFIX` 범위를 반드시 지정한다.
- 작은 범위(기본 1일, 필요 시 7일)에서 의미와 품질, 스캔 비용을 검증한 뒤 확장한다.
- 임시 SQL은 `dbt/gharchive_metrics/analyses/`에 두고, 반복 사용되면 model/test/docs로 승격한다.
- 프로젝트의 Python 명령은 `uv run`을 사용한다. dbt 명령과 결과 보고 형식은 워크플로우 문서를 따른다.
- 분석 결과에서 관찰과 인과를 구분하고, instrumentation gap, 결측, 선택 편향, 지연 도착 데이터를 명시한다.

## 저장소 고유 주의사항

- `fact_user_repo_activity` grain은 `activity_date × user_id(actor.id) × repo_id × action`이다.
- `user_id`는 실제 의미상 GitHub event actor이며 organization이 아니다.
- raw는 ingestion-time partition table이 아니라 날짜별 shard다. raw 스캔 제한은 날짜 컬럼 조건이 아니라 `_TABLE_SUFFIX` 조건으로 보장한다.
- `fact_user_repo_activity.activity_date`는 BigQuery day partition이고 `user_id`, `repo_id`, `action`으로 cluster된다.
- 로컬에 이미 존재하는 사용자 변경과 자격 증명 파일은 건드리거나 출력하지 않는다.
