# BigQuery + dbt 분석 워크플로우

이 문서는 BDA 2 GitHub Archive 프로젝트에서 AI와 사람이 비용을 통제하면서 재현 가능한 분석을 수행하기 위한 실행 계약이다.

핵심 전제는 “mart가 작아서 분석이 단순하다”가 아니다. mart가 작아 raw로 내려갈 일이 많고, raw가 크므로 **스캔 범위 통제 자체가 분석 품질의 일부**다.

## 1. 질문을 분석 계약으로 바꾸기

SQL을 쓰기 전에 아래를 짧게 명시한다.

| 항목 | 이 프로젝트의 예 |
|---|---|
| 분석 대상 | 활성 actor 감소, repo별 이벤트 변화, retention |
| 관측 grain | actor, repo, actor-repo, event, day/week/month |
| 기준 시간 | 기본 `activity_date`; snapshot 분석은 `snapshot_date` |
| 기간 | 검증 범위와 본 분석 범위를 각각 명시 |
| 세그먼트 | event type, actor 활동성, repo 등 |
| 제외 조건 | null actor/repo, bot 후보, 불완전한 최신 날짜 등 |
| 성공 지표 | active actors, active repos, events, retention rate 등 |

GitHub Archive의 `user_id`는 `actor.id`를 옮긴 이름이다. 개인, bot, organization을 자동으로 구분하지 않으며 이벤트의 `org`와도 다르다.

## 2. lineage와 grain부터 확인하기

다음 순서로 기존 모델을 찾는다.

```bash
uv run --with dbt-bigquery dbt ls \
  --project-dir dbt/gharchive_metrics \
  --profiles-dir dbt/profiles \
  --select "fqn:*키워드*"

uv run --with dbt-bigquery dbt ls \
  --project-dir dbt/gharchive_metrics \
  --profiles-dir dbt/profiles \
  --select "+모델명+"
```

우선순위는 `marts → staging/fact → raw reference → githubarchive.day.*`다.

주요 모델:

| 계층 | 모델 | grain / 역할 |
|---|---|---|
| mart | `metrics_daily` | 일별 core metric |
| mart | `metrics_event_type_daily` | 일자 × event type |
| mart | `metrics_weekly` | 주별 core metric |
| mart | `metrics_user_segments` | actor 활동성 segment |
| mart | `metrics_*retention*`, `metrics_*lifecycle*` | cohort/retention/lifecycle |
| staging | `stg_user_repo_activity` | fact 정리 + week/month 파생 |
| fact | `fact_user_repo_activity` | 날짜 × actor × repo × event type |
| raw reference | `raw_githubarchive_events_90d` | 제한된 raw lineage 확인용 |

모델 SQL과 YAML에서 grain, key, 시간 컬럼, partition/cluster, incremental 전략을 확인한다. 실제 렌더링 SQL은 다음처럼 좁혀 compile한다.

```bash
uv run --with dbt-bigquery dbt compile \
  --project-dir dbt/gharchive_metrics \
  --profiles-dir dbt/profiles \
  --select 모델명 \
  --vars '{"raw_start_date": "YYYY-MM-DD", "raw_end_date": "YYYY-MM-DD"}'
```

## 3. BigQuery 스캔 범위를 통제하기

### dbt fact/mart

- `activity_date` 조건을 건다.
- 필요한 컬럼만 선택한다. `SELECT *`는 사용하지 않는다.
- 큰 입력끼리 join하기 전에 날짜, actor/repo, event type으로 줄이고 필요한 grain으로 pre-aggregation한다.
- actor/repo 분석이면 event row를 먼저 actor/repo grain으로 집계한다.
- 정확한 distinct가 필요하지 않으면 `APPROX_COUNT_DISTINCT`를 고려하되 결과에 사용 사실을 남긴다.

### GitHub Archive raw

원천은 날짜별 sharded table이므로 `_TABLE_SUFFIX` 제한이 비용 방어선이다.

```sql
from `githubarchive.day.20*`
where concat('20', _table_suffix) between '20260701' and '20260701'
  and type in ('PushEvent', 'WatchEvent')
```

날짜 범위 없는 wildcard 조회는 금지한다. `DATE(created_at)` 조건만으로는 shard pruning을 대신할 수 없다.

쿼리를 실행하기 전에 BigQuery dry run으로 bytes processed를 확인한다. dbt 분석 SQL은 먼저 compile한 뒤 생성된 SQL을 dry run한다.

```bash
bq query \
  --use_legacy_sql=false \
  --dry_run \
  < dbt/gharchive_metrics/target/compiled/gharchive_metrics/analyses/분석파일.sql
```

dry run 결과와 예상 범위를 기록하고, 예상보다 크면 실행 전에 query plan을 다시 줄인다.

## 4. 작은 범위에서 의미와 품질 검증하기

기본은 최근 완료된 1일, 시계열이나 지연 도착 확인이 필요하면 7일이다. UTC 기준 GitHub Archive 일별 shard가 완전히 닫히지 않은 최신 날짜는 제외한다.

최소 점검 항목:

- row count와 핵심 key distinct count
- grain 기준 duplicate
- key/metric null rate
- `event_count >= 1`
- 날짜와 timezone 해석
- 최신 며칠의 late-arriving 또는 불완전 데이터
- event type 값과 예상 분포
- 동일 범위 mart/fact와 raw의 reconciliation

raw event 수와 fact row 수는 같지 않다. fact는 여러 이벤트를 grain별 한 행으로 압축하므로 `sum(event_count)`를 raw `count(*)`와 비교한다.

수치가 크게 다르면 기간 경계, null 제외, actor 의미, event type 조건, 집계 grain, 지연 도착 순서로 원인을 추적한다. 차이를 설명하지 못한 채 본 분석으로 확장하지 않는다.

## 5. 분석 SQL 작성과 승격

일회성 또는 탐색 SQL은 `dbt/gharchive_metrics/analyses/`에 둔다. 파일 상단 주석에 다음을 남긴다.

```sql
-- Question:
-- Grain:
-- Time basis / timezone:
-- Validation range:
-- Analysis range:
-- Segments:
-- Exclusions:
-- Metric definition:
-- Expected output grain:
```

CTE는 `params`, `filtered_*`, `aggregated_*`, `final`처럼 처리 단계가 드러나게 이름 짓는다. 기간 등은 dbt `var()`로 파라미터화한다. 한 결과에서 서로 다른 grain의 metric을 무심코 섞지 않는다.

반복 실행, dashboard 연결, 공통 지표 재사용 중 하나라도 해당하면 dbt model로 승격한다.

- model 설명에 grain과 기준 시간을 명시한다.
- `unique` 조합, `not_null`, 필요한 `relationships` 또는 custom reconciliation test를 추가한다.
- 날짜 partition과 자주 거는 동등 조건의 cluster를 검토한다.
- incremental이면 `unique_key`/`insert_overwrite` 범위가 실제 데이터 도착 및 재처리 방식과 맞는지 확인한다.
- 관련 모델만 선택해 build하고 upstream/downstream 영향을 확인한다.

```bash
uv run --with dbt-bigquery dbt build \
  --project-dir dbt/gharchive_metrics \
  --profiles-dir dbt/profiles \
  --select "+모델명"
```

## 6. 기본 실행 순서

1. 질문을 분석 계약으로 정리한다.
2. `dbt ls`와 model/YAML로 관련 lineage를 찾는다.
3. grain, key, time column, partition/shard, incremental 조건을 확인한다.
4. 완료된 1일 또는 7일로 품질을 점검한다.
5. compile + dry run 후 비용이 적은 1차 집계를 실행한다.
6. 필요한 경우 동일 조건으로 raw와 mart/fact를 reconcile한다.
7. 검증된 정의로 본 분석 범위만 확장한다.
8. 결론과 caveat를 작성한다.
9. 반복 분석이면 model/test/docs로 승격한다.
10. dashboard/report에 SQL 경로, 필터, 기준일, timezone, 한계를 함께 남긴다.

## 7. 결과 보고 형식

다음 순서로 간결하게 보고한다.

1. 결론
2. 핵심 숫자와 분석 기간
3. 사용한 BigQuery table/dbt model
4. 기간, segment, event type, 제외 조건
5. 수행한 품질 점검과 raw-mart reconciliation 결과
6. caveat: 인과 한계, instrumentation gap, 결측, 선택 편향, 지연 도착
7. 재현 가능한 SQL 또는 dbt model 경로

AB test나 적절한 준실험 설계가 없다면 “함께 증가했다/감소했다”라고 표현한다. “이 기능 때문에 변했다”는 인과 표현은 사용하지 않는다.
