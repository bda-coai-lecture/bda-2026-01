# Analytics Engineer: 분석을 데이터 제품으로 바꾸는 역할

이 문서는 BDA 2기 repo에서 지금까지 다룬 데이터 핸들링 작업을 기준으로, analytics engineer가 어떤 직무인지 정리한다. 핵심은 단순히 SQL이나 대시보드를 만드는 사람이 아니라, 노트북에서 한 번 계산한 분석을 팀이 계속 믿고 쓸 수 있는 데이터 제품으로 고정하는 역할이라는 점이다.

## 한 줄 정의

Analytics engineer는 분석 질문을 반복 가능한 데이터 제품으로 바꾸는 사람이다.

원천 로그를 신뢰 가능한 fact와 mart로 모델링하고, 지표 정의를 테스트와 semantic layer로 고정하며, BI 대시보드와 ML 실험이 같은 기준의 데이터를 쓰게 만든다.

## 왜 이 역할이 필요한가

노트북은 질문을 탐색하기 좋다. 이 repo에서도 `notebooks/gharchive` 아래에서 BigQuery 연결, 일별 집계 추출, 저장 포맷 비교, DAU, retention, activity, 데이터 품질 분석을 먼저 진행했다.

하지만 노트북 결과만으로는 팀 전체가 같은 지표를 안정적으로 쓰기 어렵다. 예를 들어 다음 질문들이 생긴다.

- active user는 GitHub `actor.id` 기준인가, organization까지 포함하는가?
- retention은 first-seen cohort retention인가, 전주 대비 lifecycle retention인가?
- DAU와 WAU를 계산할 때 봇/자동화 계정을 포함하는가?
- Metabase 대시보드는 raw event를 직접 집계하는가, 검증된 mart를 조회하는가?
- 추천 모델 학습 데이터는 어느 시점 기준 feature와 split으로 만들어졌는가?

Analytics engineering은 이런 질문을 코드, 테이블, 테스트, 문서, 스케줄로 고정하는 일이다.

## 이 repo에서의 흐름

```text
GitHub Archive BigQuery 원천
→ gharchive 노트북에서 추출/저장/DAU/retention/activity 탐색
→ BigQuery fact_user_repo_activity 적재
→ dbt staging/fact/mart/semantic layer 구성
→ dbt tests로 지표 정합성 검증
→ Airflow DAG로 반복 실행
→ Metabase dashboard와 추천시스템 feature mart에서 재사용
```

처음 1~6주차 작업은 "데이터를 이해하고 분석하는 단계"에 가까웠다. Week 7부터의 방향은 그 분석을 `dbt`, `Airflow`, `Metabase`, `BigQuery mart`로 옮겨 운영 가능한 데이터 제품으로 만드는 단계다.

## 핵심 책임

### 1. 원천 데이터를 분석 가능한 grain으로 바꾸기

GitHub Archive 원천 데이터는 이벤트 단위 로그다. 그대로 BI나 추천 모델에 연결하면 비용이 크고, 매번 다른 기준으로 집계될 위험이 있다.

그래서 이 repo에서는 `fact_user_repo_activity`를 중심 fact로 둔다. 핵심 grain은 다음과 같다.

```text
activity_date, user_id, repo_id, action
```

이 fact가 있으면 DAU, WAU, event type별 activity, retention, user lifecycle, 추천시스템 interaction matrix가 모두 같은 기준의 활동 데이터에서 출발한다.

관련 파일:

- `notebooks/gharchive/01_extract_daily_agg.ipynb`
- `src/gharchive/extract.py`
- `scripts/sync_bq_metrics.py`
- `dbt/gharchive_metrics/models/facts/fact_user_repo_activity.sql`
- `dbt/gharchive_metrics/models/staging/stg_user_repo_activity.sql`

### 2. 지표 mart와 semantic layer 만들기

노트북에서 계산한 DAU, WAU, retention은 반복 사용 가능한 mart로 옮겨야 한다. 이 repo의 `dbt/gharchive_metrics/models/marts/platform/` 아래에는 dashboard와 분석을 위한 지표 테이블이 있다.

예시:

- `metrics_daily`
- `metrics_event_type_daily`
- `metrics_weekly`
- `metrics_user_segments`
- `metrics_retention_weekly`
- `metrics_retention_summary`
- `metrics_user_lifecycle_weekly`
- `metrics_user_lifecycle_monthly`
- `metrics_cohort_retention_weekly_heatmap`
- `metrics_cohort_retention_monthly_heatmap`

Metabase가 raw event를 직접 집계하지 않고 작은 `metrics_*` mart를 보게 만들면 조회 비용이 낮아지고, 지표 정의도 흔들리지 않는다.

또 `dbt/gharchive_metrics/models/semantic/platform_activity.yml`에서는 `active_users`, `active_repos`, `total_events`, `push_events`, `watch_events` 같은 metric 이름과 집계 방식을 고정한다. 같은 지표명은 같은 계산식을 의미하게 만드는 것이 semantic layer의 목적이다.

### 3. 테스트로 지표 신뢰성 보장하기

좋은 mart는 숫자가 나오는 테이블이 아니라, 숫자가 틀렸을 때 실패하는 테이블이다.

이 repo에는 단순 null/unique 체크뿐 아니라 retention과 lifecycle accounting을 검증하는 custom dbt test가 있다.

예시:

- `dbt/gharchive_metrics/tests/assert_user_lifecycle_weekly_balances.sql`
- `dbt/gharchive_metrics/tests/assert_user_lifecycle_monthly_balances.sql`
- `dbt/gharchive_metrics/tests/metrics_user_retention_weekly_accounting.sql`
- `dbt/gharchive_metrics/tests/metrics_cohort_retention_weekly_heatmap_active_users_match_wau.sql`

이런 테스트는 weekly lifecycle에서 유지/복귀/이탈 유저 수의 관계가 맞는지, cohort heatmap의 active user 수가 WAU와 논리적으로 맞는지 확인한다. 대시보드 숫자 뒤에 어떤 model과 test가 있는지 추적할 수 있어야 한다.

### 4. 비용과 재현성을 관리하기

Analytics engineer는 "계산이 된다"에서 멈추지 않고 "현실적으로 계속 돌릴 수 있다"까지 봐야 한다.

이 repo에서 다룬 예시는 다음과 같다.

- BigQuery dry run으로 스캔량과 비용을 먼저 확인한다.
- 최근 90일 rolling window를 기본 운영 범위로 둔다.
- 전체 이력 backfill은 명시적 옵션을 요구한다.
- BigQuery fact는 partition/cluster와 incremental `insert_overwrite`를 고려한다.
- Metabase는 raw event 대신 작은 metric mart를 조회하게 한다.
- 추천 실험 mart는 `window_end_date`, `snapshot_date`, `experiment_id`, `split`을 둬서 학습/평가 기준을 재현 가능하게 만든다.

관련 파일:

- `docs/data_platform_local.md`
- `dbt/gharchive_metrics/README.md`
- `scripts/week6_build_recsys_marts.py`

### 5. Airflow로 반복 실행 가능하게 만들기

노트북과 CLI 명령은 사람이 직접 실행해야 한다. 운영 가능한 데이터 제품은 정해진 시간에, 정해진 범위만, 실패 시 재시도하면서 실행되어야 한다.

이 repo에서는 Airflow DAG가 그 역할을 한다.

- `dags/gharchive_platform_metrics.py`: BigQuery metric mart 갱신
- `dags/gharchive_dbt_metrics.py`: fact sync 후 dbt model/test 실행
- `dags/gharchive_recsys_features.py`: 추천 실험용 mart/feature 생성 흐름

`gharchive_dbt_metrics`는 먼저 plan 단계에서 처리 범위와 방어선을 확인하고, 통과하면 fact sync와 dbt build를 실행한다. Analytics engineer는 SQL뿐 아니라 그 SQL이 언제, 어떤 범위로, 어떤 실패 조건에서 실행되는지도 설계한다.

### 6. BI와 ML이 같이 쓰는 데이터 계약 만들기

Analytics engineering은 BI에서 끝나지 않는다. 추천시스템 실험에서도 데이터 계약이 중요하다.

Week 6 추천 실험에서는 `scripts/week6_build_recsys_marts.py`로 다음 batch mart를 만든다.

- `user_repo_interaction_mart`
- `user_profile_mart`
- `repo_feature_mart`
- `repo_repo_related_mart`
- `experiment_split_mart`

이 mart들은 ALS retrieval, LGBM re-rank, Two-Tower, FAISS benchmark 같은 추천 실험의 입력이 된다. 모델 성능은 알고리즘만으로 결정되지 않는다. 어떤 interaction을 학습 데이터로 볼지, feature가 어느 시점 기준인지, train/test split이 재현 가능한지가 성능 수치의 신뢰도를 좌우한다.

이 지점에서 analytics engineer는 ML engineer가 아니다. 다만 ML 실험이 믿을 수 있는 데이터셋 위에서 돌아가도록 feature mart, split mart, point-in-time 기준, leakage 위험을 관리한다.

## 필요한 역량

| 역량 | 이 repo에서의 예시 |
|---|---|
| SQL / BigQuery | sharded table 조회, 집계, dry run, partition/cluster, incremental load |
| 데이터 모델링 | raw event를 `activity_date, user_id, repo_id, action` grain의 fact로 정리 |
| dbt | source, staging, fact, mart, schema.yml, tests, Semantic Layer |
| 지표 설계 | DAU, WAU, active repo, event mix, cohort retention, lifecycle retention |
| 데이터 품질 | null/unique/range/accounting test, 봇/이상치 영향 분석 |
| Python 데이터 처리 | pandas, pyarrow, DuckDB, Parquet, dtype 최적화 |
| 오케스트레이션 | Airflow DAG, task dependency, retry, timeout, env/config 관리 |
| BI 이해 | Metabase가 raw table이 아니라 검증된 mart를 조회하게 설계 |
| ML 데이터 이해 | 추천 feature mart, experiment split, point-in-time feature, leakage 관리 |
| 운영 감각 | 90일 window, backfill guardrail, 비용 예산, credentials 분리 |

## 직무 경계

| 역할 | 주 관심사 | 이 repo 기준 예시 | Analytics Engineer와의 경계 |
|---|---|---|---|
| Data Analyst | 질문 정의, 분석, 해석 | DAU 추이 분석, retention 해석, 활동 segment 분석 | AE는 분석 결과를 반복 가능한 mart와 metric으로 고정한다. |
| Analytics Engineer | 신뢰 가능한 분석 데이터 모델링 | dbt staging/fact/mart, schema test, semantic metric, BI용 table | 분석과 엔지니어링 사이의 metric contract를 소유한다. |
| Data Engineer | 원천 수집, 저장, 인프라, 파이프라인 안정성 | BigQuery 접근, Airflow/Docker 환경, 대용량 처리 기반 | AE는 원천 인프라 전체보다 분석용 fact/mart/metric layer에 집중한다. |
| ML Engineer | 모델 학습, 평가, 서빙, 실험 추적 | ALS, LGBM, Two-Tower, FAISS, FastAPI | AE는 모델 자체보다 학습/평가용 데이터셋의 정의와 품질을 보장한다. |

교육 repo에서는 한 사람이 여러 역할을 경험한다. 그래도 `gharchive` 노트북에서 `dbt marts/tests`, `Airflow`, `Metabase`, `recsys feature mart`로 넘어가는 구간이 analytics engineering의 핵심 구간이다.

## End-to-End 예시

### 예시 1. DAU 분석이 dashboard 지표가 되는 과정

```text
질문:
GitHub Archive에서 일별 active user는 어떻게 변하는가?

탐색:
notebooks/gharchive/03_dau.ipynb에서 DAU 추세를 분석한다.

제품화:
fact_user_repo_activity를 만들고,
dbt metrics_daily에서 activity_date별 active_users를 계산한다.

검증:
schema.yml과 dbt tests로 기본 품질과 지표 정합성을 확인한다.

운영:
Airflow gharchive_dbt_metrics DAG가 매일 dbt build를 실행한다.

소비:
Metabase dashboard에서 DAU 시계열을 조회한다.
```

### 예시 2. Retention 분석이 검증 가능한 mart가 되는 과정

```text
질문:
첫 활동 주차별 cohort는 이후 몇 주 동안 얼마나 남는가?

탐색:
notebooks/gharchive/05_retention_activity.ipynb에서 weekly retention을 계산한다.

제품화:
metrics_retention_weekly,
metrics_retention_summary,
metrics_cohort_retention_weekly_heatmap mart를 만든다.

검증:
cohort heatmap의 active user 수와 WAU 관계,
retention accounting rule을 dbt tests로 확인한다.

소비:
Metabase에서 cohort retention heatmap과 summary 지표를 본다.
```

### 예시 3. 분석 mart가 추천시스템 feature로 확장되는 과정

```text
질문:
유저에게 어떤 GitHub repo를 추천할 것인가?

탐색:
ghrec 노트북에서 popularity baseline, ALS, Two-Stage, Two-Tower를 비교한다.

제품화:
week6_build_recsys_marts.py로
user_repo_interaction_mart,
user_profile_mart,
repo_feature_mart,
experiment_split_mart를 만든다.

모델링:
ALS retrieval,
LGBM re-rank,
Two-Tower,
FAISS candidate search에 mart를 사용한다.

검증:
NDCG, Precision@K, diversity, latency로 모델을 비교한다.

소비:
Streamlit dashboard와 FastAPI 추천 API에서 결과를 확인한다.
```

## 흔한 오해

- "Analytics engineer = SQL 잘 쓰는 analyst"가 아니다. SQL은 도구이고, 핵심은 grain, lineage, test, semantic metric, 운영 주기다.
- "dbt를 쓰면 analytics engineer"가 아니다. dbt보다 중요한 것은 지표 정의를 재사용 가능하고 검증 가능하게 만드는 설계다.
- "Dashboard를 만들면 끝"이 아니다. Dashboard는 소비면이고, 그 뒤의 mart 품질, refresh, null/중복/기간 검증이 본체다.
- "Retention/DAU는 계산식만 알면 된다"가 아니다. actor 기준, 기간 window, cohort 정의, 최신 기간 null 처리에 따라 의미가 달라진다.
- "ML 모델 성능이 좋으면 데이터 작업은 부차적"이 아니다. split, label, seen filtering, metadata leakage 관리가 없으면 추천 성능 수치는 신뢰하기 어렵다.

## 포트폴리오 문장으로 번역하기

이 repo에서 한 작업은 다음처럼 직무 역량 문장으로 바꿀 수 있다.

| Repo 작업 | 직무 역량 문장 |
|---|---|
| BigQuery dry run 후 일별 aggregate parquet 추출 | 대용량 로그를 비용을 고려해 분석 가능한 집계 단위로 추출했다. |
| JSON/CSV/Parquet 저장 포맷 비교 | 분석 워크로드에 맞는 저장 포맷과 dtype 최적화를 검증했다. |
| DAU/WAU/event mix 분석 | 제품 핵심 지표를 정의하고 시계열/세그먼트 관점으로 해석했다. |
| retention/activity 분석 | cohort와 lifecycle 관점의 사용자 유지 지표를 설계했다. |
| data quality/bot detection 분석 | 봇/이상치가 핵심 지표에 미치는 영향을 검증했다. |
| dbt fact/mart 구성 | 노트북 분석 로직을 재사용 가능한 warehouse model로 제품화했다. |
| dbt schema/custom tests | 지표 정합성을 자동 검증하는 data quality contract를 만들었다. |
| semantic layer 정의 | BI와 분석에서 같은 이름의 지표가 같은 계산식을 쓰도록 표준화했다. |
| Airflow DAG 구성 | fact sync와 dbt build를 반복 실행 가능한 운영 파이프라인으로 연결했다. |
| Metabase dashboard 자동 구성 | 검증된 mart를 최종 소비면인 BI dashboard로 연결했다. |
| 추천 feature/split mart 생성 | ML 실험이 재현 가능한 point-in-time 데이터셋 위에서 실행되도록 설계했다. |

## 결론

Analytics engineer는 데이터 분석을 "한 번 나온 결과"에서 "계속 믿고 쓸 수 있는 시스템"으로 바꾸는 사람이다.

이 repo에서는 그 흐름이 명확하다. `gharchive` 노트북에서 시작한 DAU, retention, activity 분석은 BigQuery fact와 dbt marts/tests로 정리되고, Airflow로 반복 실행되며, Metabase dashboard에서 소비된다. 같은 원칙은 추천시스템에도 이어진다. ALS, LGBM, Two-Tower 같은 모델도 결국 재현 가능한 interaction mart, user profile mart, repo feature mart, experiment split mart 위에서 안정적으로 비교될 수 있다.
