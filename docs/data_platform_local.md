# 데이터 플랫폼 로컬 구성

최종 수정: 2026-05-10

목표는 로컬에서 Airflow 3와 Metabase를 띄우고, 로컬 parquet 산출물을 BigQuery mart로 동기화한 뒤 과거 기초 지표를 Metabase에서 보는 것이다.

## 구성

| 영역 | 도구 | 위치 |
|---|---|---|
| 원천 산출물 | local parquet | `data/daily_agg/*.parquet` |
| warehouse | BigQuery | `bda-coai.mart` |
| orchestration | Airflow 3.2.1 | Docker Compose, `http://localhost:8080` |
| BI | Metabase | Docker Compose, `http://localhost:3001` |
| Python 실행 | uv | Airflow task도 `uv run` 사용 |

Airflow Docker 구성은 Apache Airflow 3.2.1 공식 Docker Compose 구조를 기준으로 했다. Airflow 3에서는 UI/API 서비스 이름이 `airflow-apiserver`이고, DAG parsing은 `airflow-dag-processor`가 담당한다.

참고: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## BigQuery 테이블

동기화 스크립트:

```bash
GCP_KEY_PATH=/path/to/gcp-key.json \
uv run python scripts/sync_bq_metrics.py \
  --project bda-coai \
  --dataset mart \
  --parquet-dir data/daily_agg \
  --start 2026-04-04 \
  --end 2026-05-08 \
  --max-days 35 \
  --mode replace-all \
  --skip-fact \
  --build-metrics
```

생성/갱신되는 테이블:

| 테이블 | 설명 |
|---|---|
| `mart.fact_user_repo_activity` | 선택 사항. 일별 `user_id, repo_id, action` 집계 fact |
| `mart.metrics_daily` | DAU, 활성 repo, 총 이벤트, 주요 action count |
| `mart.metrics_event_type_daily` | action별 일별 사용자/repo/event |
| `mart.metrics_weekly` | WAU, weekly active repo, weekly event |
| `mart.metrics_user_segments` | 활동일수 기준 유저 세그먼트 |
| `mart.metrics_retention_weekly` | 주차 cohort retention long table |
| `mart.metrics_retention_summary` | cohort별 W0/W1/W2/W3 retention 요약 |
| `mart.metrics_agent_trendy_repos` | OpenClaw/oh-my-openagent seed 기반 AI agent 트렌디 repo |
| `mart.metrics_agent_trend_validation` | trend score와 baseline들의 다음 주 예측력 비교 |

현재 로컬 parquet 기준 기간은 `2026-02-15`부터 `2026-05-08`까지다.

비용 관리를 위해 기본 Airflow DAG는 전체 83일이 아니라 최근 35일(`2026-04-04` ~ `2026-05-08`)만 처리한다. 또한 기본 DAG는 `--skip-fact`로 raw fact를 BigQuery에 올리지 않고, Metabase가 볼 지표 테이블만 업로드한다. 전체 이력 또는 fact를 올리려면 의도적으로 옵션을 바꿔야 한다.

현재 `bda-coai` 프로젝트는 BigQuery sandbox 제약이 있어서 billing이 켜져 있지 않으면 dataset/table expiration이 필요하다. 그래서 스크립트는 `mart` dataset의 기본 table/partition expiration을 58일로 맞추고, 과거 날짜 데이터를 보존하기 위해 fact table은 날짜 파티션 없이 생성한다. `activity_date` 컬럼은 그대로 있으므로 Metabase 시계열 필터링은 동일하게 가능하다.

지표 테이블은 BigQuery SQL로 fact를 여러 번 스캔해서 만들지 않는다. 로컬 parquet를 DuckDB로 직접 스캔해 작은 aggregate 결과만 BigQuery에 업로드한다. Metabase는 이 작은 지표 테이블만 보게 해서 조회 비용을 낮춘다.

## Airflow 실행

초기 1회:

```bash
mkdir -p dags logs plugins config
printf "AIRFLOW_UID=%s\n" "$(id -u)" > .env
docker compose build airflow-init
docker compose up airflow-init
```

실행:

```bash
docker compose up airflow-apiserver airflow-scheduler airflow-dag-processor metabase
```

Airflow UI:

- URL: `http://localhost:8080`
- ID/PW: `airflow` / `airflow`
- DAG:
  - `gharchive_repo_metadata_refresh`: repo metadata cache 갱신
  - `gharchive_platform_metrics`: BigQuery metric mart 갱신

metadata DAG는 매일 05:00 KST에 실행된다. 일별 주기 작업에서는 `warm` tier를 사용하고, GitHub API 호출량은 `--max-fetch 50`으로 제한한다.

Airflow `BashOperator`는 `append_env=True`를 사용한다. 따라서 Airflow 컨테이너 환경에 `GITHUB_TOKEN`을 넣으면 metadata refresh가 자동으로 토큰 인증을 사용한다. 토큰이 없으면 `gh auth token` fallback을 시도하고, 컨테이너에 `gh` 인증도 없으면 무토큰 GitHub API 한도 안에서 동작한다.

```bash
uv run --no-project --with pandas --with requests --with duckdb python scripts/refresh_repo_metadata.py --parquet-dir data/daily_agg --start 2026-04-04 --end 2026-05-08 --top-n 500 --cache-tier warm --max-fetch 50
```

metric DAG는 매일 06:00 KST에 실행된다. 첫 태스크 `plan_metric_sync`는 `--plan-only`로 날짜 범위와 `--max-days` 방어선을 먼저 확인한다. 통과하면 `sync_metrics`가 fact 업로드 없이 aggregate metric table만 BigQuery에 갱신한다.

```bash
uv run --no-project --with pandas --with pyarrow --with duckdb --with google-cloud-bigquery --with db-dtypes python scripts/sync_bq_metrics.py --project bda-coai --dataset mart --parquet-dir data/daily_agg --start 2026-04-04 --end 2026-05-08 --max-days 35 --mode replace-all --skip-fact --build-metrics
```

Airflow 태스크 안정화 설정:

| DAG | task | retry | timeout |
|---|---|---:|---:|
| `gharchive_repo_metadata_refresh` | `refresh_repo_metadata` | 1회, 5분 대기 | 20분 |
| `gharchive_platform_metrics` | `plan_metric_sync` | 1회, 5분 대기 | 5분 |
| `gharchive_platform_metrics` | `sync_metrics` | 1회, 5분 대기 | 45분 |

컨테이너 안에서는 macOS host `.venv`를 쓰지 않도록 아래 경로를 쓴다.

| 변수 | 값 |
|---|---|
| `UV_CACHE_DIR` | `/opt/airflow/uv-cache` |
| `UV_PROJECT_ENVIRONMENT` | `/opt/airflow/uv-env/bda-2` |

Airflow DAG에서는 프로젝트 전체 dependency를 설치하지 않도록 `uv run --no-project --with ...`를 쓴다. 이렇게 하면 추천 실험용 `torch`, `faiss`, `lightgbm`까지 설치하지 않고 데이터 플랫폼 동기화에 필요한 최소 패키지만 쓴다.

로컬 Docker에서 35일 parquet를 집계할 때는 DuckDB 메모리 제한을 `3GB`, thread를 `1`로 두고 `/tmp/duckdb-spill`을 사용한다. Airflow 태스크는 2026-05-10 기준 실제 실행 검증이 끝났고, `metrics_*` 8개 테이블을 갱신한다.

## Metabase 연결

Metabase UI:

- URL: `http://localhost:3001`
- 최초 접속 시 로컬 계정 생성

BigQuery 연결:

| 항목 | 값 |
|---|---|
| Project ID | `bda-coai` |
| Dataset | `mart` |
| Service account JSON | `/app/gcp-key.json` |

로컬 계정, BigQuery 연결, 질문 카드, 대시보드는 API로 자동 생성할 수 있다.

```bash
uv run python scripts/setup_metabase_dashboard.py
```

기본 로컬 로그인:

| 항목 | 값 |
|---|---|
| Email | `bda@local.dev` |
| Password | `bda-local-2026` |
| Dashboard | `GitHub Archive 기초 지표` |
| Trend Dashboard | `AI Agent 트렌디 레포` |

추천 질문/차트:

| 차트 | 테이블 | 시각화 |
|---|---|---|
| DAU 추이 | `metrics_daily` | Line, `activity_date` x `active_users` |
| 총 이벤트 추이 | `metrics_daily` | Line, `activity_date` x `total_events` |
| 이벤트 타입별 활동 | `metrics_event_type_daily` | Stacked area/bar, `activity_date`, `action`, `total_events` |
| WAU 추이 | `metrics_weekly` | Line, `week_start` x `weekly_active_users` |
| 유저 세그먼트 | `metrics_user_segments` | Bar, `user_segment` x `users` |
| W1 Retention | `metrics_retention_summary` | Scalar/Line, `cohort_week` x `w1_retention` |
| Cohort Retention | `metrics_retention_weekly` | Table, `cohort_week`, `weeks_since`, `retention_rate` |
| AI Agent 트렌디 repo | `metrics_agent_trendy_repos` | Table/Bar/Scatter |
| Agent Trend 예측력 | `metrics_agent_trend_validation` | Table |

별도 trend 대시보드:

- URL: `http://localhost:3001/dashboard/3`
- 목적: OpenClaw/oh-my-openagent seed 기반으로 트렌디 repo 후보, 점수 근거, seed affinity, 정량 검증을 한 화면에서 설명한다.

Retention 정의:

- cohort: 유저가 선택 기간 안에서 처음 활동한 주의 Monday `week_start`
- retained user: cohort 이후 N주차에도 한 번 이상 활동한 유저
- `w0_retention`은 항상 1.0
- 최신 cohort는 아직 다음 주 데이터가 없으므로 W1/W2 값이 0일 수 있다. 강의에서는 `w1_retention > 0`인 완성 cohort를 기준으로 설명한다.

AI Agent trend 정의:

- seed repo: `openclaw/openclaw`, `code-yeongyu/oh-my-openagent`
- 후보 repo: repo metadata의 name/description/topics가 agent/coding-agent/claude/codex/opencode/openclaw/openagent/mcp/llm 계열 키워드와 매칭되는 repo 또는 seed repo
- 제외 키워드: stock/trading/finance/crypto 계열. AI 일반 도구가 아니라 agent 생태계 trend를 보기 위함
- recent window: 최근 7일
- baseline window: 직전 28일
- weighted score: 이벤트별 가중치를 적용한 repo activity score
- seed affinity: recent window에서 seed repo에 반응한 유저 중 해당 repo에도 반응한 유저 비중

이벤트 가중치:

| event | weight | 해석 |
|---|---:|---|
| `WatchEvent` | 3.0 | 관심 표명. trend signal로 강하게 반영 |
| `ForkEvent` | 2.0 | 실험/도입 의도 |
| `PullRequestEvent` | 1.5 | 기여 활동 |
| `IssuesEvent` | 1.0 | 사용/관심/문제 제기 |
| `IssueCommentEvent` | 0.5 | 토론 참여 |
| `PushEvent` | 0.2 | maintainer 활동이 과도하게 지배하지 않도록 낮게 반영 |
| 기타 | 0.1 | 약한 활동 |

Trend score 공식:

```text
trend_score =
  log1p(recent_active_users)
  * log1p(recent_score)
  * min(growth_ratio, 20)
  * (1 + min(seed_affinity, 1))
```

세부 정의:

| 항목 | 정의 |
|---|---|
| `recent_active_users` | 최근 7일 동안 해당 repo에 반응한 고유 유저 수 |
| `recent_score` | 최근 7일 weighted score 합 |
| `baseline_score` | 직전 28일 weighted score 합 |
| `growth_ratio` | `(recent_score / 7 + 1) / (baseline_score / 28 + 1)` |
| `seed_affinity` | 최근 7일 seed repo 반응 유저 중 해당 repo에도 반응한 유저 비중 |

해석:

- `trend_score`는 단순 인기순이 아니라 "현재 활동량", "최근 성장", "seed 생태계와의 사용자 겹침"을 함께 보는 설명용 지표다.
- `popularity_recent_score`가 다음 주 활동 예측력은 더 높을 수 있다. 대신 `trend_score`는 왜 AI agent 생태계 trend로 볼 수 있는지 설명하기 쉽다.
- 후보 수가 metadata cache 품질에 영향을 받으므로, trend 대시보드를 보기 전에는 repo metadata cache를 갱신한다.
- GitHub metadata의 stars/forks/topics는 현재 시점 snapshot이다. 엄밀한 과거 예측 feature로 쓸 때는 temporal leakage를 별도로 통제해야 한다.

정량 검증:

- validation split: 21일 baseline, 7일 trend, 7일 label
- 비교 모델: `agent_trend_score`, `popularity_recent_score`, `growth_only`, `seed_affinity_only`, `recent_active_users`
- 지표: Spearman rank correlation, Precision@20 against next top 100, NDCG@20, Avg next score@20

## Repo metadata cache 운영

Repo metadata는 GitHub REST API 결과를 로컬 SQLite 캐시에 저장해서 재사용한다. 캐시 파일은 `data/repo_metadata.db`이고, 테이블은 `repo_metadata`다.

저장 컬럼은 `repo_id`, `repo_name`, `description`, `language`, `stargazers`, `forks`, `topics`, `license_key`, `created_at`, `updated_at`, `archived`, `fetched_at`, `http_status`다. 데이터 플랫폼 동기화 스크립트는 이 캐시를 읽어 AI agent trend 후보 repo의 이름, 설명, topic, stars/forks를 붙인다.

캐시 갱신:

```bash
uv run python scripts/refresh_repo_metadata.py \
  --start 2026-04-04 \
  --end 2026-05-08 \
  --top-n 500 \
  --max-fetch 200
```

캐시 신뢰도 정책:

| tier | 용도 | stale 기준 |
|---|---|---|
| `hot` | 유명 repo, 추천 진입점 repo, 개별 repo 상세 화면 | 3일 |
| `warm` | 데이터 플랫폼팀이 주기적으로 끌어오는 상위 repo pool | 7일 |

기본값은 `warm`이다. 개별 repo를 바로 추천/상세 화면에 써야 하면 `hot`으로 갱신한다.

```bash
uv run python scripts/refresh_repo_metadata.py \
  --top-n 0 \
  --cache-tier hot \
  --repo 937253475=anthropics/claude-code
```

안전 확인:

```bash
uv run python scripts/refresh_repo_metadata.py \
  --start 2026-04-04 \
  --end 2026-05-08 \
  --top-n 500 \
  --max-fetch 200 \
  --dry-run
```

동작 방식:

- 로컬 parquet에서 이벤트 수 기준 Top repo를 뽑는다.
- `data/repo_name_lookup.db`에서 `repo_id -> owner/repo` 이름을 먼저 찾는다.
- lookup cache에 없으면 `data/models/repo_name_map.pkl`에서 찾고, 찾은 값과 miss를 `data/repo_name_lookup.db`에 저장한다. 559MB pickle을 매번 읽지 않기 위한 로컬 보조 캐시다.
- `data/repo_metadata.db`에 없는 repo를 먼저 fetch한다.
- 그다음 `--refresh-stale-days`보다 오래된 기존 cache를 갱신한다.
- GitHub token은 `GITHUB_TOKEN` 환경변수를 우선 사용하고, 없으면 `gh auth token`을 사용한다. 토큰 값은 문서나 로그에 남기지 않는다.

캐시 상태 확인:

```bash
sqlite3 data/repo_metadata.db \
  'select http_status, count(*) from repo_metadata group by http_status order by http_status;'
```

GitHub metadata는 현재 시점의 stars/forks/topics snapshot이다. 과거 split 기반 추천 모델 평가에서는 temporal leakage 가능성이 있으므로, 강의에서는 "설명/시각화용 metadata"와 "엄밀한 과거 예측 feature"를 구분해서 설명한다.

강의에서는 `metrics_daily`로 과거 DAU와 이벤트 수를 먼저 보여주고, `metrics_event_type_daily`로 Push/Watch/Fork/PullRequest 비중 차이를 설명하면 된다.

## 운영 메모

- `replace-all`은 BigQuery load job의 `WRITE_TRUNCATE`로 fact 전체를 다시 만든 뒤 나머지 날짜를 append한다. sandbox에서도 동작하고 중복이 생기지 않는다.
- `replace-days`는 날짜별 `DELETE` 후 append 방식이다. billing이 켜진 프로젝트에서는 쓸 수 있지만 BigQuery sandbox에서는 DML 제한 때문에 막힌다.
- `skip-existing`은 이미 BigQuery에 해당 날짜 row가 있으면 건너뛴다.
- `append`는 중복 방지가 없으므로 강의용으로는 권장하지 않는다.
- GCP key는 repo에 복사하지 않고 기존 `gcp-key.json` symlink 또는 `GCP_KEY_PATH`를 사용한다.
- 먼저 `--plan-only`로 날짜 수와 로컬 업로드 크기를 확인한다.
- 기본 방어선은 `--max-days 35`다. 이보다 긴 기간은 실패하도록 했다.
- 기본 DAG는 `--skip-fact`를 사용한다. fact table은 실습상 필요할 때만 수동으로 올린다.
