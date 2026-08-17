---
name: analysis
description: bda-2 GitHub Archive 데이터로 비용이 통제되고 재현 가능한 분석을 수행한다. DAU/WAU/MAU, 리텐션, 코호트, 라이프사이클, actor/repo 세그먼트, event type 비교, push 에피소드/이탈, 데이터 품질 진단, raw와 mart 불일치, 반복 분석의 dbt 모델 승격에 사용한다. SQL·쿼리·BigQuery·dbt·dry run·스캔 비용·mart·grain·차트 관련 요청, "분석해줘", "이 수치 맞아?", "왜 줄었어?", "리텐션 어때?", "몇 명이야?" 같은 질문이 오면 이 스킬을 먼저 연다. 데이터 질문이 아닌 것(dbt 빌드 실패, Airflow 장애, 인프라·권한·배포 문제)에는 쓰지 않는다.
---

# BDA 분석 워크플로우

질문을 바로 SQL로 옮기지 않는다. **분석 계약을 먼저 고정하고, 데이터를 먼저 의심하고, 그 다음에 숫자를 낸다.**

## 문서 권한 구분

| 문서 | 무엇의 정본인가 |
|---|---|
| 이 스킬과 `references/` | **분석 절차의 정본.** 모드, 인풋, 건강성 검증, 보고 형식 |
| `docs/bigquery_dbt_analysis_workflow.md` | **비용·lineage 계약의 정본.** 스캔 상한, 승인 절차, 승격 기준 |
| `docs/analysis_workflow_review.md` | 이 절차를 왜 이렇게 만들었는지에 대한 판정 근거 |
| 루트 `AGENTS.md` | 레포 고유 주의사항 |

둘이 충돌하면 위 표대로 나눈다. 워크플로우 문서를 매번 통독하지 않는다.
**raw 스캔 승인 절차와 비용 상한을 결정할 때만** 해당 문서의 3절(스캔 범위 통제)과 5절(승격)을 편다.

## 절대 규칙

1. **`githubarchive.day.20*`를 `_TABLE_SUFFIX` 범위 없이 조회하지 않는다.** `DATE(created_at)` 조건은 shard pruning을 대신하지 못한다.
2. **새 쿼리나 실질적으로 바뀐 쿼리는 실행 전에 dry run**하고 bytes processed를 인풋 #9의 예산과 대조해 보고한다.
   **스캔 상한은 10 GiB이고 BigQuery가 강제한다.** 넘으면 `bytesBilledLimitExceeded`로 작업이 실패한다.
   dry run이 10 GiB를 넘으면 **실행하지 말고 멈춘다.** 쪼개거나 기간을 줄여서 우회하지도 않는다.
   대신 답변에 이렇게 적고 끝낸다 — `승인 필요: 예상 N GiB (상한 10 GiB). 사유: …`
   상한 상향은 운영자만 할 수 있다(`--max-scan-gib N`으로 재기동).
   **`--maximum_bytes_billed`를 직접 붙여 상한을 올리지 않는다.** 기술적으로는 가능하지만 금지다.
   턴 종료 시 실제 과금 바이트가 보고되므로 우회는 사후에 드러난다.
   이 상한이 존재하는 이유는 BigQuery 스캔이 이 시스템에서 **유일한 실지출**이기 때문이다.
   Claude 쪽 `cost=$…`는 구독 사용량의 API 환산치이지 청구가 아니다. 둘을 합산하지 않는다.
3. **대조군이 없으면 인과를 말하지 않는다.** "같이 늘었다"까지만 쓴다.
4. **`user_id`는 `actor.id`다.** 사람도 조직도 아니다. 봇도 actor다.
5. **검증이 WARN/FAIL이면 결론의 강도를 낮춘다.** 검증을 건너뛰고 낸 숫자는 산출물이 아니다.
6. **표본이 작으면 "차이 없음"이 아니라 "판단 불가"다.** 무엇을 더 봐야 답이 나오는지 함께 적는다.
7. **자격 증명 파일 내용을 출력하지 않는다.** 무관한 사용자 변경을 건드리지 않는다.

## 실행 순서

### 0. 컨텍스트 확보

```bash
git status --short          # 무관한 변경 보존
```

**자격 증명은 이미 주입되어 있다. 다시 export 하지 않는다.**
`GCP_KEY_PATH` · `GOOGLE_APPLICATION_CREDENTIALS` · `CLOUDSDK_CONFIG`은 세션에 이미 들어와 있고,
전부 **읽기 전용** 계정(`bda-analyst-ro@`)을 가리킨다.
이걸 `gcp-key.json`으로 덮어쓰면 `roles/owner` 키로 갈아타게 된다 — 절대 하지 않는다.

값이 비어 있으면 그건 환경 설정 문제다. 직접 채우지 말고 **그 사실을 보고하고 멈춘다.**

`GCP_KEY_PATH`가 없으면 dbt가 파싱 단계에서 죽는다.
Python/dbt는 `uv run --no-project --with dbt-bigquery`로 실행한다. `dbt`는 전역 설치되어 있지 않다.

### 1. 질문 접수와 모드 분류

요청이 모호하면 `references/request_intake.md`의 접수 양식을 먼저 채운다. **추측으로 메우지 않는다.**

질문을 아래 7개 중 **하나**로 분류하고, 분류 결과를 답변에 적는다. 상세는 `references/modes.md`.

| 모드 | 질문 형태 |
|---|---|
| 활동량 | 얼마나 쓰였나 / 추세가 어떤가 |
| 라이프사이클 단계 | 어디서 떨어지나 / 얼마나 살아남나 |
| 전후 비교 | 그 시점 이후 달라졌나 |
| 세그먼트 비교 | A 집단과 B 집단이 다른가 |
| 리텐션/코호트 | 다시 돌아오나 |
| 데이터 품질 진단 | 이 수치를 믿어도 되나 |
| 계측 공백 판정 | 애초에 이 데이터가 있나 |

**AB 테스트 모드는 없다.** 우리 데이터에 variant 배정이 존재하지 않는다.
AB처럼 보이는 요청은 세그먼트 비교로 내리고, 대조군이 없다는 사실을 먼저 말한다.

두 모드에 걸치면 **분해해서 순서대로** 처리한다. 섞지 않는다.

### 2. 인풋 9종 고정

모르는 항목은 `미확인`, 데이터에 없는 항목은 `계측 공백`으로 적는다. **비워두지 않는다.**
각 항목에 **근거**를 붙인다. 근거 없는 `확인됨`은 반증 불가능한 주장이다.

| # | 인풋 | 근거로 적을 것 |
|---|---|---|
| 1 | 분석 대상 | 사용자가 말한 문장 |
| 2 | 분석 단위(grain) | 모델명과 그 모델의 grain |
| 3 | 기간 (검증 / 본분석 **각각**) | 데이터 보유 범위 확인 결과 |
| 4 | 기준 시간 | 컬럼명 (기본 `activity_date`, UTC) |
| 5 | 대상 조건·세그먼트 | 필터 조건식 |
| 6 | 제외 조건 | **`automation_actor: 제외/포함` 필수.** 불완전 최신일, 미완성 코호트 |
| 7 | 핵심 지표 **1개** | 정의식. **쿼리 실행 전에 선언하고 이후 바꾸지 않는다.** 바꿨으면 바꿨다고 적는다 |
| 8 | 가드레일 지표 | 핵심 지표가 좋아져도 이게 나빠지면 결론이 뒤집히는 지표. 없으면 `없음`이라고 적는다 |
| 9 | 스캔 예산 | 목표 상한과 실제 dry run bytes |

7·8·9는 서로 다른 것이다. 8은 분석적 반대 지표(actor가 늘 때 actor당 event), 9는 비용 상한이다.

### 3. 가장 싼 신뢰 가능 소스 선택

구조를 발견하는 단계가 아니라 **선택하는** 단계다. 우선순위는 고정이다.

```
marts → staging/fact → 제한된 raw reference → githubarchive.day.20*
```

`references/data_map.md`에 모델별 grain과 알려진 함정이 있다. 먼저 읽는다.

**이름이 비슷한 모델이 둘 이상이면 SQL을 열어 정의를 확인하기 전에 숫자를 인용하지 않는다.**
`metrics_retention_weekly`(최초 관측 코호트)와 `metrics_cohort_retention_weekly_heatmap`(활동주 롤링 코호트)는
이름이 닮았고 정의가 다르다. 실측 결과 같은 주에 대해 **W1이 23.8% vs 45.6%**로 갈린다.

**mart가 0행이라고 데이터가 없는 게 아니다.** mart 커버리지가 fact보다 짧을 수 있다. 건강성 검증에서 확인한다.

raw까지 내려가야 하면 그건 **fact가 못 덮는 계측 공백**이라는 뜻이다. 리포트에 그 사실을 남긴다.

### 4. 작은 범위 건강성 검증

기본 1일, 시계열·지연 도착 확인이 필요하면 7일. **본 분석 범위로 바로 가지 않는다.**

**검증 항목의 정본은 `references/health_checks.md`의 10행 표다.** 그 표를 그대로 채운다.
쿼리도 거기에 있다. SKILL.md는 목록을 중복해 두지 않는다.

각 항목에 `PASS / WARN / FAIL`을 붙인다. WARN 이상이 하나라도 있으면 결론을 약하게 쓴다.

### 5. 분석 SQL 작성

일회성·탐색 SQL은 `dbt/gharchive_metrics/analyses/`에 둔다. 파일 상단에 헤더를 남긴다.

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

CTE는 `params`, `filtered_*`, `aggregated_*`, `final`처럼 단계가 드러나게 짓는다.
기간은 `var()`로 파라미터화한다. 한 결과에 서로 다른 grain의 지표를 섞지 않는다.

compile 후 compile된 SQL을 dry run한다.

```bash
uv run --no-project --with dbt-bigquery dbt compile \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles \
  --select 분석파일명

bq query --project_id=bda-coai --use_legacy_sql=false --dry_run \
  < dbt/gharchive_metrics/target/compiled/gharchive_metrics/analyses/분석파일.sql
```

### 6. 결과 보고

`references/report_template.md` 구조를 따른다. 순서는 고정이다.

**결론 → 인풋 → 데이터 소스 → 건강성 → 핵심 결과 → 해석 → 한계 → 재현 방법**

Slack 응답이라도 `데이터 소스`(지표→모델→조건→grain)와 `재현 방법`은 뺄 수 없다. 축약은 하되 생략은 안 된다.

숫자를 쓸 때는 항상 **기간·분모·분자·grain**을 붙인다.

```text
나쁨: 리텐션은 21%입니다.

좋음: 2025-09-01 주 최초 관측 코호트 기준 W1 리텐션은 21.3%입니다.
분모는 그 주에 처음 관측된 actor 412,880명, 분자는 다음 주에 1회 이상 활동한 87,944명입니다.
소스는 metrics_retention_weekly(최초 관측 코호트)이고 자동화 actor는 제외하지 않았습니다.
좌측 절단 때문에 2025-05 코호트는 신규로 볼 수 없어 제외했습니다.
```

### 7. 승격 판단

반복 실행 / 대시보드 연결 / 공통 지표 재사용 중 하나라도 해당하면 dbt model로 승격한다.
model 설명에 grain과 기준 시간을 적고, `unique` 조합·`not_null`·필요한 reconciliation 테스트를 붙인다.
선택 빌드만 한다. 전체 backfill은 명시적 승인 없이 돌리지 않는다.

```bash
uv run --no-project --with dbt-bigquery dbt build \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles \
  --select "+모델명"
```

## 시각화

기본은 일별 라인 차트. 표는 최종 수치와 정의 확인용.

- **새로 그리기 전에 Metabase에 이미 있는 카드를 확인한다.**
- 차트 제목에 지표·grain·기간을 넣는다. `일별 active actor 수(actor-day, 2026-06-01~2026-07-25)`
- 3단계 이상 라이프사이클은 단계별 표 + 잔존 비율. 퍼널 차트라고 부르지 않는다.
- 차트를 만들 때는 `dataviz` 스킬을 먼저 연다.

## 체크리스트

작업 종료 전 확인한다.

- [ ] 모드를 하나로 분류했고 답변에 적었다
- [ ] 인풋 9종을 빈칸 없이 채웠고 각 항목에 근거를 붙였다 (`automation_actor` 포함)
- [ ] 핵심 지표를 쿼리 전에 선언했고 이후 바꾸지 않았다 (바꿨으면 명시)
- [ ] mart → fact → raw 순으로 내려갔고, raw를 썼다면 이유를 적었다
- [ ] 이름이 겹치는 모델의 정의를 SQL로 확인했다
- [ ] `health_checks.md`의 검증표를 1일 또는 7일 범위에서 전부 채웠다
- [ ] mart 커버리지가 분석 기간을 덮는지 확인했다
- [ ] dry run bytes를 보고했고 인풋 #9 예산 안에 들어왔다
- [ ] 좌측 절단·우측 절단·shard 마감을 확인했다
- [ ] WARN/FAIL이 있었다면 결론의 강도를 낮췄다
- [ ] 여러 지표·세그먼트를 훑었다면 몇 개를 봤는지 적었다
- [ ] `데이터 소스` 표(지표→모델→조건→grain)를 남겼다
- [ ] 숫자에 기간·분모·분자·grain을 붙였다
- [ ] 인과 단정을 하지 않았다
- [ ] 재현 가능한 SQL 경로와 as-of 날짜를 남겼다
