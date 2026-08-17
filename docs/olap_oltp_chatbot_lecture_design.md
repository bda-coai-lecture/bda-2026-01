# OLAP/OLTP 강의 설계 - 분석 자동화 챗봇 사례

작성일: 2026-08-09

## 1. 강의 목표

이 강의는 지금까지 만든 Slack 분석 자동화 챗봇을 정리한 뒤, 왜 챗봇은 BigQuery 같은 OLAP 계층에서 읽고, 실행 기록과 피드백은 Postgres/RDS 같은 OLTP 계층에 쓰는지 설명한다. 여기서 한 단계 더 나아가, 데이터 플랫폼이 ML/AI 서비스의 학습, feature 생성, online serving, feedback loop로 이어질 때 OLAP/OLTP 경계가 어떻게 다시 등장하는지도 다룬다.

수강생은 대학생/취준생을 기준으로 한다. 목표는 벤더 이름 암기를 넘어서, 실무/면접에서 다음 문장을 자기 말로 설명할 수 있게 만드는 것.

> 서비스가 발생시키는 한 건 한 건의 사건은 OLTP에 안정적으로 기록하고, 여러 사건을 모아 추세와 지표를 볼 때는 OLAP에서 읽는다.

## 2. 조사 요약

### 핵심 근거

| 주제 | 확인한 내용 | 강의에 쓸 메시지 |
|---|---|---|
| OLAP/OLTP 차이 | AWS는 OLAP을 집계/분석/리포팅에, OLTP를 트랜잭션 처리와 실시간 업데이트에 최적화된 시스템으로 설명한다. | "읽는 질문"과 "기록하는 사건"은 workload가 다르다. |
| BigQuery | Google 문서는 BigQuery를 분석과 BI를 위한 완전 관리형 데이터 플랫폼으로 설명하고, 대규모 분석 엔진으로 TB/PB 단위 질의가 가능하다고 설명한다. | 챗봇이 과거 GitHub Archive와 mart를 읽는 쪽은 OLAP 성격이다. |
| StarRocks | StarRocks 공식 문서는 real-time analytics와 ad-hoc query를 위한 MPP 분석 DB이며 sub-second query를 목표로 한다고 설명한다. | OLAP도 빠를 수 있음. 목적은 많은 row를 빠르게 집계/분석하는 것. |
| 운영 DB | Cloud SQL은 MySQL/PostgreSQL/SQL Server 엔진을 제공하는 완전 관리형 관계형 DB다. | 챗봇 audit처럼 앱이 즉시 쓰고 나중에 조회할 row 단위 기록은 운영 DB가 자연스럽다. |
| 인덱스 lookup | PostgreSQL 문서는 index가 특정 row를 훨씬 빠르게 찾게 해주는 장치라고 설명한다. | 사용자 요청 중 key 기반 조회는 RDB/online store가 자연스럽다. |
| 트랜잭션 | PostgreSQL 문서는 트랜잭션 격리 수준과 committed snapshot을 명시한다. | 동시에 여러 Slack 턴이 들어와도 row 단위 일관성이 필요하다. |
| 디지털 interaction | Oracle은 OLTP transaction의 범위를 구매/주문, 웹 다운로드, 영상 조회, 자동 maintenance trigger, 소셜 댓글 같은 디지털 interaction까지 확장해 설명한다. | 챗봇 질문, 답변, feedback 클릭도 서비스 이벤트. |
| OLTP -> OLAP 연결 | Google Cloud는 Datastream으로 운영 DB 변경을 BigQuery로 복제해 near real-time insight를 만들 수 있다고 설명한다. | Postgres에 쓴 기록은 나중에 다시 OLAP으로 흘러가 품질 분석 대상이 된다. |
| RDS CDC | AWS DMS는 full load 뒤 ongoing replication 또는 CDC로 source data store의 계속되는 변경을 복제한다고 설명한다. Debezium도 committed row-level change를 event stream으로 만든다. | 운영 DB에 쌓인 상태/거래 테이블은 CDC로 OLAP raw/staging에 올린다. |
| 이벤트 스트리밍 | Apache Kafka는 event를 publish/subscribe하고 저장/처리하는 event streaming platform이다. | 클릭, 노출, 검색, 예측 요청처럼 원래 event인 데이터는 앱에서 event log로 바로 흘린다. |
| SaaS/업무 데이터 | 실제 warehouse에는 제품 DB, 광고, CRM, 결제, CS, 영업, 스프레드시트, 외부 API 데이터가 함께 적재된다. | warehouse는 조직의 분석 데이터를 모으는 통합 계층. |
| 스케줄러 | Airflow 공식 문서는 scheduler가 task와 DAG를 모니터링하고 dependency가 완료된 task instance를 trigger한다고 설명한다. DAG는 schedule, task, task dependency, callback 같은 실행 계약을 담는다. | scheduler는 실행 시점, 순서, 재시도, backfill, 동시성, 실패 알림을 함께 운영. |
| BDA2 기술 맥락 | 이 저장소는 GitHub Archive/BigQuery, dbt, Airflow/Cosmos, Metabase, Slack 분석 봇, Postgres audit, MLflow, 추천 feature DAG, FAISS/FastAPI/Streamlit/Vercel을 모두 다뤘다. | 지금까지 만든 구성요소를 OLTP/OLAP/serving 관점으로 다시 배치. |

### 학생에게 강조할 뉘앙스

- OLTP와 OLAP은 DB 제품 이름보다 workload 성격으로 이해.
- BigQuery도 행/열/ACID 같은 DB 속성을 지원하고, Postgres도 작은 데이터 분석을 할 수 있다. 그래도 주된 최적화 방향은 다르다.
- 하나의 챗봇 안에도 두 성격이 같이 존재한다. 답을 만들 때는 분석 workload, 기록을 남길 때는 트랜잭션 workload다.
- OLTP에 쌓인 기록은 끝이 아니다. CDC, batch ETL, dbt 모델을 거쳐 OLAP mart가 되면 챗봇 품질과 비용을 다시 분석할 수 있다.
- ML 서비스에서는 이 차이가 응답 시간 SLA로 더 선명해진다. warehouse/OLAP은 학습 데이터, batch feature, 리포트에 좋고, online serving path는 인덱스 lookup이나 cache로 수십 ms 수준을 노린다.
- warehouse에는 서비스 이벤트만 모이지 않는다. 마케팅 캠페인, 광고비, CRM lead, 결제/환불, CS 티켓, 영업 pipeline, 외부 시장 데이터가 합쳐져야 "왜 지표가 움직였는지"를 볼 수 있다.
- scheduler는 batch SQL을 매일 돌리는 버튼이 아니다. 데이터가 준비됐는지, 어떤 순서로 돌릴지, 실패하면 어디서 멈추고 어떻게 다시 돌릴지 관리한다.
- BDA2에서 이미 쓴 도구를 역할로 재분류. BigQuery/dbt/Metabase는 분석 소비 쪽, Airflow/Cosmos는 운영 반복 쪽, FastAPI/FAISS/RDB/cache는 serving 쪽, MLflow는 실험 추적 쪽.

## 3. 기존 HTML 스타일 적용 방향

기준 파일은 `docs/analyst_bot_feedback_loop_lecture.html`의 탭형 문서 스타일을 따른다.

| 요소 | 적용 방향 |
|---|---|
| 레이아웃 | `main.deck` 안에 sticky topbar, tablist, tabpanel section을 배치 |
| 폭/밀도 | `width: min(1180px, calc(100% - 28px))`, 문서형 lecture note 밀도 |
| 상단 | 브랜드명, 날짜 stamp, 제목, lede, 가로 tab button |
| 시각화 | SVG 대신 `snapshot`, `flow`, `lane`, `loop`, `cards`, `table-wrap` 컴포넌트 사용 |
| 색상 | 참조 파일의 `--session/#2563eb`, `--data/#0f766e`, `--guard/#b45309`, `--eval/#7c3aed` 계열 유지 |
| 카드 | 10px radius, tab section 안에서만 반복 카드 사용 |
| 문체 | 지난 주 HTML처럼 짧은 수업 메모체. `~함`, `~필요`, `~관리` 중심 |

챗봇 기존 자료(`docs/analyst_bot_overview.html`, `docs/analyst_bot_feedback_loop_lecture.html`)와 이어지도록 브랜드는 `BDA 2기 데이터 핸들링 · 분석 자동화`로 잡는다.

## 4. 전체 내러티브

1. 지금까지 만든 챗봇을 한 문장으로 wrap up한다.
2. BDA2에서 이미 쓴 기술을 데이터 플랫폼 지도 위에 다시 배치한다.
3. 같은 챗봇 안에서 "읽기"와 "쓰기"가 서로 다른 저장소로 간다는 사실을 문제로 던진다.
4. OLTP는 서비스가 방금 만든 row를 안전하게 기록하는 계층이라고 잡는다.
5. OLAP은 많은 row를 모아 질문에 답하는 계층이라고 잡는다.
6. 챗봇 사례로 돌아와 BigQuery read와 Postgres audit write를 workload 관점에서 분해한다.
7. OLTP에 쌓인 데이터가 OLAP으로 올라가는 세 가지 경로를 설명한다. RDS CDC, 이벤트 로그 파이프라인, batch extract다.
8. 제품 데이터 외에도 마케팅, 광고, CRM, 결제, CS, 영업, 외부 API 데이터가 warehouse에 모이는 이유를 설명한다.
9. OLAP에 올라온 raw/staging이 dbt/Airflow batch를 거쳐 fact, mart, feature table이 되는 과정을 보여준다.
10. scheduler가 batch를 운영 가능한 workflow로 만드는 방식을 설명한다. schedule, dependency, retry, backfill, alert, cost guard를 포함한다.
11. 같은 원리를 ML 서비스로 일반화한다. offline training/analysis는 OLAP, online prediction request는 OLTP/online store/cache 성격이다.
12. 마지막에는 "어떤 데이터를 어디에 둘지" 판단하는 기준표와 면접용 답변 문장으로 닫는다.

## 5. HTML 구성안

총 7개 탭. 긴 슬라이드 덱 대신 읽는 흐름이 있는 lecture note로 구성한다.

| # | 탭 | 핵심 메시지 | 주요 컴포넌트 |
|---:|---|---|---|
| 00 | 큰 그림 | 서비스 이벤트는 row로 남고, 분석 질문은 warehouse에서 풀림. | snapshot, flow, BDA2 기술 카드 |
| 01 | OLTP / OLAP | OLTP는 현재 상태와 작은 거래, OLAP은 누적 기록과 집계. | split, 비교 table, grain table |
| 02 | 챗봇 설계 | BigQuery/dbt mart read path와 Postgres audit write path를 나눠 보기. | flow, lane, audit schema, 증상 table |
| 03 | Warehouse | CDC, event log, batch extract, SaaS/API 데이터가 warehouse에서 만남. | cards, source table, raw-to-mart loop, SQL 예시 |
| 04 | Scheduler | batch는 시간표, 의존성, 재시도, backfill까지 운영. | DAG flow, ops cards, BDA2 DAG table, Docker lane |
| 05 | ML Serving | offline feature가 online 응답 시간과 만나는 구간. | snapshot, ML loop, offline/online split, recsys table |
| 06 | 정리 | 요구사항을 보고 저장소와 실행 위치를 분류하는 연습. | classification table, 30초 답변, 판단 질문 카드 |

## 6. 주요 슬라이드 카피 초안

### Slide 2 - Wrap-up

제목: 지금까지 만든 봇을 한 문장으로 말하면

- Slack 스레드의 질문을 하나의 분석 세션으로 이어간다.
- 분석 세션은 BigQuery/dbt mart를 읽고, 비용 상한과 lineage를 확인한다.
- 사용자는 짧은 답을 받고, 내부 실행 기록은 audit/trace/feedback으로 남는다.

노트: 이 장에서는 구현 디테일을 길게 복습하지 않는다. 다음 주제인 OLAP/OLTP로 넘어가기 위한 맥락만 만든다.

### Slide 3 - BDA2 Map

제목: 우리가 이미 쓴 기술을 다시 배치하면

- Source/warehouse: GitHub Archive, BigQuery, local parquet, SQLite metadata cache.
- Transform/ops: Python scripts, dbt Core/dbt-bigquery, Airflow 3, Cosmos, Docker Compose.
- Consume/automation: Metabase dashboard, Slack 분석 봇, Postgres/RDS audit, JSONL fallback.
- ML/serving: recsys mart, MLflow, ALS/LGBM/Two-Tower, FAISS, FastAPI, Streamlit, Vercel.

노트: 이미 만든 것들을 OLAP/OLTP, scheduler, serving의 역할로 재배치하는 시간.

### Slide 4 - Question

제목: 왜 읽는 곳과 쓰는 곳이 다를까?

- 질문에 답하려면 과거의 많은 이벤트를 읽어야 한다.
- 하지만 봇이 방금 처리한 요청은 지금 생긴 서비스 이벤트다.
- 그래서 read path와 write path의 저장소가 달라진다.

노트: 학생들이 "BigQuery에도 insert할 수 있지 않나?"라고 물을 수 있음. 여기서는 workload 적합성을 기준으로 봄.

### Slide 6 - OLTP

제목: OLTP는 서비스의 현재 상태를 지킨다

- 한 사용자가 질문을 보냈다.
- 한 턴이 시작됐다.
- 한 답변이 성공/실패/취소됐다.
- 한 사용자가 feedback 버튼을 눌렀다.

노트: 이 네 문장은 모두 row 단위 사건이다. 빠르게 쓰고, 중복을 막고, 실패 시 어디까지 저장됐는지 알아야 한다.

### Slide 7 - OLAP

제목: OLAP은 많은 기록을 모아 질문에 답한다

- 지난 4주간 active actor는 줄었나?
- PushEvent와 non-PushEvent 추세가 갈라졌나?
- 어떤 repo segment에서 감소가 컸나?
- 자동화 actor를 제외해도 패턴이 남나?

노트: 이 질문들은 많은 row를 조건별로 모아 읽는 문제.

### Slide 11 - Bot Write

제목: 챗봇 기록은 왜 Postgres에 쓰나

- `turns`: 질문 하나와 답변 하나의 상태를 저장한다.
- `turn_usage`: 같은 턴의 비용, job, permission denial을 저장한다.
- `trace_events`: 도구 실행 흐름을 순서대로 저장한다.
- `feedback`: 답변 평가 버튼 클릭을 저장한다.

노트: `audit_id`가 여러 테이블을 묶는 키. 한 턴의 성공/실패, 비용, 실행 흐름은 서비스 운영 기록.

### Slide 14 - Loop

제목: OLTP 기록은 다시 OLAP 분석 대상이 된다

- 운영 중에는 Postgres가 원천 기록을 맡는다.
- 일정 주기나 CDC로 BigQuery에 복제한다.
- dbt로 `bot_quality_daily`, `bot_cost_daily`, `bot_feedback_summary` 같은 mart를 만든다.
- 그 mart를 다시 챗봇이나 Metabase가 읽는다.

노트: 이 장이 강의의 연결부. OLTP와 OLAP을 파이프라인의 앞뒤로 연결해서 보여줌.

### Slide 15 - Ingestion

제목: OLTP에서 OLAP으로 올라가는 세 길

- RDS CDC: `orders`, `users`, `turns`처럼 DB에 commit된 row 변화를 transaction log에서 읽어 복제한다.
- Event log: impression, click, search, prediction처럼 서비스가 의도적으로 발생시킨 사건을 append-only stream으로 보낸다.
- Batch extract: freshness가 덜 중요하면 일정 주기로 DB, SaaS API, file을 읽어 lake나 warehouse에 적재한다.
- 셋 다 OLAP으로 가지만 원천의 의미와 중복/삭제/순서 처리 방식이 다르다.

노트: CDC는 "DB 상태 변화의 복제"이고, event log는 "제품 행동 기록"이다. 둘을 같은 로그라고 뭉개면 delete/update 처리, idempotency, event time 기준에서 문제가 생긴다.

### Slide 16 - Warehouse

제목: 웨어하우스에는 서비스 데이터만 쌓이지 않는다

- Product: 가입, 로그인, 클릭, 노출, 검색, 주문, 추천 요청.
- Marketing: 캠페인, 광고비, UTM, attribution, push/email 발송 결과.
- Business ops: CRM lead, 영업 pipeline, 결제, 환불, 쿠폰, CS 티켓.
- External: GitHub Archive, 환율, 공휴일, 시장 데이터, 파트너 API.

노트: warehouse를 "큰 로그 DB"로 설명하면 부족하다. 실무에서 warehouse의 가치는 서로 다른 부서의 데이터를 같은 기준 시간, 같은 key, 같은 지표 정의로 묶는 데 있다.

### Slide 17 - Join

제목: 조직 질문은 여러 원천을 join해야 답이 된다

- "DAU가 줄었다"는 제품 로그로 관측한다.
- "왜 줄었나"는 캠페인 중단, 광고비 변화, 장애, 가격 변경, CS 이슈, 시즌성을 같이 봐야 한다.
- "좋은 유저가 들어왔나"는 acquisition source와 retention/revenue를 join해야 한다.
- 그래서 warehouse에는 source별 raw와 domain별 mart가 함께 필요함.

노트: SQL 실행 실력에 더해, 서로 다른 업무 시스템의 데이터를 연결해 질문에 답하는 감각을 줌.

### Slide 18 - Batch

제목: OLAP에 올라온 뒤에는 batch가 의미를 만든다

- Bronze/raw: 원천에 가깝게 저장한다. CDC row, event payload, load metadata가 남는다.
- Staging: 타입, timezone, key, 중복, 삭제 표시를 정리한다.
- Fact: 한 행의 의미를 정한다. 예: `activity_date x user_id x repo_id x action`.
- Mart/feature: 팀이 반복해서 읽는 지표, dashboard, ML feature, label로 굳힌다.

노트: OLAP에 올렸다고 바로 분석 가능한 데이터가 되는 게 아니다. batch/dbt/Airflow가 grain과 지표 정의를 안정화해야 서비스와 분석이 같은 숫자를 쓴다.

### Slide 19 - Scheduler

제목: batch는 누가 언제 돌리나

- cron은 "몇 시에 실행"만 정하기 쉽다.
- scheduler는 DAG의 task 순서와 dependency를 같이 본다.
- 예: raw 적재 -> staging 정리 -> dbt test -> mart 갱신 -> dashboard cache refresh -> Slack 알림.
- Airflow는 metadata DB의 실행 이력을 보고 아직 돌릴 것, 다시 돌릴 것, 멈춘 것을 판단한다.

노트: scheduler를 단순 자동 실행 버튼으로 설명하지 않는다. "반복 가능한 데이터 제품을 운영 상태로 만드는 control plane"으로 잡는다.

### Slide 20 - Operations

제목: 운영 scheduler가 챙기는 것들

- Retry: 일시적 API 실패, BigQuery job 실패, network 오류를 자동 재시도한다.
- Backfill: 과거 날짜의 빠진 partition이나 잘못된 지표를 다시 계산한다.
- Freshness/SLA: 데이터가 몇 시까지 준비되어야 하는지 감시한다.
- Guardrail: 동시성, pool, 비용 상한, 실패 알림으로 downstream 피해를 줄인다.

노트: "매일 새벽 7시에 돈다"와 함께 "어제 데이터가 아직 안 들어왔을 때", "지난주 로직 버그를 재계산할 때"를 같이 다룸.

### Slide 21 - Docker

제목: 운영 포장: Docker/headless 봇

- 노트북에서 한 번 도는 코드와, 계속 켜져 있는 서비스는 다르다.
- Docker Compose는 Slack bot, Airflow, Metabase, API를 같은 실행 계약으로 묶는다.
- volume은 state/audit/log를 보존하고, env는 credential과 endpoint를 주입한다.
- headless 실행에서는 dry-run, self-test, canary, log tail이 운영 체크리스트가 된다.

노트: Docker는 OLAP/OLTP 자체는 아니지만, 데이터 플랫폼을 반복 가능한 서비스로 올리는 운영 포장이다.

### Slide 24 - Transition

제목: 여기까지가 기본 데이터 플랫폼

- OLTP는 서비스 사건과 상태를 기록한다.
- OLAP은 여러 원천을 모아 지표와 feature를 만든다.
- scheduler는 이 과정을 매일 같은 순서로 운영한다.
- 이제 질문은 "이 결과를 사용자 요청 시간 안에 어떻게 서빙할 것인가"다.

노트: 이 장에서 호흡을 끊고, 뒤 절반은 ML/AI 서비스 serving 문제로 넘어간다.

### Slide 25 - Service

제목: 서비스로 이어지면 응답 시간이 기준이 된다

- OLAP 엔진은 많은 row를 읽고 집계하는 처리량에 강하다.
- StarRocks는 sub-second analytics를 목표로 하고, BigQuery는 대용량 데이터를 초/분 단위로 처리하는 쪽에 강하다.
- 사용자 요청마다 50ms 안에 feature를 가져와야 하면 OLAP 직접 질의는 응답 path에 부담이 큼.
- 이때는 RDB index lookup, key-value store, cache, online feature store가 serving path에 들어온다.

노트: OLAP의 빠름과 서비스 API의 빠름은 기준이 다름. OLAP의 1초는 분석가에게 빠르지만, 클릭 후 추천 목록을 띄우는 API에는 길 수 있음.

### Slide 26 - ML Service

제목: ML 서비스에는 offline과 online이 같이 있다

- Offline: 로그를 모으고, label을 만들고, feature를 집계하고, 모델을 학습한다.
- Nearline: 최근 행동을 몇 분 단위로 반영해 feature나 후보를 갱신한다.
- Online: 요청이 들어오면 사용자/아이템 key로 feature를 읽고 즉시 예측한다.
- Feedback: 노출, 클릭, 전환, 오류를 다시 기록해 다음 학습 데이터가 되게 한다.

노트: 데이터 플랫폼이 서비스로 이어진다는 말은 warehouse에서 만든 데이터가 API latency와 만난다는 뜻이다.

### Slide 27 - MLflow

제목: 실험 장부와 운영 후보 선택

- BDA2 추천 실험은 MLflow에 split, feature cache, model config, metric, artifact를 남겼다.
- MLflow는 실시간 모니터링보다 "어떤 모델을 왜 선택했는가"를 남기는 장부에 가깝다.
- serving에 올릴 모델은 offline NDCG/Recall, diversity, 재현 가능한 feature snapshot으로 고른다.
- online CTR/drift/latency는 별도 event log와 monitoring으로 다시 관찰한다.

노트: MLflow를 모델 서빙 DB처럼 설명하지 않는다. 실험 추적과 운영 후보 선택의 근거 저장소로 둔다.

### Slide 28 - Feature

제목: Feature store를 OLAP/OLTP로 보면

- Offline feature store는 학습과 backtest를 위해 긴 기간의 feature를 보관한다.
- Online feature store는 같은 feature를 사용자 요청 시 낮은 지연으로 꺼내준다.
- 두 저장소가 달라도 feature 정의와 freshness 계약은 같아야 한다.
- 정의가 갈라지면 training-serving skew가 생긴다.

노트: 학생들이 feature store를 제품명으로 외우지 않게, "같은 feature를 다른 latency로 제공하는 경계"로 설명한다.

### Slide 29 - Recommendation

제목: 추천 서비스 예시

- BigQuery/StarRocks에서 최근 활동, repo profile, user profile, 후보군을 만든다.
- batch job이 사용자별 top-K 후보를 RDB, Redis, search index, online feature store로 publish한다.
- API 서버는 요청마다 warehouse를 scan하지 않고 key로 후보와 feature를 lookup한다.
- 클릭/노출/전환 로그는 다시 OLTP/event log에 기록되고 OLAP으로 적재된다.

노트: 모델 성능과 함께, 모델 결과를 어떤 저장소에 배포해야 서비스가 되는지까지 보여주는 예시.

### Slide 30 - Unified Loop

제목: 전체 루프 한 장으로 보기

- OLTP/event log: 서비스 사건을 안전하게 기록한다.
- OLAP warehouse: 여러 원천을 모아 mart와 feature를 만든다.
- Publish: batch 산출물을 online store, RDB, Redis, FAISS index, API bundle로 배포한다.
- Service: 사용자 요청을 낮은 지연으로 처리하고, 결과를 다시 event로 남긴다.

노트: 이 장이 리뷰에서 요청한 최종 통합 루프다. BigQuery에서 바로 API가 읽는 구조와, serving store로 publish하는 구조의 차이를 분명히 보여준다.

### Slide 31 - Exercise

제목: 짧은 분류 실습

- Slack 질문 한 건 저장: OLTP.
- GitHub Archive 90일 추세 계산: OLAP.
- 매일 07:00 KST dbt mart 갱신: scheduler.
- 추천 API에서 user_id로 top-K 후보 조회: online serving.

노트: 학생들이 저장소 이름보다 workload를 먼저 말하게 만든다.

### Slide 32 - Interview

제목: 면접에서 이렇게 말하면 된다

> 서비스에서 생기는 질문, 답변, 피드백은 row 단위 운영 기록으로 남김. 여러 기록을 모아 추세와 품질을 볼 때는 warehouse와 mart를 읽음.

ML 서비스까지 확장하면 다음 문장을 붙인다.

> ML 서비스에서는 학습과 feature 집계는 offline에서 만들고, API 요청 path에는 RDB, cache, online feature store를 둠. 데이터 플랫폼은 분석 결과를 만들고, 그 결과를 serving 가능한 형태로 publish하는 역할까지 맡음.

## 7. 강의 중 예시 요구사항 분류

| 요구사항 | 성격 | 이유 |
|---|---|---|
| 사용자가 Slack에서 질문을 보냈다는 사실 저장 | OLTP | 방금 발생한 row 단위 이벤트 |
| 같은 스레드에서 이전 session_id 찾기 | OLTP | 짧은 key lookup, 현재 운영 상태 |
| 최근 30일 BigQuery 비용 추이 계산 | OLAP | 많은 사용량 row를 기간별로 집계 |
| feedback 버튼 클릭 저장 | OLTP | 중복 방지와 즉시 기록 필요 |
| feedback 유형별 부정확 답변 비율 계산 | OLAP | 누적 feedback 집계와 세그먼트 분석 |
| GitHub Archive에서 event type별 DAU 계산 | OLAP | 대량 historical event scan |
| 봇 실행 중 취소 상태 업데이트 | OLTP | 특정 turn row 상태 변경 |
| 사용자의 추천 후보 top-K 조회 | OLTP/online serving | 요청 시 user_id key로 낮은 지연 lookup |
| 모델 학습용 90일 feature 생성 | OLAP | 긴 기간 로그를 집계해 training dataset 생성 |
| 추천 노출/클릭 로그 저장 | OLTP/event log | 서비스 요청 중 발생한 row 단위 이벤트 |
| feature drift 리포트 | OLAP | 누적 feature 분포를 기간별 비교 |
| RDS `users` 테이블 변경분을 BigQuery에 복제 | OLTP -> OLAP CDC | 운영 DB의 row 변화가 분석 raw/staging으로 이동 |
| 앱 서버가 `ProductViewed` 이벤트를 Kafka/Pub/Sub에 발행 | Event log -> OLAP | 서비스 행동 이벤트를 append-only stream으로 적재 |
| 매일 새벽 `metrics_daily` 갱신 | OLAP batch | raw/fact를 반복 가능한 지표 mart로 집계 |
| Google Ads 비용을 매일 warehouse에 적재 | SaaS/API -> OLAP | 제품 로그와 join해 CAC, ROAS, campaign 성과 분석 |
| Salesforce/HubSpot lead 정보를 warehouse에 적재 | SaaS/API -> OLAP | acquisition, sales pipeline, revenue funnel 분석 |
| CS 티켓과 churn을 연결 | 업무 데이터 + 제품 로그 -> OLAP | 이탈 원인과 고객 경험 문제를 분석 |
| 매일 07:00 KST에 dbt mart 갱신 | Scheduler | 시간표와 dependency를 고정해 반복 실행 |
| 누락된 7일치 partition 재계산 | Scheduler/backfill | 과거 기간을 같은 로직으로 다시 실행 |
| dbt test 실패 시 Slack 알림 | Scheduler/operations | 실패를 숨기지 않고 downstream 소비 전에 멈춤 |

## 8. HTML 구현 메모

후속 구현 파일명 제안: `docs/olap_oltp_chatbot_lecture.html`

구현은 `docs/analyst_bot_feedback_loop_lecture.html`의 구조를 따른다.

사용 컴포넌트:

- `snapshot`: read path, write path, loop, serving처럼 한눈에 보는 요약 카드.
- `flow`: Slack -> warehouse -> audit -> improvement처럼 한 턴의 이동 경로.
- `lane`: read lane, write lane, feedback lane처럼 경계를 설명하는 행.
- `loop`: raw -> staging -> fact -> mart -> feature처럼 반복되는 데이터 흐름.
- `cards`: BDA2 기술, ingestion route, scheduler operation, 판단 질문.
- `table-wrap`: 비교표, audit schema, warehouse source, DAG, 요구사항 분류.
- `note`: 수업 메모, 설계 포인트, latency 감각.

주의:

- 한 탭에 긴 설명을 몰아넣지 않고 `snapshot`, `flow`, `table`, `note`로 끊어 읽게 만듦.
- 학생용 용어는 먼저 한국어로 설명하고, 괄호로 영어 키워드를 붙인다.
- OLTP는 짧은 트랜잭션의 낮은 지연, OLAP은 큰 데이터 집계 처리량에 맞춰져 있다고 설명.
- BigQuery와 Postgres는 "우리 사례에서의 주된 역할"로 설명.
- latency 숫자는 환경별로 달라짐. 그래도 분석 질의의 1초 안팎과 online API의 수십 ms는 다른 SLA라는 점을 강조.
- CDC와 event log를 구분한다. CDC는 DB가 이미 commit한 row 변화를 읽는 통합 방식이고, event log는 앱이 도메인 사건을 직접 발행하는 방식이다. 동일한 현상을 둘 다 기록할 수 있으므로 idempotency key, event time, source of truth를 명시해야 한다.
- warehouse source를 나열할 때 도구 이름보다 업무 질문을 먼저 둠. 예: Google Ads 비용과 가입/retention/revenue 연결.
- scheduler는 orchestration/control plane으로 설명. 실제 계산은 BigQuery, dbt, Python job, Spark, API client, worker/pod가 수행하고 scheduler는 실행 시점과 순서, 상태를 관리.

## 9. 참고 자료

- AWS, "What's the Difference Between OLAP and OLTP?" https://aws.amazon.com/compare/the-difference-between-olap-and-oltp/
- Google Cloud, "BigQuery overview" https://docs.cloud.google.com/bigquery/docs/introduction
- Google Cloud, "Cloud SQL for MySQL, PostgreSQL, and SQL Server" https://cloud.google.com/sql
- Google Cloud, "Datastream for BigQuery" https://cloud.google.com/datastream-for-bigquery
- StarRocks, "What is StarRocks?" https://docs.starrocks.io/docs/introduction/what_is_starrocks/
- AWS DMS, "Creating tasks for ongoing replication using AWS DMS" https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Task.CDC.html
- Debezium, "Stream changes from your database" https://debezium.io/
- Apache Kafka, "Introduction" https://kafka.apache.org/documentation/
- Apache Airflow, "Scheduler" https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html
- Apache Airflow, "Dags" https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
- PostgreSQL Documentation, "Transaction Isolation" https://www.postgresql.org/docs/current/transaction-iso.html
- PostgreSQL Documentation, "Indexes" https://www.postgresql.org/docs/current/indexes.html
- Oracle, "What Is OLTP?" https://www.oracle.com/database/what-is-oltp/
