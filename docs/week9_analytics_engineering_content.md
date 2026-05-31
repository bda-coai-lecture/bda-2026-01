# Week 9 Analytics Engineering HTML 강의안 콘텐츠 초안

대상: 대학생/수강생  
목표: 회사에서 왜 analytics engineering이 필요한지 이해하고, 그 필요가 dbt, Airflow, BI, Slack 운영 알림 같은 기술 선택으로 어떻게 이어지는지 설명함.  
사용 맥락: HTML 강의안으로 옮기기 전의 슬라이드별 콘텐츠 초안임.

---

## 전체 흐름

이번 강의는 "분석을 잘함"에서 끝나지 않고, 회사가 매일 믿고 쓰는 데이터 제품으로 만드는 과정을 다룸.

핵심 메시지.

- 회사의 데이터 문제는 대부분 "SQL을 못 짜서"가 아니라 "같은 숫자를 모두가 다르게 계산해서" 생김.
- Analytics engineer는 분석 질문을 반복 가능한 fact, mart, metric, dashboard, 운영 파이프라인으로 고정하는 역할임.
- dbt는 지표 정의와 테스트를 코드로 고정하고, Airflow는 실행 순서와 스케줄을 운영화함.
- Slack 알림은 장식이 아니라 데이터 제품의 운영 인터페이스. 실패를 빨리 발견하고, 책임자를 연결하고, 재실행 여부를 판단하게 만듦.

---

## 슬라이드별 초안

### 1. Week 9: Analytics Engineering

핵심 문장: 오늘은 노트북 분석을 회사에서 매일 쓰는 데이터 제품으로 바꾸는 일 학습.

발표 메모: 학생들이 이미 DAU, retention, 추천 feature를 계산해봤다는 전제에서 시작함. 이번 주의 초점은 "계산할 줄 안다"가 아니라 "팀이 계속 믿고 쓰게 만듦"임.

### 2. 오늘의 질문

핵심 문장: 회사에서는 왜 분석 결과를 그냥 노트북이나 CSV로 공유하면 부족할까?

발표 메모: 노트북은 탐색에는 좋지만 운영에는 약함. 누가 언제 돌렸는지, 어떤 데이터 기준인지, 같은 결과가 다시 나오는지, 실패했을 때 누가 아는지 불명확하다는 점을 먼저 던짐.

### 3. 수업의 큰 그림

핵심 문장: 분석 질문은 raw data, model, test, schedule, dashboard, alert를 거쳐 데이터 제품이 됨.

발표 메모: 흐름을 한 줄로 보여줌. GitHub Archive 원천 로그에서 시작해 dbt mart, Airflow 실행, Metabase 대시보드, Slack 알림까지 연결함.

### 4. 수강생에게 중요한 이유

핵심 문장: 회사는 "분석 한 번 잘하는 사람"보다 "반복 가능한 분석 시스템을 만드는 사람"을 더 필요로 함.

발표 메모: 프로젝트 리뷰에서 SQL 문제만 보는 것처럼 느껴져도 실제 업무는 지표 정의, 재현성, 운영, 커뮤니케이션이 같이 붙음. 이 강의는 프로젝트 정리에서 차별화되는 부분을 만듦.

### 5. 회사의 흔한 장면

핵심 문장: 같은 DAU를 물었는데 팀마다 숫자가 다르면, 그 순간 데이터 신뢰가 무너짐.

발표 메모: 마케팅팀은 앱 접속 기준, 제품팀은 핵심 행동 기준, 데이터팀은 로그 이벤트 기준으로 active user를 계산할 수 있음. 숫자가 다른 것이 문제가 아니라 정의가 고정되어 있지 않은 것이 문제.

### 6. 숫자가 흔들리는 이유

핵심 문장: 데이터 문제는 대개 원천, 정의, 코드, 실행 시점, 필터 조건 중 하나가 달라져서 생김.

발표 메모: "SQL이 틀렸다"보다 더 넓게 봄. raw table이 바뀌었는지, 봇을 포함했는지, timezone이 다른지, incremental load가 빠졌는지 확인해야 함.

### 7. 노트북의 강점

핵심 문장: 노트북은 모르는 문제를 빠르게 탐색하고 가설을 검증하기 좋음.

발표 메모: 노트북을 깎아내리지 않음. EDA, 시각화, 샘플링, 이상치 확인, 빠른 실험에는 노트북이 가장 편함.

### 8. 노트북의 한계

핵심 문장: 노트북은 사람이 직접 실행해야 하고, 결과가 운영 계약으로 고정되기 어려움.

발표 메모: 순서대로 실행하지 않아도 돌아가는 문제, 숨은 상태, 로컬 파일 의존성, output만 남고 입력 조건이 사라지는 문제를 설명함.

### 9. 분석 결과가 제품이 되는 순간

핵심 문장: 누군가 매일 같은 링크에서 같은 정의의 지표를 보고 의사결정하면, 그 분석은 데이터 제품이 됨.

발표 메모: 제품이라는 표현을 어렵게 설명하지 않음. dashboard, metric table, feature mart, alert도 회사 안에서는 모두 데이터 제품.

### 10. Analytics Engineer 한 줄 정의

핵심 문장: Analytics engineer는 분석 질문을 반복 가능한 데이터 모델과 지표 시스템으로 바꾸는 사람임.

발표 메모: 데이터 분석가와 데이터 엔지니어 사이에 있는 역할로 설명. SQL을 많이 쓰지만 단순 SQL 작성자가 아님.

### 11. 역할의 위치

핵심 문장: AE는 raw data와 business decision 사이에서 신뢰 가능한 metric layer를 만듦.

발표 메모: DE는 수집과 인프라, DA는 질문과 해석, AE는 분석 가능한 fact/mart와 지표 계약을 담당함고 구분함.

### 12. 이 수업의 예시 데이터

핵심 문장: GitHub Archive 이벤트 로그를 회사 서비스 로그라고 생각하고 수업을 진행함.

발표 메모: PushEvent, WatchEvent, PullRequestEvent 같은 이벤트를 서비스 행동 로그로 비유함. 사용자는 actor, 상품 또는 콘텐츠는 repo로 볼 수 있음.

### 13. Raw Event의 문제

핵심 문장: raw event는 너무 자세해서 바로 대시보드나 모델 입력으로 쓰기 어려움.

발표 메모: 이벤트 단위 로그는 크고 중복이 많고 구조가 복잡함. 매번 raw를 직접 집계하면 비용도 크고 정의도 흔들림.

### 14. Grain이 먼저다

핵심 문장: 좋은 데이터 모델링은 "한 행이 무엇을 의미하는가"를 먼저 정하는 일임.

발표 메모: `activity_date, user_id, repo_id, action` 같은 grain을 예시로 듦. 이 grain이 정해져야 DAU, repo activity, 추천 interaction이 같은 기반에서 출발함.

### 15. Fact Table

핵심 문장: fact table은 회사가 반복해서 분석할 핵심 행동을 안정적인 단위로 저장함.

발표 메모: `fact_user_repo_activity`를 중심 예시로 사용함. raw event 전체가 아니라 분석에 필요한 핵심 column만 정리함.

### 16. Dimension과 Mart

핵심 문장: dimension은 설명 정보를 붙이고, mart는 특정 소비 목적에 맞게 집계한 테이블임.

발표 메모: repo 정보, user segment, event type 같은 dimension을 설명함. mart는 dashboard용, retention용, 추천 feature용으로 나뉠 수 있음.

### 17. DAU가 만들어지는 과정

핵심 문장: DAU는 raw event에서 바로 세는 숫자가 아니라, 정의된 fact에서 계산되는 metric이어야 함.

발표 메모: active user 기준, 날짜 기준, event 포함 범위를 정해야 함. 이 정의가 코드와 문서에 남아야 함.

### 18. WAU와 Retention

핵심 문장: 주간 지표와 retention은 단순 count보다 기간 정의와 cohort 정의가 더 중요함.

발표 메모: week start, first seen cohort, returning user 정의가 다르면 결과가 크게 달라짐. 그래서 dbt model과 test 필요.

### 19. 지표 정의는 회의록이 아니다

핵심 문장: 중요한 지표 정의는 문서에만 있으면 부족하고, 실제 실행되는 코드에 들어가야 함.

발표 메모: 문서는 쉽게 낡음. dbt model, schema.yml, semantic layer, test가 같이 있어야 함.

### 20. dbt가 등장하는 이유

핵심 문장: dbt는 SQL 분석 코드를 의존성, 테스트, 문서, 빌드 단위로 관리하게 해줌.

발표 메모: dbt를 "SQL 프로젝트를 소프트웨어처럼 관리하는 도구"로 소개함. Jinja 문법보다 왜 필요한지가 먼저.

### 21. dbt Source

핵심 문장: source는 원천 데이터가 어디에서 오고 어떤 테이블을 믿을지 선언하는 출발점임.

발표 메모: raw table 이름을 SQL마다 흩뿌리지 않고 source로 선언함. freshness check도 연결할 수 있음.

### 22. dbt Staging

핵심 문장: staging model은 원천 데이터를 분석하기 쉬운 이름과 타입으로 정리하는 얇은 층임.

발표 메모: column rename, type cast, timestamp/date 변환, 필요한 field 선택을 함. staging에서 복잡한 비즈니스 로직을 많이 넣지 않는다고 설명함.

### 23. dbt Fact

핵심 문장: fact model은 분석의 기준 grain을 고정하는 핵심 테이블임.

발표 메모: event log를 user-repo-date-action 단위로 정리하는 예시를 듦. downstream mart가 fact를 공유하면 숫자가 덜 흔들림.

### 24. dbt Mart

핵심 문장: mart는 특정 사용자가 바로 조회하기 좋은 형태로 만든 결과 테이블임.

발표 메모: Metabase용 `metrics_daily`, retention heatmap용 mart, 추천 feature용 mart를 예시로 듦.

### 25. SQL 파일이 많아지는 이유

핵심 문장: SQL을 여러 단계로 나누는 이유는 복잡하게 만들기 위해서가 아니라 책임을 분리하기 위해서.

발표 메모: source, staging, fact, mart가 섞이면 디버깅이 어려움. 어느 단계에서 값이 달라졌는지 추적할 수 있어야 함.

### 26. Lineage

핵심 문장: lineage는 어떤 지표가 어떤 원천과 중간 모델을 거쳐 만들어졌는지 보여주는 지도.

발표 메모: 대시보드 숫자가 이상할 때 lineage가 있으면 원인을 찾기 쉬움. 수강생 프로젝트 정리에서도 lineage를 보여주면 프로젝트 완성도가 올라감.

### 27. Test가 필요한 이유

핵심 문장: 데이터 테스트는 코드가 실행되는지보다 결과가 믿을 만한지 확인함.

발표 메모: null, unique, accepted values 같은 기본 테스트부터 설명함. 그 다음 retention accounting 같은 custom test로 넘어감.

### 28. 기본 품질 테스트

핵심 문장: null이면 안 되는 key, 중복되면 안 되는 grain, 허용된 값만 들어가야 하는 category를 먼저 테스트함.

발표 메모: user_id null, 날짜 null, action 값 이상, 중복 grain 같은 사례를 말함. 기본 테스트가 없으면 더 복잡한 분석도 신뢰하기 어려움.

### 29. 지표 정합성 테스트

핵심 문장: 좋은 테스트는 회사가 믿는 지표 관계가 깨질 때 실패해야 함.

발표 메모: weekly lifecycle에서 active user가 new, retained, resurrected 등과 논리적으로 맞는지 확인하는 사례를 듦.

### 30. Retention 테스트 예시

핵심 문장: retention mart는 cohort별 active user 합계와 전체 WAU의 관계가 맞는지 검증할 수 있음.

발표 메모: 수식 자체를 깊게 파기보다 "숫자 간 관계를 테스트함"는 감각을 전달함. 지표는 단독 숫자가 아니라 accounting 구조.

### 31. Semantic Layer

핵심 문장: semantic layer는 active_users 같은 지표 이름과 계산 방식을 한곳에 고정함.

발표 메모: BI 도구마다 SQL을 새로 짜면 같은 지표가 다르게 계산됨. metric 이름이 곧 계약이 되도록 만듦.

### 32. BI 대시보드의 역할

핵심 문장: 대시보드는 SQL을 모르는 팀원도 같은 지표를 보고 의사결정하게 만드는 인터페이스.

발표 메모: Metabase를 예시로 듦. 중요한 것은 예쁜 차트보다 "정의된 mart를 보게 하는 것"임.

### 33. BI가 Raw Table을 직접 보면 생기는 문제

핵심 문장: BI가 raw event를 직접 집계하면 비용, 속도, 정의 불일치 문제가 동시에 생김.

발표 메모: 대시보드 하나 열 때마다 큰 테이블을 스캔하는 상황을 설명함. raw table 접근 권한도 최소화하는 편이 좋음.

### 34. Dashboard용 Mart

핵심 문장: dashboard는 raw data가 아니라 작고 검증된 metric mart를 조회해야 함.

발표 메모: `metrics_daily`, `metrics_weekly`, `metrics_retention_summary` 같은 테이블이 바로 이 목적임.

### 35. 비용도 설계 대상임

핵심 문장: 회사에서는 맞는 숫자뿐 아니라 계속 돌릴 수 있는 비용 구조도 중요함.

발표 메모: BigQuery 스캔량, partition, cluster, incremental build, rolling window를 설명함. 무료 크레딧이 아니라 회사 비용으로 생각하게 함.

### 36. Partition과 Cluster

핵심 문장: partition과 cluster는 필요한 데이터만 읽게 해서 쿼리 비용과 시간을 줄임.

발표 메모: 날짜 partition, user_id/repo_id cluster 예시를 듦. 기술 세부보다 왜 필요한지에 집중함.

### 37. Incremental Model

핵심 문장: incremental model은 매번 전체를 다시 만들지 않고 새로 필요한 범위만 갱신함.

발표 메모: 매일 90일치만 갱신하는 운영을 예시로 듦. backfill은 별도 옵션으로 막아두는 것이 안전함.

### 38. Backfill은 위험한 작업임

핵심 문장: backfill은 과거 데이터를 다시 계산하는 강력한 작업이라 명시적인 승인과 범위 제한이 필요함.

발표 메모: 비용 폭증, 대시보드 숫자 변경, downstream 모델 영향이 생길 수 있음. 운영에서는 guardrail이 필요함.

### 39. Airflow가 등장하는 이유

핵심 문장: Airflow는 사람이 기억해서 실행하던 순서를 스케줄과 의존성으로 바꿈.

발표 메모: "매일 아침 이 명령어 세 개 실행"은 운영 아님. DAG는 실행 순서, retry, timeout, 실패 상태를 관리함.

### 40. DAG란 무엇인가

핵심 문장: DAG는 데이터 작업들의 실행 순서와 의존성을 표현한 그래프.

발표 메모: extract 후 dbt build, 그 다음 test, 그 다음 알림 같은 흐름을 예로 듦.

### 41. Task를 작게 나누는 이유

핵심 문장: task가 작아야 어느 단계에서 실패했는지 빠르게 알 수 있음.

발표 메모: 하나의 큰 shell script보다 plan, sync, dbt build, test, notify로 나누면 재실행과 원인 파악이 쉬움.

### 42. Plan Task

핵심 문장: plan task는 실제 처리 전에 범위, 비용, 설정을 확인하는 안전장치.

발표 메모: 처리 날짜, dry run 결과, backfill 여부, 필수 환경변수 확인을 plan에서 함. 실패를 늦게 발견하지 않게 함.

### 43. Sync Task

핵심 문장: sync task는 원천 데이터를 분석용 fact나 staging 영역으로 옮기는 단계.

발표 메모: BigQuery에서 필요한 기간의 event aggregate를 가져오거나 fact를 갱신하는 단계로 설명함.

### 44. dbt Build Task

핵심 문장: dbt build task는 model 생성과 test 실행을 같은 운영 흐름 안에 넣음.

발표 메모: build 성공은 SQL 실행과 설정된 테스트 통과를 함께 의미함. 단순 run보다 운영 신뢰도 높음.

### 45. Dashboard Refresh

핵심 문장: mart가 갱신된 뒤 dashboard가 그 결과를 보게 만드는 순서까지 생각해야 함.

발표 메모: 캐시, refresh 주기, 사용자가 보는 시간대를 고려함. 지표가 새벽에 갱신됨면 알림도 그 이후에 가야 함.

### 46. 운영에서 제일 중요한 질문

핵심 문장: 실패했을 때 누가, 언제, 무엇을 보고, 어떻게 판단할 것인가?

발표 메모: 여기서 Slack 알림 파트로 넘어감. 운영은 실패가 안 나는 시스템이 아니라 실패를 빨리 알고 적절히 대응하는 시스템임.

### 47. Slack 알림이 필요한 이유

핵심 문장: Slack 알림은 데이터 파이프라인의 상태를 팀의 업무 흐름 안으로 가져오는 운영 인터페이스.

발표 메모: Airflow UI를 매번 열어보는 사람은 거의 없음. 팀이 이미 보는 채널에 성공, 실패, 경고, 재시도 상태가 와야 함.

### 48. 나쁜 알림

핵심 문장: "DAG failed"만 보내는 알림은 대부분의 상황에서 부족함.

발표 메모: 실패 사실만 있고, 날짜/task/영향 지표/다음 행동이 없으면 대응 지연.

### 49. 좋은 알림

핵심 문장: 좋은 알림은 상태, 범위, 영향, 원인 힌트, 바로 갈 링크, 다음 행동을 함께 담음.

발표 메모: 알림은 짧아야 하지만 판단에 필요한 정보는 있어야 함. "어디를 봐야 하는가"와 "지금 급한가"가 드러나야 함.

### 50. Slack 알림 기본 필드

핵심 문장: 최소한 DAG, task, run id, execution date, 처리 범위, 상태, 로그 링크는 포함해야 함.

발표 메모: 학생들에게 알림 메시지를 설계하게 할 때 이 필드를 체크리스트로 쓰게 함.

### 51. 성공 알림의 목적

핵심 문장: 성공 알림은 축하 메시지가 아니라 오늘 지표가 갱신됐다는 운영 기록임.

발표 메모: 매번 성공 알림을 너무 자세히 보내면 소음이 됨. 핵심 mart 갱신 시간, 처리 row 수, dashboard 링크 정도가 적당함.

### 52. 실패 알림의 목적

핵심 문장: 실패 알림은 원인 파악을 시작할 수 있을 만큼 구체적이어야 함.

발표 메모: 실패한 task, 에러 요약, 최근 성공 run, retry 예정 여부, 영향받는 dashboard 포함. 로그 전체 첨부는 지양.

### 53. 경고 알림의 목적

핵심 문장: 경고 알림은 실패는 아니지만 지표 신뢰를 의심해야 하는 상황을 알려줌.

발표 메모: row count 급감, DAU 급변, freshness 지연, 비용 예상 초과, 테스트 warning 등을 예시로 듦.

### 54. 알림 레벨 설계

핵심 문장: 모든 이벤트를 같은 채널에 보내면 결국 아무도 알림을 보지 않음.

발표 메모: info, warning, critical을 나누고 채널도 다르게 설계함. 예를 들어 `#data-pipeline`, `#data-alerts`, 담당자 DM을 구분함.

### 55. Slack 메시지 예시: 성공

핵심 문장: 성공 메시지는 짧게, 갱신 범위와 확인 링크 중심으로 보냄.

발표 메모: 예시 문구를 보여줌.

```text
[SUCCESS] gharchive_dbt_metrics
Date range: 2026-05-24 ~ 2026-05-30
Updated: metrics_daily, metrics_weekly, retention marts
Rows processed: 1.2M
Dashboard: <Metabase link>
Run: <Airflow link>
```

### 56. Slack 메시지 예시: 실패

핵심 문장: 실패 메시지는 "무엇이 깨졌고 어디서 확인할지"를 바로 보여줘야 함.

발표 메모: 예시 문구를 보여줌.

```text
[FAILED] gharchive_dbt_metrics / dbt_build
Run date: 2026-05-31
Window: 2026-05-24 ~ 2026-05-30
Error: metrics_retention_weekly test failed
Impact: retention dashboard may show stale data
Retry: scheduled in 10 minutes, attempt 2/3
Log: <Airflow task log>
Owner: @data-oncall
```

### 57. Slack 메시지 예시: 품질 경고

핵심 문장: 품질 경고는 "성공했지만 믿기 전에 확인해야 하는 숫자"를 알려줌.

발표 메모: 예시 문구를 보여줌.

```text
[WARNING] metrics_daily anomaly
Metric: active_users
Current: 82,100
7-day avg: 125,400
Change: -34.5%
Possible cause: source freshness delay or event ingestion issue
Action: check source freshness and row count by event type
```

### 58. Slack 알림에 넣지 말아야 할 것

핵심 문장: 토큰, 계정키, 전체 로그, 민감한 사용자 데이터는 알림에 넣으면 안 됨.

발표 메모: Slack은 편하지만 보안 경계가 약해질 수 있음. secret은 env나 secret manager에 두고, 알림에는 링크와 요약만 보냄.

### 59. 알림과 책임자

핵심 문장: 알림에는 기술 정보뿐 아니라 누가 판단할지에 대한 책임 경로가 있어야 함.

발표 메모: owner, on-call, escalation rule을 설명함. 학생 프로젝트에서도 "담당자"를 적는 습관이 중요함.

### 60. 알림과 재시도

핵심 문장: 재시도가 예정된 실패와 사람이 바로 봐야 하는 실패는 다르게 알려야 함.

발표 메모: 일시적인 네트워크 오류는 retry 후 성공할 수 있음. 하지만 schema 변경, 테스트 실패, 비용 초과는 사람이 봐야 함.

### 61. 알림 피로 줄이기

핵심 문장: 알림이 많아질수록 중요한 실패를 놓치기 쉬움.

발표 메모: 성공 알림은 묶어서 보내고, 반복 실패는 thread로 묶고, 같은 원인 중복 알림은 suppress함. 운영 품질은 알림 수를 늘리는 것이 아니라 신호대잡음비를 높이는 것임.

### 62. Slack 알림 구현 위치

핵심 문장: Airflow callback, 별도 notify task, 공통 notifier 함수 중 하나로 알림을 구현할 수 있음.

발표 메모: 실패 callback은 모든 task 실패에 일관되게 붙이기 좋고, notify task는 성공 요약처럼 pipeline 끝에서 보내기 좋음.

### 63. Airflow Callback 패턴

핵심 문장: on_failure_callback은 task context를 받아 Slack 메시지에 DAG와 task 정보를 자동으로 넣을 수 있음.

발표 메모: context 안에 dag_id, task_id, run_id, logical_date, log_url 같은 값이 있음는 정도로 설명함. 코드는 길게 들어가지 않아도 됨.

### 64. Slack Webhook 패턴

핵심 문장: Slack Incoming Webhook은 간단하지만 URL 자체가 secret이므로 코드에 직접 쓰면 안 됨.

발표 메모: 환경변수나 Airflow Connection으로 관리함고 설명함. repo에 webhook URL을 커밋하면 안 됨.

### 65. 알림 메시지 템플릿

핵심 문장: 알림 템플릿을 표준화하면 DAG가 늘어나도 운영 경험이 일정해짐.

발표 메모: 모든 pipeline이 각자 다른 문장으로 보내면 해석 비용이 커짐. prefix, field 이름, severity 표기를 통일함.

### 66. 지표 이상 탐지와 Slack

핵심 문장: 파이프라인 성공과 지표 정상은 다른 문제라서 둘 다 알림 대상이 될 수 있음.

발표 메모: SQL이 성공해도 원천 이벤트가 절반만 들어오면 대시보드는 틀린 숫자를 보여줌. row count, freshness, metric delta check가 필요함.

### 67. 운영 Runbook

핵심 문장: 좋은 알림은 runbook 링크와 연결되어야 실제 대응으로 이어짐.

발표 메모: runbook에는 확인 순서, 재실행 명령, 담당자, 과거 유사 장애, rollback 기준이 들어감. 수업에서는 간단한 Markdown 문서로 충분함.

### 68. AE와 ML Feature Mart

핵심 문장: 추천 모델의 성능도 feature mart와 split 정의가 흔들리면 믿을 수 없음.

발표 메모: Week 6 추천 실험을 연결함. user-repo interaction, repo feature, experiment split이 재현 가능해야 모델 비교가 의미 있음.

### 69. Point-in-time 기준

핵심 문장: 모델 학습 데이터는 예측 시점 이후의 정보를 몰래 쓰지 않도록 시점 기준을 가져야 함.

발표 메모: leakage를 설명함. 미래의 star나 activity를 feature로 쓰면 offline 성능은 좋아 보이지만 실제 서비스에서는 망가짐.

### 70. Experiment Split Mart

핵심 문장: train, validation, test split은 파일 이름이 아니라 mart와 메타데이터로 재현되어야 함.

발표 메모: split이 매번 달라지면 모델 성능 비교가 불가능함. `experiment_id`, `snapshot_date`, `split` 같은 column을 예시로 듦.

### 71. AE가 ML Engineer는 아니다

핵심 문장: AE는 모델을 직접 만들지 않더라도 모델이 믿을 수 있는 데이터 위에서 학습되게 만듦.

발표 메모: 역할 경계를 명확히 함. AE는 feature 정의, 데이터 품질, split 재현성, dashboard 지표와의 연결에 강점이 있음.

### 72. 실무 프로젝트 정리 관점

핵심 문장: 프로젝트 정리에서 강한 프로젝트는 분석 결과보다 운영 구조를 보여줌.

발표 메모: "분석함"보다 "fact/mart 구성, dbt test 추가, Airflow 스케줄링, Slack 알림 설계"가 더 회사 업무에 가까움.

### 73. 프로젝트 리뷰에서 설명할 수 있어야 하는 것

핵심 문장: 어떤 지표를 왜 그 grain으로 만들었고, 틀렸을 때 어떻게 알 수 있는지 설명해야 함.

발표 메모: SQL 문법보다 설계 이유를 말하게 함. 왜 raw를 직접 보지 않는지, 왜 test가 필요한지, 왜 alert가 필요한지 답할 수 있어야 함.

### 74. 실습 방향

핵심 문장: 이번 실습은 GitHub Archive 지표를 dbt model, Airflow DAG, Slack 알림 설계로 연결하는 것임.

발표 메모: 실제 구현 범위는 수업 시간에 맞춰 조정. 최소 목표는 model/test/알림 메시지 설계까지.

### 75. 오늘의 정리

핵심 문장: Analytics engineering은 숫자를 계산하는 일이 아니라, 숫자를 조직이 믿고 반복해서 쓰게 만드는 일임.

발표 메모: dbt, Airflow, Metabase, Slack은 각각 따로 배우는 도구가 아님. 회사에서 데이터 제품을 운영하기 위해 연결되는 기술 스택이라는 메시지로 마무리.

---

## 강의 중 강조할 연결 문장

- "회사에서 중요한 건 숫자가 한 번 맞는 게 아니라, 내일도 같은 정의로 다시 나오는 것임."
- "dbt는 SQL을 잘게 쪼개는 도구가 아니라 지표 계약을 코드로 관리하는 도구."
- "Airflow는 크론 대체제가 아니라 실패, 재시도, 의존성, 책임 경로를 드러내는 운영 도구."
- "Slack 알림은 보기 좋은 부가 기능이 아니라, 데이터 제품이 팀과 대화하는 방식임."
- "대시보드가 틀리면 차트 문제가 아니라 데이터 모델, 테스트, 스케줄, 알림까지 같이 봐야 함."

## HTML 강의안 구성 제안

1. 오프닝: 회사에서 숫자가 흔들리는 장면
2. 개념: Analytics engineer 역할과 데이터 제품
3. 모델링: raw event에서 fact/mart/metric으로
4. dbt: source, staging, fact, mart, test, semantic layer
5. 운영: Airflow DAG, incremental, backfill guardrail
6. Slack 알림: 성공/실패/경고/보안/책임자/runbook
7. 확장: 추천 feature mart와 ML 실험 재현성
8. 실무 관점: 프로젝트 정리와 프로젝트 리뷰에서 설명할 포인트

## 실습 또는 토론 질문

1. DAU를 우리 팀에서 공식 지표로 만듦면 active user를 어떤 event 기준으로 정의할 것인가?
2. retention dashboard가 어제보다 30% 낮게 나왔다면 어떤 순서로 확인할 것인가?
3. Slack 실패 알림에 어떤 정보를 넣어야 담당자가 바로 판단할 수 있을까?
4. 성공 알림은 매번 보내야 할까, 아니면 요약으로 보내야 할까?
5. 추천 모델 성능이 좋아졌을 때, 데이터 split이나 feature leakage 문제를 어떻게 확인할까?

