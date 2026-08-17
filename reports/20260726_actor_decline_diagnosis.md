# 일별 active actor 급감은 적재 누락인가 — raw 대조 진단

**모드**: 데이터 품질 진단
**as-of**: 2026-07-25 (fact / mart 최신 완결일)
**작성**: 2026-07-26

## 결론

- **가설 반증(REFUTED).** "우리 적재가 row를 잘못 합쳐 actor를 잃고 있다"는 가설은 성립하지 않는다. `githubarchive.day.20*` 원천과 `mart.fact_user_repo_activity` / `mart.metrics_event_type_daily`를 **5개 날짜(2026-06-16, 06-24, 07-21, 07-24, 07-25) × action 전수**로 대조한 결과 raw `count(*)` vs fact `sum(event_count)`, raw `count(distinct actor.id)` vs fact `count(distinct user_id)`, raw `count(distinct repo.id)` vs fact `count(distinct repo_id)`, raw `count(distinct (actor.id, repo.id))` vs fact row 수가 **74개 action-행 전부 한 건의 오차 없이 일치**했다. `scripts/sync_bq_metrics.py`와 dbt `fact_user_repo_activity.sql` 어디에도 dedup/collapse 버그는 없고, event type을 제한하는 코드도 없다.
- **두 개의 signature가 지적된 대로 존재하지만, 둘 다 원천에 이미 있다.**
  - **(A) 비-Push 14종은 event와 row가 같이 줄었다** — 실제 볼륨 소실. 2026-06-16 → 07-21 (Tue↔Tue) PullRequestEvent event 30,498건 → 4,239건(-86%), row 21,786 → 3,991(-82%).
  - **(B) PushEvent는 event가 늘고 row는 줄었다** — event 3,265,107건 → 3,731,737건(+14.3%), row 914,601 → 540,694(-40.9%), **row당 event 3.570 → 6.902(+93%)**. 이것이 collapse signature로 지목된 그 지표다. 그런데 **이 두 숫자를 raw에서 직접 계산해도 3.570과 6.902가 나온다.** 3,731,737 / 540,694 = 6.902. 즉 **"적은 (actor, repo) 조합에 event가 몰려 있는" 상태는 GitHub Archive shard 안에 이미 그렇게 들어 있다.** 우리가 만든 것이 아니다.
- **`total_events`가 평평한 것은 상쇄에 의한 산술적 우연이다.** 완결 주간 평균 기준 2026-04-13주 → 2026-07-13주에 PushEvent 2,753,934건 → 3,614,222건(+31.2%), 비-Push 940,252건 → 332,215건(-64.7%), 합계 3,694,186건 → 3,946,437건(**+6.8%**). **`total_events`는 이 사안의 건강성 지표가 아니었고, 15주 동안 두 움직임을 완전히 가렸다.**
- **개시 시점: 2026-07-06(월) 주.** 비-Push 3종은 그 날 **급격한 step**을 밟는다(월↔월 PullRequestEvent 21,152건 → 8,902건 -58%, WatchEvent 4,709건 → 1,979건 -58%, IssuesEvent 6,296건 → 3,187건 -49%). PushEvent의 row당 event는 같은 주부터 **완만한 ramp**로 올라간다(07-05 4.68 → 07-11 8.25). 다만 비-Push 볼륨은 **2026-04월 중순부터 이미 단조 감소 중**이었고(주간 평균 1,110,632건 → 535,545건, -52%) 07-06은 그 위에 얹힌 두 번째 사건이다.
- **인과는 말할 수 없다.** 우리가 닿는 최상류가 `githubarchive.day.20*`이고 변화가 이미 그 안에 있다. 따라서 (a) GitHub 활동의 실제 구성 변화인지 (b) **GH Archive 수집 단계의 상류 누락**인지 이 데이터로는 구분 불가다. 소유자의 "데이터 누락" 직감은 **우리 파이프라인에 대해서는 반증됐고, 상류에 대해서는 여전히 열려 있다.** step 형태(07-06 하루 만에 -58%)는 실제 사용자 행동보다 상류 설정 변경 쪽에 더 어울리는 모양이지만, **그것을 증명할 데이터를 우리는 갖고 있지 않다.**

**다음 액션** — [권고](#권고) 4항. 최우선은 상류 재확인(GH Archive 시간별 원본과 `day.20260706` 대조)이고, 그 다음이 `dim_push_automation_actor` 재적재다.

## 인풋

| 항목 | 값 | 상태 | 근거 |
|---|---|---|---|
| 분석 대상 | "최근 일별 active actor 감소가 data 누락/적재 문제인가, 실제 변화인가. 특정 event type이 사라진 것인가" | 확인됨 | 의뢰 문장 + 코디네이터 후속 지시 |
| 분석 단위(grain) | 입력 `activity_date × user_id × repo_id × action` (`fact_user_repo_activity`) / 대조군 raw event 1건 / 출력 `activity_date × action` | 확인됨 | `references/data_map.md` 2절, `metrics_event_type_daily` grain = `activity_date × action` |
| 기간 (검증 / 본분석) | 검증 2026-07-19~07-25 (7일) + raw 대조 5일(2026-06-16, 06-24, 07-21, 07-24, 07-25) / 본분석 2026-04-06~2026-07-25 | 확인됨 | mart 커버리지 실측 2025-09-01~2026-07-25 (328일, 결측일 0), fact 2025-05-01~2026-07-25 (451일) |
| 기준 시간 | `activity_date`, UTC (raw측은 `_TABLE_SUFFIX` shard = UTC 일자) | 확인됨 | 컬럼명. `created_at` 시각은 fact에 없음 |
| 대상 조건·세그먼트 | 전체 event, action 전수. raw측은 로더와 동일하게 `actor.id is not null and repo.id is not null and type is not null` | 확인됨 | `sync_bq_metrics.py:298-300` 및 `fact_user_repo_activity.sql` WHERE 절을 그대로 복제 |
| 제외 조건 | **`automation_actor`: 포함** (본문 기준). 제외본은 병렬 산출해 규모만 보고. 불완전 최신일 없음(2026-07-25 shard 24시간 완결 확인). 미완성 코호트 해당 없음 | 확인됨 | raw `count(distinct extract(hour from created_at)) = 24`, min 00:00:01 / max 23:59:59 |
| 핵심 지표 1개 | **fact↔raw 정합률**: 같은 UTC 일자·action에 대해 `fact.sum(event_count) / raw.count(*)` 와 `fact.count(distinct user_id) / raw.count(distinct actor.id)` | 확인됨 | 쿼리 실행 전 선언, 이후 변경 없음 |
| 가드레일 지표 | **fact row 수 vs raw `count(distinct (actor.id, repo.id))`** (action별). event 합은 맞는데 이 값이 어긋나면 그것이 곧 collapse 증거 | 확인됨 | 의뢰문의 decisive test를 지표로 고정 |
| 스캔 예산 | 목표 raw 누적 5 GB 이하 / 실제 raw dry run·실행 **568,330,770 B = 0.568 GB (예산의 11.4%)** | 확인됨 | [dry run 내역](#dry-run--실제-스캔-bytes) |

## 데이터 소스

| 지표 | 모델/테이블 | 조건 | grain | 비고 |
|---|---|---|---|---|
| 대조군 raw event/actor/repo/(actor,repo) | `githubarchive.day.20*` | `_table_suffix` 등호로 `260616`/`260624`/`260721`/`260724`/`260725` **각각 단일일 5회 분리 실행**, null key 제외 | event 1건 | **정본.** `payload` 미조회 |
| 검증 대상 fact | `bda-coai.mart.fact_user_repo_activity` | `activity_date in ('2026-06-24','2026-07-24','2026-07-25')` 및 `between '2026-07-19' and '2026-07-25'` | `activity_date × user_id × repo_id × action` | 항상 partition 조건. 최대 7일 |
| action별 actor/event/row | `bda-coai.mart.metrics_event_type_daily` | `activity_date between '2026-06-22' and '2026-07-25'` (개시 시점 추적), 평일 5일 vs 5일 | `activity_date × action` | `user_repo_action_rows` = fact row 수. **collapse 판정의 결정적 컬럼** |
| 일별 actor/repo/event/row | `bda-coai.mart.metrics_daily` | `activity_date between '2026-04-06' and '2026-07-25'` | `activity_date` | push/non-push 분해에 사용 |
| 자동화 actor 판별 | `bda-coai.mart.dim_push_automation_actor` | `user_id` left join | `user_id` | 20,992행 (explicit_bot 20,706 / machine_rate_suspect 305) |
| 적재 코드 | `dbt/gharchive_metrics/models/facts/fact_user_repo_activity.sql`, `scripts/sync_bq_metrics.py`, `dags/gharchive_dbt_metrics.py` | — | — | 코드 리뷰 |

**mart → fact → raw로 내려간 이유** — 질문이 "mart 숫자를 믿어도 되나"이므로 mart만으로는 원리적으로 답할 수 없다. raw까지 내려간 것은 계측 공백이 아니라 **독립 대조군 확보** 목적이다.

**이름이 겹치는 모델** — 리텐션 계열은 쓰지 않았다. `metrics_event_type_daily`의 `active_users`를 event type끼리 **더하지 않았다**(한 actor가 여러 type을 내므로 중복 계산). 3절 표의 actor 열은 일별 값의 합인 **actor-day**로 명시했다.

## 데이터 건강성

| 검증 | 결과 | 근거 |
|---|---|---|
| shard 마감 | **PASS** | raw `githubarchive.day.20260725`: distinct hour 24개, min `2026-07-25 00:00:01`, max `2026-07-25 23:59:59`. 최신일이 부분 shard가 아니다 |
| mart 커버리지 | **PASS** | `metrics_daily` 2025-09-01~2026-07-25 = 328행이고 그 구간 일수와 정확히 같아 결측일 0. `metrics_event_type_daily`도 328일. 본분석 구간이 전부 담겨 있다 |
| 적재 지연 | **PASS (표본 2일)** | DAG는 최근 3일만 replace-days한다. 창 밖 날짜인 **2026-06-16(40일 전)** 과 **2026-06-24(32일 전)** 가 raw와 action 전수 일치 → 미반영 지연 도착 없음. 창 밖 날짜를 2개만 검증했다 |
| 좌측 절단 | **N/A** | 본분석 구간이 보유 범위 중간. 최초 관측 코호트 미사용 |
| 우측 절단 | **N/A** | 코호트·생존·이탈 분석 아님. 28일 이탈창 미사용 |
| 자동화 혼입 | **PASS (명시적 처리)** | 포함본을 본문 기준으로, 제외본 병렬 산출. PushEvent actor 중 flagged 0.38%(06-24) / 0.44%(07-24), event 비중 17.70% → 8.68%. **dim 자체의 한계는 [한계](#한계) 참조** |
| grain 중복 | **PASS** | 2026-07-19~07-25: `count(*)` 4,910,280 = distinct `activity_date\|user_id\|repo_id\|action` 4,910,280 |
| 단계 보존 | **N/A** | 다단계 라이프사이클 분석 아님 |
| 합계 보존 | **PASS** | 5개 날짜 모두 raw `count(*)` = fact `sum(event_count)`, action 단위까지. 일 합계 2026-06-24 3,844,621 / 07-24 3,983,982 / 07-25 4,063,884, 세 값 모두 `metrics_daily.total_events`와 동일 |
| 분모 정의 | **PASS** | raw측 제외 row는 `actor.id`/`repo.id`/`type` null뿐이고 로더 WHERE 절과 동일. 그 외 필터 없음 |
| raw 정합 | **PASS** | [핵심 결과 1절](#1-결정적-대조--fact--raw-action별-정합). 5일 × 74 action-행 × 4지표 전부 정합률 **1.000000** |
| null / 범위 | **PASS** | 2026-07-19~07-25: `null_user` 0, `null_repo` 0, `null_action` 0, `event_count < 1` 0건 |
| **희소 type의 결측일** | **PASS (버그 아님)** | 아래 1절 마지막 문단. raw에서도 그 날 0건이다 |
| 스캔 예산 | **PASS** | raw 누적 0.568 GB / 목표 5 GB. $6.25/TiB 기준 약 $0.003 |

**WARN/FAIL 없음.** 정합성 판정은 정상 강도로 서술한다. 단 상류(GH Archive 내부) 검증은 이 데이터로 불가능하므로 그 부분 결론은 의도적으로 약하게 썼다.

## 핵심 결과

### 1. 결정적 대조 — fact ↔ raw, action별 정합

**요일을 맞춘 두 날짜(Tue↔Tue)의 action별 대조.** raw측과 mart/fact측 값이 **모든 칸에서 동일**하여 한 열로 표기했다. 대조 대상은 raw `count(*)` ↔ `total_events`, raw `count(distinct actor.id)` ↔ `active_users`, raw `count(distinct (actor.id, repo.id))` ↔ `user_repo_action_rows`(= fact row 수)다.

| action | 2026-06-16 (Tue, 정상) | | | 2026-07-21 (Tue, 감소) | | | row당 event |
|---|---|---|---|---|---|---|---|
| | event | actor | row | event | actor | row | 전 → 후 |
| **PushEvent** | 3,265,107 | 607,115 | 914,601 | 3,731,737 | 353,833 | 540,694 | **3.570 → 6.902** |
| CreateEvent | 346,341 | 167,426 | 237,775 | 150,667 | 79,543 | 114,981 | 1.457 → 1.311 |
| DeleteEvent | 145,735 | 40,073 | 66,314 | 66,332 | 24,346 | 40,259 | 2.198 → 1.648 |
| PullRequestEvent | 30,498 | 11,856 | 21,786 | 4,239 | 2,138 | 3,991 | 1.400 → 1.062 |
| IssueCommentEvent | 12,836 | 3,987 | 9,354 | 1,731 | 760 | 1,623 | 1.372 → 1.067 |
| IssuesEvent | 9,432 | 4,255 | 5,101 | 1,118 | 851 | 937 | 1.849 → 1.193 |
| WatchEvent | 7,232 | 6,723 | 7,220 | 858 | 855 | 858 | 1.002 → 1.000 |
| PullRequestReviewEvent | 5,931 | 2,799 | 4,546 | 897 | 537 | 836 | 1.305 → 1.073 |
| PullRequestReviewCommentEvent | 4,359 | 1,539 | 3,097 | 770 | 358 | 688 | 1.407 → 1.119 |
| ReleaseEvent | 2,107 | 870 | 1,895 | 242 | 136 | 241 | 1.112 → 1.004 |
| ForkEvent | 1,526 | 1,386 | 1,455 | 215 | 203 | 206 | 1.049 → 1.044 |
| MemberEvent | 495 | 473 | 489 | 55 | 53 | 55 | 1.012 → 1.000 |
| CommitCommentEvent | 238 | 46 | 200 | 32 | 6 | 26 | 1.190 → 1.231 |
| PublicEvent | 141 | 141 | 141 | 21 | 21 | 21 | 1.000 → 1.000 |
| GollumEvent | 88 | 73 | 76 | 8 | 7 | 7 | 1.158 → 1.143 |
| DiscussionEvent | 51 | 43 | 48 | 4 | 3 | 4 | 1.063 → 1.000 |

추가로 **2026-06-24 (Wed, 10 action)**, **2026-07-24 (Fri, 16 action)** 도 action 단위 전수 일치, **2026-07-25 (Sat)** 는 일 집계로 일치(event 4,063,884 / actor 334,320 / repo 518,697 / row 595,416 / action 종수 16, 네 값 모두 raw = fact = mart). **총 5일, 74 action-행, 정합률 1.000000.**

**(B) signature의 판정** — collapse가 있었다면 `fact row 수 < raw count(distinct (actor,repo))` 여야 한다. PushEvent 2026-07-21에서 fact row 540,694 = raw distinct (actor,repo) 540,694이다. **어긋남이 0이다.** 그리고 row당 event 6.902는 raw에서 raw 값만으로 계산한 값이다(3,731,737 / 540,694). **"event가 더 적은 (actor, repo) 조합에 몰려 있다"는 상태 자체가 원천의 사실이다.** 우리 GROUP BY가 만든 것이 아니다.

**(A) signature의 판정** — 비-Push 14종은 event와 row와 actor가 **같은 방향으로 같이** 줄었다(PullRequestEvent event -86% / row -82% / actor -82%). row당 event는 1.400 → 1.062로 오히려 **1에 가까워졌다**. 이는 collapse의 반대 형태이며 "이벤트가 실제로 없다"에 해당한다. 그리고 그 부재가 raw에 있다.

**희소 type의 "결측일"은 버그가 아니다.** CommitCommentEvent·GollumEvent·MemberEvent·PullRequestReviewCommentEvent·ReleaseEvent가 328일 중 321일만, DiscussionEvent가 284일만 나타나는 것은 **그 날 raw에 해당 type이 0건이어서 `GROUP BY`가 행을 만들지 않은 것**이다. 실증: **2026-06-24 raw는 action이 10종만 나온다** — ReleaseEvent, MemberEvent, GollumEvent, CommitCommentEvent, DiscussionEvent, PullRequestReviewCommentEvent가 그날 raw에 없다. 같은 날 mart에도 없다. 일치한다. 반면 2026-06-16과 07-24는 raw·mart 모두 16종이다. **type이 사라진 것이 아니고, mart가 type을 빠뜨린 것도 아니다.**

### 2. 개시 시점 — 일별 walk (`metrics_event_type_daily`, 2026-06-22~2026-07-25, UTC)

| activity_date | dow | push actor | push event | push row | **push ev/row** | PR actor | **PR event** | **Watch event** | **Issues event** |
|---|---|---|---|---|---|---|---|---|---|
| 2026-06-22 | Mon | 566,238 | 3,085,948 | 894,153 | 3.45 | 8,853 | 28,464 | 5,295 | 5,879 |
| 2026-06-23 | Tue | 581,152 | 3,218,622 | 899,422 | 3.58 | 5,922 | 11,889 | 3,160 | 5,243 |
| 2026-06-24 | Wed | 583,709 | 3,286,649 | 916,228 | 3.59 | 10,685 | 24,928 | 6,192 | 6,065 |
| 2026-06-25 | Thu | 575,053 | 3,296,353 | 890,181 | 3.70 | 10,757 | 24,361 | 7,400 | 10,547 |
| 2026-06-26 | Fri | 541,272 | 3,261,516 | 866,557 | 3.76 | 11,095 | 26,027 | 7,177 | 10,410 |
| 2026-06-27 | Sat | 456,217 | 3,368,673 | 750,218 | 4.49 | 17,095 | 46,354 | 14,040 | 18,769 |
| 2026-06-28 | Sun | 448,739 | 3,459,588 | 765,283 | 4.52 | 10,998 | 26,844 | 10,834 | 10,069 |
| 2026-06-29 | Mon | 558,698 | 3,289,935 | 890,520 | 3.69 | 8,033 | 21,152 | 4,709 | 6,296 |
| 2026-06-30 | Tue | 574,836 | 3,287,115 | 895,155 | 3.67 | 11,873 | 28,337 | 7,915 | 8,630 |
| 2026-07-01 | Wed | 567,481 | 3,240,726 | 885,293 | 3.66 | 13,624 | 37,327 | 9,105 | 9,979 |
| 2026-07-02 | Thu | 562,290 | 3,300,991 | 880,374 | 3.75 | 16,311 | 38,991 | 10,882 | 13,729 |
| 2026-07-03 | Fri | 527,603 | 3,379,911 | 842,256 | 4.01 | 8,979 | 20,407 | 4,684 | 7,558 |
| 2026-07-04 | Sat | 453,837 | 3,499,078 | 751,721 | 4.65 | 13,158 | 34,491 | 8,505 | 15,126 |
| 2026-07-05 | Sun | 441,339 | 3,397,019 | 726,612 | 4.68 | 8,883 | 20,734 | 6,236 | 10,645 |
| **2026-07-06** | **Mon** | **510,448** | 3,504,429 | 815,748 | **4.30** | **4,120** | **8,902** | **1,979** | **3,187** |
| 2026-07-07 | Tue | 514,855 | 3,459,692 | 833,330 | 4.15 | 4,633 | 8,652 | 2,244 | 3,285 |
| 2026-07-08 | Wed | 475,193 | 3,477,698 | 821,031 | 4.24 | 3,846 | 7,151 | 1,809 | 2,458 |
| 2026-07-09 | Thu | 422,809 | 3,645,087 | 696,720 | 5.23 | 2,817 | 4,904 | 1,272 | 1,900 |
| 2026-07-10 | Fri | 376,254 | 3,693,636 | 566,048 | 6.53 | 2,780 | 5,235 | 1,090 | 1,910 |
| 2026-07-11 | Sat | 305,038 | 3,909,526 | 474,167 | **8.25** | 2,175 | 3,703 | 863 | 1,499 |
| 2026-07-12 | Sun | 292,057 | 3,855,839 | 460,034 | 8.38 | 2,096 | 3,513 | 1,510 | 1,450 |
| 2026-07-13 | Mon | 409,769 | 3,691,285 | 608,792 | 6.06 | 2,873 | 6,092 | 1,182 | 1,865 |
| 2026-07-14 | Tue | 538,298 | 3,473,683 | 829,436 | 4.19 | 5,002 | 9,629 | 1,962 | 3,392 |
| 2026-07-15 | Wed | 502,827 | 3,541,257 | 784,779 | 4.51 | 5,327 | 10,946 | 2,486 | 3,942 |
| 2026-07-16 | Thu | 423,173 | 3,396,497 | 657,111 | 5.17 | 4,542 | 9,047 | 2,039 | 3,243 |
| 2026-07-17 | Fri | 431,845 | 3,618,643 | 678,745 | 5.33 | 4,955 | 10,201 | 2,125 | 3,374 |
| 2026-07-18 | Sat | 362,887 | 3,764,868 | 582,673 | 6.46 | 3,871 | 6,855 | 1,480 | 2,607 |
| 2026-07-19 | Sun | 332,220 | 3,813,318 | 536,373 | 7.11 | 4,193 | 7,900 | 2,084 | 3,037 |
| 2026-07-20 | Mon | 359,716 | 3,716,255 | 551,586 | 6.74 | 2,220 | 4,867 | 928 | 1,297 |
| 2026-07-21 | Tue | 353,833 | 3,731,737 | 540,694 | 6.90 | 2,138 | 4,239 | 858 | 1,118 |
| 2026-07-22 | Wed | 372,309 | 3,679,042 | 555,296 | 6.63 | 3,560 | 6,545 | 1,347 | 2,260 |
| 2026-07-23 | Thu | 368,904 | 3,626,767 | 565,098 | 6.42 | 3,897 | 7,573 | 1,583 | 2,571 |
| 2026-07-24 | Fri | 359,374 | 3,745,917 | 552,622 | 6.78 | 3,573 | 7,485 | 1,499 | 2,382 |
| 2026-07-25 | Sat | 294,243 | 3,878,390 | 462,412 | 8.39 | 3,015 | 6,175 | 1,264 | 2,211 |

**요일을 맞춘 월↔월 비교 (요일 효과 제거)**

| 지표 | 06-22 | 06-29 | **07-06** | 07-13 | 07-20 |
|---|---|---|---|---|---|
| PullRequestEvent event | 28,464 | 21,152 | **8,902 (-57.9%)** | 6,092 | 4,867 |
| WatchEvent event | 5,295 | 4,709 | **1,979 (-58.0%)** | 1,182 | 928 |
| IssuesEvent event | 5,879 | 6,296 | **3,187 (-49.4%)** | 1,865 | 1,297 |
| PushEvent actor | 566,238 | 558,698 | 510,448 (-8.6%) | 409,769 | 359,716 |
| PushEvent ev/row | 3.45 | 3.69 | 4.30 | 6.06 | 6.74 |

**판정** — 비-Push 3종은 **2026-07-06(월)에 하루 만에 약 -58% step**을 밟고, 이후 계속 흘러내린다. PushEvent의 row당 event는 같은 주부터 **완만한 ramp**(07-05 4.68 → 07-06 4.30 → 07-09 5.23 → 07-10 6.53 → 07-11 8.25)이며 하루짜리 step이 아니다. **두 signature의 개시 주가 같다(2026-07-06 주).** 형태가 다르므로 하나의 지표 정의 변경이라기보다 같은 시점에 상류에서 발생한 사건의 두 가지 발현으로 보이지만, **대조군이 없어 단일 원인이라고 말할 수 없다.**

**07-06 이전에도 이미 내려가고 있었다.** 주간 평균 비-Push event는 2026-04-13주 940,252건 → 06-29주 535,545건으로 07-06 전에 이미 -43%다. 그래서 **"07-06에 시작됐다"가 아니라 "4월부터의 완만한 감소 위에 07-06의 step이 얹혔다"** 가 정확한 서술이다.

### 3. 감소의 전체 형태 — 주간 평균 (일별 지표의 7일 평균, 2026-04-06~2026-07-25, UTC)

| week_start | 일수 | actor | repo | total_events | push_events | **non-push events** | fact row |
|---|---|---|---|---|---|---|---|
| 2026-04-06 | 7 | 708,613 | 956,257 | 3,678,545 | 2,567,912 | **1,110,632** | 1,389,848 |
| 2026-04-13 | 7 | 716,746 | 977,907 | 3,694,186 | 2,753,934 | **940,252** | 1,381,127 |
| 2026-04-20 | 7 | 712,713 | 970,573 | 3,642,904 | 2,778,328 | **864,576** | 1,356,189 |
| 2026-04-27 | 7 | 656,298 | 907,342 | 3,438,465 | 2,581,562 | **856,903** | 1,239,839 |
| 2026-05-04 | 7 | 690,286 | 960,518 | 3,633,280 | 2,812,300 | **820,980** | 1,308,785 |
| 2026-05-11 | 7 | 681,715 | 961,099 | 3,561,736 | 2,810,770 | **750,966** | 1,294,530 |
| 2026-05-18 | 7 | 675,879 | 954,910 | 3,566,925 | 2,859,368 | **707,557** | 1,264,542 |
| 2026-05-25 | 7 | 671,802 | 980,415 | 3,716,690 | 3,040,687 | **676,003** | 1,285,170 |
| 2026-06-01 | 7 | 635,971 | 919,564 | 3,454,101 | 2,891,763 | **562,338** | 1,172,777 |
| 2026-06-08 | 7 | 670,554 | 1,000,909 | 3,809,445 | 3,207,617 | **601,828** | 1,269,252 |
| 2026-06-15 | 7 | 626,061 | 951,587 | 3,856,844 | 3,306,888 | **549,956** | 1,196,514 |
| 2026-06-22 | 7 | 618,366 | 958,888 | 3,813,712 | 3,282,478 | **531,233** | 1,193,660 |
| 2026-06-29 | 7 | 607,329 | 937,974 | 3,877,656 | 3,342,111 | **535,545** | 1,170,392 |
| 2026-07-06 | 7 | 474,406 | 762,083 | 3,982,901 | 3,649,415 | **333,486** | 894,646 |
| 2026-07-13 | 7 | 496,775 | 747,755 | 3,946,437 | 3,614,222 | **332,215** | 889,825 |
| 2026-07-20 | **6** | 403,344 | 602,519 | 3,964,163 | 3,729,685 | **234,479** | 703,136 |

**2026-07-20주는 6일치(Mon~Sat)다.** 가장 낮은 요일인 일요일이 빠져 actor 평균이 오히려 상향 편향돼 있다. 완결 주간끼리(2026-04-13주 → 07-13주): actor -30.7%, non-push event -64.7%, push event +31.2%, total_events **+6.8%**, fact row -35.6%.

`total_events`는 15주 내내 3.44M~3.98M 밴드를 벗어나지 않는다. 그 아래에서 non-push가 -79%(1,110,632 → 234,479) 붕괴하고 push가 +45%(2,567,912 → 3,729,685) 증가했다.

### 4. PushEvent 집중도 (actor 단위, 2026-06-24 vs 2026-07-24)

| 지표 | 2026-06-24 | 2026-07-24 | 변화 |
|---|---|---|---|
| push actor | 583,709 | 359,374 | -38.4% |
| push event | 3,286,649 | 3,745,917 | +14.0% |
| **actor당 push event** | **5.63** | **10.42** | **+85.1%** |
| push event 1건뿐인 actor | 278,649 | 208,575 | -25.1% |
| push event 2~9건 actor | 275,722 | 128,445 | **-53.4%** |
| **push event 100건 이상 actor** | **2,037** | **10,146** | **+398.1%** |
| 100건 이상 actor의 event 점유율 | 46.33% | **73.01%** | +26.7pp |
| 상위 100 actor의 event 점유율 | 26.03% | **12.21%** | **-13.8pp** |
| flagged 자동화 actor (dim) | 2,207 (0.38%) | 1,588 (0.44%) | — |
| flagged actor의 event 점유율 | 17.70% | **8.68%** | **-9.0pp** |
| 미flagged actor당 push event | 4.65 | 9.56 | +105.6% |

읽는 법: **소수 거대 actor가 밀어올린 것이 아니다.** 상위 100명의 점유율은 오히려 절반으로 떨어졌다. 늘어난 것은 **하루 100건 이상 push하는 actor 계층 자체의 두께**(2,037명 → 10,146명)이고, 이 계층이 push event의 73%를 차지한다. 동시에 2~9건짜리 중간층 actor가 절반 넘게 사라졌다. `dim_push_automation_actor`가 flag한 actor의 event 점유율은 오히려 **줄었다** — 이 증가분은 **기존에 알려진 봇이 만든 것이 아니다.**

## 코드 경로 판정

지목된 세 가지를 각각 확인했다. **어느 것도 원인이 아니다.**

### (a) event type을 제한하는 코드 — 없다

fact를 실제로 만드는 정본은 dbt 모델 `dbt/gharchive_metrics/models/facts/fact_user_repo_activity.sql`이다.

```sql
select
  cast(actor.id as int64) as user_id,
  cast(repo.id as int64) as repo_id,
  cast(type as string) as action,
  count(*) as event_count,
  parse_date('%Y%m%d', concat('20', _table_suffix)) as activity_date
from `githubarchive.day.20*`
where concat('20', _table_suffix) between replace('{{ raw_start_date }}', '-', '')
                                      and replace('{{ raw_end_date }}', '-', '')
  and actor.id is not null
  and repo.id is not null
  and type is not null
group by user_id, repo_id, action, activity_date
```

`scripts/sync_bq_metrics.py:290-302`의 SQL도 동일한 형태다.

```sql
    SELECT
      CAST(actor.id AS INT64) AS user_id,
      CAST(repo.id AS INT64) AS repo_id,
      CAST(type AS STRING) AS action,
      COUNT(*) AS event_count,
      DATE '{activity_date.isoformat()}' AS activity_date
    FROM `githubarchive.day.{date_str}`
    WHERE actor.id IS NOT NULL
      AND repo.id IS NOT NULL
      AND type IS NOT NULL
    GROUP BY user_id, repo_id, action, activity_date
```

`type`에 대한 조건은 `IS NOT NULL` 뿐이다. **allowlist/denylist가 없다.** 하류도 마찬가지다. `stg_user_repo_activity.sql`은 null 제거와 `week_start`/`month_start` 파생만 하고, `metrics_event_type_daily.sql`은 `group by activity_date, action`으로 **모든** action을 통과시킨다. `metrics_daily.sql`의 `sum(if(action = 'PushEvent', event_count, 0)) as push_events` 같은 구문은 **필터가 아니라 pivot**이며, `total_events`는 별도로 `sum(event_count)` 전체를 잡는다.

`sync_bq_metrics.py`의 `AGENT_SEED_REPOS`(33-36행)와 `AI_KEYWORDS`(38-53행)는 `metrics_agent_trendy_repos` / `metrics_agent_trend_validation` 두 테이블만 좁힌다(`make_agent_trendy_table`, `build_agent_metrics_from_bq`). fact나 `metrics_daily` 계열에는 닿지 않는다.

### (b) PushEvent만 grain/dedup이 달라지는 코드 — 없다

`GROUP BY`는 `user_id, repo_id, action, activity_date` 하나뿐이고 `action`으로 분기하는 집계 로직이 없다. PushEvent가 특별 취급되는 유일한 곳은 `weighted_activity()`(415-426행)의 가중치 `"PushEvent": 0.2`인데, 이는 agent trend 점수 계산용이고 **fact나 `metrics_daily`/`metrics_event_type_daily`에 반영되지 않는다.**

그리고 이 판정은 코드 독해가 아니라 **실측으로 확정됐다.** PushEvent 2026-07-21의 fact row 540,694가 raw `count(distinct (actor.id, repo.id))` 540,694와 같다. 어떤 dedup도 일어나지 않았다.

### (c) `--mode replace-days` × 3일 rolling window의 부분 재읽기 상호작용 — 실재하는 위험이지만 이 현상의 원인은 아니다

`load_one_day_from_public_bigquery`(268-313행)는 **DELETE 후 WRITE_APPEND**로 원자적이지 않다.

```python
    if mode == "replace-days":
        delete_sql = f"DELETE FROM `{names.fact_id}` WHERE activity_date = @activity_date"
        ...
        client.query(delete_sql, job_config=job_config).result()
```
```python
    job_config = bigquery.QueryJobConfig(
        destination=names.fact_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
```

가능한 고장 모드와 각각이 남길 흔적:

| 고장 | 흔적 | 관측? |
|---|---|---|
| DELETE 성공 후 SELECT 실패 | 해당 파티션이 **빈다**. event·actor·row 모두 0 | **아니다.** 모든 날짜에 event 3.4M+ |
| DELETE 실패 후 APPEND 성공 | **중복 적재.** row와 event가 배로 뛰고 actor는 그대로, grain 중복 발생 | **아니다.** 2026-07-19~25 `count(*)` = distinct grain (4,910,280) |
| 열려 있는 shard를 읽어 부분 적재 | event·actor·row가 **함께** 낮음 | **아니다.** PushEvent는 event가 올랐다 |

또한 `DELETE`에 `maximum_bytes_billed`(기본 1 GiB, 1629-1633행) 상한이 걸려 있어 초과 시 **예외로 실패**한다 — 조용히 넘어가지 않는다.

**DAG 스케줄** — 작업 트리에 미커밋 변경 `schedule="30 0 * * *"` → `"30 9 * * *"`(`dags/gharchive_dbt_metrics.py:134`)이 있다. 00:30 KST 실행은 UTC 기준 아직 열려 있는 shard(15:30 UTC 시점)를 읽어 부분 적재를 일으킬 수 있는 **실재하는 결함**이고, 이 변경이 그 구멍을 막는다. 그러나 (i) 최신 파티션 2026-07-25가 24시간 완결이고 raw와 정확히 일치하며, (ii) 3일 rolling replace가 완결 후 재적재로 덮어쓰므로, **현재 테이블에 부분 적재는 남아 있지 않다.** 개시일 2026-07-06과 이 스케줄 변경을 연결할 배포 증거는 확인하지 않았고, 애초에 raw가 일치하므로 연결할 필요도 없다.

**부수 관찰(원인 아님)** — `ensure_fact_table`(172행)은 `table.clustering_fields = ["action", "repo_id"]`로 설정하는데 dbt 모델과 `references/data_map.md`는 `user_id, repo_id, action`이다. 테이블이 이미 존재하면 `create_table(exists_ok=True)`가 아무것도 바꾸지 않으므로 실제 영향은 없지만, replace-all로 재생성하면 clustering이 문서와 달라진다. 정확성 문제는 아니고 성능·문서 정합 문제다.

## 해석

**관측된 패턴 (넷을 분리해야 한다)**

1. **적재는 무결하다.** 5일 × 74 action-행 × 4지표에서 정합률 1.000000. actor·repo·row 감소는 전부 원천에 있다.
2. **비-Push 14종: event·actor·row 동반 감소** (-50%~-88%). 실제 볼륨 부재의 형태다.
3. **PushEvent: event 증가 + row 감소 → row당 event 배증.** 이 배증도 **raw 안에서 계산된다.** 증가분은 하루 100건 이상 push하는 ~10k actor 계층에 몰려 있다(점유율 46% → 73%).
4. **개시: 2026-04월부터의 완만한 감소 + 2026-07-06의 급격한 step.** 두 signature의 개시 주가 같다.

**가능한 설명 (복수, 어느 것도 확정하지 않는다)**

1. **상류(GH Archive) 수집 누락.** 비-Push 14종이 **같은 날(07-06) 동시에** 서로 다른 폭으로 꺾인 것은 수집 경로가 열화됐을 때 나오는 모양이다. 하루 만의 -58% step은 사용자 행동 변화로 설명하기 어렵다. **그러나 우리 데이터로 검증 불가** — `githubarchive.day.*`가 접근 가능한 최상류이고, "그 shard에 원래 몇 건 있어야 했나"는 그 안에 없다. **소유자의 "누락" 직감이 살아남는 유일한 자리이고, 나는 이 설명을 가장 유력하다고 보지만 증명하지 못했다.**
2. **실제 구성 변화.** Watch/Fork류 가벼운 참여 actor(-75~-87%)와 2~9건짜리 중간층 actor(-53%)가 빠지고 고빈도 push 트래픽만 남는 형태. 4월부터의 완만한 부분은 이 설명과 잘 맞는다. 07-06의 step은 잘 맞지 않는다.
3. **`dim_push_automation_actor`가 못 잡는 신규 자동화의 확산.** ~10k actor 계층 급증은 부합하지만 **그 계층의 84%(10,146명 중 8,558명)가 flag되지 않았고** dim의 machine_rate_suspect는 전체 305명뿐이다. **부합은 증거가 아니다. 미확정.**
4. **우리 파이프라인의 적재·집계 버그** — **반증됨.** 위 [코드 경로 판정](#코드-경로-판정) 및 1절.
5. **DAG 스케줄로 인한 부분 shard 적재** — **반증됨.** (c) 참조.

**반례 또는 이 결론을 뒤집을 수 있는 것**

- GH Archive의 시간별 원본(`gharchive.org`의 `.json.gz`)이나 `day.20260706` 재수집본에서 비-Push event가 지금 `githubarchive.day.*`가 돌려주는 것보다 많이 나오면 → 설명 1이 **확정**된다. **이것이 결정적 후속 검증이다.**
- ~10k 고빈도 push actor가 `int_push_automation_actor_day`의 분당 velocity 기준에서 machine-rate로 판정되면 → 설명 3이 강해진다. 사람으로 판정되면 집중 서사를 다시 써야 한다.
- 우리가 검증한 3일 rolling 창 밖 날짜는 2026-06-16, 06-24 둘뿐이다. 4월·5월 날짜 하나가 raw와 어긋나면 지연 도착 판정(PASS)이 뒤집힌다.
- 2026-07-06 전후 shard의 시간대별 분포(`created_at` 기준)에서 특정 시간 구간만 비어 있으면 상류 수집 중단의 직접 증거가 된다. 이번에 확인하지 않았다.

**인과 진술 금지 확인** — 무작위 배정 대조군이 없으므로 "무엇 때문에 줄었다"고 쓸 수 없다. 데이터가 지지하는 문장은 여기까지다: *"우리 적재는 원천과 완전히 일치한다. 원천 안에서 비-Push 14종의 event·actor·row가 4월부터 감소하다 2026-07-06에 약 -58% 급락했고, 같은 주부터 PushEvent는 더 적은 (actor, repo) 조합에 더 많은 event가 몰리는 방향으로 이동했다."*

## 한계

- **상류 검증 불가 (계측 공백).** `githubarchive.day.*`가 우리가 닿는 최상류다. "실제 GitHub 활동" 대비 누락률은 fact에도 raw에도 없다. GH Archive 시간별 원본 파일 또는 GitHub REST/GraphQL API로 표본 repo의 event 수를 직접 세면 가능하다(BigQuery 예산 밖, `data/repo_metadata.db` 방식의 외부 수집 필요).
- **표본 5일.** 정합 대조는 2026-06-16 / 06-24 / 07-21 / 07-24 / 07-25다. 74 action-행 × 4지표가 전부 정확히 일치했으므로 우연 일치 가능성은 사실상 없지만, **모든 날짜가 무결하다는 증명은 아니다.** 3일 rolling 창 밖 날짜는 2개만 봤고, 2026-07-06(개시일) 자체는 raw로 대조하지 않았다.
- **개시일 2026-07-06을 raw로 직접 확인하지 않았다.** 2절 walk는 mart 기준이다. mart와 raw가 인접 날짜들에서 일치하므로 step 자체는 신뢰할 수 있지만, 그 날 shard의 시간대별 분포는 보지 않았다.
- **`dim_push_automation_actor`가 신뢰할 수 없다.** 20,992행 중 20,706행이 로그인에 `[bot]`이 붙은 explicit_bot이고 machine_rate_suspect는 305명뿐이다. 하루 100건 이상 push하는 actor 10,146명 중 1,588명만 flagged다. **자동화 제외본을 "사람만"으로 읽을 수 없다.** 이 dim이 언제 어느 범위로 빌드됐는지도 확인하지 않았다.
- **시각(hour) 계측 공백.** fact에 `created_at`이 없어 감소가 하루 중 특정 시간대에 몰렸는지 볼 수 없다. shard 완결 확인 때만 raw `created_at`을 썼다. 시간대별 분해는 raw에서 1일 약 0.14 GB로 가능하다.
- **`event_count`는 event 수다.** PushEvent 1건에 커밋이 여러 개 들어간다. **"커밋이 늘었다"고 말할 수 없다.** row당 event 6.902도 커밋 밀도가 아니라 event 밀도다. 커밋 수는 raw `payload`의 `$.size`/`$.distinct_size`에만 있다.
- **actor는 사람이 아니다.** 봇·조직 계정·CI 모두 `actor.id`다.
- **actor-day를 distinct actor로 읽지 말 것.** 3절 이전 표의 5일 합산 actor 열은 actor-day다. event type끼리 더하면 중복 계산이다.
- **분포를 보지 않았다.** 4절은 구간별 actor 수와 점유율까지다. actor당 push event의 중앙값·p90을 보지 않아 "계층이 두꺼워진 것"과 "기존 계층이 더 많이 push한 것"을 완전히 분리하지 못했다.
- **계절성 미통제.** 2026-04~07 한 구간이다. "4월 중순 시작"은 그 이전(2025-09~2026-03)을 같은 해상도로 보지 않은 상태의 관측이다.
- **대조군 없음.** 무작위 배정이 존재하지 않으므로 인과 해석 불가.
- **UTC 기준.** KST로 보면 요일 경계가 9시간 이동한다.

## 권고

1. **상류를 직접 재확인한다 (최우선).** `day.20260705`와 `day.20260706` 두 shard의 **시간대별(hour) event 수를 type별로** 뽑고, 가능하면 GH Archive 시간별 원본 `.json.gz`와 대조한다. 특정 시간부터 비-Push type이 끊겼다면 상류 수집 사고가 확정되고, 균일하게 낮으면 실제 변화 쪽으로 무게가 이동한다. **이것이 소유자의 "누락" 가설을 살리거나 죽이는 유일한 검증이다.** raw 2일 × 시간별 약 0.3 GB로 가능하다.
2. **`dim_push_automation_actor` 재적재 + `int_push_automation_actor_day`를 2026-07-20~07-25에 실행.** 하루 100건 이상 push하는 10,146 actor가 machine-rate인지가 설명 2와 3을 가른다. 지금 dim은 사실상 `[bot]` 접미사 탐지기여서 이 질문에 답할 수 없다.
3. **정합 테스트를 dbt에 넣어 이 종류의 질문을 조사 대신 테스트로 답하게 한다.** 최신 파티션 1일에 대해 일자별·action별로 `raw count(*)` vs `fact sum(event_count)`, `raw count(distinct (actor,repo))` vs `fact row 수`. raw 1일 약 0.1 GB이므로 비용이 무의미하다. 오늘 이 조사에 쓴 시간이 매일 자동으로 확보된다.
4. **`total_events`를 건강성 지표에서 내리고 드리프트 알림을 두 종류로 쪼갠다.** `total_events`는 15주 동안 상쇄 덕분에 밴드를 유지하며 -79%/+45% 두 움직임을 완전히 가렸다. 대시보드에 **non-push events**, **type별 event**, **row당 event**를 올린다. 그리고 현재 `events_per_user PSI=1.5431` / `events_per_repo PSI=1.6105` 경보는 "우리 로더가 깨졌다"와 "상류가 변했다"를 구분하지 못해 이번에 잘못된 가설로 이어졌다 — PSI 경보에 raw 대조 한 줄(권고 3)을 붙이면 그 자리에서 갈린다. 또한 `metrics_daily`만 보는 카드에 actor와 event를 같은 축에 놓지 않는다(선행 리포트 `reports/20260726_weekday_push_divergence.md`와 같은 권고).

## 재현 방법

- **실행일**: 2026-07-26
- **as-of**: 2026-07-25 (fact/mart 최신 완결일)
- **주요 파라미터**: raw 대조일 `260616` / `260624` / `260721` / `260724` / `260725`; 건강성 검증 창 `2026-07-19`~`2026-07-25`; 개시 추적 창 `2026-06-22`~`2026-07-25`; 본분석 창 `2026-04-06`~`2026-07-25`
- **SQL 경로**: 별도 `analyses/` 파일을 추가하지 않았다(이번 작업 범위가 리포트 1개 파일로 제한됨). 아래 쿼리를 그대로 쓰면 재현된다. 반복 실행하게 되면 권고 3번대로 dbt 테스트로 승격한다.

```bash
export GCP_KEY_PATH=/Users/kakao/bda-2/gcp-key.json
export GOOGLE_APPLICATION_CREDENTIALS=/Users/kakao/bda-2/gcp-key.json
```

**(a) 결정적 대조 — raw측. 날짜마다 따로 실행한다. `--dry_run` 먼저.**

```sql
-- _table_suffix를 260616 / 260624 / 260721 / 260724 / 260725로 바꿔 5회
select
  type as action,
  count(*) as raw_events,
  count(distinct actor.id) as raw_actors,
  count(distinct repo.id) as raw_repos,
  count(distinct format('%d|%d', actor.id, repo.id)) as raw_actor_repo
from `githubarchive.day.20*`
where _table_suffix = '260721'
  and actor.id is not null and repo.id is not null and type is not null
group by action
```

**(b) 결정적 대조 — mart/fact측. (a)와 칸 대 칸으로 맞춰 본다.**

```sql
-- mart (스캔 작음). raw_events↔total_events, raw_actors↔active_users,
-- raw_actor_repo↔user_repo_action_rows 를 대조한다
select activity_date, action, active_users, active_repos, total_events, user_repo_action_rows
from `bda-coai.mart.metrics_event_type_daily`
where activity_date in ('2026-06-16','2026-07-21')
order by activity_date, total_events desc
```

```sql
-- fact 직접 확인 (partition 조건 필수)
select activity_date, action,
  sum(event_count) as fact_events,
  count(distinct user_id) as fact_actors,
  count(distinct repo_id) as fact_repos,
  count(*) as fact_rows,
  count(distinct format('%d|%d', user_id, repo_id)) as fact_actor_repo
from `bda-coai.mart.fact_user_repo_activity`
where activity_date in ('2026-06-24','2026-07-24')
group by activity_date, action
order by activity_date, fact_events desc
```

**(c) 최신 shard 완결 확인 (`--dry_run` 142,681,293 B)**

```sql
select count(*) as raw_events, count(distinct actor.id) as raw_actors,
  count(distinct repo.id) as raw_repos, count(distinct type) as raw_types,
  min(created_at) as min_ts, max(created_at) as max_ts,
  count(distinct extract(hour from created_at)) as hours_present
from `githubarchive.day.20*`
where _table_suffix = '260725'
  and actor.id is not null and repo.id is not null and type is not null
```

**(d) 개시 시점 walk (mart, 2절 표)**

```sql
select activity_date, format_date('%a', activity_date) as dow,
  max(if(action='PushEvent', active_users, null)) as push_actors,
  max(if(action='PushEvent', total_events, null)) as push_events,
  max(if(action='PushEvent', user_repo_action_rows, null)) as push_rows,
  round(max(if(action='PushEvent', safe_divide(total_events, user_repo_action_rows), null)),2) as push_ev_per_row,
  max(if(action='PullRequestEvent', active_users, null)) as pr_actors,
  max(if(action='PullRequestEvent', total_events, null)) as pr_events,
  max(if(action='WatchEvent', total_events, null)) as watch_events,
  max(if(action='IssuesEvent', total_events, null)) as issue_events
from `bda-coai.mart.metrics_event_type_daily`
where activity_date between '2026-06-22' and '2026-07-25'
  and action in ('PushEvent','PullRequestEvent','WatchEvent','IssuesEvent')
group by 1 order by 1
```

**(e) 주간 형태 (mart, 3절 표)**

```sql
select date_trunc(activity_date, week(monday)) as wk,
  count(*) as days,
  round(avg(active_users)) as avg_actors, round(avg(active_repos)) as avg_repos,
  round(avg(total_events)) as avg_events, round(avg(push_events)) as avg_push,
  round(avg(total_events - push_events)) as avg_nonpush,
  round(avg(user_repo_action_rows)) as avg_rows
from `bda-coai.mart.metrics_daily`
where activity_date between '2026-04-06' and '2026-07-25'
group by wk order by wk
```

**(f) PushEvent 집중도 (fact, 4절) / (g) 자동화 제외본 / (h) grain·null 검증** — 각각 `activity_date in ('2026-06-24','2026-07-24')`, `activity_date between '2026-07-19' and '2026-07-25'` 파티션 조건으로 실행. 쿼리 본문은 `.claude/skills/analysis/references/health_checks.md`의 「자동화 actor 영향 크기」·「grain 중복」·「null / 범위」 절과 동일하며 날짜만 위와 같이 바꿨다.

### dry run / 실제 스캔 bytes

| 쿼리 | dry run bytes | 실행 | 비고 |
|---|---|---|---|
| (a) raw 2026-06-16 action별 | 104,968,176 | 실행 | 0.098 GiB |
| (a) raw 2026-06-24 action별 | 105,144,101 | 실행 | 0.098 GiB |
| (a) raw 2026-07-21 action별 | 107,399,901 | 실행 | 0.100 GiB |
| (a) raw 2026-07-24 action별 | 108,137,299 | 실행 | 0.101 GiB |
| (c) raw 2026-07-25 완결 확인 | 142,681,293 | 실행 | 0.133 GiB. `created_at` 포함으로 약간 큼 |
| **raw 누적** | **568,330,770 B (0.568 GB / 0.529 GiB)** | | **목표 5 GB의 11.4%.** $6.25/TiB 기준 약 **$0.003** |
| mart 쿼리 (b)(d)(e) | 소형 mart 테이블 | 실행 | `metrics_daily` 328행 / `metrics_event_type_daily` 소형 |
| fact 쿼리 (b)(f)(g)(h) | 파티션 한정 | 실행 | 전부 `activity_date` 조건 있음. 최대 7일(2026-07-19~25) |

`payload` 컬럼은 어느 쿼리에서도 조회하지 않았다. raw는 항상 `_TABLE_SUFFIX` **등호** 조건으로 단일 shard만 읽었다. dry run은 과금되지 않으므로 위 bytes는 실행 스캔량과 동일하다.
