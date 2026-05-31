# 입력 피처 드리프트 감지 설계

## 목표

recsys / analytics 두 레이어의 **입력 분포 이동**을 매일 감지한다.
공통 원시 연산은 하나: *일별 수치 피처 행렬 + 고정 레퍼런스 분포 + 피처별 PSI + 백테스트로 잡은 threshold*.

라벨 불필요(예측·성능 드리프트는 후속 phase). threshold 초과는 **warn-only** — 태스크 실패 아님, Slack 경고만.

## 추상화

모든 드리프트 대상은 `(day) × (feature)` 수치 행렬로 환원된다.

- **analytics**: `data/daily_agg`의 **within-day 분포**(유저/레포별 이벤트 분포: events_per_user, events_per_repo, repos_per_user). 일별 스칼라 PSI는 트렌드에 포화돼 못 씀.
- **recsys**: 75개 리랭커 피처(sparse 드롭 → ~55개). blessed 피처 parquet 빈티지를 레퍼런스로, 새 빈티지를 비교(train/serve 스큐). 동일 config 시계열 빈티지가 없어 **부트스트랩 노이즈 플로어**로 null 잡음.

## PSI + 백테스트 threshold

1. 레퍼런스 윈도우(앞쪽 N일) 값으로 피처별 **분위 빈(quantile bins)** + 레퍼런스 비율 산출.
2. 레퍼런스 이후 구간에 **W일 롤링 윈도우**를 굴려 피처별 PSI 시계열 = "정상 변동" null 분포.
3. threshold = null 분포의 **p95 = warn / p99 = alert** (피처별). `data/drift/thresholds.json`.
4. 검증: recsys는 PSI 급등이 NDCG 하락과 겹치는지 / analytics는 주말 변동 오탐 여부.

PSI 0.1·0.25 교과서값 대신 데이터 자연 변동으로 잡으므로 "왜 이 숫자냐"를 데이터로 설명.

## 아티팩트

```
data/drift/reference/<layer>_reference.json   # 피처별 bin edges + 레퍼런스 비율
data/drift/thresholds.json                    # 피처별 warn/alert PSI
data/drift/reports/<layer>_<date>.json        # 일별 PSI 리포트
```

## 모듈 / 스크립트

```
src/ghrec/drift.py                     # 코어: build_reference, compute_psi, score, calibrate_thresholds
scripts/drift_calibrate_thresholds.py  # 백테스트 → reference + thresholds.json (주기적/수동)
scripts/drift_detect_platform.py       # 일별 감지 (analytics DAG가 호출)
scripts/drift_detect_recsys.py         # phase 2
tests/test_drift.py
```

## DAG 연결 (warn-only)

- `gharchive_dbt_metrics` → 끝에 `detect_metric_drift` task (build_dbt_metrics 이후).
- `gharchive_recsys_features` → 끝에 `detect_feature_drift` task (phase 2, 신선 스코어링 step 포함).
- Slack: `dags/utils/slack_alert.py`에 `notify_drift()` 추가 — 실패 콜백과 별개 경고 경로.

## Phase

1. **analytics 입력 드리프트** — 데이터 로컬 보유, BQ 비용 0 백테스트. (현재 phase)
2. recsys 75피처 — 신선 윈도우 재스코어링 + NDCG 교차검증.
3. 예측·성능(컨셉) 드리프트 — 라벨 윈도우 기반.
