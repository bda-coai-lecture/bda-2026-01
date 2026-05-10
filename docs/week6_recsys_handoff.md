# Week 6 추천시스템 강의용 정리

최종 수정: 2026-05-10

이 문서는 6주차 강의에서 사용할 추천시스템 실험 내용을 요약한다. 핵심은 GitHub Archive 데이터를 유저-레포 상호작용으로 바꾸고, ALS 후보 생성과 LGBM 재정렬을 결합한 two-stage 추천을 평가하는 것이다.

## 한 장 요약

- 데이터: `data/daily_agg`, `2026-02-15` ~ `2026-05-08`
- 학습 history: 기본 `2026-03-14` ~ `2026-04-24`
- rank-label: `2026-04-25` ~ `2026-05-01`
- test: `2026-05-02` ~ `2026-05-08`
- 추천 구조: popularity baseline -> ALS retrieval -> LGBM ranker
- 평가 정책: warm user만 보지 않고 cold-start user도 fallback으로 포함
- 현재 최고 기준: `related80_anchor20_full_als96_i12_lgbm63`
- 최고 성능: `Two-Stage/Fallback NDCG@10 = 0.016029`, `Recall@100 = 0.074161`
- 잔차 분석 결론: watch/fork 중심 유저는 기존에도 강했고, push/PR/comment/issue 중심 유저는 item-to-item related 후보 source를 추가했을 때 크게 개선됐다.
- 반복 집계 방지용 mart 생성 스크립트: `scripts/week6_build_recsys_marts.py`
- two-stage 실험 스크립트는 기본 `--use-marts auto`로 mart가 있으면 feedback/split/related 후보와 user/repo feature context를 mart에서 읽는다.

## 강의에서 설명할 핵심 개념

### 1. Raw event를 그대로 쓰지 않는다

GitHub Archive 원본 event는 "누가, 언제, 어떤 repo에, 어떤 event를 발생시켰는가"가 한 줄씩 쌓인 로그다. 같은 유저가 같은 repo에 여러 번 push하거나 comment하면 row가 계속 늘어난다. 이걸 그대로 추천 모델에 넣으면 너무 크고, 반복 로그가 모델을 지배할 수 있다.

그래서 먼저 유저-레포 단위로 압축한다.

```text
raw event rows
-> actor_id, repo_id별 weighted score로 집계
-> retained catalog 안의 user-repo sparse matrix 생성
-> ALS retrieval
-> LGBM ranker 학습/평가
```

이 집계는 이제 `data/marts/week6/user_repo_interaction_mart.parquet`로 고정할 수 있다. 기본 grain은 `window_end_date, actor_id, repo_id`이고, `watch_cnt`, `fork_cnt`, `pr_cnt`, `push_cnt`, `issue_cnt`, `comment_cnt`, `weighted_score`, `first_seen_at`, `last_seen_at`, `active_days`를 가진다.

mart 사용을 강제하려면:

```bash
uv run python scripts/week6_two_stage_v2.py --use-marts always
```

기존 raw 집계 경로로 비교하려면:

```bash
uv run python scripts/week6_two_stage_v2.py --use-marts never
```

예:

| actor_id | repo_id | event | cnt | weight | score |
|---:|---:|---|---:|---:|---:|
| 1 | 100 | WatchEvent | 1 | 1.0 | 1.0 |
| 1 | 100 | PullRequestEvent | 2 | 3.0 | 6.0 |
| 1 | 100 | IssueCommentEvent | 5 | 0.3 | 1.5 |

모델에는 위 세 줄을 따로 넣지 않고 `(actor_id=1, repo_id=100, score=8.5)`라는 하나의 feedback으로 넣는다.

### 2. Retained catalog를 둔다

모든 repo를 후보로 두지 않는다. 너무 희귀한 repo까지 전부 넣으면 matrix가 커지고 후보 생성도 느려진다. 현재는 history 기간에 충분히 등장한 repo 중 상위 `300,000`개를 retained catalog로 유지한다.

강의 포인트:

- catalog 제한은 실서비스에서도 흔한 설계다.
- 품질이 낮거나 거의 등장하지 않는 item까지 모두 추천 후보로 두면 속도와 품질이 둘 다 나빠질 수 있다.
- 대신 long-tail repo 추천 기회는 줄어든다. 즉 `max_items`는 속도, 품질, 다양성 사이의 trade-off다.

### 3. Two-stage 추천 구조

전체 repo를 한 번에 정렬하지 않는다.

1. Retrieval: ALS가 유저별 후보 repo 수백 개를 빠르게 뽑는다.
2. Ranking: LGBM ranker가 후보를 다시 정렬한다.

LGBM은 ALS 점수뿐 아니라 최근 인기도, 유저 활동량, 후보 출처, 과거에 본 repo와의 유사도 같은 피처를 함께 본다. 이 구조는 "빠른 후보 생성 + 더 정교한 재정렬"이라는 실전 추천시스템의 기본 패턴이다.

## Event weight

event 종류마다 추천 신호의 강도가 다르다고 보고 가중치를 다르게 준다.

| event | weight | 해석 |
|---|---:|---|
| `WatchEvent` | 1.0 | 관심 표현. 기준값 |
| `ForkEvent` | 2.0 | 명시적 재사용/실험 의도 |
| `IssuesEvent` | 0.5 | 관심 신호지만 질문/버그 신고가 섞임 |
| `PullRequestEvent` | 3.0 | 강한 기여 신호 |
| `IssueCommentEvent` | 0.3 | 논의 참여지만 노이즈가 많음 |
| `PushEvent` | 0.2 | 본인 repo의 반복 작업 로그일 수 있음 |

이 값은 정답이 아니라 강의용 baseline이다. 처음에는 domain knowledge로 가중치를 정하고, 이후 validation 성능과 정성 평가를 보며 조정한다.

다음 실험 후보:

- 보수적 기여형: PR/Fork 가중치를 조금 낮추고 Push를 더 낮춘다.
- 활동성 강조형: Issue/Comment/Push를 조금 높인다.
- 명시적 관심 강조형: Watch/Fork 중심으로 둔다.

## 시간 기준 split

현재 split은 test 정보가 학습에 들어가지 않게 시간 순서를 지킨다.

| 구간 | 기간 | 역할 |
|---|---|---|
| history | `2026-03-14` ~ `2026-04-24` | ALS 학습, 피처 생성, fallback 후보 생성 |
| rank-label | `2026-04-25` ~ `2026-05-01` | LGBM ranker 학습 label |
| test | `2026-05-02` ~ `2026-05-08` | 최종 평가 |

주의할 점:

- 현재 `train_seen`은 history만 필터링한다.
- 만약 "2026-05-02에 서빙하며 2026-05-01까지의 모든 행동을 알고 있다"는 정책으로 평가하려면 rank-label 기간 상호작용도 seen item으로 필터링해야 한다.
- metadata의 stars/forks가 split 이후 snapshot이면 temporal leakage가 될 수 있다.
- `scripts/week6_build_recsys_marts.py`는 `experiment_split_mart.parquet`에 `history`, `rank_label`, `test` split을 저장해서 날짜 기준이 실험 코드마다 흔들리지 않게 한다.
- mart 모드에서는 `scripts/week6_two_stage_v2.py`가 `daily_agg`를 다시 읽지 않는다. 최근/인기 fallback 후보도 `repo_feature_mart`의 `total_score_7d`, `total_score_42d`에서 만든다. raw parquet 재집계와 비교하려면 명시적으로 `--use-marts never`를 쓴다.
- `scripts/train_two_tower_10pct.py`도 `--use-marts auto|always|never`를 지원한다. mart 모드에서는 `user_repo_interaction_mart`, `experiment_split_mart`, `repo_feature_mart`를 사용하고, metadata 결측 stars/forks는 0으로 처리한다.

## 평가 정책

현재 평가는 warm-only가 아니다.

- warm user: history 기간에 상호작용이 있어 개인화 추천을 만들 수 있는 유저
- cold user: history에는 없고 test에만 등장한 유저
- fallback: cold user처럼 개인화 후보를 만들 수 없을 때 쓰는 인기/최근성 기반 추천

실서비스에는 과거 이력이 충분한 유저만 오지 않는다. 그래서 cold-start user도 평가에 포함한다. summary에는 `eval_warm_users`, `eval_cold_users`를 같이 남긴다.

## 지표 해석

| 지표 | 의미 | 강의 포인트 |
|---|---|---|
| `precision@k` | 추천 k개 중 실제 정답 비율 | catalog가 크면 절대값이 낮게 나온다 |
| `recall@k` | test 정답 중 추천 k개 안에 들어간 비율 | 후보를 얼마나 놓치지 않는지 본다 |
| `ndcg@k` | 정답이 위쪽에 있을수록 높은 점수 | ranker 비교에는 특히 중요 |
| `unique_recommended` | 전체 평가에서 추천된 서로 다른 repo 수 | 너무 낮으면 인기 repo만 반복 추천하는 모델일 수 있다 |

추천 문제에서는 정답 repo가 매우 희소하므로 precision 절대값이 작게 보일 수 있다. 그래서 모델 간 상대 비교와 `NDCG@10`, `Recall@50`, coverage를 함께 본다.

## 주요 결과

### 기준선: `latest`

설정:

- history: 42일
- retained catalog: `300,000` repos
- ALS factors: `64`
- ALS iterations: `12`
- runtime: `20.48` min

결과:

| model | k | precision | recall | ndcg | unique_recommended |
|---|---:|---:|---:|---:|---:|
| Popularity | 10 | 0.000984 | 0.008283 | 0.003014 | 18 |
| ALS/Fallback | 10 | 0.002016 | 0.016333 | 0.007844 | 4,456 |
| Two-Stage/Fallback | 10 | 0.002986 | 0.023409 | 0.012225 | 8,596 |
| Two-Stage/Fallback | 50 | 0.001319 | 0.050752 | 0.018440 | 15,087 |
| Two-Stage/Fallback | 100 | 0.000885 | 0.068104 | 0.021417 | 17,415 |

해석:

- popularity보다 ALS가 낫다.
- ALS 후보를 LGBM으로 재정렬하면 한 번 더 좋아진다.
- 즉 two-stage 구조가 단순 인기 추천과 retrieval-only보다 낫다.

### History window 비교

`hist28`은 28일 history만 쓴 실험이다.

| run | history | `NDCG@10` | `NDCG@50` | `NDCG@100` | runtime |
|---|---:|---:|---:|---:|---:|
| `latest` | 42일 | 0.012225 | 0.018440 | 0.021417 | 20.48분 |
| `hist28` | 28일 | 0.010996 | 0.016826 | 0.019534 | 19.69분 |

해석:

- 28일 history는 더 최근 데이터에 집중하지만 학습 가능한 유저-레포 관계가 줄어든다.
- 현재 데이터에서는 42일 history가 더 좋았다.

### 피처 + 튜닝 성과

새 full run `tune_full_als96_i12_lgbm63`은 기존 `latest`보다 성능이 올랐다.

주의: 이 비교는 피처만의 순수 ablation이 아니다. 새 full run은 피처 추가와 hyperparameter 변경이 함께 들어갔다.

| k | 기존 NDCG | 새 NDCG | 변화율 | 기존 recall | 새 recall | unique 추천 변화율 |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 0.012225 | 0.012663 | +3.58% | 0.023409 | 0.023897 | +17.28% |
| 50 | 0.018440 | 0.018947 | +2.75% | 0.050752 | 0.051590 | +20.61% |
| 100 | 0.021417 | 0.021981 | +2.64% | 0.068104 | 0.069246 | +23.61% |

해석:

- NDCG와 recall이 모두 개선됐다.
- unique recommendation이 크게 늘어, 인기 repo 몇 개에 덜 몰리게 됐다.
- 다만 "피처 하나하나의 순수 기여도"를 말하려면 별도 ablation이 필요하다.

강의 표현:

> 추가 피처와 약간의 튜닝을 함께 적용한 full run에서는 NDCG와 recall이 모두 개선됐다. 다만 엄밀하게는 피처+튜닝 묶음의 개선으로 해석해야 한다.

### 피처-only ablation: `feature_only_like_latest`

기존 `latest`와 hyperparameter를 최대한 맞추고, 추가 피처가 들어간 현재 스크립트만 사용한 full run이다.

실행:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --max-items 300000 \
  --candidate-k 300 \
  --hybrid-extra 200 \
  --rank-users 100000 \
  --eval-users 10000000 \
  --qual-users 300 \
  --factors 64 \
  --iterations 12 \
  --als-regularization 0.01 \
  --lgbm-num-leaves 31 \
  --output-suffix feature_only_like_latest
```

runtime: `41.17`분

| run | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` |
|---|---:|---:|---:|---:|
| `latest` | 0.012225 | 0.018440 | 0.068104 | 17,415 |
| `feature_only_like_latest` | 0.012179 | 0.018404 | 0.068104 | 16,962 |
| `tune_full_als96_i12_lgbm63` | 0.012663 | 0.018947 | 0.069246 | 21,527 |

해석:

- 피처만 추가하고 `latest`에 가까운 hyperparameter를 쓰면 성능은 거의 변하지 않았다.
- 따라서 `tune_full_als96_i12_lgbm63`의 개선은 "추가 피처만의 효과"라기보다 ALS factor/regularization, LGBM leaf/colsample 등 튜닝과 함께 나온 효과로 보는 게 안전하다.
- 강의에서는 feature engineering이 중요하다는 메시지는 유지하되, 이번 수치만으로 "피처 추가가 단독으로 개선했다"고 말하지 않는다.

### Event weight screening

`scripts/week6_two_stage_v2.py`에 `--event-weight EventType=value` 반복 옵션을 추가했다. 옵션을 주지 않으면 기존 baseline weight를 그대로 쓴다.

작은 screening 설정:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --sample-ratio 0.04 \
  --max-items 80000 \
  --candidate-k 160 \
  --hybrid-extra 80 \
  --rank-users 6000 \
  --eval-users 5000 \
  --qual-users 50 \
  --factors 48 \
  --iterations 8 \
  --als-regularization 0.01 \
  --lgbm-num-leaves 31 \
  --lgbm-min-child-samples 50 \
  --output-suffix weight_screen_baseline
```

후보별 Two-Stage/Fallback 결과:

| run | 핵심 변경 | runtime | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` |
|---|---|---:|---:|---:|---:|---:|
| `weight_screen_baseline` | 기존 weight | 2.61분 | 0.013402 | 0.018337 | 0.047124 | 2,657 |
| `weight_screen_conservative_contrib` | Fork 1.5, PR 2.4, Push 0.1 | 2.45분 | 0.013650 | 0.020372 | 0.059456 | 2,363 |
| `weight_screen_activity` | Issue 0.8, Comment 0.6, Push 0.4 | 2.57분 | 0.013947 | 0.019953 | 0.052885 | 3,402 |
| `weight_screen_explicit_interest` | Watch 1.2, Fork 2.5, PR 2.0, Issue/Comment/Push 낮춤 | 3.10분 | 0.013143 | 0.020278 | 0.062461 | 2,434 |

해석:

- 작은 screening에서는 `activity`가 `NDCG@10`과 coverage가 가장 좋았다.
- `explicit_interest`는 `Recall@100`이 가장 높았지만 `NDCG@10`은 baseline보다 낮았다.
- `conservative_contrib`는 `NDCG@50`과 recall이 좋아 full 후보로 볼 만하다.
- screening은 샘플과 catalog가 작아 변동성이 크다. full run 후보는 `activity` 1순위, `conservative_contrib` 2순위로 잡는다.

### Event weight full run: `weight_activity_full_als64_i12_lgbm31`

screening 1순위였던 활동성 강조형 weight를 `latest`에 가까운 full 설정으로 돌렸다.

실행:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --max-items 300000 \
  --candidate-k 300 \
  --hybrid-extra 200 \
  --rank-users 100000 \
  --eval-users 10000000 \
  --qual-users 300 \
  --factors 64 \
  --iterations 12 \
  --als-regularization 0.01 \
  --lgbm-num-leaves 31 \
  --event-weight IssuesEvent=0.8 \
  --event-weight IssueCommentEvent=0.6 \
  --event-weight PushEvent=0.4 \
  --output-suffix weight_activity_full_als64_i12_lgbm31
```

runtime: `42.41`분

| run | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` |
|---|---:|---:|---:|---:|
| `latest` | 0.012225 | 0.018440 | 0.068104 | 17,415 |
| `feature_only_like_latest` | 0.012179 | 0.018404 | 0.068104 | 16,962 |
| `weight_activity_full_als64_i12_lgbm31` | 0.011931 | 0.017626 | 0.064405 | 20,746 |
| `tune_full_als96_i12_lgbm63` | 0.012663 | 0.018947 | 0.069246 | 21,527 |

해석:

- 작은 screening에서는 activity weight가 좋아 보였지만, full run에서는 `latest`보다 NDCG/recall이 모두 낮았다.
- 대신 `unique@100`은 늘어 coverage는 좋아졌다. 즉 더 다양한 repo를 추천하지만 정확도는 손해를 본 설정이다.
- 강의 결론에는 activity full을 개선 사례로 쓰지 않는다. "screening 결과는 full에서 뒤집힐 수 있다"는 실험 설계 사례로 쓰기 좋다.
- 다음 weight full 후보를 돌린다면 `conservative_contrib`가 더 타당하지만, 현재 최고 모델을 갱신할 가능성은 불확실하다.

### 잔차/유저 패턴 quick analysis

저장된 `week6_qual_cases_*` 샘플로 Two-Stage와 ALS의 hit@10 차이를 봤다. 샘플은 run별 300명 수준이라 강한 통계 결론이 아니라 다음 실험 후보를 찾기 위한 탐색이다.

산출물:

- `data/models/week6/week6_residual_pattern_qual_summary.parquet`

핵심 관찰:

- `tune_full_als96_i12_lgbm63`에서 watch 중심 유저는 Two-Stage가 ALS보다 뚜렷하게 좋았다.
  - watch dominant: Two-Stage hit@10 `0.0635`, ALS hit@10 `0.0000`
  - event-match가 높고 추천 item의 watch share도 높은 케이스에서 reranker가 이득을 봤다.
- push 중심 유저는 수가 많지만 hit rate가 낮다.
  - tuned best 기준 push dominant: Two-Stage hit@10 `0.0091`
  - activity full도 push dominant를 조금 더 맞추긴 했지만 전체 NDCG는 떨어졌다.
- PR/comment/issue 중심 유저는 거의 맞추지 못했다.
  - 샘플 수는 적지만 `pr`, `comment`, `issue` dominant에서 대부분 hit@10이 `0`이었다.
  - 이 구간은 event weight를 조금 조정하는 것보다 별도 후보 생성이나 피처가 필요해 보인다.
- activity full은 high-recent 유저에서 ALS보다 오히려 손해가 났다.
  - high recent bin: Two-Stage hit@10 `0.0133`, ALS hit@10 `0.0267`
  - 활동 로그를 세게 주면 최근 활동성은 반영되지만, 정확한 top-k 정렬에는 노이즈가 커질 수 있다.
- Two-Stage가 ALS를 이기는 케이스는 추천 후보가 ALS source 비중이 높고, seen/profile cosine이 높은 경향이 있었다.
  - 즉 fallback/recent 후보를 많이 섞는 것보다 ALS 후보 안에서 정렬을 잘하는 쪽이 더 안정적이다.

이후 처리:

1. 유저 세그먼트별 reranker 또는 feature interaction
   - watch 중심 유저에게는 지금 구조가 잘 맞는다.
   - push/PR 중심 유저에게는 동일한 scoring을 쓰면 노이즈가 커질 수 있다.
   - `dominant_event`, `user_event_entropy`, `user_push_share x item_push_share`, `user_pr_share x item_pr_share` 같은 interaction을 명시적으로 추가해볼 수 있다.
2. PR/comment/issue 중심 유저용 후보 생성 보강
   - item-to-item related 후보 source를 추가했고 full run에서 새 최고 성능을 냈다.
   - 남은 확장은 같은 repo owner/org, language/topic, maintainer graph 기반 source 비교다.
3. recent 후보 투입량 조절
   - source cap screening에서 실패했다.
   - activity full은 coverage를 늘렸지만 NDCG를 낮췄다.
   - `hybrid_extra`를 늘리는 실험보다, recent 후보를 user segment에 따라 제한하거나 source별 cap을 두는 쪽이 더 안전하다.
4. per-user diagnostics 저장
   - 현재는 qual sample만 있어서 잔차 분석이 약하다.
   - 다음 실험 전에 `--save-user-diagnostics` 옵션을 추가해 전체 eval user별 hit/ndcg, user event mix, source mix를 저장하면 어떤 세그먼트에서 실제로 손해나는지 더 정확히 볼 수 있다.

### 전체 user diagnostics full run

`scripts/week6_two_stage_v2.py`에 `--save-user-diagnostics` 옵션을 추가했다. 옵션을 켜면 전체 eval user별 metric, 유저 event mix, recent 활동 비중, candidate/source mix, top-10 평균 feature를 parquet로 저장한다.

검증:

```bash
uv run python -m py_compile scripts/week6_two_stage_v2.py
uv run python scripts/week6_two_stage_v2.py --smoke --save-user-diagnostics --output-suffix smoke_diagnostics
uv run python scripts/week6_analyze_user_diagnostics.py --suffix smoke_diagnostics
```

당시 최고 설정에 diagnostics를 붙인 full run:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --max-items 300000 \
  --candidate-k 300 \
  --hybrid-extra 200 \
  --rank-users 100000 \
  --eval-users 10000000 \
  --qual-users 300 \
  --factors 96 \
  --iterations 12 \
  --als-regularization 0.03 \
  --lgbm-num-leaves 63 \
  --lgbm-min-child-samples 50 \
  --lgbm-colsample 0.85 \
  --save-user-diagnostics \
  --output-suffix tune_full_als96_i12_lgbm63_diagnostics
```

runtime: 약 `60`분

결과:

| model | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` |
|---|---:|---:|---:|---:|
| Popularity | 0.003014 | 0.007929 | 0.043526 | 183 |
| ALS/Fallback | 0.007034 | 0.011568 | 0.050114 | 17,366 |
| Two-Stage/Fallback | 0.012663 | 0.018947 | 0.069246 | 21,527 |

산출물:

- `data/models/week6/week6_user_diagnostics_tune_full_als96_i12_lgbm63_diagnostics.parquet`
- `data/models/week6/diagnostics/tune_full_als96_i12_lgbm63_diagnostics_summary.md`
- `data/models/week6/diagnostics/tune_full_als96_i12_lgbm63_diagnostics_by_dominant_event.csv`
- `data/models/week6/diagnostics/tune_full_als96_i12_lgbm63_diagnostics_by_activity_bin.csv`
- `data/models/week6/diagnostics/tune_full_als96_i12_lgbm63_diagnostics_by_recent_bin.csv`

전체 잔차:

| users | Two-Stage `NDCG@10` | ALS `NDCG@10` | 차이 | Two-Stage `Recall@100` | ALS `Recall@100` | 차이 |
|---:|---:|---:|---:|---:|---:|---:|
| 321,124 | 0.012663 | 0.007034 | +0.005629 | 0.069246 | 0.050114 | +0.019132 |

dominant event별:

| dominant event | users | Two-Stage `NDCG@10` | ALS `NDCG@10` | 차이 | Two-Stage `Recall@100` | ALS `Recall@100` | 차이 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `watch` | 64,276 | 0.031774 | 0.010909 | +0.020865 | 0.166729 | 0.092367 | +0.074362 |
| `fork` | 5,830 | 0.033130 | 0.013447 | +0.019683 | 0.175458 | 0.094631 | +0.080827 |
| `comment` | 8,740 | 0.013325 | 0.007637 | +0.005688 | 0.040954 | 0.024142 | +0.016813 |
| `pr` | 13,443 | 0.009712 | 0.005752 | +0.003960 | 0.028703 | 0.017953 | +0.010750 |
| `issue` | 7,005 | 0.007499 | 0.004441 | +0.003058 | 0.028378 | 0.014404 | +0.013974 |
| `push` | 125,701 | 0.003677 | 0.001869 | +0.001808 | 0.009387 | 0.005383 | +0.004004 |
| `none` | 96,129 | 0.011122 | 0.011122 | 0.000000 | 0.087114 | 0.087114 | 0.000000 |

해석:

- watch/fork 중심 유저는 reranker 이득이 가장 크다. explicit interest 성격의 event는 현재 feature와 ALS 후보가 잘 맞는다.
- push 중심 유저는 가장 큰 세그먼트지만 절대 성능이 낮다. `NDCG@10 = 0.003677`, `Recall@100 = 0.009387`이라 전체 품질의 병목이다.
- PR/comment/issue 중심 유저는 Two-Stage가 ALS보다 이기긴 하지만 절대 hit가 낮다. 단순 weight 조정보다 후보 source 보강이 더 유망하다.
- `none`은 history가 없는 cold/fallback 성격이라 Two-Stage와 ALS가 동일 추천을 내며 잔차가 0이다.
- activity bin 기준으로는 `mid_low`, `mid_high`에서 이득이 크고, high activity는 이득이 작다. high activity는 push 비중이 높아 노이즈가 커진다.
- recent bin 기준으로는 high recent도 이득은 있지만 push 비중이 높다. "최근 활동이 많다"가 곧 좋은 추천 신호는 아니다.

이후 실험 결과:

1. source cap / segment-aware hybrid
   - 작은 screening에서 실패했다.
   - recent 후보를 줄여도 push-heavy 유저의 `NDCG@10`은 개선되지 않았다.
   - 따라서 병목은 "recent가 너무 많다"가 아니라 "정답 후보 source가 부족하다"에 가깝다.
2. PR/comment/issue/push 유저용 후보 source 추가
   - item-to-item related repo 후보 source를 추가한 full run이 새 최고 성능을 냈다.
   - `related80_anchor20_full_als96_i12_lgbm63`: `NDCG@10 = 0.016029`, `Recall@100 = 0.074161`.
3. 남은 다음 후보: event interaction feature 추가
   - `user_push_share * item_push_share`
   - `user_pr_share * item_pr_share`
   - `user_watch_share * item_watch_share`
   - `dominant_event` 또는 event entropy bucket

### Source cap screening

전체 diagnostics에서 push/high-activity 유저의 top-10에 recent 후보가 많이 섞이는 패턴이 보여 source cap을 작은 screening으로 확인했다.

`scripts/week6_two_stage_v2.py`에 다음 옵션을 추가했다.

- `--recent-candidate-cap`
- `--popular-candidate-cap`

기본값은 `None`이라 기존 동작과 같다.

공통 설정:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --sample-ratio 0.04 \
  --max-items 80000 \
  --candidate-k 160 \
  --hybrid-extra 80 \
  --rank-users 6000 \
  --eval-users 5000 \
  --qual-users 50 \
  --factors 48 \
  --iterations 8 \
  --als-regularization 0.01 \
  --lgbm-num-leaves 31 \
  --lgbm-min-child-samples 50 \
  --save-user-diagnostics
```

결과:

| run | 변경 | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` | 판단 |
|---|---|---:|---:|---:|---:|---|
| `sourcecap_screen_baseline` | 기존 방식, 사실상 recent 최대 80 | 0.013402 | 0.018337 | 0.047124 | 2,657 | 기준 |
| `sourcecap_screen_recent40_pop40` | recent 40, popular 40 | 0.012142 | 0.017384 | 0.045784 | 2,626 | 하락 |
| `sourcecap_screen_recent0_pop80` | recent 0, popular 80 | 0.012279 | 0.017368 | 0.046184 | 2,668 | 하락 |
| `sourcecap_screen_als200_recent40` | ALS 200, recent 40 | 0.011399 | 0.016181 | 0.046184 | 2,777 | 하락 |

segment 관찰:

- baseline에서 push dominant 유저의 top-10 recent source 비중은 `0.896`으로 매우 높다.
- 그러나 recent를 줄여도 push dominant `NDCG@10`은 개선되지 않았다.
  - baseline push `NDCG@10 = 0.000427`
  - recent40/pop40 push `NDCG@10 = 0.000364`
  - recent0/pop80 push `NDCG@10 = 0.000282`
- issue/comment/fork 일부 세그먼트는 cap에서 약간 좋아졌지만, 표본이 작고 전체 평균 손실을 상쇄하지 못했다.
- ALS 후보를 200으로 늘리고 recent를 40으로 줄인 설정도 평균 성능이 가장 낮았다.

해석:

- 단순히 recent 후보를 줄이거나 popular로 대체하는 것은 답이 아니다.
- push-heavy 유저의 문제는 recent 후보 과다 투입만이 아니라, 현재 후보 source 자체가 해당 유저군의 다음 repo를 잘 포착하지 못하는 쪽에 가깝다.
- source cap은 full run 후보에서 제외한다.
- 이 결론에 따라 PR/comment/issue/push 유저용 후보 source 보강으로 넘어갔다.
  - 같은 owner/org
  - same language/topic
  - co-contribution item-to-item
  - maintainer가 같이 활동한 repo graph

### Related candidate source screening/full

source cap이 실패했기 때문에 후보 source 자체를 보강했다. 이미 만들어둔 `data/models/week6/item2item_related_latest.parquet`를 읽어, 유저가 history에서 본 repo를 anchor로 삼고 같이 등장한 related repo를 후보에 추가했다.

`scripts/week6_two_stage_v2.py`에 다음 옵션을 추가했다.

- `--related-candidate-cap`
- `--related-top-per-anchor`
- `--related-max-seen-anchors`
- `--related-path`

related 후보는 source id `4`로 저장하고, ranker 피처에는 `source_is_related`와 top-k source mix의 `top{k}_source_related_share`를 추가했다. full run에서는 유저별 anchor를 최대 20개로 제한해 unbounded related merge 병목을 피했다.

screening 결과:

| run | 변경 | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` | 판단 |
|---|---|---:|---:|---:|---:|---|
| `sourcecap_screen_baseline` | related 없음 | 0.013402 | 0.018337 | 0.047124 | 2,657 | 기준 |
| `related_source_screen_related40` | related cap 40, anchor 제한 없음 | 0.014553 | 0.020022 | 0.050933 | 6,369 | 개선 |
| `related_source_screen_related80` | related cap 80, anchor 제한 없음 | 0.015100 | 0.020659 | 0.050933 | 6,368 | screening 최고 |
| `related_source_screen_related80_anchor20` | related cap 80, anchor 20 | 0.014879 | 0.020271 | 0.050973 | 6,269 | full 후보 |

full run:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --max-items 300000 \
  --candidate-k 300 \
  --hybrid-extra 200 \
  --related-candidate-cap 80 \
  --related-top-per-anchor 10 \
  --related-max-seen-anchors 20 \
  --rank-users 100000 \
  --eval-users 10000000 \
  --qual-users 300 \
  --factors 96 \
  --iterations 12 \
  --als-regularization 0.03 \
  --lgbm-num-leaves 63 \
  --lgbm-min-child-samples 50 \
  --lgbm-colsample 0.85 \
  --save-user-diagnostics \
  --output-suffix related80_anchor20_full_als96_i12_lgbm63
```

| run | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` |
|---|---:|---:|---:|---:|
| `tune_full_als96_i12_lgbm63_diagnostics` | 0.012663 | 0.018947 | 0.069246 | 21,527 |
| `related80_anchor20_full_als96_i12_lgbm63` | 0.016029 | 0.022487 | 0.074161 | 94,926 |

segment 변화:

| segment | 기존 `NDCG@10` | related `NDCG@10` | 기존 `Recall@100` | related `Recall@100` |
|---|---:|---:|---:|---:|
| push | 0.003677 | 0.009922 | 0.009387 | 0.018298 |
| PR | 0.009712 | 0.020649 | 0.028703 | 0.046887 |
| comment | 0.013325 | 0.022093 | 0.040954 | 0.057805 |
| issue | 0.007499 | 0.015000 | 0.028378 | 0.039080 |
| high activity | 0.006289 | 0.011259 | 0.023721 | 0.031316 |

해석:

- 잔차 분석에서 약했던 push/PR/comment/issue/high-activity segment를 related source가 직접 개선했다.
- top-10 related source 비중은 push `0.198`, PR `0.221`, comment `0.324`, issue `0.188`, high activity `0.237`이다.
- coverage도 `unique@100`이 21,527에서 94,926으로 크게 늘었다.
- 결론적으로 이번 병목은 source cap이나 event weight보다 후보 source recall 문제였다. 강의 결론은 "후보 생성 source를 늘리면 reranker가 쓸 수 있는 정답 후보가 늘어난다"로 잡는다.

## 추가 피처 방향

새 ranker에는 다음 방향의 피처를 추가했다.

- 최근성: 최근 14일 인기가 이전 기간보다 얼마나 늘었는지
- 개인화: 유저가 과거에 본 repo들과 후보 repo가 얼마나 가까운지
- 행동 패턴: watch 중심 유저인지, PR 중심 유저인지
- 후보 출처: ALS 후보인지, 최근 인기 후보인지, 전체 인기 후보인지

강의 포인트:

- ALS score 하나만 쓰면 collaborative filtering 점수에 의존한다.
- 피처를 잘 만들면 LGBM 같은 비교적 단순한 모델도 강력해진다.
- feature engineering은 추천시스템에서 여전히 중요한 작업이다.

## DL ranker 맛보기

이번 주 강의에서는 딥러닝 추천 모델을 깊게 다루지 않고, "reranker를 GBDT 대신 neural model로 바꿀 수도 있다"는 정도만 소개한다. 그래서 별도 full tuning보다는 같은 후보셋 위에서 FM, Deep&Wide, DeepFM을 가볍게 비교했다.

비교 구조:

```text
GitHub event
-> user-repo feedback matrix
-> ALS로 후보 repo 생성
-> 같은 candidate set을 여러 ranker가 재정렬
   - LGBM LambdaRank
   - FM
   - Deep&Wide
   - DeepFM
```

강의 포인트:

- FM은 sparse feature 사이의 2차 상호작용을 factor로 학습한다.
- Deep&Wide는 선형/수작업 피처의 강점과 MLP의 비선형 표현을 같이 쓴다.
- DeepFM은 FM의 feature interaction과 deep network를 결합한다.
- 다만 짧은 학습과 약한 튜닝만으로는 잘 만든 LGBM reranker를 바로 이기기 어렵다.

중간 스케일 실험 설정:

- history: `2026-03-28` ~ `2026-04-24`
- rank-label: `2026-04-25` ~ `2026-05-01`
- test: `2026-05-02` ~ `2026-05-08`
- retained catalog: `100,000` repos
- 평가 유저: `30,000`
- 후보: `candidate_k=200`, `hybrid_extra=100`
- neural 학습: CPU, 3 epochs
- cache 생성: 8.08분
- cache 재사용 학습/평가: 3.00분

결과:

| 모델 | NDCG@10 | NDCG@50 | NDCG@100 | 해석 |
|---|---:|---:|---:|---|
| Popularity | 0.00417 | 0.01167 | 0.01477 | 단순 인기 기준선 |
| ALS/Fallback | 0.00952 | 0.01571 | 0.01908 | 개인화 retrieval만 사용 |
| Two-Stage/LGBM | 0.01455 | 0.02229 | 0.02574 | 현재 비교 1위 |
| FM | 0.01255 | 0.02023 | 0.02396 | ALS보다 좋지만 LGBM보다 낮음 |
| Deep&Wide | 0.01342 | 0.02101 | 0.02479 | neural 중 가장 좋음 |
| DeepFM | 0.01316 | 0.02084 | 0.02451 | Deep&Wide와 비슷한 수준 |

해석:

- popularity < ALS < neural reranker < LGBM reranker 흐름이 나온다.
- neural 모델도 후보 재정렬에는 효과가 있다.
- 하지만 이번 강의 범위에서는 LGBM two-stage를 주력 모델로 두고, DL ranker는 다음 단계 확장 아이디어로 소개하는 편이 낫다.
- neural 모델을 제대로 비교하려면 epoch, learning rate, embedding size, negative sampling, candidate size를 더 튜닝해야 한다.

산출물:

| 파일 | 경로 |
|---|---|
| 결과 CSV | `data/models/week6/week6_ranker_compare_hist28_mid_e3_metrics.csv` |
| 요약 JSON | `data/models/week6/week6_ranker_compare_hist28_mid_e3_summary.json` |
| 실행 로그 | `docs/week6_neural_ranker_run_log.md` |
| 캐시 실행 계획 | `docs/week6_neural_ranker_cache_plan.md` |

## 연관 추천과 트렌디 repo

개인화 추천 외에 두 가지 산출물을 만들었다.

| 산출물 | 경로 | 용도 |
|---|---|---|
| 연관 repo | `data/models/week6/item2item_related_latest.parquet` | "이 repo와 같이 등장한 repo" 추천 |
| 트렌디 repo | `data/models/week6/trendy_repos_latest.parquet` | 최근 history window에서 상승한 repo 찾기 |
| 정성 평가 case | `data/models/week6/week6_related_cases_latest.parquet` | 강의/dashboard 예시 |

역할 차이:

- Two-stage 추천: 유저별 개인화 추천
- Item-to-item 추천: repo 기준 연관 추천
- Trendy repo: 전체 기준 최근 상승 repo

## 실행 커맨드

기준 two-stage 실행:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --max-items 300000 \
  --candidate-k 300 \
  --hybrid-extra 200 \
  --rank-users 100000 \
  --eval-users 10000000 \
  --qual-users 1000 \
  --factors 64 \
  --iterations 12
```

현재 최고 full run:

```bash
uv run python scripts/week6_two_stage_v2.py --best-full
```

`--best-full`은 `related80_anchor20_full_als96_i12_lgbm63` 설정을 적용한다. 핵심 설정은 `max_items=300000`, `candidate_k=300`, `hybrid_extra=200`, `related_candidate_cap=80`, `related_top_per_anchor=10`, `related_max_seen_anchors=20`, `factors=96`, `iterations=12`, `als_regularization=0.03`, `lgbm_num_leaves=63`, `lgbm_min_child_samples=50`, `lgbm_colsample=0.85`, `save_user_diagnostics=True`다.

연관 추천/트렌디 repo 생성:

```bash
uv run python scripts/week6_item2item_trends.py \
  --history-start 2026-03-14 \
  --history-end 2026-04-24 \
  --max-items 300000 \
  --max-users 300000 \
  --max-items-per-user 30 \
  --related-top-k 50 \
  --trendy-top-n 5000 \
  --output-suffix latest
```

정성 평가 dashboard:

```bash
uv run streamlit run app_week6_qual_eval.py \
  --server.port 8502 \
  --server.address 127.0.0.1
```

## 다음 실험

완료:

- related source를 기본 모델로 승격했다.
- `app_week6_qual_eval.py`는 기본 artifact로 `related80_anchor20_full_als96_i12_lgbm63`를 선택한다.
- `scripts/week6_two_stage_v2.py --best-full`은 현재 최고 full-run 설정을 재현한다.
- `scripts/week6_analyze_user_diagnostics.py`는 suffix 생략 시 현재 최고 diagnostics를 분석한다.

다음 우선순위:

1. Event interaction feature 추가
2. owner/org, language/topic, maintainer graph 후보 source 비교
3. `max_items=500000`
4. `candidate_k=500`
5. history window 56일/70일

피처-only ablation 예시:

```bash
uv run python scripts/week6_two_stage_v2.py \
  --max-items 300000 \
  --candidate-k 300 \
  --hybrid-extra 200 \
  --rank-users 100000 \
  --eval-users 10000000 \
  --qual-users 300 \
  --factors 64 \
  --iterations 12 \
  --als-regularization 0.01 \
  --lgbm-num-leaves 31 \
  --output-suffix feature_only_like_latest
```

## 주의사항

- `data/`는 gitignored라 산출물은 local only다.
- metadata가 split 시점 snapshot이 아니면 누수 가능성이 있다.
- 현재 평가는 rank-label 기간을 seen item으로 필터링하지 않는다.
- smoke 결과는 변동성이 크므로 강의 결론에는 full run 기준 수치를 사용한다.
