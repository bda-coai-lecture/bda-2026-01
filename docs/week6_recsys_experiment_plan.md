# Week 6 추천시스템 실험 계획

최종 수정: 2026-05-10

상세 설명은 `docs/week6_recsys_handoff.md`를 본다. 이 문서는 다음 실험을 놓치지 않기 위한 짧은 체크리스트다.

## 현재 기준

- 기준선: `latest`
- 현재 최고 run: `related80_anchor20_full_als96_i12_lgbm63`
- 최고 성능: `Two-Stage/Fallback NDCG@10 = 0.016029`, `Recall@100 = 0.074161`
- 핵심 해석: 피처/튜닝 개선 위에 item-to-item related 후보 source를 추가하자 기존 최고 full run보다 top-k 품질과 coverage가 모두 크게 올랐다.
- 피처-only ablation인 `feature_only_like_latest`는 `NDCG@10 = 0.012179`로 `latest`와 거의 같았다. 즉 `tune_full_als96_i12_lgbm63`까지의 개선은 피처 단독 효과가 아니라 튜닝과 함께 해석한다.
- `--save-user-diagnostics` full 분석 결과, 기존 약점이던 push/PR/comment/issue/high-activity segment가 related source 추가로 가장 크게 개선됐다.
- 코드 기준화 완료:
  - `app_week6_qual_eval.py`는 기본 artifact를 `related80_anchor20_full_als96_i12_lgbm63`로 선택한다.
  - `scripts/week6_two_stage_v2.py --best-full`은 현재 최고 full-run 설정을 재현한다.
  - `scripts/week6_analyze_user_diagnostics.py`는 suffix 생략 시 현재 최고 diagnostics를 분석한다.

## 다음 실험 우선순위

1. event interaction feature 추가
2. same owner/org, language/topic, maintainer graph 후보 source 추가 비교
3. `max_items=500000`
4. `candidate_k=500`
5. history window 56일/70일

## Event weight ablation

현재 event weight는 합리적인 baseline이지만 최적값은 아니다.

현재 baseline:

| event | weight |
|---|---:|
| `WatchEvent` | 1.0 |
| `ForkEvent` | 2.0 |
| `IssuesEvent` | 0.5 |
| `PullRequestEvent` | 3.0 |
| `IssueCommentEvent` | 0.3 |
| `PushEvent` | 0.2 |

비교할 후보:

- 보수적 기여형: PR/Fork를 조금 낮추고 Push를 더 낮춘다.
- 활동성 강조형: Issue/Comment/Push를 조금 높인다.
- 명시적 관심 강조형: Watch/Fork 중심으로 둔다.

주의:

- weight를 바꾸면 ALS 후보 생성과 LGBM 피처가 모두 바뀐다.
- 작은 screening run으로 먼저 비교하고, 좋은 설정만 full run으로 올린다.
- offline metric만 보지 말고 추천 결과가 PR-heavy, popularity-heavy, maintainer-heavy가 되는지도 확인한다.

### 2026-05-09 screening 결과

`scripts/week6_two_stage_v2.py`에 `--event-weight EventType=value` 반복 옵션을 추가했다.

작은 screening 설정:

- `sample_ratio=0.04`
- `max_items=80000`
- `candidate_k=160`
- `hybrid_extra=80`
- `rank_users=6000`
- `eval_users=5000`
- `factors=48`
- `iterations=8`

| run | 변경 | `NDCG@10` | `NDCG@50` | `Recall@100` | 판단 |
|---|---|---:|---:|---:|---|
| `weight_screen_baseline` | 기존 weight | 0.013402 | 0.018337 | 0.047124 | 기준 |
| `weight_screen_conservative_contrib` | Fork 1.5, PR 2.4, Push 0.1 | 0.013650 | 0.020372 | 0.059456 | full 후보 |
| `weight_screen_activity` | Issue 0.8, Comment 0.6, Push 0.4 | 0.013947 | 0.019953 | 0.052885 | full 1순위 후보 |
| `weight_screen_explicit_interest` | Watch/Fork 중심, Issue/Comment/Push 낮춤 | 0.013143 | 0.020278 | 0.062461 | recall은 좋지만 top-10 약함 |

다음 액션:

- `activity` full run은 완료했다. full에서는 `NDCG@10 = 0.011931`로 `latest`보다 낮았다.
- 시간이 있으면 `conservative_contrib`를 full로 돌릴 수 있지만, 현재 최고 run 갱신 가능성은 불확실하다.
- weight screening은 방향성 확인용으로만 쓰고, 강의 결론에는 full run 기준 수치를 사용한다.

### Activity full 결과

| run | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` | runtime |
|---|---:|---:|---:|---:|---:|
| `latest` | 0.012225 | 0.018440 | 0.068104 | 17,415 | 20.48분 |
| `weight_activity_full_als64_i12_lgbm31` | 0.011931 | 0.017626 | 0.064405 | 20,746 | 42.41분 |
| `tune_full_als96_i12_lgbm63` | 0.012663 | 0.018947 | 0.069246 | 21,527 | 47.83분 |

해석: activity weight는 coverage를 늘렸지만 정확도는 낮췄다. "활동 로그를 더 세게 주면 더 다양해질 수 있지만, top-k 품질은 손해 볼 수 있다"는 사례로 볼 수 있다.

## 잔차 패턴 기반 다음 후보

`week6_qual_cases_*` 샘플로 본 탐색 결과:

- watch 중심 유저는 Two-Stage가 ALS보다 잘 맞춘다.
- push 중심 유저는 많지만 hit rate가 낮다.
- PR/comment/issue 중심 유저는 거의 못 맞춘다.
- activity full은 high-recent 유저에서 ALS보다 손해가 났다.
- Two-Stage가 이긴 케이스는 ALS source 비중과 seen/profile cosine이 높은 편이다.

진행 결과:

1. `--save-user-diagnostics` 옵션 추가
   - 완료했다.
   - 전체 eval user별 hit/ndcg, user event mix, source mix, candidate source 비중을 저장한다.
2. source cap / segment-aware hybrid
   - screening에서 실패했다.
   - recent 후보를 줄여도 push-heavy segment가 개선되지 않았다.
3. PR/comment/issue 중심 유저용 후보 source 추가
   - item-to-item related repo source를 추가했고, full run에서 새 최고 성능을 냈다.
   - 병목은 source mix 비율보다 후보 recall 부족이었다.
4. 남은 후보: interaction feature 추가
   - `user_push_share * item_push_share`
   - `user_pr_share * item_pr_share`
   - `user_watch_share * item_watch_share`
   - `dominant_event` one-hot 또는 event entropy bucket

### 전체 user diagnostics 결과

`--save-user-diagnostics` 옵션을 추가하고 당시 최고 설정으로 full run을 완료했다.

실행 suffix:

- `tune_full_als96_i12_lgbm63_diagnostics`

산출물:

- `data/models/week6/week6_user_diagnostics_tune_full_als96_i12_lgbm63_diagnostics.parquet`
- `data/models/week6/diagnostics/tune_full_als96_i12_lgbm63_diagnostics_summary.md`

전체:

| users | Two-Stage `NDCG@10` | ALS `NDCG@10` | 차이 | Two-Stage `Recall@100` | ALS `Recall@100` | 차이 |
|---:|---:|---:|---:|---:|---:|---:|
| 321,124 | 0.012663 | 0.007034 | +0.005629 | 0.069246 | 0.050114 | +0.019132 |

dominant event별 핵심:

| segment | users | `NDCG@10` 차이 | `Recall@100` 차이 | 판단 |
|---|---:|---:|---:|---|
| `watch` | 64,276 | +0.020865 | +0.074362 | 현재 reranker가 가장 잘 맞는 축 |
| `fork` | 5,830 | +0.019683 | +0.080827 | watch와 유사하게 강한 이득 |
| `comment` | 8,740 | +0.005688 | +0.016813 | 이득은 있지만 절대 recall 낮음 |
| `pr` | 13,443 | +0.003960 | +0.010750 | 후보 source 보강 필요 |
| `issue` | 7,005 | +0.003058 | +0.013974 | 후보 source 보강 필요 |
| `push` | 125,701 | +0.001808 | +0.004004 | 가장 큰 병목 세그먼트 |
| `none` | 96,129 | 0.000000 | 0.000000 | cold/fallback이라 동일 추천 |

결론:

- watch/fork 유저는 지금 구조를 유지해도 된다.
- push 유저는 수가 가장 많고 절대 성능이 낮다. event weight를 키우는 방향은 full run에서 실패했으므로 source cap이나 별도 후보 source가 먼저다.
- PR/comment/issue는 Two-Stage가 ALS보다 이기지만 정답 후보 자체가 약해 보인다. 같은 owner/org, same language, co-contribution, item-to-item 후보를 추가하는 실험이 타당하다.
- 다음 실험은 `max_items`를 키우기 전에 source composition을 제어하는 쪽이 더 해석 가능하다.

### Source cap screening 결과

단순 source cap은 작은 screening에서 실패했다.

공통 설정:

- `sample_ratio=0.04`
- `max_items=80000`
- `candidate_k + hybrid_extra = 240`
- `rank_users=6000`
- `eval_users=5000`
- `save_user_diagnostics=True`

| run | 변경 | `NDCG@10` | `NDCG@50` | `Recall@100` | 판단 |
|---|---|---:|---:|---:|---|
| `sourcecap_screen_baseline` | ALS 160 + recent 최대 80 | 0.013402 | 0.018337 | 0.047124 | 기준 |
| `sourcecap_screen_recent40_pop40` | recent 40 + popular 40 | 0.012142 | 0.017384 | 0.045784 | 하락 |
| `sourcecap_screen_recent0_pop80` | recent 0 + popular 80 | 0.012279 | 0.017368 | 0.046184 | 하락 |
| `sourcecap_screen_als200_recent40` | ALS 200 + recent 40 | 0.011399 | 0.016181 | 0.046184 | 하락 |

segment 해석:

- baseline에서 push dominant 유저의 top-10 recent source 비중은 높았지만, recent를 줄여도 push `NDCG@10`은 좋아지지 않았다.
- issue/comment/fork 일부 segment는 cap에서 조금 좋아졌지만 전체 손실이 더 컸다.
- 따라서 "recent가 많아서 문제"라는 단순 가설은 기각한다.
- 다음은 source cap full run이 아니라 후보 source 자체를 추가하는 쪽으로 간다.

### Related candidate source 결과

후보 source 보강 방향으로 `item2item_related_latest.parquet`를 ALS/recent 후보에 추가했다. history에서 본 repo를 anchor로 삼아 co-occurrence related repo를 후보에 넣고, source id `4` 및 `source_is_related` 피처를 ranker에 제공했다.

screening:

| run | 변경 | `NDCG@10` | `NDCG@50` | `Recall@100` | 판단 |
|---|---|---:|---:|---:|---|
| `sourcecap_screen_baseline` | related 없음 | 0.013402 | 0.018337 | 0.047124 | 기준 |
| `related_source_screen_related40` | related cap 40 | 0.014553 | 0.020022 | 0.050933 | 개선 |
| `related_source_screen_related80` | related cap 80 | 0.015100 | 0.020659 | 0.050933 | screening 최고 |
| `related_source_screen_related80_anchor20` | related cap 80, anchor 20 | 0.014879 | 0.020271 | 0.050973 | full 후보 |

full:

| run | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` |
|---|---:|---:|---:|---:|
| `tune_full_als96_i12_lgbm63_diagnostics` | 0.012663 | 0.018947 | 0.069246 | 21,527 |
| `related80_anchor20_full_als96_i12_lgbm63` | 0.016029 | 0.022487 | 0.074161 | 94,926 |

segment:

- push `NDCG@10`: 0.003677 -> 0.009922
- PR `NDCG@10`: 0.009712 -> 0.020649
- comment `NDCG@10`: 0.013325 -> 0.022093
- issue `NDCG@10`: 0.007499 -> 0.015000
- high activity `NDCG@10`: 0.006289 -> 0.011259

결론:

- source cap은 실패했지만 related source는 성공했다.
- 병목은 recent 비중 과다가 아니라 weak segment의 후보 recall 부족이었다.
- 다음 실험은 related source를 유지한 상태에서 interaction feature나 graph/language 기반 source를 추가 비교한다.

## 피처-only ablation

현재 `tune_full_als96_i12_lgbm63` 개선은 피처 추가와 hyperparameter 변경이 섞인 결과다. 피처만의 효과를 보려면 기존 `latest`와 hyperparameter를 최대한 맞춘 run이 필요하다.

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

비교 대상:

- `latest`: 기존 피처 + 기존 hyperparameter
- `feature_only_like_latest`: 추가 피처 + 기존에 가까운 hyperparameter
- `tune_full_als96_i12_lgbm63`: 추가 피처 + 튜닝된 hyperparameter

완료 결과:

| run | `NDCG@10` | `NDCG@50` | `Recall@100` | `unique@100` | runtime |
|---|---:|---:|---:|---:|---:|
| `latest` | 0.012225 | 0.018440 | 0.068104 | 17,415 | 20.48분 |
| `feature_only_like_latest` | 0.012179 | 0.018404 | 0.068104 | 16,962 | 41.17분 |
| `tune_full_als96_i12_lgbm63` | 0.012663 | 0.018947 | 0.069246 | 21,527 | 47.83분 |

결론: 피처-only는 거의 중립이다. `tune_full_als96_i12_lgbm63`의 개선은 피처+튜닝 묶음 효과로 설명한다.

## Catalog / candidate 실험

`max_items`와 `candidate_k`는 추천 품질과 속도의 trade-off다.

후보:

- `max_items=500000`
- `candidate_k=500`
- `hybrid_extra=300` 또는 `500`

볼 지표:

- `NDCG@10`
- `NDCG@50`
- `Recall@100`
- `unique_recommended`
- runtime

## History window 실험

현재 42일 history가 28일 history보다 좋았다. 다음은 더 긴 history를 비교한다.

후보:

- 56일
- 70일

해석 기준:

- history가 길어지면 유저 이력이 풍부해진다.
- 대신 오래된 관심사가 섞여 최근 취향과 멀어질 수 있다.
- NDCG뿐 아니라 추천 결과가 오래된 인기 repo에 치우치는지도 정성 평가한다.

## 연관 추천 / 트렌디 repo 산출물

이미 생성된 산출물:

- `data/models/week6/item2item_related_latest.parquet`
- `data/models/week6/trendy_repos_latest.parquet`
- `data/models/week6/week6_related_cases_latest.parquet`

역할:

- item-to-item: 특정 repo와 같이 등장한 repo 추천
- trendy repo: 최근 history window에서 상승한 repo 탐색
- related cases: 강의/dashboard 예시

재생성 커맨드:

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

## 결과 기록 규칙

새 실험을 돌리면 `docs/week6_recsys_handoff.md`에 다음만 추가한다.

- 실행 command
- 핵심 설정
- runtime
- `NDCG@10`, `NDCG@50`, `Recall@100`, `unique_recommended@100`
- 기존 최고 run 대비 개선/하락
- 정성적으로 이상한 추천이 있었는지
