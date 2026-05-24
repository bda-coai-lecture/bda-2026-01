# Recsys V2 Full Users Rerank Handoff

## Metric Priority

- Reranker: NDCG@100 중심
- Candidate generation: Recall 중심

## Performance Summary

### Current Best Result

Overnight retrieval-side experiments changed the best practical baseline. The strongest
policy-compatible run is now a hybrid candidate cache plus the existing LGBM n20 reranker:

```text
candidate cache: retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0
ranker:          existing LGBM n20 norel
eval suffix:     retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_lgbm_n20_t1

LGBM-on-hybrid @100:
recall@100: 0.021773
NDCG@100:   0.007082
```

Comparison to previous baselines:

```text
model / candidate policy             recall@100  NDCG@100
LGBM n20 on ALS candidates              0.012644  0.004459
ALS retrieval ordering                  0.009527  0.003609
Hybrid ALS30+related140+recent30        0.020927  0.006098
LGBM n20 on that hybrid candidate set   0.021773  0.007082
```

Interpretation: the biggest gain came from candidate generation/source ordering, not from
a new neural reranker. The old LGBM n20 model still helps once it is applied to the stronger
hybrid candidate set.

Important evaluation note: the first LGBM-on-hybrid eval crashed with exit code `139`.
The successful run used single-threaded numeric libraries and a smaller prediction batch:

```text
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
--predict-batch-size 32768
```

### Candidate Generation

Original candidate generator was ALS retrieval from:

```text
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502.parquet
```

Held-out test metrics are identical across reranker eval files because all full reranker runs use the same candidate cache:

```text
ALS Retrieval @10:  recall 0.003556, NDCG 0.002268, unique_recommended 3,719
ALS Retrieval @50:  recall 0.007347, NDCG 0.003207, unique_recommended 6,852
ALS Retrieval @100: recall 0.009527, NDCG 0.003609, unique_recommended 9,248
ALS Retrieval @200: recall 0.012445, NDCG 0.004077, unique_recommended 13,044
```

Original candidate generation bottleneck:

```text
candidate recall@100 = 0.009527
candidate recall@200 = 0.012445
```

Interpretation: rerankers could only reorder this ALS candidate set. The best original
reranker improved observed recall@100 from `0.009527` to `0.012644` by moving relevant
candidates upward, but the candidate-set/source policy was the real bottleneck.

Hybrid candidate generation results:

```text
candidate source policy                  recall@50  NDCG@50  recall@100  NDCG@100  recall@200  NDCG@200
ALS baseline                              0.007347 0.003207    0.009527  0.003609    0.012445  0.004077
Hybrid related80 recent20 popular20       0.007347 0.003207    0.019369  0.005377    0.024291  0.006174
Hybrid ALS50 related120 recent20 pop10    0.007347 0.003207    0.020407  0.005749    0.023519  0.006250
Hybrid ALS30 related140 recent30 pop0     0.016751 0.005316    0.020927  0.006098    0.023854  0.006570
```

Hybrid cache paths:

```text
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_hybrid_related80_recent20_pop20.parquet
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_hybrid_als50_related120_recent20_pop10.parquet
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0.parquet
```

Hybrid caveats:

```text
1. Retrieval-only hybrid metrics use -candidate_rank, so they measure source ordering policy.
2. Final hybrid parquet intentionally keeps the standard 5 columns only; source counts are in the summary JSON.
3. The current best hybrid policy keeps ALS top 30, inserts related/recent candidates, then fills with ALS tail.
```

### Reranker Leaderboard

Primary comparison is `NDCG@100`.

```text
model              recall@100  NDCG@100  vs ALS NDCG  vs LGBM best
LGBM n20 norel       0.012644  0.004459  +0.000851    baseline
LGBM hard100         0.012613  0.004291  +0.000683    -0.000168
ALS Retrieval        0.009527  0.003609  baseline     -0.000851
Deep&Wide ID Emb     0.009397  0.003026  -0.000583    -0.001433
Deep&Wide BCE        0.009326  0.002910  -0.000699    -0.001549
Deep&Wide BPR        0.007594  0.002800  -0.000808    -0.001659
DLRM                 0.009052  0.002183  -0.001425    -0.002276
DeepFM               0.007357  0.001815  -0.001794    -0.002644
FM                   0.003248  0.000666  -0.002942    -0.003793
LGBM n5 norel        0.001580  0.000651  -0.002957    -0.003808
```

Result:

```text
best original ALS-candidate reranker: LGBM n20 norel, NDCG@100 = 0.004459
best current end-to-end run:          LGBM n20 on hybrid candidates, NDCG@100 = 0.007082
best neural reranker:                 Deep&Wide ID Emb, NDCG@100 = 0.003026
```

### Training Data Variants

```text
variant        rows       positives  negatives  groups   NDCG@100
n20 norel      8,119,509    459,303  7,660,206  261,796  0.004459
n20 hard100    8,229,354    459,303  7,770,051  261,796  0.004291
n5 norel       2,512,014    459,303  2,052,711  261,671  0.000651
```

Negative sampling notes:

```text
n20 norel:   hard 3,478,540, popular 2,636,487, random 1,545,179
n20 hard100: hard 3,588,530, popular 2,636,391, random 1,545,130
n5 norel:    hard   802,168, popular   807,122, random   443,421
```

All three variants used unlabeled sampled negatives, excluded history/rank-label positives, did not use test positives for training, and had `rank_label_positive_coverage = 1.0`.

### Takeaway

```text
1. Current production-quality baseline is LGBM n20 norel on hybrid ALS30+related140+recent30 candidates.
2. More hard negatives alone did not help: hard100 reduced shortfall but NDCG@100 dropped 0.004459 -> 0.004291.
3. Current dense neural rankers do not beat ALS retrieval, let alone LGBM.
4. Simple BPR objective alignment did not improve Deep&Wide.
5. Retrieval/source mixing gave the largest gain: LGBM-on-hybrid NDCG@100 reached 0.007082.
6. Next meaningful work is hybrid source-feature reranking, repeat-policy decision, or improved filtered two-tower retrieval.
```

## Best Current Production Candidate

Suffix:

```text
retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_lgbm_n20_t1
```

Key files:

```text
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0.parquet
data/models/recsys_v2/retrieval_als_retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_summary.json
data/models/recsys_v2/ranker_lgbm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.pkl
data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_lgbm_n20_t1.csv
data/results/recsys_v2/eval_retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_lgbm_n20_t1_summary.json
```

Metrics:

```text
Hybrid retrieval-only
recall@50:  0.016751
NDCG@50:    0.005316
recall@100: 0.020927
NDCG@100:   0.006098
recall@200: 0.023854
NDCG@200:   0.006570

LGBM Re-rank on hybrid
recall@50:  0.015261
NDCG@50:    0.005853
recall@100: 0.021773
NDCG@100:   0.007082
recall@200: 0.025229
NDCG@200:   0.007666
```

The LGBM reranker reduces recall@50 slightly versus raw hybrid ordering but improves
NDCG@50/100/200 and recall@100/200. It is the current best NDCG@100 result.

## Source Feature Rerank Attempt

Tried preserving hybrid candidate source metadata and retraining LGBM n20 with:

```text
candidate_source_code
source_rank
source_score
```

Implementation suffixes:

```text
candidate cache:
retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_srcfeat

first LGBM attempt, flawed positive fallback:
retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_srcfeat_n20_norel

fixed fallback attempt:
retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_srcfeat2_n20_norel
```

The first attempt was invalid as a useful candidate because missing positive candidate
metadata used `rank_label_positive` as `candidate_source_code`. That source code never
appears at evaluation time, so the ranker learned an out-of-distribution label shortcut.

After fixing missing positive candidate source fallback to the hard retrieval default, the
source-feature LGBM still did not beat the existing best:

```text
model                                      recall@100  NDCG@100
Hybrid retrieval-only                        0.020927  0.006098
Existing LGBM n20 on hybrid candidates       0.021773  0.007082
Source-feature LGBM n20 fixed fallback       0.017873  0.006285
```

Interpretation: source metadata is worth preserving for observability and future model
variants, but simply adding these columns to the current LGBM training setup is not enough.
The current best remains the existing LGBM n20 model applied to the hybrid candidate cache.

## Best Original ALS-Candidate Reranker

Suffix:

```text
retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel
```

Key files:

```text
data/features/recsys_v2/rerank_train_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.parquet
data/features/recsys_v2/rerank_train_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_summary.json
data/models/recsys_v2/ranker_lgbm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.pkl
data/models/recsys_v2/ranker_lgbm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_summary.json
data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.csv
data/results/recsys_v2/eval_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_summary.json
```

## Best Run Summary

```text
train rows: 8,119,509
positives: 459,303
negatives: 7,660,206
groups: 261,796
positive_rate: 0.056568
```

Group invariants:

```text
max_group_rows: 8,542
groups_without_positive: 0
groups_with_multiple_actors: 0
duplicate_user_repo: 0
```

## Best Original Metrics

```text
ALS Retrieval
recall@100: 0.009527
NDCG@100:   0.003609

LGBM Re-rank n20
recall@100: 0.012644
NDCG@100:   0.004459
```

Original reranker baseline to beat:

```text
LGBM n20 NDCG@100 = 0.004459
```

Original candidate generation baseline:

```text
candidate recall@100 = 0.009527
candidate recall@200 = 0.012445
```

## Other Runs

### Neural rankers on n20 norel

All neural rankers below use the same `n20_norel` train parquet:

```text
data/features/recsys_v2/rerank_train_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.parquet
```

Safe local training setting:

```text
--device cpu --torch-threads 1 --batch-size 32768 --epochs 3
```

Do not use `--torch-threads 4` for the full FM run on this machine. It crashed at the first full epoch with exit code `139` after `4.51s`; max RSS was only `3.73GB`, so this looked like a torch/threading crash rather than memory OOM.

#### FM

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_fm
model: data/models/recsys_v2/ranker_fm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_fm.pkl
metrics: data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_fm.csv
```

Metrics:

```text
FM Re-rank recall@100: 0.003248
FM Re-rank NDCG@100:   0.000666
```

Runtime:

```text
trainer: 132.26s, max RSS 4.04GB
eval:    603.85s, max RSS 7.62GB
```

FM is much weaker than LGBM and also weaker than ALS retrieval.

#### Deep&Wide

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide
model: data/models/recsys_v2/ranker_deepwide_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide.pkl
metrics: data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide.csv
```

Metrics:

```text
Deep&Wide Re-rank recall@100: 0.009326
Deep&Wide Re-rank NDCG@100:   0.002910
```

Runtime:

```text
trainer: 179.38s, max RSS 4.05GB
eval:    624.38s, max RSS 9.47GB
```

Deep&Wide is much better than FM, but still below both ALS retrieval NDCG@100 `0.003609` and LGBM NDCG@100 `0.004459`.

#### Deep&Wide BPR

Added a pairwise BPR objective to `scripts/recsys_neural_rankers.py` / `scripts/recsys_train_rerank_v2.py` via:

```text
--neural-loss bpr
```

Full n20 run:

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide_bpr
model: data/models/recsys_v2/ranker_deepwide_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide_bpr.pkl
metrics: data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide_bpr.csv
```

Metrics:

```text
Deep&Wide BPR Re-rank recall@100: 0.007594
Deep&Wide BPR Re-rank NDCG@100:   0.002800
```

Runtime:

```text
trainer: 23.98s, max RSS 4.31GB
eval:    642.50s, max RSS 7.68GB
```

BPR used all positives with in-group negatives:

```text
pairwise_positive_count: 459,303
pairwise_skipped_positive_count: 0
pairwise_groups_with_negatives: 261,796
```

Conclusion: simple pairwise BPR improved objective alignment but did not improve quality. It is below BCE Deep&Wide NDCG@100 `0.002910`, ALS retrieval `0.003609`, and LGBM `0.004459`.

#### DeepFM

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepfm
model: data/models/recsys_v2/ranker_deepfm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepfm.pkl
metrics: data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepfm.csv
```

Metrics:

```text
DeepFM Re-rank recall@100: 0.007357
DeepFM Re-rank NDCG@100:   0.001815
```

Runtime:

```text
trainer: 185.24s, max RSS 4.04GB
eval:    620.92s, max RSS 8.09GB
```

DeepFM underperformed Deep&Wide and did not beat ALS retrieval or LGBM.

#### DLRM

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_dlrm
model: data/models/recsys_v2/ranker_dlrm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_dlrm.pkl
metrics: data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_dlrm.csv
```

Metrics:

```text
DLRM Re-rank recall@100: 0.009052
DLRM Re-rank NDCG@100:   0.002183
```

Runtime:

```text
trainer: 298.73s, max RSS 4.06GB
eval:    748.13s, max RSS 7.61GB
```

DLRM was the slowest neural run and still below Deep&Wide.

#### Deep&Wide ID Embeddings

Added `deepwide_idemb`: dense features + `actor_id` embedding + `repo_id` embedding Deep&Wide. It keeps the existing dense features and standardization path, so it can be evaluated on the same candidate set as LGBM. Train-time ID vocabs are encoded as `1..N`; unknown eval actor/repo IDs map to `0`. Eval summaries now record `id_embedding_eval_unknowns` for ID embedding models.

Implementation notes:

```text
ranker: deepwide_idemb
unknown eval ids: 0
threading: configure_torch_threads() mitigates PyTorch interop thread reset issues
BPR: mechanically wired and tiny-smoke passed, but full-scale quality/stability is unverified
```

Tiny smoke result only, not comparable to full runs:

```text
BCE train rows: 45
groups: 13
positive_rate: 0.3111
eval: success, but tiny-smoke NDCG/recall were 0.0
BPR tiny loss: 0.805803
BPR skipped positives: 0
```

Full `deepwide_idemb` run on `n20_norel` completed. It improves slightly over dense Deep&Wide BCE, but still does not beat ALS retrieval or LGBM.

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide_idemb
model: data/models/recsys_v2/ranker_deepwide_idemb_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide_idemb.pkl
metrics: data/results/recsys_v2/eval_metrics_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel_deepwide_idemb.csv
```

Metrics:

```text
Deep&Wide ID Emb Re-rank recall@100: 0.009397
Deep&Wide ID Emb Re-rank NDCG@100:   0.003026
```

Comparison:

```text
vs dense Deep&Wide BCE NDCG@100 0.002910: +0.000116
vs ALS retrieval NDCG@100      0.003609: -0.000583
vs LGBM n20 NDCG@100           0.004459: -0.001433
```

Training summary:

```text
rows: 8,119,509
groups: 261,796
positive_rate: 0.056568
actor_id vocab: 261,631
repo_id vocab: 180,318
id_embedding_dim: 32
losses: 0.485152 -> 0.265971 -> 0.241122
eval unknown actor rows: 0
eval unknown repo rows: 0
```

Runtime:

```text
trainer: 473.41s, max RSS 4.64GB
eval:    1190.52s, max RSS 6.91GB
```

Interpretation: ID embeddings are wired correctly, and eval had no unknown actor/repo IDs. The weak result is not an unknown-ID problem. This is still below `LGBM n20 NDCG@100 = 0.004459`, `ALS candidate recall@100 = 0.009527`, and `ALS retrieval NDCG@100 = 0.003609`.

Neural model comparison at `@100`:

```text
FM:        recall 0.003248, NDCG 0.000666
ID Emb D&W: recall 0.009397, NDCG 0.003026
Deep&Wide: recall 0.009326, NDCG 0.002910
BPR D&W:   recall 0.007594, NDCG 0.002800
DeepFM:    recall 0.007357, NDCG 0.001815
DLRM:      recall 0.009052, NDCG 0.002183
```

Current conclusion:

```text
Best neural ranker: Deep&Wide ID Emb NDCG@100 = 0.003026
Pairwise BPR Deep&Wide did not improve over BCE Deep&Wide.
Still below ALS retrieval NDCG@100 = 0.003609
Still below LGBM NDCG@100 = 0.004459
```

### Two-tower retrieval experiments

`scripts/recsys_train_retrieval_v2.py` now supports:

```text
--retriever als|two_tower
--embedding-dim
--epochs
--temperature
--keep-rank-label-items
```

Two-tower uses user/item embeddings with normalized dot product and in-batch cross entropy.
The default candidate filtering removes both history and rank-label items so it is comparable
to the original ALS policy. `--keep-rank-label-items` keeps rank-label items eligible and
therefore measures a repeat-friendly policy, not the original baseline policy.

Results:

```text
run                              recall@10  NDCG@10  recall@100  NDCG@100  recall@300  NDCG@300
TwoTower d64 e3 keeptrain         0.017974 0.018415    0.019752  0.018739    0.021097  0.018934
TwoTower d128 e5 keeptrain        0.019564 0.019537    0.022033  0.020016    0.023757  0.020271
TwoTower d64 e3 filtertrain       0.003329 0.002943    0.004982  0.003342    0.006313  0.003548
```

Interpretation:

```text
1. The keeptrain numbers are very strong but not apples-to-apples with ALS/LGBM.
2. The filtered run is apples-to-apples and is below ALS recall@100 0.009527.
3. This exposes a product-policy question: should repeated train-period repos be recommendable?
4. If repeat recommendation is allowed, two-tower retrieval is the strongest ranking signal seen so far.
5. If repeat recommendation is not allowed, hybrid ALS+related/recent is the current best path.
```

Known two-tower limitations:

```text
in-batch accidental negatives
no item bias / popularity prior
no source features
no sampled-softmax or explicit hard negative objective yet
```

Key candidate caches:

```text
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_twotower_d64_e3.parquet
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_twotower_d128_e5_keeptrain.parquet
data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_twotower_d64_e3_filtertrain.parquet
```

### Next experiments

Highest value next steps:

```text
1. Preserve candidate_source_code/source_rank in hybrid candidate parquet and train LGBM with source features.
2. Run a small hybrid sweep around ALS head 20-50, related 120-160, recent 20-40, popular 0-10.
3. Improve filtered two-tower with item bias/popularity prior and sampled negatives.
4. If repeat recommendations are allowed, formalize keeptrain two-tower evaluation as a separate policy track.
```

### n5 norel

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n5_norel
rows: 2,512,014
negatives: 2,052,711
LGBM NDCG@100: 0.000651
```

Too weak. Not the baseline.

### n20 hard100 norel

```text
suffix: retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_hard100_norel
rows: 8,229,354
negatives: 7,770,051
LGBM NDCG@100: 0.004291
```

Hard negatives increased, but performance dropped vs n20:

```text
NDCG@100: 0.004459 -> 0.004291
```

Do not use as best baseline.

## Shortfall Notes

Best n20 shortfall:

```text
negative_requested: 9,186,060
negative_count:     7,660,206

retrieval_hard shortfall: 1,114,490
popular_recent shortfall:   201,695
random_catalog shortfall:   209,669
```

`hard100` reduced hard shortfall by `109,990`, but hurt NDCG@100. More hard negatives alone is not clearly useful.

## Runtime

Timing was measured with `/usr/bin/time -l`.

### Best n20 norel

```text
sampler: 464.45s, max RSS 5.08GB
trainer: 101.52s, max RSS 2.11GB
eval:    526.56s, max RSS 6.65GB
```

Grouped view:

```text
training total: 565.97s
inference/eval: 526.56s
end-to-end total: 1,092.53s
```

### n20 hard100 norel

```text
sampler: 461.36s, max RSS 5.39GB
trainer: 92.55s, max RSS 2.10GB
eval:    528.82s, max RSS 7.59GB
```

Grouped view:

```text
training total: 553.91s
inference/eval: 528.82s
end-to-end total: 1,082.73s
```

## Code State

Main scripts:

```text
scripts/recsys_sample_rerank_data.py
scripts/recsys_train_rerank_v2.py
scripts/recsys_eval_v2.py
scripts/recsys_neural_rankers.py
```

Important changes:

- sampler now streams/chunks parquet writes
- `--max-catalog-items` limits item pool only, not users
- heavy users are split by `--max-rows-per-group`
- each LightGBM group keeps at least one positive
- trainer uses `group_index` when present
- trainer now supports `--ranker fm|deepwide|deepfm|dlrm|deepwide_idemb` using the reusable V2 train parquet
- neural trainer supports `--neural-loss bce|bpr`; BPR is smoke-tested for `deepwide_idemb` but not full-scale validated
- eval now loads neural ranker payloads and writes model-specific metrics when explicit paths are passed
- eval writes `id_embedding_eval_unknowns` for ID embedding rankers

## Next Step

Use `n20_norel` as the LGBM baseline.

Recommended next work:

```text
1. Do not spend more time on the current BCE dense neural rankers as-is
2. Do not spend more full-run time on ID-embedding reranker variants unless changing objective/sampling materially
3. Move to retrieval/candidate generation experiments if the goal is meaningful lift
4. Beat LGBM NDCG@100 = 0.004459
```

Candidate generation work should be evaluated separately with recall:

```text
candidate recall@100 / recall@200
```
