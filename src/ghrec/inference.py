"""Recommendation inference & evaluation with parallelism."""

import math
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from scipy import sparse


def recommend_popularity(
    pop_candidates: list[int],
    train_seen: dict[int, set[int]],
    user_ids: list[int],
    k: int,
) -> dict[int, list[int]]:
    """Popularity 기반 추천. seen item 제외."""
    recs = {}
    for uid in user_ids:
        seen = train_seen.get(uid, set())
        recs[uid] = [r for r in pop_candidates if r not in seen][:k]
    return recs


def recommend_als(
    model,
    train_sparse: sparse.csr_matrix,
    user2idx: dict[int, int],
    idx2item: dict[int, int],
    user_ids: list[int],
    k: int,
) -> dict[int, list[int]]:
    """ALS 배치 추천."""
    uidxs = np.array([user2idx[uid] for uid in user_ids])
    item_ids_batch, _ = model.recommend(
        uidxs, train_sparse[uidxs], N=k, filter_already_liked_items=True
    )
    return {
        uid: [idx2item[j] for j in item_ids_batch[i]]
        for i, uid in enumerate(user_ids)
    }


def _eval_chunk(chunk, test_gt, k_values):
    """유저 chunk에 대해 메트릭 계산 (worker 함수)."""
    results = []
    for uid, pop_recs, als_recs in chunk:
        relevant = test_gt[uid]
        row = {"uid": uid}
        for k in k_values:
            pop_p, pop_r = _precision_recall(pop_recs, relevant, k)
            pop_n = _ndcg(pop_recs, relevant, k)
            als_p, als_r = _precision_recall(als_recs, relevant, k)
            als_n = _ndcg(als_recs, relevant, k)
            row[f"pop_precision@{k}"] = pop_p
            row[f"pop_recall@{k}"] = pop_r
            row[f"pop_ndcg@{k}"] = pop_n
            row[f"als_precision@{k}"] = als_p
            row[f"als_recall@{k}"] = als_r
            row[f"als_ndcg@{k}"] = als_n
        results.append(row)
    return results


def _precision_recall(recommended, relevant, k):
    rec_set = set(recommended[:k])
    hits = rec_set & relevant
    precision = len(hits) / k if k > 0 else 0
    recall = len(hits) / len(relevant) if relevant else 0
    return precision, recall


def _ndcg(recommended, relevant, k):
    dcg = sum(
        1.0 / math.log2(i + 2)
        for i, rid in enumerate(recommended[:k])
        if rid in relevant
    )
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant), k)))
    return dcg / idcg if idcg > 0 else 0


def evaluate_parallel(
    pop_recs: dict[int, list[int]],
    als_recs: dict[int, list[int]],
    test_gt: dict[int, set[int]],
    k_values: list[int],
    n_workers: int = 4,
) -> list[dict]:
    """멀티프로세싱으로 유저별 메트릭 계산."""
    # chunk 분할
    items = [
        (uid, pop_recs[uid], als_recs[uid])
        for uid in pop_recs
        if uid in test_gt
    ]
    chunk_size = max(1, len(items) // n_workers)
    chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

    all_results = []
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = [
            executor.submit(_eval_chunk, chunk, test_gt, k_values)
            for chunk in chunks
        ]
        for f in as_completed(futures):
            all_results.extend(f.result())

    return all_results
