"""Local GitHub-user recommendation simulator.

Usage:
    uv run streamlit run app_recsys_user_simulator.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from ghrec.user_simulator import (
    DEFAULT_API_URL,
    DEFAULT_REPO_METADATA_DB,
    call_recommendation_api,
    ensure_repo_metadata,
    event_label,
    format_topics,
    healthcheck_api,
    load_repo_name_map,
    metadata_lookup,
    resolve_github_user,
    summarize_history,
)


st.set_page_config(page_title="GitHub User 추천 시뮬레이터", layout="wide")
st.title("GitHub User 추천 시뮬레이터")


@st.cache_resource
def cached_repo_names() -> dict[int, str]:
    return load_repo_name_map()


@st.cache_data(ttl=300)
def cached_history(actor_id: int, limit: int) -> pd.DataFrame:
    return summarize_history(actor_id, limit=limit)


@st.cache_data(ttl=300)
def cached_metadata(repo_ids: tuple[int, ...], repo_names: tuple[tuple[int, str], ...]) -> pd.DataFrame:
    return ensure_repo_metadata(
        list(repo_ids),
        dict(repo_names),
        db_path=DEFAULT_REPO_METADATA_DB,
        max_fetch=25,
    )


def repo_link(repo_name: str) -> str:
    return f"https://github.com/{repo_name}" if "/" in repo_name else ""


def number_text(value: object) -> str:
    if isinstance(value, (int, float)) and pd.notna(value):
        return f"{int(value):,}"
    return "-"


def enrich_repos(repo_ids: list[int], repo_names: dict[int, str]) -> dict[int, dict]:
    wanted_names = {rid: repo_names[rid] for rid in repo_ids if rid in repo_names}
    meta = cached_metadata(tuple(repo_ids), tuple(sorted(wanted_names.items())))
    return metadata_lookup(meta)


def recommendation_reason(item: dict, meta: dict) -> str:
    source = item.get("candidate_source", "retrieval_hard")
    source_text = {
        "retrieval_hard": "비슷한 활동 패턴을 가진 사용자-저장소 행렬에서 먼저 후보로 잡혔습니다.",
        "popular_recent": "최근 전체 사용자 사이에서 많이 반응한 저장소라 후보에 들어왔습니다.",
        "related_source": "사용자가 과거에 본 저장소와 가까운 주변 저장소로 잡혔습니다.",
        "random_catalog": "후보 다양성을 위해 일부 catalog 후보에서 들어왔습니다.",
        "rank_label_positive": "평가 기간의 실제 positive 후보입니다.",
    }.get(source, "candidate 생성 단계에서 후보로 잡혔습니다.")
    stars = meta.get("stargazers")
    lang = meta.get("language")
    extra = []
    if lang:
        extra.append(f"주 언어는 {lang}입니다")
    if isinstance(stars, (int, float)) and stars > 0:
        extra.append(f"stars {int(stars):,}개")
    suffix = f" ({', '.join(extra)})" if extra else ""
    return f"{source_text} 그 뒤 ranker가 후보 순위 {item.get('candidate_rank')}에서 최종 {item.get('rank')}위로 점수를 다시 매겼습니다{suffix}."


repo_names = cached_repo_names()

with st.sidebar:
    st.header("설정")
    api_url = st.text_input("API URL", value=DEFAULT_API_URL)
    top_k = st.slider("추천 개수", 5, 50, 20)
    history_limit = st.slider("과거 행적 개수", 5, 30, 12)
    include_features = st.toggle("ranker feature 보기", value=False)

user_input = st.text_input(
    "GitHub username 또는 URL",
    placeholder="torvalds 또는 https://github.com/torvalds",
)

if not user_input:
    st.info("GitHub username을 입력하면 로컬 모델의 actor_id 기준 추천 결과를 보여줍니다.")
    st.stop()

try:
    user = resolve_github_user(user_input)
except Exception as exc:
    st.error(str(exc))
    st.stop()

top = st.columns([2, 1, 1, 1])
top[0].markdown(f"### [{user.username}]({user.html_url})")
top[0].caption(user.name or "GitHub profile")
top[1].metric("actor_id", f"{user.actor_id:,}")
top[2].metric("public repos", f"{user.public_repos:,}" if user.public_repos is not None else "?")
top[3].metric("followers", f"{user.followers:,}" if user.followers is not None else "?")

tabs = st.tabs(["최근 행적", "추천 결과", "왜 추천됐나", "Smoke"])

history = cached_history(user.actor_id, history_limit)
history_repo_ids = [int(rid) for rid in history["repo_id"].tolist()] if not history.empty else []
history_meta = enrich_repos(history_repo_ids, repo_names) if history_repo_ids else {}

with tabs[0]:
    if history.empty:
        st.warning("로컬 학습/마트 데이터에서 이 사용자의 과거 행적을 찾지 못했습니다. cold-start 사용자입니다.")
    else:
        total_score = float(history.get("weighted_score", pd.Series(dtype=float)).sum())
        active_days = int(history.get("active_days", pd.Series([0])).max() or 0)
        cols = st.columns(3)
        cols[0].metric("history repos", f"{history['repo_id'].nunique():,}")
        cols[1].metric("weighted score", f"{total_score:,.1f}")
        cols[2].metric("active days", f"{active_days:,}")

        rows = []
        for row in history.itertuples(index=False):
            repo_id = int(row.repo_id)
            repo_name = repo_names.get(repo_id, f"repo_{repo_id}")
            meta = history_meta.get(repo_id, {})
            rows.append(
                {
                    "Repo": repo_name,
                    "Score": f"{float(getattr(row, 'weighted_score', 0.0)):,.1f}",
                    "Events": event_label(pd.Series(row._asdict())),
                    "Language": meta.get("language") or "-",
                    "Stars": number_text(meta.get("stargazers")),
                    "Last seen": str(getattr(row, "last_seen_at", ""))[:10],
                    "Topics": format_topics(meta.get("topics")),
                    "Link": repo_link(repo_name),
                }
            )
        st.dataframe(
            pd.DataFrame(rows)[["Repo", "Score", "Events", "Language", "Stars", "Last seen", "Topics"]],
            hide_index=True,
            use_container_width=True,
        )

with tabs[1]:
    try:
        result = call_recommendation_api(
            user.actor_id,
            k=top_k,
            api_url=api_url,
            include_features=include_features,
        )
    except Exception as exc:
        st.error(str(exc))
        st.caption("API가 켜져 있는지 확인: uv run uvicorn ghrec.api:app --host 0.0.0.0 --port 8000")
        result = None

    if result:
        items = result.get("items", [])
        rec_repo_ids = [int(item["repo_id"]) for item in items]
        rec_meta = enrich_repos(rec_repo_ids, repo_names)
        st.caption(f"bundle: {result.get('bundle_id')} · candidates: {result.get('metadata', {}).get('candidate_count')}")
        rows = []
        for item in items:
            repo_id = int(item["repo_id"])
            repo_name = repo_names.get(repo_id, f"repo_{repo_id}")
            meta = rec_meta.get(repo_id, {})
            rows.append(
                {
                    "Rank": item["rank"],
                    "Repo": repo_name,
                    "Score": f"{float(item['score']):.4f}",
                    "Source": item.get("candidate_source", "-"),
                    "Candidate rank": item.get("candidate_rank"),
                    "Language": meta.get("language") or "-",
                    "Stars": number_text(meta.get("stargazers")),
                    "Description": str(meta.get("description") or "-")[:100],
                    "Link": repo_link(repo_name),
                }
            )
        st.dataframe(
            pd.DataFrame(rows)[["Rank", "Repo", "Score", "Source", "Candidate rank", "Language", "Stars", "Description"]],
            hide_index=True,
            use_container_width=True,
        )

with tabs[2]:
    if "result" not in locals() or not result:
        st.info("추천 결과가 생성되면 설명을 보여줍니다.")
    else:
        items = result.get("items", [])[:10]
        rec_repo_ids = [int(item["repo_id"]) for item in items]
        rec_meta = enrich_repos(rec_repo_ids, repo_names)
        for item in items:
            repo_id = int(item["repo_id"])
            repo_name = repo_names.get(repo_id, f"repo_{repo_id}")
            meta = rec_meta.get(repo_id, {})
            link = repo_link(repo_name)
            title = f"{item['rank']}. {repo_name}"
            st.markdown(f"**[{title}]({link})**" if link else f"**{title}**")
            st.write(recommendation_reason(item, meta))
            if include_features and item.get("features"):
                st.json(item["features"], expanded=False)

with tabs[3]:
    checks = []
    try:
        health = healthcheck_api(api_url)
        checks.append({"Check": "API health", "Status": "ok", "Detail": health.get("status")})
        checks.append({"Check": "Active bundle", "Status": "ok", "Detail": health.get("promoted_bundle_id")})
    except Exception as exc:
        checks.append({"Check": "API health", "Status": "fail", "Detail": str(exc)})
    checks.append(
        {
            "Check": "Repo metadata cache",
            "Status": "ok" if Path(DEFAULT_REPO_METADATA_DB).exists() else "missing",
            "Detail": str(DEFAULT_REPO_METADATA_DB),
        }
    )
    checks.append(
        {
            "Check": "User id cache",
            "Status": "ok",
            "Detail": "data/github_user_cache.db",
        }
    )
    st.dataframe(pd.DataFrame(checks), hide_index=True, use_container_width=True)
