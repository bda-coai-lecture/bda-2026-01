"""Local GitHub-user recommendation simulator.

Usage:
    uv run streamlit run app_recsys_user_simulator.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from ghrec.user_simulator import (
    ColdStartRecommendationError,
    DEFAULT_API_URL,
    DEFAULT_HISTORY_PATH,
    DEFAULT_REPO_METADATA_DB,
    GitHubUser,
    call_recommendation_api,
    ensure_repo_metadata,
    event_label,
    format_topics,
    healthcheck_api,
    lookup_repo_names_bigquery,
    load_repo_name_map,
    metadata_lookup,
    resolve_github_user,
    rows_dataframe,
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


@st.cache_data(ttl=600)
def cached_actor_examples(limit: int = 20) -> pd.DataFrame:
    if not DEFAULT_HISTORY_PATH.exists():
        return pd.DataFrame(columns=["actor_id", "history_repos", "weighted_score"])
    df = pd.read_parquet(DEFAULT_HISTORY_PATH, columns=["actor_id", "repo_id", "weighted_score"])
    grouped = (
        df.groupby("actor_id", as_index=False)
        .agg(history_repos=("repo_id", "nunique"), weighted_score=("weighted_score", "sum"))
        .sort_values(["history_repos", "weighted_score"], ascending=False)
        .head(limit)
    )
    grouped["actor_id"] = grouped["actor_id"].astype(int)
    return grouped


@st.cache_data(ttl=300)
def cached_metadata(repo_ids: tuple[int, ...], repo_names: tuple[tuple[int, str], ...]) -> pd.DataFrame:
    return ensure_repo_metadata(
        list(repo_ids),
        dict(repo_names),
        db_path=DEFAULT_REPO_METADATA_DB,
        max_fetch=25,
    )


@st.cache_data(ttl=3600)
def cached_repo_name_lookup(repo_ids: tuple[int, ...]) -> dict[int, str]:
    return lookup_repo_names_bigquery(list(repo_ids))


def repo_link(repo_name: str) -> str:
    return f"https://github.com/{repo_name}" if "/" in repo_name else ""


def number_text(value: object) -> str:
    if isinstance(value, (int, float)) and pd.notna(value):
        return f"{int(value):,}"
    return "-"


def resolve_repo_names(repo_ids: list[int], base_names: dict[int, str]) -> dict[int, str]:
    names = {int(rid): name for rid, name in base_names.items() if int(rid) in set(repo_ids)}
    missing = sorted({int(rid) for rid in repo_ids if int(rid) not in names})
    if missing:
        names.update(cached_repo_name_lookup(tuple(missing)))
    return names


def enrich_repos(repo_ids: list[int], repo_names: dict[int, str]) -> dict[int, dict]:
    resolved_names = resolve_repo_names(repo_ids, repo_names)
    wanted_names = {rid: resolved_names[rid] for rid in repo_ids if rid in resolved_names}
    meta = cached_metadata(tuple(repo_ids), tuple(sorted(wanted_names.items())))
    return metadata_lookup(meta)


def resolve_user_input(value: str) -> GitHubUser:
    text = value.strip()
    if text.isdigit():
        actor_id = int(text)
        return GitHubUser(
            username=f"actor_{actor_id}",
            actor_id=actor_id,
            html_url="",
            name="Local GHArchive actor_id",
        )
    return resolve_github_user(text)


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


def source_label(source: str) -> str:
    return {
        "retrieval_hard": "ALS retrieval",
        "popular_recent": "Recent popular",
        "related_source": "Related repo",
        "random_catalog": "Catalog mix",
        "rank_label_positive": "Eval positive",
    }.get(source, source or "-")


def repo_column_config(label: str = "GitHub") -> dict[str, object]:
    return {"Link": st.column_config.LinkColumn(label, display_text="open")}


def table_height(row_count: int, *, max_height: int = 900) -> int:
    return min(max_height, 38 + max(1, row_count) * 36)


HISTORY_COLUMNS = ["Repo", "Repo ID", "Score", "Events", "Language", "Stars", "Last seen", "Topics", "Link"]
RECOMMENDATION_COLUMNS = [
    "Rank",
    "Repo",
    "Repo ID",
    "Score",
    "Candidate source",
    "Candidate rank",
    "Language",
    "Stars",
    "Description",
    "Link",
]


repo_names = cached_repo_names()
actor_examples = cached_actor_examples()

with st.sidebar:
    st.header("설정")
    api_url = st.text_input("API URL", value=DEFAULT_API_URL, key="api_url_8001")
    top_k = st.slider("추천 개수", 5, 50, 20)
    history_limit = st.slider("과거 행적 개수", 5, 30, 12)
    include_features = st.toggle("ranker feature 보기", value=False)
    st.divider()
    st.caption("로컬 mart에 있는 actor_id 예시")
    if actor_examples.empty:
        st.caption("history mart가 없어서 예시를 만들 수 없습니다.")
    else:
        example_labels = [
            f"{int(row.actor_id)} · repos {int(row.history_repos):,} · score {float(row.weighted_score):,.1f}"
            for row in actor_examples.itertuples(index=False)
        ]
        selected_example = st.selectbox("예시 actor_id", ["직접 입력"] + example_labels, label_visibility="collapsed")
        if selected_example != "직접 입력":
            st.session_state["user_input"] = selected_example.split(" · ", 1)[0]
    st.divider()
    if st.button("API 상태 확인", use_container_width=True):
        try:
            health = healthcheck_api(api_url)
            st.success(f"{health.get('status')} · {health.get('promoted_bundle_id')}")
        except Exception as exc:
            st.error(str(exc))

user_input = st.text_input(
    "GitHub username, URL 또는 actor_id",
    key="user_input",
    placeholder="torvalds, https://github.com/torvalds 또는 12345",
)

if not user_input:
    st.info("GitHub username이나 로컬 mart에 있는 actor_id를 입력하면 추천 결과를 보여줍니다.")
    st.stop()

try:
    user = resolve_user_input(user_input)
except Exception as exc:
    st.error(str(exc))
    st.stop()

top = st.columns([2, 1, 1, 1])
if user.html_url:
    top[0].markdown(f"### [{user.username}]({user.html_url})")
else:
    top[0].markdown(f"### {user.username}")
top[0].caption(user.name or "GitHub profile")
top[1].metric("actor_id", f"{user.actor_id:,}")
top[2].metric("public repos", f"{user.public_repos:,}" if user.public_repos is not None else "?")
top[3].metric("followers", f"{user.followers:,}" if user.followers is not None else "?")

tabs = st.tabs(["최근 행적", "추천 결과", "왜 추천됐나", "Smoke"])

history = cached_history(user.actor_id, history_limit)
history_repo_ids = [int(rid) for rid in history["repo_id"].tolist()] if not history.empty else []
history_repo_names = resolve_repo_names(history_repo_ids, repo_names) if history_repo_ids else {}
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
            repo_name = history_repo_names.get(repo_id, f"repo_{repo_id}")
            meta = history_meta.get(repo_id, {})
            rows.append(
                {
                    "Repo": repo_name,
                    "Repo ID": repo_id,
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
            rows_dataframe(rows, HISTORY_COLUMNS),
            hide_index=True,
            use_container_width=True,
            column_config=repo_column_config(),
        )

with tabs[1]:
    try:
        result = call_recommendation_api(
            user.actor_id,
            k=top_k,
            api_url=api_url,
            include_features=include_features,
        )
    except ColdStartRecommendationError:
        st.warning(
            "이 사용자는 현재 promoted bundle의 candidate cache에 없습니다. "
            "학습 기간 이후에 생겼거나, 로컬 GHArchive mart에서 활동 이력이 없는 cold-start 사용자입니다."
        )
        st.caption(
            "강의 포인트: production 추천은 unknown actor에 대해 인기/트렌딩/온보딩 기반 fallback 정책을 별도로 둬야 합니다."
        )
        result = {"actor_id": user.actor_id, "bundle_id": None, "items": [], "metadata": {"candidate_count": 0}}
    except Exception as exc:
        st.error(str(exc))
        st.caption("API가 켜져 있는지 확인: uv run uvicorn ghrec.api:app --host 0.0.0.0 --port 8001")
        result = None

    if result:
        items = result.get("items", [])
        rec_repo_ids = [int(item["repo_id"]) for item in items]
        rec_repo_names = resolve_repo_names(rec_repo_ids, repo_names)
        rec_meta = enrich_repos(rec_repo_ids, repo_names)
        st.caption(f"bundle: {result.get('bundle_id')} · candidates: {result.get('metadata', {}).get('candidate_count')}")
        if not items:
            st.info("표시할 추천 결과가 없습니다. cold-start fallback 정책을 붙이면 여기서 인기/트렌딩 저장소를 보여줄 수 있습니다.")
        else:
            source_counts = (
                pd.Series([source_label(str(item.get("candidate_source", "-"))) for item in items])
                .value_counts()
                .rename_axis("Source")
                .reset_index(name="Count")
            )
            st.bar_chart(source_counts.set_index("Source"), horizontal=True)
        rows = []
        for item in items:
            repo_id = int(item["repo_id"])
            repo_name = rec_repo_names.get(repo_id, f"repo_{repo_id}")
            meta = rec_meta.get(repo_id, {})
            rows.append(
                {
                    "Rank": item["rank"],
                    "Repo": repo_name,
                    "Repo ID": repo_id,
                    "Score": f"{float(item['score']):.4f}",
                    "Candidate source": source_label(str(item.get("candidate_source", "-"))),
                    "Candidate rank": item.get("candidate_rank"),
                    "Language": meta.get("language") or "-",
                    "Stars": number_text(meta.get("stargazers")),
                    "Description": str(meta.get("description") or "-")[:100],
                    "Link": repo_link(repo_name),
                }
            )
        st.dataframe(
            rows_dataframe(rows, RECOMMENDATION_COLUMNS),
            hide_index=True,
            use_container_width=True,
            height=table_height(len(rows)),
            column_config=repo_column_config(),
        )

with tabs[2]:
    if "result" not in locals() or not result:
        st.info("추천 결과가 생성되면 설명을 보여줍니다.")
    else:
        items = result.get("items", [])
        rec_repo_ids = [int(item["repo_id"]) for item in items]
        rec_repo_names = resolve_repo_names(rec_repo_ids, repo_names)
        rec_meta = enrich_repos(rec_repo_ids, repo_names)
        st.caption(f"{len(items):,}개 추천 설명")
        for item in items:
            repo_id = int(item["repo_id"])
            repo_name = rec_repo_names.get(repo_id, f"repo_{repo_id}")
            meta = rec_meta.get(repo_id, {})
            link = repo_link(repo_name)
            title = f"{item['rank']}. {repo_name}"
            with st.container(border=True):
                st.markdown(f"**[{title}]({link})**" if link else f"**{title}**")
                cols = st.columns([1, 1, 1, 3])
                cols[0].metric("score", f"{float(item['score']):.4f}")
                cols[1].metric("source", source_label(str(item.get("candidate_source", "-"))))
                cols[2].metric("candidate", item.get("candidate_rank") or "-")
                cols[3].write(recommendation_reason(item, meta))
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
