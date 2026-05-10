"""Week 6 two-stage recommendation evaluation dashboard.

Usage:
    uv run streamlit run app_week6_qual_eval.py
"""

from __future__ import annotations

import json
import pickle
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


DATA_DIR = Path("data")
MODEL_DIR = DATA_DIR / "models"
WEEK6_DIR = MODEL_DIR / "week6"
NAME_MAP_PATH = MODEL_DIR / "repo_name_map.pkl"
DB_PATH = DATA_DIR / "repo_metadata.db"
CANONICAL_SUFFIX = "related80_anchor20_full_als96_i12_lgbm63"

BASE_COLUMNS = ["model", "rank", "repo_id", "is_hit", "score"]
HIDDEN_CASE_COLUMNS = {"actor_id", "history_repo_ids", "test_repo_ids"}


st.set_page_config(page_title="Week 6 정성 평가", layout="wide")
st.title("Week 6 Two-Stage 추천 평가")


def _artifact_paths(suffix: str) -> dict[str, Path]:
    return {
        "metrics": WEEK6_DIR / f"week6_two_stage_{suffix}_metrics.csv",
        "summary": WEEK6_DIR / f"week6_two_stage_{suffix}_summary.json",
        "qual": WEEK6_DIR / f"week6_qual_cases_{suffix}.parquet",
    }


def _suffix_is_usable(suffix: str) -> bool:
    paths = _artifact_paths(suffix)
    return paths["metrics"].exists() and paths["summary"].exists()


def discover_suffixes() -> list[str]:
    found = {
        path.name.removeprefix("week6_two_stage_").removesuffix("_metrics.csv")
        for path in WEEK6_DIR.glob("week6_two_stage_*_metrics.csv")
    }
    preferred = [CANONICAL_SUFFIX, "latest", "smoke"]
    ordered = [suffix for suffix in preferred if suffix in found]
    ordered.extend(sorted(found - set(ordered)))
    return ordered or ["smoke"]


def default_suffix_index(suffixes: list[str]) -> int:
    if CANONICAL_SUFFIX in suffixes and _suffix_is_usable(CANONICAL_SUFFIX):
        return suffixes.index(CANONICAL_SUFFIX)
    if "latest" in suffixes and _suffix_is_usable("latest"):
        return suffixes.index("latest")
    if "smoke" in suffixes:
        return suffixes.index("smoke")
    return 0


@st.cache_data(show_spinner=False)
def load_metrics(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    for col in ["k", "unique_recommended"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["precision", "recall", "ndcg"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_summary(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@st.cache_data(show_spinner=False)
def load_qual_cases(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_repo_name_map(path: str) -> dict[int, str]:
    if not Path(path).exists():
        return {}
    try:
        with open(path, "rb") as f:
            raw = pickle.load(f)
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[int, str] = {}
    for key, value in raw.items():
        try:
            out[int(key)] = str(value)
        except Exception:
            continue
    return out


@st.cache_data(show_spinner=False)
def load_repo_metadata(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(path)
        df = pd.read_sql_query(
            """
            SELECT repo_id, repo_name, description, language, stargazers, forks
            FROM repo_metadata
            WHERE http_status = 200
            """,
            conn,
        )
        conn.close()
    except Exception:
        return pd.DataFrame()
    if "repo_id" in df:
        df["repo_id"] = pd.to_numeric(df["repo_id"], errors="coerce").astype("Int64")
    return df


def metric_label(value: Any, digits: int = 4) -> str:
    if pd.isna(value):
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def flatten_summary(summary: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, value in summary.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                rows.append({"section": key, "key": sub_key, "value": sub_value})
        else:
            rows.append({"section": "run", "key": key, "value": value})
    return pd.DataFrame(rows)


def add_repo_display(df: pd.DataFrame, name_map: dict[int, str], meta_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "repo_id" not in df:
        return df

    out = df.copy()
    out["repo_id"] = pd.to_numeric(out["repo_id"], errors="coerce").astype("Int64")
    out["repo_name"] = out["repo_id"].map(lambda rid: name_map.get(int(rid), "") if pd.notna(rid) else "")

    if not meta_df.empty:
        meta_cols = [
            col
            for col in ["repo_id", "repo_name", "description", "language", "stargazers", "forks"]
            if col in meta_df
        ]
        out = out.merge(
            meta_df[meta_cols].drop_duplicates("repo_id"),
            on="repo_id",
            how="left",
            suffixes=("", "_meta"),
        )
        if "repo_name_meta" in out:
            out["repo_name"] = out["repo_name"].where(out["repo_name"].ne(""), out["repo_name_meta"].fillna(""))
            out = out.drop(columns=["repo_name_meta"])

    out["repo"] = out.apply(
        lambda row: f"{row['repo_name']} ({int(row['repo_id'])})"
        if row.get("repo_name") and pd.notna(row.get("repo_id"))
        else f"repo_{int(row['repo_id'])}"
        if pd.notna(row.get("repo_id"))
        else "",
        axis=1,
    )
    return out


def preferred_case_columns(df: pd.DataFrame) -> list[str]:
    feature_cols = [
        col
        for col in df.columns
        if col not in set(BASE_COLUMNS) | HIDDEN_CASE_COLUMNS | {"repo", "repo_name", "description", "language", "stargazers", "forks"}
    ]
    display_cols = ["model", "rank", "repo", "repo_id", "is_hit", "score"]
    for optional in ["language", "stargazers", "forks", "description"]:
        if optional in df:
            display_cols.append(optional)
    display_cols.extend(feature_cols)
    return [col for col in display_cols if col in df]


suffixes = discover_suffixes()
selected_suffix = st.sidebar.selectbox(
    "Artifact suffix",
    suffixes,
    index=default_suffix_index(suffixes),
)
paths = _artifact_paths(selected_suffix)

st.sidebar.caption(f"metrics: {'있음' if paths['metrics'].exists() else '없음'}")
st.sidebar.caption(f"summary: {'있음' if paths['summary'].exists() else '없음'}")
st.sidebar.caption(f"qual cases: {'있음' if paths['qual'].exists() else '없음'}")
st.sidebar.caption(f"canonical: `{CANONICAL_SUFFIX}`")

name_map = load_repo_name_map(str(NAME_MAP_PATH))
meta_df = load_repo_metadata(str(DB_PATH))

if not paths["metrics"].exists() or not paths["summary"].exists():
    st.error(
        f"`{selected_suffix}` 산출물이 없습니다. "
        f"`{paths['metrics']}`와 `{paths['summary']}`를 먼저 생성하세요."
    )
    st.stop()

try:
    metrics = load_metrics(str(paths["metrics"]))
    summary = load_summary(str(paths["summary"]))
except Exception as exc:
    st.error(f"산출물 로드 실패: {exc}")
    st.stop()

summary_cols = st.columns(5)
summary_cols[0].metric("suffix", selected_suffix)
summary_cols[1].metric("eval users", metric_label(summary.get("eval_users")))
summary_cols[2].metric("history users", metric_label(summary.get("history_users")))
summary_cols[3].metric("history repos", metric_label(summary.get("history_repos")))
summary_cols[4].metric("elapsed min", metric_label(summary.get("elapsed_min"), digits=2))

st.subheader("Metrics comparison")
if metrics.empty:
    st.info("metrics CSV가 비어 있습니다.")
else:
    display_metrics = metrics.copy()
    metric_cols = [col for col in ["precision", "recall", "ndcg"] if col in display_metrics]
    for col in metric_cols:
        display_metrics[col] = display_metrics[col].map(lambda x: metric_label(x))
    st.dataframe(display_metrics, hide_index=True, use_container_width=True)

    if {"model", "k"}.issubset(metrics.columns):
        chart_metric = st.selectbox(
            "Chart metric",
            [col for col in ["precision", "recall", "ndcg", "unique_recommended"] if col in metrics.columns],
        )
        chart_df = metrics.pivot(index="k", columns="model", values=chart_metric).sort_index()
        st.line_chart(chart_df)

with st.expander("Run summary"):
    st.dataframe(flatten_summary(summary), hide_index=True, use_container_width=True)

st.subheader("Qualitative user cases")
if not paths["qual"].exists():
    st.info(f"정성 평가 케이스 파일이 없습니다: `{paths['qual']}`")
    st.stop()

try:
    qual = load_qual_cases(str(paths["qual"]))
except Exception as exc:
    st.warning(f"정성 평가 케이스를 읽을 수 없습니다: {exc}")
    st.stop()

if qual.empty:
    st.info("정성 평가 케이스가 비어 있습니다.")
    st.stop()

qual = add_repo_display(qual, name_map, meta_df)

if "actor_id" not in qual:
    st.warning("정성 평가 케이스에 actor_id 컬럼이 없어 전체 테이블만 표시합니다.")
    st.dataframe(qual[preferred_case_columns(qual)], hide_index=True, use_container_width=True)
    st.stop()

actor_ids = sorted(qual["actor_id"].dropna().unique().tolist())
default_limit = min(20, len(actor_ids))
case_limit = st.sidebar.slider("User cases", 1, max(1, len(actor_ids)), default_limit)
model_filter = st.sidebar.multiselect(
    "Models",
    sorted(qual["model"].dropna().unique().tolist()) if "model" in qual else [],
    default=sorted(qual["model"].dropna().unique().tolist()) if "model" in qual else [],
)

shown = qual[qual["actor_id"].isin(actor_ids[:case_limit])].copy()
if model_filter and "model" in shown:
    shown = shown[shown["model"].isin(model_filter)]

if "rank" in shown:
    shown = shown.sort_values(["actor_id", "model", "rank"], kind="stable")

display_cols = preferred_case_columns(shown)
for actor_id, actor_df in shown.groupby("actor_id", sort=True):
    hits = int(actor_df["is_hit"].sum()) if "is_hit" in actor_df else 0
    with st.expander(f"actor_id={actor_id} · hits={hits}", expanded=False):
        context_cols = st.columns(2)
        if "history_repo_ids" in actor_df:
            context_cols[0].caption(f"history_repo_ids: {actor_df['history_repo_ids'].iloc[0]}")
        if "test_repo_ids" in actor_df:
            context_cols[1].caption(f"test_repo_ids: {actor_df['test_repo_ids'].iloc[0]}")
        st.dataframe(actor_df[display_cols], hide_index=True, use_container_width=True)
