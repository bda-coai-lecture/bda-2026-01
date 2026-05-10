"""Provision the local Metabase dashboard for BDA platform metrics."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


DEFAULT_URL = "http://localhost:3001"
DEFAULT_EMAIL = "bda@local.dev"
DEFAULT_PASSWORD = "bda-local-2026"
DEFAULT_PROJECT = "bda-coai"
DEFAULT_DATASET = "mart"


@dataclass
class Metabase:
    base_url: str
    email: str
    password: str
    session: requests.Session

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        response = self.session.request(method, f"{self.base_url}{path}", timeout=60, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} failed: {response.status_code} {response.text}")
        return response

    def get_json(self, path: str) -> Any:
        return self.request("GET", path).json()

    def post_json(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("POST", path, json=payload).json()

    def put_json(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("PUT", path, json=payload).json()


def wait_for_metabase(base_url: str) -> dict[str, Any]:
    for _ in range(60):
        try:
            response = requests.get(f"{base_url}/api/session/properties", timeout=10)
            if response.ok:
                return response.json()
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"Metabase is not ready: {base_url}")


def setup_or_login(base_url: str, email: str, password: str) -> Metabase:
    props = wait_for_metabase(base_url)
    session = requests.Session()
    mb = Metabase(base_url, email, password, session)

    setup_token = props.get("setup-token")
    if setup_token:
        payload = {
            "token": setup_token,
            "user": {
                "first_name": "BDA",
                "last_name": "Admin",
                "email": email,
                "password": password,
                "site_name": "BDA Local Metrics",
            },
            "prefs": {
                "site_name": "BDA Local Metrics",
                "site_locale": "ko",
                "allow_tracking": False,
            },
        }
        try:
            result = mb.post_json("/api/setup", payload)
            session.headers.update({"X-Metabase-Session": result["id"]})
            return mb
        except RuntimeError as exc:
            if "403" not in str(exc):
                raise

    result = mb.post_json("/api/session", {"username": email, "password": password})
    session.headers.update({"X-Metabase-Session": result["id"]})
    return mb


def find_database(mb: Metabase, name: str) -> dict[str, Any] | None:
    databases = mb.get_json("/api/database").get("data", [])
    for database in databases:
        if database.get("name") == name:
            return database
    return None


def ensure_bigquery_database(
    mb: Metabase,
    name: str,
    project: str,
    dataset: str,
    key_path: Path,
) -> int:
    existing = find_database(mb, name)
    if existing:
        return int(existing["id"])

    service_account_json = key_path.read_text()
    details = {
        "project-id": project,
        "service-account-json": service_account_json,
        "dataset-filters-type": "inclusion",
        "dataset-filters-patterns": dataset,
        "advanced-options": True,
        "auto_run_queries": False,
        "let-user-control-scheduling": True,
        "refingerprint": False,
    }
    payload = {
        "name": name,
        "engine": "bigquery-cloud-sdk",
        "details": details,
        "is_full_sync": False,
        "is_on_demand": True,
        "schedules": {},
    }
    database = mb.post_json("/api/database", payload)
    database_id = int(database["id"])
    try:
        mb.post_json(f"/api/database/{database_id}/sync_schema", {})
    except RuntimeError as exc:
        print(f"WARN schema sync skipped: {exc}")
    return database_id


def find_collection(mb: Metabase, name: str) -> dict[str, Any] | None:
    collections = mb.get_json("/api/collection")
    for collection in collections:
        if collection.get("name") == name:
            return collection
    return None


def ensure_collection(mb: Metabase, name: str) -> int:
    existing = find_collection(mb, name)
    if existing:
        return int(existing["id"])
    collection = mb.post_json("/api/collection", {"name": name, "color": "#509EE3"})
    return int(collection["id"])


def card_payload(
    name: str,
    database_id: int,
    sql: str,
    display: str,
    collection_id: int,
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": sql},
        },
        "display": display,
        "visualization_settings": visualization_settings or {},
        "collection_id": collection_id,
    }


def list_cards(mb: Metabase) -> list[dict[str, Any]]:
    return mb.get_json("/api/card")


def delete_existing_cards(mb: Metabase, collection_id: int, names: set[str]) -> None:
    for card in list_cards(mb):
        if card.get("collection_id") == collection_id and card.get("name") in names:
            mb.request("DELETE", f"/api/card/{card['id']}")


def create_card(mb: Metabase, payload: dict[str, Any]) -> int:
    card = mb.post_json("/api/card", payload)
    return int(card["id"])


def find_dashboard(mb: Metabase, name: str) -> dict[str, Any] | None:
    dashboards = mb.get_json("/api/dashboard")
    for dashboard in dashboards:
        if dashboard.get("name") == name:
            return dashboard
    return None


def ensure_dashboard(mb: Metabase, name: str, collection_id: int) -> int:
    existing = find_dashboard(mb, name)
    if existing:
        return int(existing["id"])
    dashboard = mb.post_json(
        "/api/dashboard",
        {
            "name": name,
            "description": "GitHub Archive 기초 지표: DAU, 이벤트, WAU, 유저 세그먼트",
            "collection_id": collection_id,
        },
    )
    return int(dashboard["id"])


def ensure_dashboard_with_description(
    mb: Metabase,
    name: str,
    collection_id: int,
    description: str,
) -> int:
    existing = find_dashboard(mb, name)
    if existing:
        return int(existing["id"])
    dashboard = mb.post_json(
        "/api/dashboard",
        {
            "name": name,
            "description": description,
            "collection_id": collection_id,
        },
    )
    return int(dashboard["id"])


def clear_dashboard_cards(mb: Metabase, dashboard_id: int) -> None:
    mb.put_json(f"/api/dashboard/{dashboard_id}", {"dashcards": [], "tabs": []})


def replace_dashboard_cards(
    mb: Metabase,
    dashboard_id: int,
    card_ids: list[int],
    layout: list[tuple[int, int, int, int]],
) -> None:
    dashcards = []
    for index, (card_id, (row, col, size_x, size_y)) in enumerate(
        zip(card_ids, layout, strict=True), start=1
    ):
        dashcards.append(
            {
                "id": -index,
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": size_x,
                "size_y": size_y,
                "parameter_mappings": [],
                "series": [],
                "inline_parameters": [],
            }
        )
    mb.put_json(
        f"/api/dashboard/{dashboard_id}",
        {"dashcards": dashcards, "tabs": []},
    )


def build_cards(database_id: int, collection_id: int, project: str, dataset: str) -> list[dict[str, Any]]:
    table = lambda name: f"`{project}.{dataset}.{name}`"
    return [
        card_payload(
            "KPI - 최근 일자 DAU",
            database_id,
            f"""
            SELECT active_users
            FROM {table("metrics_daily")}
            ORDER BY activity_date DESC
            LIMIT 1
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "KPI - 최근 일자 이벤트 수",
            database_id,
            f"""
            SELECT total_events
            FROM {table("metrics_daily")}
            ORDER BY activity_date DESC
            LIMIT 1
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "DAU 추이",
            database_id,
            f"""
            SELECT activity_date, active_users
            FROM {table("metrics_daily")}
            ORDER BY activity_date
            """,
            "line",
            collection_id,
        ),
        card_payload(
            "총 이벤트 추이",
            database_id,
            f"""
            SELECT activity_date, total_events
            FROM {table("metrics_daily")}
            ORDER BY activity_date
            """,
            "line",
            collection_id,
        ),
        card_payload(
            "이벤트 타입별 일별 이벤트",
            database_id,
            f"""
            SELECT activity_date, action, total_events
            FROM {table("metrics_event_type_daily")}
            WHERE action IN ('PushEvent', 'WatchEvent', 'ForkEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent')
            ORDER BY activity_date, action
            """,
            "area",
            collection_id,
        ),
        card_payload(
            "WAU 추이",
            database_id,
            f"""
            SELECT week_start, weekly_active_users
            FROM {table("metrics_weekly")}
            ORDER BY week_start
            """,
            "line",
            collection_id,
        ),
        card_payload(
            "유저 세그먼트 분포",
            database_id,
            f"""
            SELECT user_segment, users
            FROM {table("metrics_user_segments")}
            ORDER BY users DESC
            """,
            "bar",
            collection_id,
        ),
        card_payload(
            "최근 7일 기초 지표",
            database_id,
            f"""
            SELECT activity_date, active_users, active_repos, total_events, events_per_active_user
            FROM {table("metrics_daily")}
            ORDER BY activity_date DESC
            LIMIT 7
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "KPI - 최근 완성 코호트 W1 Retention",
            database_id,
            f"""
            SELECT w1_retention
            FROM {table("metrics_retention_summary")}
            WHERE w1_retention > 0
            ORDER BY cohort_week DESC
            LIMIT 1
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "Weekly Cohort Retention",
            database_id,
            f"""
            SELECT cohort_week, weeks_since, retention_rate
            FROM {table("metrics_retention_weekly")}
            WHERE weeks_since BETWEEN 0 AND 4
            ORDER BY cohort_week, weeks_since
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "W1 Retention 추이",
            database_id,
            f"""
            SELECT cohort_week, w1_retention
            FROM {table("metrics_retention_summary")}
            WHERE w1_retention > 0
            ORDER BY cohort_week
            """,
            "line",
            collection_id,
        ),
        card_payload(
            "Retention Summary",
            database_id,
            f"""
            SELECT cohort_week, cohort_users, w0_retention, w1_retention, w2_retention, w3_retention
            FROM {table("metrics_retention_summary")}
            ORDER BY cohort_week DESC
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "AI Agent 트렌디 repo Top 20",
            database_id,
            f"""
            SELECT
              repo_name,
              trend_score,
              growth_ratio,
              seed_affinity,
              recent_active_users,
              recent_score,
              recent_top_action,
              why_trendy
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 20
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "Agent Trend Score Top 20",
            database_id,
            f"""
            SELECT repo_name, trend_score
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 20
            """,
            "bar",
            collection_id,
        ),
        card_payload(
            "Growth vs Seed Affinity",
            database_id,
            f"""
            SELECT growth_ratio, seed_affinity, recent_active_users, trend_score, repo_name
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 100
            """,
            "scatter",
            collection_id,
            {
                "graph.dimensions": ["growth_ratio"],
                "graph.metrics": ["seed_affinity"],
                "graph.x_axis.title_text": "Growth ratio",
                "graph.y_axis.title_text": "Seed affinity",
                "graph.x_axis.scale": "linear",
                "graph.y_axis.scale": "linear",
                "graph.show_values": False,
            },
        ),
        card_payload(
            "Agent Trend 예측력 비교",
            database_id,
            f"""
            SELECT model, spearman_next_score, precision_at_20_next_top100, ndcg_at_20, avg_next_score_at_20
            FROM {table("metrics_agent_trend_validation")}
            ORDER BY ndcg_at_20 DESC
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "Agent Trend 주요 신호",
            database_id,
            f"""
            SELECT recent_top_action, COUNT(*) AS repos, AVG(trend_score) AS avg_trend_score
            FROM {table("metrics_agent_trendy_repos")}
            GROUP BY recent_top_action
            ORDER BY repos DESC
            """,
            "bar",
            collection_id,
        ),
    ]


def build_trendy_repo_cards(
    database_id: int,
    collection_id: int,
    project: str,
    dataset: str,
) -> list[dict[str, Any]]:
    table = lambda name: f"`{project}.{dataset}.{name}`"
    return [
        card_payload(
            "Trend - 후보 repo 수",
            database_id,
            f"""
            SELECT COUNT(*) AS repos
            FROM {table("metrics_agent_trendy_repos")}
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "Trend - 최고 trend score",
            database_id,
            f"""
            SELECT trend_score
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 1
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "Trend - 예측 NDCG@20",
            database_id,
            f"""
            SELECT ndcg_at_20
            FROM {table("metrics_agent_trend_validation")}
            WHERE model = 'agent_trend_score'
            LIMIT 1
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "Trend - 트렌디 repo 리더보드",
            database_id,
            f"""
            SELECT
              repo_name,
              trend_score,
              growth_ratio,
              seed_affinity,
              recent_active_users,
              recent_score,
              recent_top_action,
              stargazers,
              forks,
              why_trendy
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 20
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "Trend - Score Top 20",
            database_id,
            f"""
            SELECT repo_name, trend_score
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 20
            """,
            "bar",
            collection_id,
            {
                "graph.dimensions": ["repo_name"],
                "graph.metrics": ["trend_score"],
                "graph.x_axis.title_text": "Repo",
                "graph.y_axis.title_text": "Trend score",
            },
        ),
        card_payload(
            "Trend - Growth vs Seed Affinity",
            database_id,
            f"""
            SELECT growth_ratio, seed_affinity, recent_active_users, trend_score, repo_name
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 100
            """,
            "scatter",
            collection_id,
            {
                "graph.dimensions": ["growth_ratio"],
                "graph.metrics": ["seed_affinity"],
                "graph.x_axis.title_text": "Growth ratio",
                "graph.y_axis.title_text": "Seed affinity",
                "graph.x_axis.scale": "linear",
                "graph.y_axis.scale": "linear",
                "graph.show_values": False,
            },
        ),
        card_payload(
            "Trend - Recent Users vs Score",
            database_id,
            f"""
            SELECT recent_active_users, trend_score, growth_ratio, seed_affinity, repo_name
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY trend_score DESC
            LIMIT 100
            """,
            "scatter",
            collection_id,
            {
                "graph.dimensions": ["recent_active_users"],
                "graph.metrics": ["trend_score"],
                "graph.x_axis.title_text": "Recent active users",
                "graph.y_axis.title_text": "Trend score",
                "graph.x_axis.scale": "linear",
                "graph.y_axis.scale": "linear",
                "graph.show_values": False,
            },
        ),
        card_payload(
            "Trend - 주요 액션별 repo 수",
            database_id,
            f"""
            SELECT
              recent_top_action,
              COUNT(*) AS repos,
              AVG(trend_score) AS avg_trend_score,
              AVG(growth_ratio) AS avg_growth_ratio
            FROM {table("metrics_agent_trendy_repos")}
            GROUP BY recent_top_action
            ORDER BY repos DESC
            """,
            "bar",
            collection_id,
        ),
        card_payload(
            "Trend - 검증 지표 비교",
            database_id,
            f"""
            SELECT
              model,
              candidates,
              spearman_next_score,
              precision_at_20_next_top100,
              ndcg_at_20,
              avg_next_score_at_20
            FROM {table("metrics_agent_trend_validation")}
            ORDER BY ndcg_at_20 DESC
            """,
            "table",
            collection_id,
        ),
        card_payload(
            "Trend - Seed Affinity Top",
            database_id,
            f"""
            SELECT
              repo_name,
              seed_affinity,
              recent_seed_users,
              recent_active_users,
              trend_score,
              growth_ratio
            FROM {table("metrics_agent_trendy_repos")}
            ORDER BY seed_affinity DESC, trend_score DESC
            LIMIT 20
            """,
            "table",
            collection_id,
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("METABASE_URL", DEFAULT_URL))
    parser.add_argument("--email", default=os.environ.get("METABASE_EMAIL", DEFAULT_EMAIL))
    parser.add_argument("--password", default=os.environ.get("METABASE_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--key-path", type=Path, default=Path("gcp-key.json"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mb = setup_or_login(args.url, args.email, args.password)
    database_id = ensure_bigquery_database(
        mb,
        name=f"BigQuery {args.project}.{args.dataset}",
        project=args.project,
        dataset=args.dataset,
        key_path=args.key_path,
    )
    collection_id = ensure_collection(mb, "BDA 데이터 플랫폼")
    dashboard_id = ensure_dashboard(mb, "GitHub Archive 기초 지표", collection_id)
    trendy_dashboard_id = ensure_dashboard_with_description(
        mb,
        "AI Agent 트렌디 레포",
        collection_id,
        "OpenClaw와 oh-my-openagent seed를 기준으로 AI agent 생태계의 트렌디 repo를 설명하고 검증한다.",
    )

    cards = build_cards(database_id, collection_id, args.project, args.dataset)
    trendy_cards = build_trendy_repo_cards(database_id, collection_id, args.project, args.dataset)
    delete_existing_cards(
        mb,
        collection_id,
        {card["name"] for card in cards + trendy_cards},
    )
    clear_dashboard_cards(mb, dashboard_id)
    clear_dashboard_cards(mb, trendy_dashboard_id)

    card_ids = [create_card(mb, card) for card in cards]
    layout = [
        (0, 0, 6, 4),
        (0, 6, 6, 4),
        (0, 12, 12, 4),
        (4, 0, 12, 7),
        (4, 12, 12, 7),
        (11, 0, 12, 6),
        (11, 12, 12, 6),
        (17, 0, 24, 6),
        (23, 0, 6, 4),
        (23, 6, 18, 6),
        (29, 0, 12, 6),
        (29, 12, 12, 6),
        (35, 0, 24, 8),
        (43, 0, 12, 7),
        (43, 12, 12, 7),
        (50, 0, 16, 6),
        (50, 16, 8, 6),
    ]
    replace_dashboard_cards(mb, dashboard_id, card_ids, layout)

    trendy_card_ids = [create_card(mb, card) for card in trendy_cards]
    trendy_layout = [
        (0, 0, 6, 4),
        (0, 6, 6, 4),
        (0, 12, 12, 4),
        (4, 0, 24, 8),
        (12, 0, 12, 8),
        (12, 12, 12, 8),
        (20, 0, 12, 7),
        (20, 12, 12, 7),
        (27, 0, 14, 7),
        (27, 14, 10, 7),
    ]
    replace_dashboard_cards(mb, trendy_dashboard_id, trendy_card_ids, trendy_layout)

    print(
        json.dumps(
            {
                "metabase_url": args.url,
                "database_id": database_id,
                "collection_id": collection_id,
                "dashboard_id": dashboard_id,
                "dashboard_url": f"{args.url}/dashboard/{dashboard_id}",
                "trendy_dashboard_id": trendy_dashboard_id,
                "trendy_dashboard_url": f"{args.url}/dashboard/{trendy_dashboard_id}",
                "cards": len(card_ids),
                "trendy_cards": len(trendy_card_ids),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
