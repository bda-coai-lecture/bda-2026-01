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
OLD_DASHBOARD_CARD_NAMES = {
    "Signal - Latest Active Users",
    "Signal - Latest Events",
    "Signal - Latest W1 Retention",
    "Signal - Top Trend Score",
    "Signal - Activity Trend",
    "Signal - Event Mix",
    "Signal - Trend Leaderboard",
    "Signal - Growth vs Affinity",
    "Signal - Retention Health",
    "Signal - Model Validation",
    "D1 Retention",
    "D7 Retention",
    "D30 Retention",
    "Carrying Capacity",
    "D1/D7/D30 Active Users",
    "D1 Active Users",
    "D7 Active Users",
    "D30 Active Users",
    "Weekly User Retention",
    "Monthly User Retention",
    "User Retention Latest Periods",
    "Cohort retention heatmap",
    "Weekly Cohort Retention Heatmap",
    "Monthly Cohort Retention Heatmap",
    "Monthly Active User Retention Heatmap",
    "OSS Weekly Cohort Retention Heatmap",
    "OSS Monthly Cohort Retention Heatmap",
    "OSS Monthly Active User Retention Heatmap",
    "GitHub Core Weekly Retention Heatmap",
    "GitHub Core Weekly Active User Retention Heatmap",
    "GitHub Core Monthly Active User Retention Heatmap",
    "GitHub Core D1/D7/D30 Active Users",
}


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


def percent_column_settings(prefix: str, count: int) -> dict[str, dict[str, Any]]:
    settings: dict[str, dict[str, Any]] = {}
    for index in range(1, count + 1):
        name = f"{prefix}{index}"
        value = {"number_style": "percent", "decimals": 1}
        settings[json.dumps(["name", name], separators=(",", ":"))] = value
        settings[json.dumps(["field", name, {"base-type": "type/Float"}], separators=(",", ":"))] = value
    return settings


def retention_heatmap_settings(
    prefix: str,
    count: int,
    min_value: float,
    max_value: float,
    leading_columns: list[str],
) -> dict[str, Any]:
    columns = [f"{prefix}{index}" for index in range(1, count + 1)]
    return {
        "table.columns": [
            *[{"name": column, "enabled": True} for column in leading_columns],
            *[
                {"name": column, "enabled": True, "number_style": "percent", "decimals": 1}
                for column in columns
            ],
        ],
        "column_settings": percent_column_settings(prefix, count),
        "table.column_formatting": [
            {
                "columns": columns,
                "type": "range",
                "colors": ["#fca5a5", "#fde68a", "#86efac"],
                "min_type": "custom",
                "min_value": min_value,
                "max_type": "custom",
                "max_value": max_value,
            },
        ],
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


def find_dashboard_by_names(mb: Metabase, names: list[str]) -> dict[str, Any] | None:
    dashboards = mb.get_json("/api/dashboard")
    for name in names:
        for dashboard in dashboards:
            if dashboard.get("name") == name and not dashboard.get("archived"):
                return dashboard
    return None


def ensure_dashboard(
    mb: Metabase,
    name: str,
    collection_id: int,
    aliases: list[str] | None = None,
) -> int:
    existing = find_dashboard_by_names(mb, [*(aliases or []), name])
    if existing:
        mb.put_json(
            f"/api/dashboard/{existing['id']}",
            {
                "name": name,
                "description": "GitHub Archive 기초 지표: DAU, 이벤트, WAU, 유저 세그먼트",
                "collection_id": collection_id,
            },
        )
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
    aliases: list[str] | None = None,
) -> int:
    existing = find_dashboard_by_names(mb, [*(aliases or []), name])
    if existing:
        mb.put_json(
            f"/api/dashboard/{existing['id']}",
            {
                "name": name,
                "description": description,
                "collection_id": collection_id,
            },
        )
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
            "일별 DAU",
            database_id,
            f"""
            SELECT activity_date, active_users
            FROM {table("metrics_daily")}
            ORDER BY activity_date
            """,
            "line",
            collection_id,
            {
                "graph.y_axis.auto_range": False,
                "graph.y_axis.min": 500000,
                "graph.y_axis.max": 800000,
                "graph.y_axis.title_text": "Active users",
            },
        ),
        card_payload(
            "일별 이벤트 수",
            database_id,
            f"""
            SELECT activity_date, total_events
            FROM {table("metrics_daily")}
            ORDER BY activity_date
            """,
            "line",
            collection_id,
            {
                "graph.y_axis.auto_range": False,
                "graph.y_axis.min": 2000000,
                "graph.y_axis.max": 4000000,
                "graph.y_axis.title_text": "Events",
            },
        ),
        card_payload(
            "이벤트 타입별 일별 이벤트 수",
            database_id,
            f"""
            SELECT activity_date, action, total_events
            FROM {table("metrics_event_type_daily")}
            WHERE action IN ('PushEvent', 'WatchEvent', 'ForkEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent')
            ORDER BY activity_date, action
            """,
            "line",
            collection_id,
            {
                "graph.y_axis.auto_range": False,
                "graph.y_axis.min": 1000,
                "graph.y_axis.max": 4000000,
                "graph.y_axis.scale": "log",
                "graph.y_axis.title_text": "Events, log scale",
            },
        ),
        card_payload(
            "이벤트 타입별 일별 활성 유저 수",
            database_id,
            f"""
            SELECT activity_date, action, COUNT(DISTINCT user_id) AS active_users
            FROM {table("fact_user_repo_activity")}
            WHERE action IN ('PushEvent', 'WatchEvent', 'ForkEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent')
            GROUP BY activity_date, action
            ORDER BY activity_date, action
            """,
            "line",
            collection_id,
            {
                "graph.y_axis.auto_range": False,
                "graph.y_axis.min": 0,
                "graph.y_axis.max": 700000,
                "graph.y_axis.title_text": "Active users",
            },
        ),
        card_payload(
            "First seen 기준 유저 lifecycle",
            database_id,
            f"""
            SELECT
              activity_date,
              new_users,
              existing_users,
              returning_users,
              churned_users
            FROM {table("metrics_user_lifecycle_sample_daily")}
            WHERE is_complete_28d_window
            ORDER BY activity_date
            """,
            "line",
            collection_id,
            {
                "graph.y_axis.auto_range": False,
                "graph.y_axis.min": 0,
                "graph.y_axis.max": 1100,
                "graph.y_axis.title_text": "Users",
            },
        ),
        card_payload(
            "Weekly Cohort Retention Heatmap",
            database_id,
            f"""
            SELECT
              week_start,
              active_users,
              w1,
              w2,
              w3,
              w4,
              w5,
              w6,
              w7,
              w8,
              w9,
              w10,
              w11,
              w12
            FROM {table("metrics_cohort_retention_weekly_heatmap")}
            ORDER BY week_start
            """,
            "table",
            collection_id,
            retention_heatmap_settings("w", 12, 0.15, 0.55, ["week_start", "active_users"]),
        ),
        card_payload(
            "Monthly Active User Retention Heatmap",
            database_id,
            f"""
            SELECT
              month_start,
              active_users,
              m1,
              m2,
              m3,
              m4,
              m5,
              m6,
              m7,
              m8,
              m9,
              m10,
              m11,
              m12
            FROM {table("metrics_cohort_retention_monthly_heatmap")}
            ORDER BY month_start
            """,
            "table",
            collection_id,
            retention_heatmap_settings("m", 12, 0.20, 0.50, ["month_start", "active_users"]),
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


def build_github_core_cards(
    database_id: int,
    collection_id: int,
    project: str,
    dataset: str,
) -> list[dict[str, Any]]:
    table = lambda name: f"`{project}.{dataset}.{name}`"
    month_bounds = f"""
      WITH bounds AS (
        SELECT
          DATE_TRUNC(MAX(activity_date), MONTH) AS month_start,
          MAX(activity_date) AS latest_date
        FROM {table("metrics_daily")}
      )
    """

    active_user_windows_sql = f"""
            WITH fact_bounds AS (
              SELECT MIN(activity_date) AS min_date
              FROM {table("fact_user_repo_activity")}
            ),
            date_spine AS (
              SELECT activity_date
              FROM {table("metrics_daily")}, fact_bounds
              WHERE activity_date >= DATE_ADD(fact_bounds.min_date, INTERVAL 29 DAY)
            )
            SELECT
              date_spine.activity_date,
              COUNT(DISTINCT IF(fact.activity_date = date_spine.activity_date, fact.user_id, NULL)) AS d1_active_users,
              COUNT(DISTINCT IF(fact.activity_date >= DATE_SUB(date_spine.activity_date, INTERVAL 6 DAY), fact.user_id, NULL)) AS d7_active_users,
              COUNT(DISTINCT fact.user_id) AS d30_active_users
            FROM date_spine
            JOIN {table("fact_user_repo_activity")} fact
              ON fact.activity_date BETWEEN DATE_SUB(date_spine.activity_date, INTERVAL 29 DAY)
                                     AND date_spine.activity_date
            GROUP BY date_spine.activity_date
            ORDER BY date_spine.activity_date
            """

    return [
        card_payload(
            "이번 달 AU",
            database_id,
            f"""
            {month_bounds}
            SELECT COUNT(DISTINCT user_id) AS au
            FROM {table("fact_user_repo_activity")}, bounds
            WHERE activity_date BETWEEN bounds.month_start AND bounds.latest_date
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "이번 달 AR",
            database_id,
            f"""
            {month_bounds}
            SELECT COUNT(DISTINCT repo_id) AS ar
            FROM {table("fact_user_repo_activity")}, bounds
            WHERE activity_date BETWEEN bounds.month_start AND bounds.latest_date
            """,
            "scalar",
            collection_id,
        ),
        card_payload(
            "GitHub Core D1/D7/D30 Active Users",
            database_id,
            active_user_windows_sql,
            "line",
            collection_id,
            {
                "graph.dimensions": ["activity_date"],
                "graph.metrics": ["d1_active_users", "d7_active_users", "d30_active_users"],
                "graph.y_axis.title_text": "Active users",
            },
        ),
        card_payload(
            "GitHub Core Weekly Active User Retention Heatmap",
            database_id,
            f"""
            SELECT
              week_start,
              active_users,
              w1,
              w2,
              w3,
              w4,
              w5,
              w6,
              w7,
              w8,
              w9,
              w10,
              w11,
              w12
            FROM {table("metrics_cohort_retention_weekly_heatmap")}
            ORDER BY week_start
            """,
            "table",
            collection_id,
            retention_heatmap_settings("w", 12, 0.15, 0.55, ["week_start", "active_users"]),
        ),
        card_payload(
            "GitHub Core Monthly Active User Retention Heatmap",
            database_id,
            f"""
            SELECT
              month_start,
              active_users,
              m1,
              m2,
              m3,
              m4,
              m5,
              m6,
              m7,
              m8,
              m9,
              m10,
              m11,
              m12
            FROM {table("metrics_cohort_retention_monthly_heatmap")}
            ORDER BY month_start
            """,
            "table",
            collection_id,
            retention_heatmap_settings("m", 12, 0.20, 0.50, ["month_start", "active_users"]),
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
    dashboard_id = ensure_dashboard(
        mb,
        "GitHub Archive 기초 지표",
        collection_id,
        aliases=["GitHub Archive Core Metrics"],
    )
    trendy_dashboard_id = ensure_dashboard_with_description(
        mb,
        "AI Agent 트렌디 레포",
        collection_id,
        "OpenClaw와 oh-my-openagent seed를 기준으로 AI agent 생태계의 트렌디 repo를 설명하고 검증한다.",
    )
    github_core_dashboard_id = ensure_dashboard_with_description(
        mb,
        "GitHub Core Metrics",
        collection_id,
        "GitHub Archive 기반 핵심 활동 지표와 retention을 요약한다.",
        aliases=["OSS Signal 운영 대시보드"],
    )

    cards = build_cards(database_id, collection_id, args.project, args.dataset)
    trendy_cards = build_trendy_repo_cards(database_id, collection_id, args.project, args.dataset)
    github_core_cards = build_github_core_cards(database_id, collection_id, args.project, args.dataset)
    delete_existing_cards(
        mb,
        collection_id,
        {card["name"] for card in cards + trendy_cards + github_core_cards}
        | OLD_DASHBOARD_CARD_NAMES,
    )
    clear_dashboard_cards(mb, dashboard_id)
    clear_dashboard_cards(mb, trendy_dashboard_id)
    clear_dashboard_cards(mb, github_core_dashboard_id)

    card_ids = [create_card(mb, card) for card in cards]
    layout = [
        (0, 0, 12, 8),
        (0, 12, 12, 8),
        (8, 0, 12, 8),
        (8, 12, 12, 8),
        (16, 0, 24, 8),
        (24, 0, 24, 9),
        (33, 0, 24, 9),
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

    github_core_card_ids = [create_card(mb, card) for card in github_core_cards]
    github_core_layout = [
        (0, 0, 12, 5),
        (0, 12, 12, 5),
        (5, 0, 24, 8),
        (13, 0, 24, 10),
        (23, 0, 24, 10),
    ]
    replace_dashboard_cards(mb, github_core_dashboard_id, github_core_card_ids, github_core_layout)

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
                "github_core_dashboard_id": github_core_dashboard_id,
                "github_core_dashboard_url": f"{args.url}/dashboard/{github_core_dashboard_id}",
                "cards": len(card_ids),
                "trendy_cards": len(trendy_card_ids),
                "github_core_cards": len(github_core_card_ids),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
