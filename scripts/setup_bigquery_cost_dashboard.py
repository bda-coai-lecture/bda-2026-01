"""Provision a Metabase dashboard for BigQuery query-cost monitoring."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from setup_metabase_dashboard import (
    DEFAULT_EMAIL,
    DEFAULT_PASSWORD,
    DEFAULT_PROJECT,
    DEFAULT_URL,
    card_payload,
    clear_dashboard_cards,
    create_card,
    delete_existing_cards,
    ensure_bigquery_database,
    ensure_collection,
    ensure_dashboard_with_description,
    replace_dashboard_cards,
    setup_or_login,
)


DEFAULT_DATASET = "mart"
DEFAULT_LOCATION = "us"
DEFAULT_PRICE_PER_TIB = 6.25
DASHBOARD_NAME = "BigQuery 비용 관리"


def jobs_table(location: str) -> str:
    return f"`region-{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`"


def cost_expr(price_per_tib: float) -> str:
    return f"SAFE_DIVIDE(total_bytes_billed, POW(2, 40)) * {price_per_tib}"


def base_where(project: str) -> str:
    return f"""
      project_id = '{project}'
      AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
      AND job_type = 'QUERY'
      AND state = 'DONE'
      AND error_result IS NULL
      AND total_bytes_billed IS NOT NULL
      AND COALESCE(cache_hit, FALSE) = FALSE
    """


def build_cost_cards(
    database_id: int,
    collection_id: int,
    project: str,
    location: str,
    price_per_tib: float,
) -> list[dict[str, Any]]:
    table = jobs_table(location)
    where = base_where(project)
    usd = cost_expr(price_per_tib)

    return [
        card_payload(
            "BQ Cost - 최근 14일 추정 요금",
            database_id,
            f"""
            SELECT ROUND(SUM({usd}), 2) AS estimated_on_demand_cost_usd
            FROM {table}
            WHERE {where}
            """,
            "scalar",
            collection_id,
            {
                "scalar.field": "estimated_on_demand_cost_usd",
                "column_settings": {
                    '["name","estimated_on_demand_cost_usd"]': {
                        "number_style": "currency",
                        "currency": "USD",
                        "decimals": 2,
                    }
                },
            },
        ),
        card_payload(
            "BQ Cost - 최근 14일 전체 요금 추이",
            database_id,
            f"""
            SELECT
              DATE(creation_time) AS usage_date,
              ROUND(SUM({usd}), 2) AS estimated_on_demand_cost_usd,
              ROUND(SAFE_DIVIDE(SUM(total_bytes_billed), POW(2, 40)), 3) AS billed_tib,
              COUNT(*) AS query_jobs
            FROM {table}
            WHERE {where}
            GROUP BY usage_date
            ORDER BY usage_date
            """,
            "line",
            collection_id,
            {
                "graph.dimensions": ["usage_date"],
                "graph.metrics": ["estimated_on_demand_cost_usd"],
                "graph.y_axis.title_text": "Estimated cost, USD",
                "column_settings": {
                    '["name","estimated_on_demand_cost_usd"]': {
                        "number_style": "currency",
                        "currency": "USD",
                        "decimals": 2,
                    },
                    '["name","billed_tib"]': {"decimals": 3},
                },
            },
        ),
        card_payload(
            "BQ Cost - 최근 14일 비용 Top 쿼리",
            database_id,
            f"""
            SELECT
              creation_time,
              user_email,
              job_id,
              statement_type,
              ROUND({usd}, 2) AS estimated_on_demand_cost_usd,
              ROUND(SAFE_DIVIDE(total_bytes_billed, POW(2, 40)), 3) AS billed_tib,
              ROUND(SAFE_DIVIDE(total_slot_ms, 1000 * 60), 1) AS slot_minutes,
              SUBSTR(REGEXP_REPLACE(query, r'\\s+', ' '), 1, 500) AS query_preview
            FROM {table}
            WHERE {where}
            ORDER BY estimated_on_demand_cost_usd DESC, creation_time DESC
            LIMIT 100
            """,
            "table",
            collection_id,
            {
                "table.columns": [
                    {"name": "creation_time", "enabled": True},
                    {"name": "user_email", "enabled": True},
                    {"name": "estimated_on_demand_cost_usd", "enabled": True},
                    {"name": "billed_tib", "enabled": True},
                    {"name": "slot_minutes", "enabled": True},
                    {"name": "statement_type", "enabled": True},
                    {"name": "job_id", "enabled": True},
                    {"name": "query_preview", "enabled": True},
                ],
                "column_settings": {
                    '["name","estimated_on_demand_cost_usd"]': {
                        "number_style": "currency",
                        "currency": "USD",
                        "decimals": 2,
                    },
                    '["name","billed_tib"]': {"decimals": 3},
                    '["name","slot_minutes"]': {"decimals": 1},
                },
            },
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("METABASE_URL", DEFAULT_URL))
    parser.add_argument("--email", default=os.environ.get("METABASE_EMAIL", DEFAULT_EMAIL))
    parser.add_argument("--password", default=os.environ.get("METABASE_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--project", default=os.environ.get("BQ_BILLING_PROJECT", DEFAULT_PROJECT))
    parser.add_argument("--dataset", default=os.environ.get("BQ_METABASE_DATASET", DEFAULT_DATASET))
    parser.add_argument("--location", default=os.environ.get("BQ_JOB_LOCATION", DEFAULT_LOCATION))
    parser.add_argument(
        "--price-per-tib",
        type=float,
        default=float(os.environ.get("BQ_ON_DEMAND_PRICE_PER_TIB", DEFAULT_PRICE_PER_TIB)),
    )
    parser.add_argument("--key-path", type=Path, default=Path(os.environ.get("GCP_KEY_PATH", "gcp-key.json")))
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
    dashboard_id = ensure_dashboard_with_description(
        mb,
        DASHBOARD_NAME,
        collection_id,
        "BigQuery INFORMATION_SCHEMA 기반 최근 14일 쿼리 비용 추정과 비용 상위 쿼리를 추적한다.",
    )

    cards = build_cost_cards(
        database_id=database_id,
        collection_id=collection_id,
        project=args.project,
        location=args.location,
        price_per_tib=args.price_per_tib,
    )
    delete_existing_cards(mb, collection_id, {card["name"] for card in cards})
    clear_dashboard_cards(mb, dashboard_id)

    card_ids = [create_card(mb, card) for card in cards]
    replace_dashboard_cards(
        mb,
        dashboard_id,
        card_ids,
        [
            (0, 0, 8, 4),
            (4, 0, 24, 8),
            (12, 0, 24, 10),
        ],
    )

    print(
        json.dumps(
            {
                "metabase_url": args.url,
                "dashboard_id": dashboard_id,
                "dashboard_url": f"{args.url}/dashboard/{dashboard_id}",
                "database_id": database_id,
                "collection_id": collection_id,
                "cards": len(card_ids),
                "project": args.project,
                "location": args.location,
                "price_per_tib": args.price_per_tib,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
