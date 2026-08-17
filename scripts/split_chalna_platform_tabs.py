#!/usr/bin/env python3
"""Split Chalna's device dashboard tab into iPhone and Android tabs."""

import argparse
import json
import os
import urllib.error
import urllib.request


DEFAULT_URL = "http://localhost:3001"
DEFAULT_DASHBOARD_ID = 7
SOURCE_TAB_ID = 33
SOURCE_TAB_NAME = "iPhone (기기 기준)"
ANDROID_TAB_NAME = "Android (기기 기준)"


def api(base_url, method, path, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(base_url + path, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("X-Metabase-Session", token)
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode() or "{}")
    except urllib.error.HTTPError as error:
        detail = error.read().decode()[:1000]
        raise RuntimeError(f"{method} {path} failed ({error.code}): {detail}") from error


def native_sql(card):
    query = card["dataset_query"]
    if "stages" in query:
        return query["stages"][0]["native"]
    return query["native"]["query"]


def platform_sql(sql, platform):
    if "events_kpi" not in sql:
        raise ValueError("Expected events_kpi in native query")
    return sql.replace(
        "events_kpi",
        f"(select * from events_kpi where platform = '{platform}')",
    )


def card_payload(card, sql, name):
    return {
        "name": name,
        "description": card.get("description"),
        "dataset_query": {
            "database": card["database_id"],
            "type": "native",
            "native": {"query": sql},
        },
        "display": card["display"],
        "visualization_settings": card.get("visualization_settings") or {},
        "parameters": card.get("parameters") or [],
        "collection_id": card.get("collection_id"),
    }


def dashcard_payload(dashcard, card_id=None, tab_id=None, dashcard_id=None):
    return {
        "id": dashcard["id"] if dashcard_id is None else dashcard_id,
        "card_id": dashcard["card_id"] if card_id is None else card_id,
        "dashboard_tab_id": (
            dashcard.get("dashboard_tab_id") if tab_id is None else tab_id
        ),
        "row": dashcard["row"],
        "col": dashcard["col"],
        "size_x": dashcard["size_x"],
        "size_y": dashcard["size_y"],
        "series": dashcard.get("series") or [],
        "parameter_mappings": dashcard.get("parameter_mappings") or [],
        "visualization_settings": dashcard.get("visualization_settings") or {},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("METABASE_URL", DEFAULT_URL))
    parser.add_argument("--dashboard-id", type=int, default=DEFAULT_DASHBOARD_ID)
    parser.add_argument("--email", default=os.environ.get("METABASE_EMAIL"))
    parser.add_argument("--password", default=os.environ.get("METABASE_PASSWORD"))
    args = parser.parse_args()
    if not args.email or not args.password:
        parser.error("Set METABASE_EMAIL and METABASE_PASSWORD")

    token = api(
        args.url,
        "POST",
        "/api/session",
        body={"username": args.email, "password": args.password},
    )["id"]
    dashboard = api(args.url, "GET", f"/api/dashboard/{args.dashboard_id}", token)

    if any(tab["name"] == ANDROID_TAB_NAME for tab in dashboard.get("tabs", [])):
        raise RuntimeError(f"Dashboard already has a {ANDROID_TAB_NAME!r} tab")

    source_dashcards = [
        dashcard
        for dashcard in dashboard.get("dashcards", [])
        if dashcard.get("dashboard_tab_id") == SOURCE_TAB_ID
    ]
    if not source_dashcards:
        raise RuntimeError(f"No dashcards found for source tab {SOURCE_TAB_ID}")

    originals = []
    for dashcard in source_dashcards:
        card = api(args.url, "GET", f"/api/card/{dashcard['card_id']}", token)
        sql = native_sql(card)
        ios_sql = platform_sql(sql, "ios")
        android_sql = platform_sql(sql, "android")
        for candidate in (ios_sql, android_sql):
            result = api(
                args.url,
                "POST",
                "/api/dataset",
                token,
                {
                    "database": card["database_id"],
                    "type": "native",
                    "native": {"query": candidate},
                },
            )
            if result.get("error"):
                raise RuntimeError(f"Query validation failed: {result['error']}")
        originals.append((dashcard, card, ios_sql, android_sql))

    android_cards = []
    for _, card, _, android_sql in originals:
        result = api(
            args.url,
            "POST",
            "/api/card",
            token,
            card_payload(card, android_sql, f"{card['name']} · Android"),
        )
        android_cards.append(result["id"])

    for _, card, ios_sql, _ in originals:
        api(
            args.url,
            "PUT",
            f"/api/card/{card['id']}",
            token,
            card_payload(card, ios_sql, card["name"]),
        )

    new_tab_id = -1000
    tabs = []
    for tab in dashboard.get("tabs", []):
        name = SOURCE_TAB_NAME if tab["id"] == SOURCE_TAB_ID else tab["name"]
        tabs.append({"id": tab["id"], "name": name})
        if tab["id"] == SOURCE_TAB_ID:
            tabs.append({"id": new_tab_id, "name": ANDROID_TAB_NAME})

    dashcards = [dashcard_payload(dashcard) for dashcard in dashboard["dashcards"]]
    for index, ((source, _, _, _), card_id) in enumerate(
        zip(originals, android_cards), start=1
    ):
        dashcards.append(
            dashcard_payload(
                source,
                card_id=card_id,
                tab_id=new_tab_id,
                dashcard_id=-index,
            )
        )

    api(
        args.url,
        "PUT",
        f"/api/dashboard/{args.dashboard_id}",
        token,
        {"tabs": tabs, "dashcards": dashcards},
    )
    print(
        f"Created {ANDROID_TAB_NAME!r} with {len(android_cards)} cards; "
        f"filtered {SOURCE_TAB_NAME!r} to iOS."
    )


if __name__ == "__main__":
    main()
