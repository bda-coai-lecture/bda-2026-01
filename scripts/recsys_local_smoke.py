"""Smoke test the local recommendation API.

Usage:
    uv run python scripts/recsys_local_smoke.py --actor-id 4
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import requests


def raise_with_body(resp: requests.Response) -> None:
    try:
        body = resp.json()
    except ValueError:
        body = resp.text
    raise RuntimeError(f"HTTP {resp.status_code} from {resp.url}: {body}")


def get_json(url: str) -> dict[str, Any]:
    resp = requests.get(url, timeout=10)
    if resp.status_code >= 400:
        raise_with_body(resp)
    return resp.json()


def post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    resp = requests.post(url, json=payload, timeout=60)
    if resp.status_code >= 400:
        raise_with_body(resp)
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--actor-id", type=int, default=4)
    parser.add_argument("--k", type=int, default=5)
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    health = get_json(f"{base_url}/health")
    bundle = get_json(f"{base_url}/v1/recsys/bundles/active")
    recs = post_json(
        f"{base_url}/v1/recsys/recommendations",
        {
            "actor_id": args.actor_id,
            "k": args.k,
            "bundle_id": None,
            "include_features": False,
            "include_sources": True,
        },
    )
    payload = {
        "health": health,
        "active_bundle_id": bundle.get("bundle_id"),
        "actor_id": recs.get("actor_id"),
        "recommendation_count": len(recs.get("items", [])),
        "top_items": recs.get("items", [])[: min(args.k, 5)],
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
