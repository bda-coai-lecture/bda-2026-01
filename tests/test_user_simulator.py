from __future__ import annotations

import pandas as pd
import pytest

import ghrec.user_simulator as user_simulator
from ghrec.user_simulator import (
    ColdStartRecommendationError,
    RecsysApiError,
    call_recommendation_api,
    event_label,
    format_topics,
    load_repo_name_map,
    parse_github_username,
    rows_dataframe,
)


class StubResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("torvalds", "torvalds"),
        ("@torvalds", "torvalds"),
        ("https://github.com/torvalds", "torvalds"),
        ("github.com/torvalds", "torvalds"),
        ("www.github.com/torvalds?tab=repositories", "torvalds"),
    ],
)
def test_parse_github_username(raw: str, expected: str) -> None:
    assert parse_github_username(raw) == expected


@pytest.mark.parametrize("raw", ["", "https://gitlab.com/foo", "bad/name", "-bad", "bad-"])
def test_parse_github_username_rejects_invalid_values(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_github_username(raw)


def test_event_label_shows_top_nonzero_counts() -> None:
    row = pd.Series(
        {
            "watch_cnt": 2,
            "fork_cnt": 0,
            "pr_cnt": 3,
            "push_cnt": 4,
            "issue_cnt": 0,
            "comment_cnt": 1,
        }
    )

    assert event_label(row) == "watch 2, pr 3, push 4"


def test_event_label_falls_back_for_model_history() -> None:
    assert event_label(pd.Series({})) == "model history"


def test_format_topics_accepts_json_list() -> None:
    assert format_topics('["python", "ml", "api"]') == "python, ml, api"


def test_format_topics_handles_nullish_values() -> None:
    assert format_topics(None) == ""
    assert format_topics(float("nan")) == ""


def test_load_repo_name_map_skips_nullish_keys_and_values(tmp_path) -> None:
    import pickle

    path = tmp_path / "repo_name_map.pkl"
    with path.open("wb") as f:
        pickle.dump({1: "owner/repo", pd.NA: "bad/repo", 2: pd.NA, "3": "other/repo"}, f)

    assert load_repo_name_map(path) == {1: "owner/repo", 3: "other/repo"}


def test_rows_dataframe_preserves_columns_for_empty_rows() -> None:
    columns = ["Rank", "Repo", "Score"]

    df = rows_dataframe([], columns)

    assert df.empty
    assert list(df.columns) == columns
    assert df[columns].empty


def test_call_recommendation_api_raises_cold_start_for_unknown_actor(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args, **kwargs) -> StubResponse:
        return StubResponse(
            404,
            {
                "detail": {
                    "error": {
                        "code": "actor_not_found",
                        "message": "No candidates found for actor_id=251872223",
                        "details": {},
                    }
                }
            },
        )

    monkeypatch.setattr(user_simulator.requests, "post", fake_post)

    with pytest.raises(ColdStartRecommendationError) as exc_info:
        call_recommendation_api(251872223, k=20)

    assert exc_info.value.status_code == 404
    assert exc_info.value.code == "actor_not_found"
    assert "251872223" in str(exc_info.value)


def test_call_recommendation_api_preserves_non_cold_start_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args, **kwargs) -> StubResponse:
        return StubResponse(
            503,
            {
                "detail": {
                    "error": {
                        "code": "active_bundle_not_configured",
                        "message": "No active bundle configured",
                        "details": {},
                    }
                }
            },
        )

    monkeypatch.setattr(user_simulator.requests, "post", fake_post)

    with pytest.raises(RecsysApiError) as exc_info:
        call_recommendation_api(12345, k=20)

    assert not isinstance(exc_info.value, ColdStartRecommendationError)
    assert exc_info.value.status_code == 503
    assert exc_info.value.code == "active_bundle_not_configured"
