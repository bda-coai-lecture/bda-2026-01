from __future__ import annotations

import pandas as pd
import pytest

from ghrec.user_simulator import (
    event_label,
    format_topics,
    load_repo_name_map,
    parse_github_username,
)


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
