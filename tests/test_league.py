from unittest.mock import MagicMock

from gpt_racing.league import fetch_new_races


def _mock_client(sessions):
    client = MagicMock()
    client.get_league_sessions.return_value = sessions
    return client


def test_returns_new_sessions():
    sessions = [
        {"subsession_id": 1, "track_name": "Spa", "launch_at": "2026-01-01"},
        {"subsession_id": 2, "track_name": "Monza", "launch_at": "2026-02-01"},
    ]
    result = fetch_new_races(_mock_client(sessions), 99, 1, existing_ids={1})
    assert len(result) == 1
    assert result[0]["subsession_id"] == 2


def test_returns_empty_when_all_known():
    sessions = [
        {"subsession_id": 1, "track_name": "Spa", "launch_at": "2026-01-01"},
    ]
    result = fetch_new_races(_mock_client(sessions), 99, 1, existing_ids={1})
    assert result == []


def test_returns_all_when_none_known():
    sessions = [
        {"subsession_id": 1, "track_name": "Spa", "launch_at": "2026-01-01"},
        {"subsession_id": 2, "track_name": "Monza", "launch_at": "2026-02-01"},
    ]
    result = fetch_new_races(_mock_client(sessions), 99, 1, existing_ids=set())
    assert [s["subsession_id"] for s in result] == [1, 2]


def test_empty_league_sessions():
    result = fetch_new_races(_mock_client([]), 99, 1, existing_ids={1, 2})
    assert result == []
