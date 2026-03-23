#!/usr/bin/env python3


import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from gpt_racing import iracing_data


class TestCachedIRClient:
    @pytest.mark.skip("not implemented")
    def test_result(self):
        client = iracing_data.CachedIRClient()

        x = client.result(63788123)
        import ipdb

        ipdb.set_trace()
        pass


@pytest.mark.integration
class TestRaceResults:
    def test_race_result(self, client):
        subsession_id = 65426723
        result = client.get_race_result(subsession_id)

        assert len(result) == 23

        expected = pl.DataFrame(
            [
                {
                    "cust_id": 407068,
                    "display_name": "Christian Youngwall",
                    "finish_position": 0,
                    "finish_position_in_class": 0,
                    "interval": 0,
                    "average_lap": 901658,
                    "laps_complete": 14,
                    "class_interval": 0,
                    "qual_lap_time": -1,
                    "starting_position": 1,
                    "starting_position_in_class": 1,
                    "incidents": 0,
                    "car_class_name": "Hosted All Cars Class",
                    "reason_out": "Running",
                    "subsession_id": 65426723,
                    "session_end_time": datetime.datetime(2023, 12, 12, 2, 16, 29).replace(
                        tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "cust_id": 483350,
                    "display_name": "Reed Gibson",
                    "finish_position": 1,
                    "finish_position_in_class": 1,
                    "interval": 47405,
                    "average_lap": 905043,
                    "laps_complete": 14,
                    "class_interval": 47405,
                    "qual_lap_time": -1,
                    "starting_position": 2,
                    "starting_position_in_class": 2,
                    "incidents": 4,
                    "car_class_name": "Hosted All Cars Class",
                    "reason_out": "Running",
                    "subsession_id": 65426723,
                    "session_end_time": datetime.datetime(2023, 12, 12, 2, 16, 29).replace(
                        tzinfo=datetime.timezone.utc
                    ),
                },
            ]
        )
        assert_frame_equal(
            result.head(2),
            expected,
            check_dtypes=False,
        )

    def test_qualy_result(self, client):
        subsession_id = 65426723
        result = client.get_qualy_result(subsession_id)

        assert set(result.columns) == {
            "cust_id",
            "display_name",
            "finish_position",
            "finish_position_in_class",
            "average_lap",
            "best_lap_time",
            "interval",
            "laps_complete",
            "class_interval",
            "qual_lap_time",
            "incidents",
            "reason_out",
            "subsession_id",
            "session_end_time",
        }

        assert len(result) == 23
        assert_frame_equal(
            result.head(2),
            pl.DataFrame(
                [
                    {
                        "cust_id": 260862,
                        "display_name": "Rafael Amorim",
                        "finish_position": 0,
                        "finish_position_in_class": 0,
                        "average_lap": 887449,
                        "best_lap_time": 886039,
                        "interval": 0,
                        "laps_complete": 2,
                        "class_interval": 0,
                        "qual_lap_time": -1,
                        "incidents": 0,
                        "reason_out": "Running",
                        "subsession_id": 65426723,
                        "session_end_time": datetime.datetime(2023, 12, 12, 2, 16, 29).replace(
                            tzinfo=datetime.timezone.utc
                        ),
                    },
                    {
                        "cust_id": 154321,
                        "display_name": "Leif Peterson",
                        "finish_position": 1,
                        "finish_position_in_class": 1,
                        "average_lap": 888409,
                        "best_lap_time": 887636,
                        "interval": 1597,
                        "laps_complete": 2,
                        "class_interval": 1597,
                        "qual_lap_time": -1,
                        "incidents": 0,
                        "reason_out": "Running",
                        "subsession_id": 65426723,
                        "session_end_time": datetime.datetime(2023, 12, 12, 2, 16, 29).replace(
                            tzinfo=datetime.timezone.utc
                        ),
                    },
                ]
            ),
            check_dtypes=False,
        )


class TestGetLeagueSessions:
    def _make_session(self, subsession_id, has_results, track_name="Sometrack", launch_at="2026-01-01T00:00:00Z"):
        return {
            "subsession_id": subsession_id,
            "has_results": has_results,
            "track": {"track_name": track_name},
            "launch_at": launch_at,
        }

    def _make_client(self, sessions):
        from unittest.mock import MagicMock
        from gpt_racing.iracing_data import IracingDataClient

        mock_ir = MagicMock()
        mock_ir.league_season_sessions.return_value = {"sessions": sessions}

        client = IracingDataClient.__new__(IracingDataClient)
        client._client = mock_ir
        client._cust_id = 0
        return client

    def test_returns_only_completed(self):
        sessions = [
            self._make_session(1001, has_results=True, track_name="Spa"),
            self._make_session(None, has_results=False, track_name="Monza"),
            self._make_session(1003, has_results=True, track_name="Silverstone"),
        ]
        client = self._make_client(sessions)
        result = client.get_league_sessions(99, 1)
        assert len(result) == 2
        assert result[0]["subsession_id"] == 1001
        assert result[0]["track_name"] == "Spa"
        assert result[1]["subsession_id"] == 1003

    def test_excludes_no_subsession_id(self):
        sessions = [
            self._make_session(None, has_results=True, track_name="Spa"),
        ]
        client = self._make_client(sessions)
        result = client.get_league_sessions(99, 1)
        assert result == []

    def test_empty_sessions(self):
        client = self._make_client([])
        result = client.get_league_sessions(99, 1)
        assert result == []

    def test_result_fields(self):
        sessions = [self._make_session(42, has_results=True, track_name="Daytona", launch_at="2026-03-01T18:00:00Z")]
        client = self._make_client(sessions)
        result = client.get_league_sessions(99, 1)
        assert result == [{"subsession_id": 42, "track_name": "Daytona", "launch_at": "2026-03-01T18:00:00Z"}]


@pytest.mark.skip(reason="Not implemented")
class TestGetLapData:
    def test_basic(self):
        pass


@pytest.mark.skip(reason="Not implemented")
class TestSearch:
    pass
