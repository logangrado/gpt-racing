#!/usr/bin/env python3


import pandas as pd
import pytest

from gpt_racing import iracing_data


class TestCachedIRClient:
    @pytest.mark.skip("not implemented")
    def test_result(self):
        client = iracing_data.CachedIRClient()

        x = client.result(63788123)
        import ipdb

        ipdb.set_trace()
        pass


class TestRaceResults:
    def test_race_result(self, client):
        subsession_id = 65426723
        result = client.get_race_result(subsession_id)

        assert len(result) == 23

        pd.testing.assert_frame_equal(
            result.iloc[:2],
            pd.DataFrame(
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
                        "session_end_time": pd.Timestamp("2023-12-12 02:16:29"),
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
                        "session_end_time": pd.Timestamp("2023-12-12 02:16:29"),
                    },
                ]
            ),
            check_dtype=False,
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
        pd.testing.assert_frame_equal(
            result.iloc[:2],
            pd.DataFrame(
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
                        "session_end_time": pd.Timestamp("2023-12-12 02:16:29"),
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
                        "session_end_time": pd.Timestamp("2023-12-12 02:16:29"),
                    },
                ]
            ),
            check_dtype=False,
        )


@pytest.mark.skip(reason="Not implemented")
class TestGetLapData:
    def test_basic(self):
        pass


@pytest.mark.skip(reason="Not implemented")
class TestSearch:
    pass
