#!/usr/bin/env python3

import polars as pl

from gpt_racing.summary import summarize_results


class TestSummary:
    def test_basic(self):
        points_results = pl.DataFrame(
            [
                {"user_id": 0, "contest_id": 0, "finish_position": 3, "contest_time": 0, "points": 2, "drop": True},
                {"user_id": 0, "contest_id": 1, "finish_position": 2, "contest_time": 1, "points": 3, "drop": True},
                {"user_id": 0, "contest_id": 2, "finish_position": 1, "contest_time": 2, "points": 4, "drop": False},
                {"user_id": 0, "contest_id": 3, "finish_position": 0, "contest_time": 3, "points": 5, "drop": False},
                {"user_id": 1, "contest_id": 0, "finish_position": 2, "contest_time": 0, "points": 3, "drop": True},
                {"user_id": 1, "contest_id": 1, "finish_position": 1, "contest_time": 1, "points": 4, "drop": False},
                {"user_id": 1, "contest_id": 2, "finish_position": 0, "contest_time": 2, "points": 5, "drop": False},
                {"user_id": 2, "contest_id": 0, "finish_position": 1, "contest_time": 0, "points": 4, "drop": False},
                {"user_id": 2, "contest_id": 1, "finish_position": 0, "contest_time": 1, "points": 5, "drop": False},
                {"user_id": 3, "contest_id": 0, "finish_position": 0, "contest_time": 0, "points": 5, "drop": False},
            ]
        )

        race_name_df = pl.DataFrame(
            [
                {"contest_id": 0, "race_name": "race 0"},
                {"contest_id": 1, "race_name": "race 1"},
                {"contest_id": 2, "race_name": "race 2"},
                {"contest_id": 3, "race_name": "race 3"},
            ]
        )
        driver_info_df = pl.DataFrame(
            [
                {"user_id": 0, "name": "Robby"},
                {"user_id": 0, "name": "Jesse"},
                {"user_id": 0, "name": "Logan"},
                {"user_id": 0, "name": "Marc"},
            ]
        )

        out = summarize_results(points_results, race_name_df, driver_info_df)

    def test_incident_points(self):
        pass
