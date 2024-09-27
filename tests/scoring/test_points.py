#!/usr/bin/env python3

import pandas as pd
import polars as pl
from polars.testing import assert_frame_equal

from gpt_racing.scoring.points import compute_points_score
from gpt_racing.config import PointsScoringConfig


class TestPointsScoring:
    def test_single_contest(self):
        data = pl.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
                {"user_id": 1, "finish_position": 1},
                {"user_id": 2, "finish_position": 2},
            ]
        )
        data = data.with_columns(pl.lit(0).alias("contest_id"), pl.lit(0).alias("contest_time"))

        config = PointsScoringConfig.model_validate({"points": [5, 2]})
        result = compute_points_score(data, config)

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2],
                "finish_position": [0, 1, 2],
                "contest_id": [0, 0, 0],
                "contest_time": [0, 0, 0],
                "points": [5, 2, 0],
                "drop": [False, False, False],
            }
        )
        # assert_frame_equal(result, expected)

    def test_multi_contest(self):
        data = pl.DataFrame(
            [
                {"user_id": 0, "finish_position": 0, "contest_id": 0, "contest_time": 0},
                {"user_id": 1, "finish_position": 1, "contest_id": 0, "contest_time": 0},
                {"user_id": 2, "finish_position": 2, "contest_id": 0, "contest_time": 0},
                {"user_id": 0, "finish_position": 0, "contest_id": 1, "contest_time": 2},
                {"user_id": 1, "finish_position": 1, "contest_id": 1, "contest_time": 2},
                {"user_id": 3, "finish_position": 2, "contest_id": 1, "contest_time": 2},
                {"user_id": 1, "finish_position": 0, "contest_id": 2, "contest_time": 1},
                {"user_id": 2, "finish_position": 1, "contest_id": 2, "contest_time": 1},
                {"user_id": 3, "finish_position": 2, "contest_id": 2, "contest_time": 1},
            ]
        )

        config = PointsScoringConfig.model_validate({"points": [5, 2]})
        result = compute_points_score(data, config)

        expected = pl.DataFrame(
            {
                "user_id": [0, 0, 1, 1, 1, 2, 2, 3, 3],
                "finish_position": [0, 0, 1, 1, 0, 2, 1, 2, 2],
                "contest_id": [0, 1, 0, 1, 2, 0, 2, 2, 1],
                "contest_time": [0, 2, 0, 2, 1, 0, 1, 1, 2],
                "points": [5, 5, 2, 2, 5, 0, 2, 0, 0],
                "drop": [False, False, False, False, False, False, False, False, False],
            }
        )

        assert_frame_equal(result, expected)

    def test_multi_contest_drop_weeks(self):
        data = pl.DataFrame(
            [
                {"user_id": 0, "contest_id": 0, "finish_position": 3},
                {"user_id": 0, "contest_id": 1, "finish_position": 2},
                {"user_id": 0, "contest_id": 2, "finish_position": 1},
                {"user_id": 0, "contest_id": 3, "finish_position": 0},
                {"user_id": 1, "contest_id": 0, "finish_position": 2},
                {"user_id": 1, "contest_id": 1, "finish_position": 1},
                {"user_id": 1, "contest_id": 2, "finish_position": 0},
                {"user_id": 2, "contest_id": 0, "finish_position": 1},
                {"user_id": 2, "contest_id": 1, "finish_position": 0},
                {"user_id": 3, "contest_id": 0, "finish_position": 0},
            ]
        )
        data = data.with_columns(pl.col("contest_id").alias("contest_time"))

        config = PointsScoringConfig.model_validate({"points": [5, 4, 3, 2], "drop_races": 2})
        result = compute_points_score(data, config)

        expected = pl.DataFrame(
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

        assert_frame_equal(result, expected)
