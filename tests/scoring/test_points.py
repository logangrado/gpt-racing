#!/usr/bin/env python3

import pandas as pd
import polars as pl
from polars.testing import assert_frame_equal

from gpt_racing.scoring.points import compute_points_score
from gpt_racing.config import PointsConfig


class TestPointsScoring:
    def test_single_contest(self):
        data = pl.DataFrame(
            [
                {"subsession_id": 1, "user_id": 0, "finish_position": 0},
                {"subsession_id": 1, "user_id": 1, "finish_position": 1},
                {"subsession_id": 1, "user_id": 2, "finish_position": 2},
            ]
        )
        data = data.with_columns(pl.lit(0).alias("contest_id"), pl.lit(0).alias("contest_time"))

        config = PointsConfig.model_validate({"points": [5, 2]})
        result = compute_points_score(data, config)

        result.to_dict()
        expected = pl.DataFrame(
            {
                "subsession_id": [1, 1, 1],
                "user_id": [0, 1, 2],
                "finish_position": [0, 1, 2],
                "contest_id": [0, 0, 0],
                "contest_time": [0, 0, 0],
                "points_type": ["default", "default", "default"],
                "points": [5, 2, 0],
                "drop": [False, False, False],
                "cumulative_points": [5, 2, 0],
                "num_races": [1, 1, 1],
                "rank": [1, 2, 3],
                "rank_change": [None, None, None],
            },
        )

        assert_frame_equal(result, expected, check_dtypes=False)

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
        data = data.with_columns(pl.col("contest_id").alias("subsession_id"))

        config = PointsConfig.model_validate({"points": [5, 2]})
        result = compute_points_score(data, config)

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3],
                "finish_position": [0, 1, 2, None, 0, 1, None, 2, None, 0, 1, 2],
                "contest_id": [0, 0, 0, None, 1, 1, None, 1, None, 2, 2, 2],
                "contest_time": [0, 0, 0, None, 2, 2, None, 2, None, 1, 1, 1],
                "subsession_id": [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
                "points_type": [
                    "default",
                    "default",
                    "default",
                    None,
                    "default",
                    "default",
                    None,
                    "default",
                    None,
                    "default",
                    "default",
                    "default",
                ],
                "points": [5, 2, 0, None, 5, 2, None, 0, None, 5, 2, 0],
                "drop": [False, False, False, False, False, False, False, False, False, False, False, False],
                "cumulative_points": [5, 2, 0, None, 10, 4, None, 0, None, 9, 2, 0],
                "num_races": [1, 1, 1, 0, 2, 2, 1, 1, 2, 3, 2, 2],
                "rank": [1, 2, 3, None, 1, 2, None, 3, None, 1, 2, 3],
                "rank_change": [None, None, None, None, 0, 0, None, None, None, -1, None, 0],
            }
        )

        assert_frame_equal(result, expected, check_dtypes=False)

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
        data = data.with_columns(pl.col("contest_id").alias("subsession_id"))

        data = data.with_columns(pl.col("contest_id").alias("contest_time"))

        config = PointsConfig.model_validate({"points": [5, 4, 3, 2], "drop_races": 2})
        result = compute_points_score(data, config)

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3],
                "contest_id": [0, 0, 0, 0, 1, 1, 1, None, 2, 2, None, None, 3, None, None, None],
                "finish_position": [3, 2, 1, 0, 2, 1, 0, None, 1, 0, None, None, 0, None, None, None],
                "subsession_id": [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
                "contest_time": [0, 0, 0, 0, 1, 1, 1, None, 2, 2, None, None, 3, None, None, None],
                "points_type": [
                    "default",
                    "default",
                    "default",
                    "default",
                    "default",
                    "default",
                    "default",
                    None,
                    "default",
                    "default",
                    None,
                    None,
                    "default",
                    None,
                    None,
                    None,
                ],
                "points": [2, 3, 4, 5, 3, 4, 5, None, 4, 5, None, None, 5, None, None, None],
                "drop": [
                    True,
                    True,
                    False,
                    False,
                    True,
                    False,
                    False,
                    True,
                    False,
                    False,
                    True,
                    True,
                    False,
                    True,
                    True,
                    False,
                ],
                "cumulative_points": [0, 0, 4, 5, 0, 4, 9, 5, 4, 9, 9, 5, 9, 9, 9, None],
                "num_races": [1, 1, 1, 1, 2, 2, 2, 1, 3, 3, 2, 1, 4, 3, 2, 1],
                "rank": [3, 3, 2, 1, 4, 3, 1, 2, 4, 1, 1, 3, 1, 1, 1, None],
                "rank_change": [None, None, None, None, 1, 0, -1, 1, 0, -2, 0, 1, -3, 0, 0, None],
            }
        )

        assert_frame_equal(result, expected, check_dtypes=False)

    def test_multi_contest_drop_weeks_regression(self):
        """
        We had an error when calculating "cumulative_points" and then
        rank_change, when we have a drop week.

        For example, if we had 1 drop week, and a driver was first both weeks,
        the cumulative points would be [0, 5], instead of [5,5], and as a result
        the rank_change would be incorrect as well
        """
        data = pl.DataFrame(
            [
                {"user_id": 0, "contest_id": 0, "finish_position": 0},
                {"user_id": 0, "contest_id": 1, "finish_position": 0},
                {"user_id": 1, "contest_id": 0, "finish_position": 2},
                {"user_id": 1, "contest_id": 1, "finish_position": 1},
                {"user_id": 2, "contest_id": 0, "finish_position": 1},
                {"user_id": 2, "contest_id": 1, "finish_position": 2},
            ]
        )
        data = data.with_columns(pl.col("contest_id").alias("subsession_id"))

        data = data.with_columns(pl.col("contest_id").alias("contest_time"))

        config = PointsConfig.model_validate({"points": [5, 4, 3, 2], "drop_races": 1})
        result = compute_points_score(data, config)

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2, 0, 1, 2],
                "contest_id": [0, 0, 0, 1, 1, 1],
                "finish_position": [0, 2, 1, 0, 1, 2],
                "subsession_id": [0, 0, 0, 1, 1, 1],
                "contest_time": [0, 0, 0, 1, 1, 1],
                "points_type": ["default", "default", "default", "default", "default", "default"],
                "points": [5, 3, 4, 5, 4, 3],
                "cumulative_points": [5, 3, 4, 5, 4, 4],
                "num_races": [1, 1, 1, 2, 2, 2],
                "rank": [1, 3, 2, 1, 2, 2],
                "rank_change": [None, None, None, 0, -1, 0],
                "drop": [True, True, False, False, False, True],
            }
        )

        assert_frame_equal(result, expected, check_dtypes=False)
