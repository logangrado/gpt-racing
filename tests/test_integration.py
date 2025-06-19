#!/usr/bin/env python3

from pathlib import Path
import datetime

import pytest
import pandas as pd
import polars as pl

from polars.testing import assert_frame_equal

from gpt_racing import core
from gpt_racing.config import RatingConfig

from gpt_racing import _testing
from gpt_racing._testing import assert_object_equal, generate_data


def _df_to_schema_str(df):
    out = "{" + ",".join([f'"{k}":pl.{v}' for k, v in df.schema.items()]) + "}"
    return out


def _print_result(result):
    """Print a full result in a friendly way that we can just copy/paste back in"""

    print("")
    print("ACTUAL")
    print("{")
    for key, value in result.items():
        print(f'"{key}":[')
        for item in value:
            print(f"pl.DataFrame({item.to_dict(as_series=False)}, schema={_df_to_schema_str(item)}),")
        print("],")

    print("}")


def _assert_result_equal(actual, expected):
    assert type(actual) == type(expected)
    if isinstance(actual, dict):
        assert set(actual.keys()) == set(expected.keys())
        for key in actual.keys():
            _assert_result_equal(actual[key], expected[key])

    elif isinstance(actual, list):
        assert len(actual) == len(expected)
        for a, b in zip(actual, expected):
            _assert_result_equal(a, b)

    elif isinstance(actual, pl.DataFrame):
        assert_frame_equal(actual, expected)

    else:
        raise NotImplementedError(f"Comparison not implemented for type: {type(actual)}")


class TestIntegration:
    def test_single_race(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "races": [{"subsession_id": 0}],
                "elo": {},
                "points": {
                    "points": [3, 2, 1],
                },
            },
        )
        summary_data = [
            {
                "subsession_id": 0,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            }
        ]

        _testing.generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        _print_result(result)

        expected = {
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["c", "b", "a"],
                        "start_position": [2, 4, 1],
                        "qualify_lap_time": [0.0042, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [3, 2, 1],
                        "rating": [1715, 1500, 1285],
                        "rating_change": [215, 0, -215],
                        "rank": [1, 2, 3],
                        "rank_change": [None, None, None],
                        "laps_complete": [3, 3, 3],
                        "total_time": [36.0, 33.0, 30.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [12.0, 11.0, 10.0],
                        "fastest_lap_time": [12.0, 11.0, 10.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                    },
                    schema={
                        "display_name": pl.String,
                        "start_position": pl.Int64,
                        "qualify_lap_time": pl.Float64,
                        "finish_position": pl.Int64,
                        "interval": pl.String,
                        "points": pl.Int64,
                        "rating": pl.Int64,
                        "rating_change": pl.Int64,
                        "rank": pl.Int32,
                        "rank_change": pl.Int32,
                        "laps_complete": pl.Int64,
                        "total_time": pl.Float64,
                        "penalty": pl.Float64,
                        "average_lap_time": pl.Float64,
                        "fastest_lap_time": pl.Float64,
                        "num_incidents": pl.Int32,
                        "fastest_lap": pl.Boolean,
                        "cleanest_driver": pl.Boolean,
                    },
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 0],
                        "points_race_1": [3, 2, 1],
                        "drop_race_1": [False, False, False],
                        "fastest_lap_race_1": [False, False, True],
                        "cleanest_driver_race_1": [True, True, True],
                        "points_total": [3, 2, 1],
                        "points_rank": [1, 2, 3],
                        "points_rank_change": [None, None, None],
                        "rating": [1715, 1500, 1285],
                        "rating_rank": [1, 2, 3],
                        "rating_rank_change": [None, None, None],
                        "num_races": [1, 1, 1],
                        "display_name": ["c", "b", "a"],
                    },
                    schema={
                        "user_id": pl.Int64,
                        "points_race_1": pl.Int64,
                        "drop_race_1": pl.Boolean,
                        "fastest_lap_race_1": pl.Boolean,
                        "cleanest_driver_race_1": pl.Boolean,
                        "points_total": pl.Int64,
                        "points_rank": pl.Int32,
                        "points_rank_change": pl.Int32,
                        "rating": pl.Int64,
                        "rating_rank": pl.Int32,
                        "rating_rank_change": pl.Int32,
                        "num_races": pl.Int64,
                        "display_name": pl.String,
                    },
                ),
            ],
            "points": [
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 0],
                        "finish_position": [0, 1, 2],
                        "subsession_id": [0, 0, 0],
                        "session_end_time": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "fastest_lap_time": [12.0, 11.0, 10.0],
                        "num_incidents": [0, 0, 0],
                        "laps_complete": [3, 3, 3],
                        "points_type": ["default", "default", "default"],
                        "points": [3, 2, 1],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                        "cumulative_points": [3, 2, 1],
                        "num_races": [1, 1, 1],
                        "rank": [1, 2, 3],
                        "rank_change": [None, None, None],
                        "drop": [False, False, False],
                    },
                    schema={
                        "user_id": pl.Int64,
                        "finish_position": pl.Int64,
                        "subsession_id": pl.Int64,
                        "session_end_time": pl.Datetime(time_unit="us", time_zone=None),
                        "fastest_lap_time": pl.Float64,
                        "num_incidents": pl.Int32,
                        "laps_complete": pl.Int64,
                        "points_type": pl.String,
                        "points": pl.Int64,
                        "fastest_lap": pl.Boolean,
                        "cleanest_driver": pl.Boolean,
                        "cumulative_points": pl.Int64,
                        "num_races": pl.Int32,
                        "rank": pl.Int32,
                        "rank_change": pl.Int32,
                        "drop": pl.Boolean,
                    },
                ),
            ],
            "ELO": [
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 0],
                        "rating": [1715, 1500, 1285],
                        "num_contests": [1, 1, 1],
                        "participated": [True, True, True],
                        "subsession_id": [0, 0, 0],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "num_valid_contests": [1, 1, 1],
                        "rank": [1, 2, 3],
                        "rank_change": [None, None, None],
                        "rating_change": [215, 0, -215],
                    },
                    schema={
                        "user_id": pl.Int64,
                        "rating": pl.Int64,
                        "num_contests": pl.Int64,
                        "participated": pl.Boolean,
                        "subsession_id": pl.Int64,
                        "contest_date": pl.Datetime(time_unit="us", time_zone=None),
                        "num_valid_contests": pl.Int32,
                        "rank": pl.Int32,
                        "rank_change": pl.Int32,
                        "rating_change": pl.Int64,
                    },
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False})
        # _assert_result_equal(result, expected)
