#!/usr/bin/env python3

import datetime

import pandas as pd
import polars as pl

from gpt_racing import core
from gpt_racing.config import RatingConfig
from gpt_racing._testing import assert_object_equal


def _generate_qualy_data(session_data):
    df = pl.DataFrame(session_data)
    df = df.sort("best_lap_time").with_columns(pl.int_range(pl.len()).alias("finish_position"))

    return df.to_dicts()


def _generate_lap_data(session_data):
    df = pl.DataFrame(session_data)

    df = df.rename({"average_lap_time": "lap_time"})

    df = df.with_columns(pl.col("laps_complete").map_elements(lambda x: range(x + 1)).alias("lap_number"))

    df = df.explode("lap_number")

    df = df.with_columns(pl.col("lap_time") * 10000)

    # Compute interval
    df = df.with_columns((pl.col("lap_time") * pl.col("lap_number")).alias("total_time"))
    leader_df = (
        df.sort("total_time")
        .group_by("lap_number")
        .first()["lap_number", "total_time"]
        .rename({"total_time": "total_time_leader"})
    )
    df = df.join(leader_df, on="lap_number")
    df = df.with_columns(((pl.col("total_time") - pl.col("total_time_leader")) / 10).alias("interval"))

    # Make lap_number - lap_time = -1
    df = df.with_columns(
        [pl.when(pl.col("lap_number") == 0).then(-1).otherwise(pl.col("interval")).alias("interval")],
    )

    df = df.with_columns(pl.lit([]).alias("lap_events"))

    # Also need incidents column
    df = df.with_columns(pl.lit(False).alias("incident"))

    # Select columns of interest
    df = df["cust_id", "display_name", "lap_number", "lap_time", "interval", "lap_events", "incident"]
    return df.to_dicts()


def _generate_race_result(subsession_id, session_data):
    """
    Race data takes the following form:

    Returns one row per driver, in the following format:

    cust_id                                          346566
    display_name                            Derek M Cyphers
    finish_position                                       0
    finish_position_in_class                              0
    interval                                              0
    average_lap                                      896126
    laps_complete                                        41
    class_interval                                        0
    qual_lap_time                                        -1
    starting_position                                     0
    starting_position_in_class                            0
    incidents                                             0
    car_class_name                    Hosted All Cars Class
    reason_out                                      Running
    subsession_id                                  66010063
    session_end_time              2024-01-09 02:34:35+00:00

    ...
    cust_id                                          367874
    display_name                            Matthew Siddall
    finish_position                                       1
    finish_position_in_class                              1
    interval                                         136641
    average_lap                                      899458
    laps_complete                                        41
    class_interval                                   136641
    qual_lap_time                                        -1
    starting_position                                     2
    starting_position_in_class                            2
    incidents                                             0
    car_class_name                    Hosted All Cars Class
    reason_out                                      Running
    subsession_id                                  66010063
    session_end_time              2024-01-09 02:34:35+00:00
    Name: 1, dtype: object


    dtypes:

    cust_id                                    int64
    display_name                              object
    finish_position                            int64
    finish_position_in_class                   int64
    interval                                   int64
    average_lap                                int64
    laps_complete                              int64
    class_interval                             int64
    qual_lap_time                              int64
    starting_position                          int64
    starting_position_in_class                 int64
    incidents                                  int64
    car_class_name                            object
    reason_out                                object
    subsession_id                              int64
    session_end_time              datetime64[s, UTC]

    """
    # Create dataframe directly from inputs. Explicitly select required columns
    df = pl.DataFrame(session_data).select(["cust_id", "display_name", "laps_complete", "average_lap_time"])

    # Compute values

    df = df.with_columns(
        pl.lit(subsession_id).alias("subsession_id"),
        pl.lit(0).alias("incidents"),
        (pl.col("laps_complete") * pl.col("average_lap_time")).alias("_total_time"),
    )

    df = df.with_columns(
        pl.col("_total_time").rank().alias("finish_position").cast(pl.Int64),
        ((pl.col("_total_time").min() - pl.col("_total_time")) * 10000).alias(
            "interval"
        ),  # Factor might be 1000, unsure
        pl.col("subsession_id").cast(pl.Datetime).alias("session_end_time"),
    )

    # Still operating in Pandas land over there...
    return df.to_pandas()


def _generate_data(summary_data, fake_client):
    for session in summary_data:
        fake_client._set_qualy_result(
            session["subsession_id"], pd.DataFrame(_generate_qualy_data(session["qualifying"]))
        )
        fake_client._set_lap_data(session["subsession_id"], pd.DataFrame(_generate_lap_data(session["race"])))

        fake_client._set_race_result(
            session["subsession_id"], _generate_race_result(session["subsession_id"], session["race"])
        )


class TestComputeRatings:
    def test_single_race_fake(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "races": [{"subsession_id": 0}],
                "elo": {},
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

        _generate_data(summary_data, fake_client)

        rating_df, result_df = core.compute_ratings(config, client=fake_client)

        # Check result dataframes
        pd.testing.assert_frame_equal(
            rating_df,
            pd.DataFrame(
                {
                    "display_name": ["a", "b", "c", "d"],
                    "user_id": [0, 1, 2, 3],
                    "rating": [1771, 1579, 1421, 1229],
                    "rating_change": [271, 79, -79, -271],
                    "rank": [1, 2, 3, 4],
                    "rank_change": [pd.NA, pd.NA, pd.NA, pd.NA],
                    "subsession_id": [0, 0, 0, 0],
                }
            ),
            check_dtype=False,
        )

        pd.testing.assert_frame_equal(
            result_df,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2, 3],
                    "laps_complete": [3, 3, 3, 0],
                    "total_time": ["30.000", "33.000", "36.000", "-"],
                    "penalty": [0.0, 0.0, 0.0, 0.0],
                    "interval": ["0.000", "3.000", "6.000", "-3L"],
                    "finish_position": [1, 2, 3, 4],
                    "average_lap_time": ["10.000", "11.000", "12.000", "-"],
                    "start_position": [1, 4, 2, 3],
                    "qualify_lap_time": ["0.004", "0.004", "0.004", "0.004"],
                    "subsession_id": [0, 0, 0, 0],
                    "rating": [1771, 1579, 1421, 1229],
                    "rating_change": [271, 79, -79, -271],
                    "num_contests": [1, 1, 1, 1],
                    "participated": [True, True, True, True],
                    "rank": [1, 2, 3, 4],
                    "rank_change": [pd.NA, pd.NA, pd.NA, pd.NA],
                    "display_name": ["a", "b", "c", "d"],
                }
            ),
            check_dtype=False,
        )

    def test_multi_race_fake(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "scoring": {"type": "ELO"},
                "races": [
                    {"subsession_id": 0},
                    {"subsession_id": 1},
                ],
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
                    # {"cust_id": 3, "display_name": "d", "laps_complete": 3, "average_lap_time": 130},
                ],
            },
            {
                "subsession_id": 1,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        _generate_data(summary_data, fake_client)

        rating_df, result_df = core.compute_ratings(
            config,
            client=fake_client,
        )

        # Check result dataframes
        pd.testing.assert_frame_equal(
            rating_df,
            pd.DataFrame(
                {
                    "display_name": ["a", "b", "c", "d", "a", "b", "c", "e", "d"],
                    "user_id": [0, 1, 2, 3, 0, 1, 2, 4, 3],
                    "rating": [1771, 1579, 1421, 1229, 1793, 1586, 1421, 1387, 1229],
                    "rating_change": [271, 79, -79, -271, 22, 7, pd.NA, -113, pd.NA],
                    "rank": [1, 2, 3, 4, 1, 2, 3, 4, 5],
                    "rank_change": [pd.NA, pd.NA, pd.NA, pd.NA, 0, 0, 0, pd.NA, 1],
                    "subsession_id": [0, 0, 0, 0, 1, 1, 1, 1, 1],
                }
            ),
            check_dtype=False,
        )

        pd.testing.assert_frame_equal(
            result_df,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2, 3, 0, 1, 4],
                    "laps_complete": [3, 3, 3, 0, 3, 3, 3],
                    "total_time": ["30.000", "33.000", "36.000", "-", "30.000", "33.000", "36.000"],
                    "penalty": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    "interval": ["0.000", "3.000", "6.000", "-3L", "0.000", "3.000", "6.000"],
                    "finish_position": [1, 2, 3, 4, 1, 2, 3],
                    "average_lap_time": ["10.000", "11.000", "12.000", "-", "10.000", "11.000", "12.000"],
                    "start_position": [1, 4, 2, 3, 1, 3, 2],
                    "qualify_lap_time": ["0.004", "0.004", "0.004", "0.004", "0.004", "0.004", "0.004"],
                    "subsession_id": [0, 0, 0, 0, 1, 1, 1],
                    "rating": [1771, 1579, 1421, 1229, 1793, 1586, 1387],
                    "rating_change": [271, 79, -79, -271, 22, 7, -113],
                    "num_contests": [1, 1, 1, 1, 2, 2, 1],
                    "participated": [True, True, True, True, True, True, True],
                    "rank": [1, 2, 3, 4, 1, 2, 4],
                    "rank_change": [pd.NA, pd.NA, pd.NA, pd.NA, 0, 0, pd.NA],
                    "display_name": ["a", "b", "c", "d", "a", "b", "e"],
                }
            ),
            check_dtype=False,
        )


class TestPointsScoring:
    def test_single_race_fake(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "elo": {},
                "points": {
                    "points": [10, 5, 3],
                },
                "races": [
                    {
                        "subsession_id": 0,
                    },
                ],
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

        _generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["c", "b", "a"],
                        "start_position": [2, 4, 1],
                        "qualify_lap_time": [0.0042, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
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
                    }
                )
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 0],
                        "points_race_1": [10, 5, 3],
                        "drop_race_1": [False, False, False],
                        "fastest_lap_race_1": [False, False, True],
                        "cleanest_driver_race_1": [True, True, True],
                        "points_total": [10, 5, 3],
                        "points_rank": [1, 2, 3],
                        "points_rank_change": [None, None, None],
                        "rating": [1715, 1500, 1285],
                        "rating_rank": [1, 2, 3],
                        "rating_rank_change": [None, None, None],
                        "num_races": [1, 1, 1],
                        "display_name": ["c", "b", "a"],
                    }
                )
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
                        "points": [10, 5, 3],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                        "drop": [False, False, False],
                        "cumulative_points": [10, 5, 3],
                        "num_races": [1, 1, 1],
                        "rank": [1, 2, 3],
                        "rank_change": [None, None, None],
                    }
                )
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
                    }
                )
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False})

    def test_multi_race_fake(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "elo": {},
                "points": {
                    "points": [10, 5, 3],
                },
                "races": [
                    {"subsession_id": 0},
                    {"subsession_id": 1},
                ],
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
                    # {"cust_id": 3, "display_name": "d", "laps_complete": 3, "average_lap_time": 130},
                ],
            },
            {
                "subsession_id": 1,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        _generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["c", "b", "a"],
                        "start_position": [2, 4, 1],
                        "qualify_lap_time": [0.0042, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
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
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["e", "b", "a"],
                        "start_position": [2, 3, 1],
                        "qualify_lap_time": [0.0043, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1487, 1436, 1222],
                        "rating_change": [-13, -64, -63],
                        "rank": [2, 3, 4],
                        "rank_change": [None, 1, 1],
                        "laps_complete": [3, 3, 3],
                        "total_time": [36.0, 33.0, 30.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [12.0, 11.0, 10.0],
                        "fastest_lap_time": [12.0, 11.0, 10.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 0],
                        "points_race_1": [10, 5, 3],
                        "drop_race_1": [False, False, False],
                        "fastest_lap_race_1": [False, False, True],
                        "cleanest_driver_race_1": [True, True, True],
                        "points_total": [10, 5, 3],
                        "points_rank": [1, 2, 3],
                        "points_rank_change": [None, None, None],
                        "rating": [1715, 1500, 1285],
                        "rating_rank": [1, 2, 3],
                        "rating_rank_change": [None, None, None],
                        "num_races": [1, 1, 1],
                        "display_name": ["c", "b", "a"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 4, 0],
                        "points_race_1": [10, 5, None, 3],
                        "points_race_2": [None, 5, 10, 3],
                        "drop_race_1": [False, False, False, False],
                        "drop_race_2": [False, False, False, False],
                        "fastest_lap_race_1": [False, False, None, True],
                        "fastest_lap_race_2": [None, False, False, True],
                        "cleanest_driver_race_1": [True, True, None, True],
                        "cleanest_driver_race_2": [None, True, True, True],
                        "points_total": [None, 10, 10, 6],
                        "points_rank": [None, 1, 1, 3],
                        "points_rank_change": [None, -1, None, 0],
                        "rating": [1715, 1436, 1487, 1222],
                        "rating_rank": [1, 3, 2, 4],
                        "rating_rank_change": [0, 1, None, 1],
                        "num_races": [1, 2, 1, 2],
                        "display_name": ["c", "b", "e", "a"],
                    }
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
                        "points": [10, 5, 3],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                        "drop": [False, False, False],
                        "cumulative_points": [10, 5, 3],
                        "num_races": [1, 1, 1],
                        "rank": [1, 2, 3],
                        "rank_change": [None, None, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "finish_position": [None, 0, 1, 2],
                        "subsession_id": [1, 1, 1, 1],
                        "session_end_time": [
                            None,
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "fastest_lap_time": [None, 12.0, 11.0, 10.0],
                        "num_incidents": [None, 0, 0, 0],
                        "laps_complete": [None, 3, 3, 3],
                        "points_type": [None, "default", "default", "default"],
                        "points": [None, 10, 5, 3],
                        "fastest_lap": [None, False, False, True],
                        "cleanest_driver": [None, True, True, True],
                        "drop": [False, False, False, False],
                        "cumulative_points": [None, 10, 10, 6],
                        "num_races": [1, 1, 2, 2],
                        "rank": [None, 1, 1, 3],
                        "rank_change": [None, None, -1, 0],
                    }
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
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "rating": [1715, 1487, 1436, 1222],
                        "num_contests": [1, 1, 2, 2],
                        "participated": [False, True, True, True],
                        "subsession_id": [1, 1, 1, 1],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "num_valid_contests": [2, 1, 2, 2],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [0, None, 1, 1],
                        "rating_change": [None, -13, -64, -63],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False})

    def test_multi_race_with_drop_fake(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "elo": {},
                "points": {
                    "points": [10, 5, 3],
                    "drop_races": 1,
                },
                "races": [
                    {"subsession_id": 0},
                    {"subsession_id": 1},
                    {"subsession_id": 2},
                ],
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
                    # {"cust_id": 3, "display_name": "d", "laps_complete": 3, "average_lap_time": 130},
                ],
            },
            {
                "subsession_id": 1,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
            {
                "subsession_id": 2,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        _generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["c", "b", "a"],
                        "start_position": [2, 4, 1],
                        "qualify_lap_time": [0.0042, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
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
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["e", "b", "a"],
                        "start_position": [2, 3, 1],
                        "qualify_lap_time": [0.0043, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1487, 1436, 1222],
                        "rating_change": [-13, -64, -63],
                        "rank": [2, 3, 4],
                        "rank_change": [None, 1, 1],
                        "laps_complete": [3, 3, 3],
                        "total_time": [36.0, 33.0, 30.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [12.0, 11.0, 10.0],
                        "fastest_lap_time": [12.0, 11.0, 10.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["e", "b", "a"],
                        "start_position": [2, 3, 1],
                        "qualify_lap_time": [0.0043, 0.0044, 0.004],
                        "finish_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1480, 1413, 1249],
                        "rating_change": [-7, -23, 27],
                        "rank": [2, 3, 4],
                        "rank_change": [0, 0, 0],
                        "laps_complete": [3, 3, 3],
                        "total_time": [36.0, 33.0, 30.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [12.0, 11.0, 10.0],
                        "fastest_lap_time": [12.0, 11.0, 10.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [2, 1, 0],
                        "points_race_1": [10, 5, 3],
                        "drop_race_1": [False, False, False],
                        "fastest_lap_race_1": [False, False, True],
                        "cleanest_driver_race_1": [True, True, True],
                        "points_total": [10, 5, 3],
                        "points_rank": [1, 2, 3],
                        "points_rank_change": [None, None, None],
                        "rating": [1715, 1500, 1285],
                        "rating_rank": [1, 2, 3],
                        "rating_rank_change": [None, None, None],
                        "num_races": [1, 1, 1],
                        "display_name": ["c", "b", "a"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "points_race_1": [10, None, 5, 3],
                        "points_race_2": [None, 10, 5, 3],
                        "drop_race_1": [False, True, True, True],
                        "drop_race_2": [True, False, False, False],
                        "fastest_lap_race_1": [False, None, False, True],
                        "fastest_lap_race_2": [None, False, False, True],
                        "cleanest_driver_race_1": [True, None, True, True],
                        "cleanest_driver_race_2": [None, True, True, True],
                        "points_total": [10, 10, 5, 3],
                        "points_rank": [1, 1, 3, 4],
                        "points_rank_change": [0, None, 1, 2],
                        "rating": [1715, 1487, 1436, 1222],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [0, None, 1, 1],
                        "num_races": [1, 1, 2, 2],
                        "display_name": ["c", "e", "b", "a"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "points_race_1": [10, None, 5, 3],
                        "points_race_2": [None, 10, 5, 3],
                        "points_race_3": [None, 10, 5, 3],
                        "drop_race_1": [False, True, True, True],
                        "drop_race_2": [True, False, False, False],
                        "drop_race_3": [False, False, False, False],
                        "fastest_lap_race_1": [False, None, False, True],
                        "fastest_lap_race_2": [None, False, False, True],
                        "fastest_lap_race_3": [None, False, False, True],
                        "cleanest_driver_race_1": [True, None, True, True],
                        "cleanest_driver_race_2": [None, True, True, True],
                        "cleanest_driver_race_3": [None, True, True, True],
                        "points_total": [None, 20, 10, 6],
                        "points_rank": [None, 1, 2, 3],
                        "points_rank_change": [None, 0, -1, -1],
                        "rating": [1715, 1480, 1413, 1249],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [0, 0, 0, 0],
                        "num_races": [1, 2, 3, 3],
                        "display_name": ["c", "e", "b", "a"],
                    }
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
                        "points": [10, 5, 3],
                        "fastest_lap": [False, False, True],
                        "cleanest_driver": [True, True, True],
                        "drop": [False, False, False],
                        "cumulative_points": [10, 5, 3],
                        "num_races": [1, 1, 1],
                        "rank": [1, 2, 3],
                        "rank_change": [None, None, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "finish_position": [None, 0, 1, 2],
                        "subsession_id": [1, 1, 1, 1],
                        "session_end_time": [
                            None,
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "fastest_lap_time": [None, 12.0, 11.0, 10.0],
                        "num_incidents": [None, 0, 0, 0],
                        "laps_complete": [None, 3, 3, 3],
                        "points_type": [None, "default", "default", "default"],
                        "points": [None, 10, 5, 3],
                        "fastest_lap": [None, False, False, True],
                        "cleanest_driver": [None, True, True, True],
                        "drop": [True, False, False, False],
                        "cumulative_points": [10, 10, 5, 3],
                        "num_races": [1, 1, 2, 2],
                        "rank": [1, 1, 3, 4],
                        "rank_change": [0, None, 1, 2],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "finish_position": [None, 0, 1, 2],
                        "subsession_id": [2, 2, 2, 2],
                        "session_end_time": [
                            None,
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                        ],
                        "fastest_lap_time": [None, 12.0, 11.0, 10.0],
                        "num_incidents": [None, 0, 0, 0],
                        "laps_complete": [None, 3, 3, 3],
                        "points_type": [None, "default", "default", "default"],
                        "points": [None, 10, 5, 3],
                        "fastest_lap": [None, False, False, True],
                        "cleanest_driver": [None, True, True, True],
                        "drop": [False, False, False, False],
                        "cumulative_points": [None, 20, 10, 6],
                        "num_races": [1, 2, 3, 3],
                        "rank": [None, 1, 2, 3],
                        "rank_change": [None, 0, -1, -1],
                    }
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
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "rating": [1715, 1487, 1436, 1222],
                        "num_contests": [1, 1, 2, 2],
                        "participated": [False, True, True, True],
                        "subsession_id": [1, 1, 1, 1],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "num_valid_contests": [2, 1, 2, 2],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [0, None, 1, 1],
                        "rating_change": [None, -13, -64, -63],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 4, 1, 0],
                        "rating": [1715, 1480, 1413, 1249],
                        "num_contests": [1, 2, 3, 3],
                        "participated": [False, True, True, True],
                        "subsession_id": [2, 2, 2, 2],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                        ],
                        "num_valid_contests": [3, 2, 3, 3],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [0, 0, 0, 0],
                        "rating_change": [None, -7, -23, 27],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False})


class _TestComputeRatings:
    def test_single_race(self, client):
        config = RatingConfig(**{"races": [{"subsession_id": 64681712}]})

        result = core.compute_ratings(config, client=client)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {"rank": 1, "user_id": 260862, "rating": 1770, "display_name": "Rafael Amorim"},
                    {"rank": 2, "user_id": 622340, "rating": 1634, "display_name": "Logan Grado"},
                    {"rank": 3, "user_id": 500135, "rating": 1550, "display_name": "Peter Sigourney"},
                    {"rank": 4, "user_id": 367874, "rating": 1485, "display_name": "Matthew Siddall"},
                    {"rank": 5, "user_id": 263933, "rating": 1432, "display_name": "Jeremy Mazzariello"},
                    {"rank": 6, "user_id": 573726, "rating": 1385, "display_name": "Jonathan Reber"},
                    {"rank": 7, "user_id": 708130, "rating": 1343, "display_name": "Chad Kacyon"},
                    {"rank": 8, "user_id": 286691, "rating": 1303, "display_name": "Jesse Lyon2"},
                    {"rank": 9, "user_id": 705709, "rating": 1264, "display_name": "Mark Collingwood"},
                    {"rank": 10, "user_id": 847107, "rating": 1227, "display_name": "Jeffrey Gardner3"},
                    {"rank": 11, "user_id": 574554, "rating": 1190, "display_name": "Avijit Barua"},
                    {"rank": 12, "user_id": 295695, "rating": 1151, "display_name": "Chris Braun"},
                    {"rank": 13, "user_id": 407523, "rating": 1111, "display_name": "Claus Nielsen"},
                    {"rank": 14, "user_id": 546220, "rating": 1069, "display_name": "Andrew McCune"},
                    {"rank": 15, "user_id": 663448, "rating": 1022, "display_name": "Scott Kessel"},
                    {"rank": 16, "user_id": 679440, "rating": 969, "display_name": "Steve Komatz"},
                    {"rank": 17, "user_id": 415509, "rating": 904, "display_name": "Nikki Chan"},
                    {"rank": 18, "user_id": 660936, "rating": 820, "display_name": "Thomas S Briggs"},
                    {"rank": 19, "user_id": 599305, "rating": 684, "display_name": "Nick Melaragno2"},
                ]
            ),
        )

    def test_multi_race(self, client):
        config = RatingConfig(
            **{
                "races": [
                    {"subsession_id": 60407964},
                    {"subsession_id": 60673413},
                    {"subsession_id": 60937028},
                    {"subsession_id": 61460241},
                ],
            }
        )

        result = core.compute_ratings(config, client=client)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {"rank": 1, "user_id": 15826, "rating": 1991, "display_name": "Wyatt Gooden"},
                    {"rank": 2, "user_id": 360788, "rating": 1906, "display_name": "Tres Drawhorn"},
                    {"rank": 3, "user_id": 159404, "rating": 1893, "display_name": "Christopher Paiz"},
                    {"rank": 4, "user_id": 260862, "rating": 1893, "display_name": "Rafael Amorim"},
                    {"rank": 5, "user_id": 286691, "rating": 1884, "display_name": "Jesse Lyon2"},
                    {"rank": 6, "user_id": 414448, "rating": 1669, "display_name": "Justin SW Tsang"},
                    {"rank": 7, "user_id": 80283, "rating": 1625, "display_name": "James Huth"},
                    {"rank": 8, "user_id": 637337, "rating": 1613, "display_name": "Conner Fisk"},
                    {"rank": 9, "user_id": 489352, "rating": 1605, "display_name": "Dave Kraige"},
                    {"rank": 10, "user_id": 345393, "rating": 1565, "display_name": "Shaibal Bandyopadhyay"},
                    {"rank": 11, "user_id": 622340, "rating": 1523, "display_name": "Logan Grado"},
                    {"rank": 12, "user_id": 19888, "rating": 1508, "display_name": "Ryan Cowley"},
                    {"rank": 13, "user_id": 295695, "rating": 1503, "display_name": "Chris Braun"},
                    {"rank": 14, "user_id": 586629, "rating": 1482, "display_name": "Enrico Gregoratto"},
                    {"rank": 15, "user_id": 335343, "rating": 1469, "display_name": "Nick Facciolo"},
                    {"rank": 16, "user_id": 497965, "rating": 1444, "display_name": "Marc Nistor"},
                    {"rank": 17, "user_id": 464635, "rating": 1440, "display_name": "Ed Eijsenring"},
                    {"rank": 18, "user_id": 346566, "rating": 1435, "display_name": "Derek M Cyphers"},
                    {"rank": 19, "user_id": 599305, "rating": 1435, "display_name": "Nick Melaragno2"},
                    {"rank": 20, "user_id": 420747, "rating": 1432, "display_name": "Edward Nelson2"},
                    {"rank": 21, "user_id": 511529, "rating": 1430, "display_name": "Rick Reinsberg"},
                    {"rank": 22, "user_id": 550942, "rating": 1412, "display_name": "Robert Rose4"},
                    {"rank": 23, "user_id": 154321, "rating": 1408, "display_name": "Leif Peterson"},
                    {"rank": 24, "user_id": 48076, "rating": 1384, "display_name": "Rich Minkler"},
                    {"rank": 25, "user_id": 38402, "rating": 1352, "display_name": "Andy Jenks"},
                    {"rank": 26, "user_id": 475127, "rating": 1346, "display_name": "Michael Patterson7"},
                    {"rank": 27, "user_id": 46710, "rating": 1311, "display_name": "John Mallia"},
                    {"rank": 28, "user_id": 360917, "rating": 1300, "display_name": "Jonathan Waltman"},
                    {"rank": 29, "user_id": 48282, "rating": 1256, "display_name": "Dale Green"},
                    {"rank": 30, "user_id": 578615, "rating": 1245, "display_name": "James Franznick"},
                    {"rank": 31, "user_id": 660936, "rating": 1239, "display_name": "Thomas S Briggs"},
                    {"rank": 32, "user_id": 493846, "rating": 1228, "display_name": "Jake Faulkner"},
                    {"rank": 33, "user_id": 321763, "rating": 1207, "display_name": "Garrett Taylor"},
                    {"rank": 34, "user_id": 406239, "rating": 1062, "display_name": "Jacob Shum"},
                    {"rank": 35, "user_id": 379283, "rating": 1049, "display_name": "Dave Schwartz"},
                    {"rank": 36, "user_id": 535583, "rating": 964, "display_name": "Daniel Ruble"},
                    {"rank": 37, "user_id": 135611, "rating": 860, "display_name": "Michael Coholich"},
                    {"rank": 38, "user_id": 464803, "rating": 856, "display_name": "Stephen Chen2"},
                    {"rank": 39, "user_id": 21769, "rating": 816, "display_name": "Eon Simon"},
                    {"rank": 40, "user_id": 395256, "rating": 783, "display_name": "Shea A. McNeely"},
                    {"rank": 41, "user_id": 477806, "rating": 661, "display_name": "Zendla Collins"},
                    {"rank": 42, "user_id": 152016, "rating": 577, "display_name": "Joe Stiefel"},
                ]
            ),
        )

    def test_order_invariance(self, client):
        races = [
            {"subsession_id": 60407964},
            {"subsession_id": 60673413},
            {"subsession_id": 60937028},
            {"subsession_id": 61460241},
        ]

        config0 = RatingConfig(**{"races": races})
        config1 = RatingConfig(**{"races": races[::-1]})

        result0 = core.compute_ratings(config0, client)
        result1 = core.compute_ratings(config1, client)

        pd.testing.assert_frame_equal(result0, result1),
