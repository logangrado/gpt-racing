#!/usr/bin/env python3

import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from gpt_racing import core
from gpt_racing._testing import assert_object_equal, generate_data
from gpt_racing.config import RatingConfig


@pytest.mark.skip(reason="Rating-only socring not implemented")
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42_0000, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43_0000, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            }
        ]

        generate_data(summary_data, fake_client)

        rating_df, result_df = core.compute_ratings(config, client=fake_client)

        # Check result dataframes
        assert_frame_equal(
            rating_df,
            pl.DataFrame(
                {
                    "display_name": ["a", "b", "c", "d"],
                    "user_id": [0, 1, 2, 3],
                    "rating": [1771, 1579, 1421, 1229],
                    "rating_change": [271, 79, -79, -271],
                    "rank": [1, 2, 3, 4],
                    "rank_change": [None, None, None, None],
                    "subsession_id": [0, 0, 0, 0],
                }
            ),
            check_dtypes=False,
        )

        assert_frame_equal(
            result_df,
            pl.DataFrame(
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
                    "rank_change": [None, None, None, None],
                    "display_name": ["a", "b", "c", "d"],
                }
            ),
            check_dtypes=False,
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42_0000, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43_0000, "laps_complete": 3},
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43_0000, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        generate_data(summary_data, fake_client)

        rating_df, result_df = core.compute_ratings(
            config,
            client=fake_client,
        )

        # Check result dataframes
        assert_frame_equal(
            rating_df,
            pl.DataFrame(
                {
                    "display_name": ["a", "b", "c", "d", "a", "b", "c", "e", "d"],
                    "user_id": [0, 1, 2, 3, 0, 1, 2, 4, 3],
                    "rating": [1771, 1579, 1421, 1229, 1793, 1586, 1421, 1387, 1229],
                    "rating_change": [271, 79, -79, -271, 22, 7, None, -113, None],
                    "rank": [1, 2, 3, 4, 1, 2, 3, 4, 5],
                    "rank_change": [None, None, None, None, 0, 0, 0, None, 1],
                    "subsession_id": [0, 0, 0, 0, 1, 1, 1, 1, 1],
                }
            ),
            check_dtypes=False,
        )

        assert_frame_equal(
            result_df,
            pl.DataFrame(
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
                    "rank_change": [None, None, None, None, 0, 0, None],
                    "display_name": ["a", "b", "c", "d", "a", "b", "e"],
                }
            ),
            check_dtypes=False,
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42_0000, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43_0000, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            }
        ]

        generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "ELO": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "rating": [1769, 1579, 1421, 1231],
                        "num_contests": [1, 1, 1, 1],
                        "participated": [True, True, True, True],
                        "subsession_id": [0, 0, 0, 0],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "num_valid_contests": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "rating_change": [269, 79, -79, -269],
                        "display_name": ["a", "b", "c", "d"],
                    }
                ),
            ],
            "points": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "finish_position": [0, 1, 2, 3],
                        "subsession_id": [0, 0, 0, 0],
                        "session_end_time": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "laps_complete": [3, 3, 3, 0],
                        "points_type": ["default", "default", "default", "default"],
                        "points": [10, 5, 3, 0],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                        "cumulative_points": [10, 5, 3, 0],
                        "num_races": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "drop": [False, False, False, False],
                    }
                ),
            ],
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "start_position": [1, 4, 2, 3],
                        "qualify_lap_time": [40.0, 44.0, 42.0, 43.0],
                        "finish_position": [1, 2, 3, 4],
                        "class_position": [1, 2, 3, 4],
                        "interval": ["0.000", "-3.000", "-6.000", "-3L"],
                        "points": [10, 5, 3, 0],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_change": [269, 79, -79, -269],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "laps_complete": [3, 3, 3, 0],
                        "total_time": [30.0, 33.0, 36.0, None],
                        "penalty": [0.0, 0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0, None],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "points_race_1": [10, 5, 3, 0],
                        "drop_race_1": [False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None],
                        "cleanest_driver_race_1": [True, True, True, None],
                        "points_total": [10, 5, 3, 0],
                        "points_rank": [1, 2, 3, 4],
                        "points_rank_change": [None, None, None, None],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [None, None, None, None],
                        "num_races": [1, 1, 1, 1],
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "class_rank": [1, 2, 3, 4],
                        "class_rank_change": [None, None, None, None],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False, "check_row_order": False})

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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42_0000, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43_0000, "laps_complete": 3},
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43_0000, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        # Expected race results:
        # 1: a,b,c,d
        # 2: a,b,d

        generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "ELO": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "rating": [1769, 1579, 1421, 1231],
                        "num_contests": [1, 1, 1, 1],
                        "participated": [True, True, True, True],
                        "subsession_id": [0, 0, 0, 0],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "num_valid_contests": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "rating_change": [269, 79, -79, -269],
                        "display_name": ["a", "b", "c", "d"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 4, 3],
                        "rating": [1776, 1584, 1421, 1416, 1231],
                        "num_contests": [2, 2, 1, 1, 1],
                        "participated": [True, True, False, True, False],
                        "subsession_id": [1, 1, 1, 1, 1],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "num_valid_contests": [2, 2, 2, 1, 2],
                        "rank": [1, 2, 3, 4, 5],
                        "rank_change": [0, 0, 0, None, 1],
                        "rating_change": [7, 5, None, -84, None],
                        "display_name": ["a", "b", "c", "e", "d"],
                    }
                ),
            ],
            "points": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "finish_position": [0, 1, 2, 3],
                        "subsession_id": [0, 0, 0, 0],
                        "session_end_time": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "laps_complete": [3, 3, 3, 0],
                        "points_type": ["default", "default", "default", "default"],
                        "points": [10, 5, 3, 0],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                        "cumulative_points": [10, 5, 3, 0],
                        "num_races": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "drop": [False, False, False, False],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 3, 0, 1, 4],
                        "finish_position": [None, None, 0, 1, 2],
                        "subsession_id": [1, 1, 1, 1, 1],
                        "session_end_time": [
                            None,
                            None,
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "fastest_lap_time": [None, None, 10.0, 11.0, 12.0],
                        "num_incidents": [None, None, 0, 0, 0],
                        "laps_complete": [None, None, 3, 3, 3],
                        "points_type": [None, None, "default", "default", "default"],
                        "points": [None, None, 10, 5, 3],
                        "fastest_lap": [None, None, True, False, False],
                        "cleanest_driver": [None, None, True, True, True],
                        "cumulative_points": [3, 0, 20, 10, 3],
                        "num_races": [1, 1, 2, 2, 1],
                        "rank": [3, 5, 1, 2, 3],
                        "rank_change": [0, 1, 0, 0, None],
                        "drop": [False, False, False, False, False],
                    }
                ),
            ],
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "start_position": [1, 4, 2, 3],
                        "qualify_lap_time": [40.0, 44.0, 42.0, 43.0],
                        "finish_position": [1, 2, 3, 4],
                        "class_position": [1, 2, 3, 4],
                        "interval": ["0.000", "-3.000", "-6.000", "-3L"],
                        "points": [10, 5, 3, 0],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_change": [269, 79, -79, -269],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "laps_complete": [3, 3, 3, 0],
                        "total_time": [30.0, 33.0, 36.0, None],
                        "penalty": [0.0, 0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0, None],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "e"],
                        "class_name": [None, None, None],
                        "class_symbol": [None, None, None],
                        "class_color": [None, None, None],
                        "start_position": [1, 3, 2],
                        "qualify_lap_time": [40.0, 44.0, 43.0],
                        "finish_position": [1, 2, 3],
                        "class_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1776, 1584, 1416],
                        "rating_change": [7, 5, -84],
                        "rank": [1, 2, 4],
                        "rank_change": [0, 0, None],
                        "laps_complete": [3, 3, 3],
                        "total_time": [30.0, 33.0, 36.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0],
                        "fastest_lap_time": [10.0, 11.0, 12.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [True, False, False],
                        "cleanest_driver": [True, True, True],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "points_race_1": [10, 5, 3, 0],
                        "drop_race_1": [False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None],
                        "cleanest_driver_race_1": [True, True, True, None],
                        "points_total": [10, 5, 3, 0],
                        "points_rank": [1, 2, 3, 4],
                        "points_rank_change": [None, None, None, None],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [None, None, None, None],
                        "num_races": [1, 1, 1, 1],
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "class_rank": [1, 2, 3, 4],
                        "class_rank_change": [None, None, None, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 4, 3],
                        "points_race_1": [10, 5, 3, None, 0],
                        "points_race_2": [10, 5, None, 3, None],
                        "drop_race_1": [False, False, False, False, False],
                        "drop_race_2": [False, False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None, None],
                        "fastest_lap_race_2": [True, False, None, False, None],
                        "cleanest_driver_race_1": [True, True, True, None, None],
                        "cleanest_driver_race_2": [True, True, None, True, None],
                        "points_total": [20, 10, 3, 3, 0],
                        "points_rank": [1, 2, 3, 3, 5],
                        "points_rank_change": [0, 0, 0, None, 1],
                        "rating": [1776, 1584, 1421, 1416, 1231],
                        "rating_rank": [1, 2, 3, 4, 5],
                        "rating_rank_change": [0, 0, 0, None, 1],
                        "num_races": [2, 2, 1, 1, 1],
                        "display_name": ["a", "b", "c", "e", "d"],
                        "class_name": [None, None, None, None, None],
                        "class_symbol": [None, None, None, None, None],
                        "class_color": [None, None, None, None, None],
                        "class_rank": [1, 2, 3, 3, 5],
                        "class_rank_change": [0, 0, 0, None, 1],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False, "check_row_order": False})

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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42_0000, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43_0000, "laps_complete": 3},
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43_0000, "laps_complete": 3},
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
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 4},
                    {"cust_id": 4, "display_name": "e", "best_lap_time": 43_0000, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 4, "display_name": "e", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "ELO": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "rating": [1769, 1579, 1421, 1231],
                        "num_contests": [1, 1, 1, 1],
                        "participated": [True, True, True, True],
                        "subsession_id": [0, 0, 0, 0],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "num_valid_contests": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "rating_change": [269, 79, -79, -269],
                        "display_name": ["a", "b", "c", "d"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 4, 3],
                        "rating": [1776, 1584, 1421, 1416, 1231],
                        "num_contests": [2, 2, 1, 1, 1],
                        "participated": [True, True, False, True, False],
                        "subsession_id": [1, 1, 1, 1, 1],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "num_valid_contests": [2, 2, 2, 1, 2],
                        "rank": [1, 2, 3, 4, 5],
                        "rank_change": [0, 0, 0, None, 1],
                        "rating_change": [7, 5, None, -84, None],
                        "display_name": ["a", "b", "c", "e", "d"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 4, 3],
                        "rating": [1779, 1586, 1421, 1412, 1231],
                        "num_contests": [3, 3, 1, 2, 1],
                        "participated": [True, True, False, True, False],
                        "subsession_id": [2, 2, 2, 2, 2],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                        ],
                        "num_valid_contests": [3, 3, 3, 2, 3],
                        "rank": [1, 2, 3, 4, 5],
                        "rank_change": [0, 0, 0, 0, 0],
                        "rating_change": [3, 2, None, -4, None],
                        "display_name": ["a", "b", "c", "e", "d"],
                    }
                ),
            ],
            "points": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "finish_position": [0, 1, 2, 3],
                        "subsession_id": [0, 0, 0, 0],
                        "session_end_time": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "laps_complete": [3, 3, 3, 0],
                        "points_type": ["default", "default", "default", "default"],
                        "points": [10, 5, 3, 0],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                        "cumulative_points": [10, 5, 3, 0],
                        "num_races": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "drop": [False, False, False, False],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 3, 0, 1, 4],
                        "finish_position": [None, None, 0, 1, 2],
                        "subsession_id": [1, 1, 1, 1, 1],
                        "session_end_time": [
                            None,
                            None,
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                        ],
                        "fastest_lap_time": [None, None, 10.0, 11.0, 12.0],
                        "num_incidents": [None, None, 0, 0, 0],
                        "laps_complete": [None, None, 3, 3, 3],
                        "points_type": [None, None, "default", "default", "default"],
                        "points": [None, None, 10, 5, 3],
                        "fastest_lap": [None, None, True, False, False],
                        "cleanest_driver": [None, None, True, True, True],
                        "cumulative_points": [3, 0, 10, 5, 3],
                        "num_races": [1, 1, 2, 2, 1],
                        "rank": [3, 5, 1, 2, 3],
                        "rank_change": [0, 1, 0, 0, None],
                        "drop": [True, True, False, False, False],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [2, 3, 0, 1, 4],
                        "finish_position": [None, None, 0, 1, 2],
                        "subsession_id": [2, 2, 2, 2, 2],
                        "session_end_time": [
                            None,
                            None,
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                            datetime.datetime(1970, 1, 1, 0, 0, 0, 2),
                        ],
                        "fastest_lap_time": [None, None, 10.0, 11.0, 12.0],
                        "num_incidents": [None, None, 0, 0, 0],
                        "laps_complete": [None, None, 3, 3, 3],
                        "points_type": [None, None, "default", "default", "default"],
                        "points": [None, None, 10, 5, 3],
                        "fastest_lap": [None, None, True, False, False],
                        "cleanest_driver": [None, None, True, True, True],
                        "cumulative_points": [3, 0, 20, 10, 6],
                        "num_races": [1, 1, 3, 3, 2],
                        "rank": [4, 5, 1, 2, 3],
                        "rank_change": [1, 0, 0, 0, 0],
                        "drop": [False, False, False, False, False],
                    }
                ),
            ],
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "start_position": [1, 4, 2, 3],
                        "qualify_lap_time": [40.0, 44.0, 42.0, 43.0],
                        "finish_position": [1, 2, 3, 4],
                        "class_position": [1, 2, 3, 4],
                        "interval": ["0.000", "-3.000", "-6.000", "-3L"],
                        "points": [10, 5, 3, 0],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_change": [269, 79, -79, -269],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "laps_complete": [3, 3, 3, 0],
                        "total_time": [30.0, 33.0, 36.0, None],
                        "penalty": [0.0, 0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0, None],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "e"],
                        "class_name": [None, None, None],
                        "class_symbol": [None, None, None],
                        "class_color": [None, None, None],
                        "start_position": [1, 3, 2],
                        "qualify_lap_time": [40.0, 44.0, 43.0],
                        "finish_position": [1, 2, 3],
                        "class_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1776, 1584, 1416],
                        "rating_change": [7, 5, -84],
                        "rank": [1, 2, 4],
                        "rank_change": [0, 0, None],
                        "laps_complete": [3, 3, 3],
                        "total_time": [30.0, 33.0, 36.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0],
                        "fastest_lap_time": [10.0, 11.0, 12.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [True, False, False],
                        "cleanest_driver": [True, True, True],
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "e"],
                        "class_name": [None, None, None],
                        "class_symbol": [None, None, None],
                        "class_color": [None, None, None],
                        "start_position": [1, 3, 2],
                        "qualify_lap_time": [40.0, 44.0, 43.0],
                        "finish_position": [1, 2, 3],
                        "class_position": [1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1779, 1586, 1412],
                        "rating_change": [3, 2, -4],
                        "rank": [1, 2, 4],
                        "rank_change": [0, 0, 0],
                        "laps_complete": [3, 3, 3],
                        "total_time": [30.0, 33.0, 36.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0],
                        "fastest_lap_time": [10.0, 11.0, 12.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [True, False, False],
                        "cleanest_driver": [True, True, True],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "points_race_1": [10, 5, 3, 0],
                        "drop_race_1": [False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None],
                        "cleanest_driver_race_1": [True, True, True, None],
                        "points_total": [10, 5, 3, 0],
                        "points_rank": [1, 2, 3, 4],
                        "points_rank_change": [None, None, None, None],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [None, None, None, None],
                        "num_races": [1, 1, 1, 1],
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "class_rank": [1, 2, 3, 4],
                        "class_rank_change": [None, None, None, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 4, 3],
                        "points_race_1": [10, 5, 3, None, 0],
                        "points_race_2": [10, 5, None, 3, None],
                        "drop_race_1": [True, True, False, True, False],
                        "drop_race_2": [False, False, True, False, True],
                        "fastest_lap_race_1": [True, False, False, None, None],
                        "fastest_lap_race_2": [True, False, None, False, None],
                        "cleanest_driver_race_1": [True, True, True, None, None],
                        "cleanest_driver_race_2": [True, True, None, True, None],
                        "points_total": [10, 5, 3, 3, 0],
                        "points_rank": [1, 2, 3, 3, 5],
                        "points_rank_change": [0, 0, 0, None, 1],
                        "rating": [1776, 1584, 1421, 1416, 1231],
                        "rating_rank": [1, 2, 3, 4, 5],
                        "rating_rank_change": [0, 0, 0, None, 1],
                        "num_races": [2, 2, 1, 1, 1],
                        "display_name": ["a", "b", "c", "e", "d"],
                        "class_name": [None, None, None, None, None],
                        "class_symbol": [None, None, None, None, None],
                        "class_color": [None, None, None, None, None],
                        "class_rank": [1, 2, 3, 3, 5],
                        "class_rank_change": [0, 0, 0, None, 1],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 4, 2, 3],
                        "points_race_1": [10, 5, None, 3, 0],
                        "points_race_2": [10, 5, 3, None, None],
                        "points_race_3": [10, 5, 3, None, None],
                        "drop_race_1": [True, True, True, False, False],
                        "drop_race_2": [False, False, False, True, True],
                        "drop_race_3": [False, False, False, False, False],
                        "fastest_lap_race_1": [True, False, None, False, None],
                        "fastest_lap_race_2": [True, False, False, None, None],
                        "fastest_lap_race_3": [True, False, False, None, None],
                        "cleanest_driver_race_1": [True, True, None, True, None],
                        "cleanest_driver_race_2": [True, True, True, None, None],
                        "cleanest_driver_race_3": [True, True, True, None, None],
                        "points_total": [20, 10, 6, 3, 0],
                        "points_rank": [1, 2, 3, 4, 5],
                        "points_rank_change": [0, 0, 0, 1, 0],
                        "rating": [1779, 1586, 1412, 1421, 1231],
                        "rating_rank": [1, 2, 4, 3, 5],
                        "rating_rank_change": [0, 0, 0, 0, 0],
                        "num_races": [3, 3, 2, 1, 1],
                        "display_name": ["a", "b", "e", "c", "d"],
                        "class_name": [None, None, None, None, None],
                        "class_symbol": [None, None, None, None, None],
                        "class_color": [None, None, None, None, None],
                        "class_rank": [1, 2, 3, 4, 5],
                        "class_rank_change": [0, 0, 0, 1, 0],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False, "check_row_order": False})

    def test_multi_race_with_classes_fake(self, fake_client):
        """class_position and class_rank must reflect position *within* a class,
        not overall.  Two races:

        Race 1: a (Pro) 1st overall, b (Am) 2nd, c (Am) 3rd, d (Am) DNF
          → class_position: a=1, b=1, c=2, d=3
          → class_rank:     a=1 (only Pro), b=1, c=2, d=3

        Race 2: a (Pro) 1st, c (Am) 2nd, b (Am) 3rd  (b/c swap)
          → class_position: a=1, c=1, b=2
          → class_rank:     a=1, b=1 (tie on points with c), c=1 (tie)
          → class_rank_change for b: 0 (was 1, now 1); c: -1 (was 2, now 1)
        """
        config = RatingConfig.model_validate(
            {
                "elo": {},
                "points": {"points": [10, 5, 3]},
                "races": [{"subsession_id": 0}, {"subsession_id": 1}],
                "classes": [
                    {"name": "Pro", "symbol": "P", "color": "#ff0000", "drivers": [{"name": "a"}]},
                    {"name": "Am", "symbol": "A", "color": "#0000ff", "default": True},
                ],
            }
        )

        summary_data = [
            {
                "subsession_id": 0,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 3},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
            {
                "subsession_id": 1,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 3},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        generate_data(summary_data, fake_client)
        result = core.compute_ratings(config, client=fake_client)

        expected = {
            "ELO": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "rating": [1769, 1579, 1421, 1231],
                        "num_contests": [1, 1, 1, 1],
                        "participated": [True, True, True, True],
                        "subsession_id": [0, 0, 0, 0],
                        "contest_date": [datetime.datetime(1970, 1, 1)] * 4,
                        "num_valid_contests": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "rating_change": [269, 79, -79, -269],
                        "display_name": ["a", "b", "c", "d"],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "rating": [1772, 1509, 1489, 1231],
                        "num_contests": [2, 2, 2, 1],
                        "participated": [True, True, True, False],
                        "subsession_id": [1, 1, 1, 1],
                        "contest_date": [datetime.datetime(1970, 1, 1, 0, 0, 0, 1)] * 4,
                        "num_valid_contests": [2, 2, 2, 2],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [0, 0, 0, 0],
                        "rating_change": [3, -70, 68, None],
                        "display_name": ["a", "b", "c", "d"],
                    }
                ),
            ],
            "points": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "finish_position": [0, 1, 2, 3],
                        "subsession_id": [0, 0, 0, 0],
                        "session_end_time": [datetime.datetime(1970, 1, 1)] * 4,
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "laps_complete": [3, 3, 3, 0],
                        "points_type": ["default", "default", "default", "default"],
                        "points": [10, 5, 3, 0],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                        "cumulative_points": [10, 5, 3, 0],
                        "num_races": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "drop": [False, False, False, False],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [3, 0, 2, 1],
                        "finish_position": [None, 0, 1, 2],
                        "subsession_id": [1, 1, 1, 1],
                        "session_end_time": [None] + [datetime.datetime(1970, 1, 1, 0, 0, 0, 1)] * 3,
                        "fastest_lap_time": [None, 10.0, 11.0, 12.0],
                        "num_incidents": [None, 0, 0, 0],
                        "laps_complete": [None, 3, 3, 3],
                        "points_type": [None, "default", "default", "default"],
                        "points": [None, 10, 5, 3],
                        "fastest_lap": [None, True, False, False],
                        "cleanest_driver": [None, True, True, True],
                        "cumulative_points": [0, 20, 8, 8],
                        "num_races": [1, 2, 2, 2],
                        "rank": [4, 1, 2, 2],
                        "rank_change": [0, 0, -1, 0],
                        "drop": [False, False, False, False],
                    }
                ),
            ],
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": ["Pro", "Am", "Am", "Am"],
                        "class_symbol": ["P", "A", "A", "A"],
                        "class_color": ["#ff0000", "#0000ff", "#0000ff", "#0000ff"],
                        "start_position": [1, 4, 2, 3],
                        "qualify_lap_time": [0.004, 0.0044, 0.004200000000000001, 0.0043],
                        "finish_position": [1, 2, 3, 4],
                        "class_position": [1, 1, 2, 3],
                        "interval": ["0.000", "-3.000", "-6.000", "-3L"],
                        "points": [10, 5, 3, 0],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_change": [269, 79, -79, -269],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "laps_complete": [3, 3, 3, 0],
                        "total_time": [30.0, 33.0, 36.0, None],
                        "penalty": [0.0, 0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0, None],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "display_name": ["a", "c", "b"],
                        "class_name": ["Pro", "Am", "Am"],
                        "class_symbol": ["P", "A", "A"],
                        "class_color": ["#ff0000", "#0000ff", "#0000ff"],
                        "start_position": [1, 2, 3],
                        "qualify_lap_time": [0.004, 0.004200000000000001, 0.0044],
                        "finish_position": [1, 2, 3],
                        "class_position": [1, 1, 2],
                        "interval": ["0.000", "-3.000", "-6.000"],
                        "points": [10, 5, 3],
                        "rating": [1772, 1489, 1509],
                        "rating_change": [3, 68, -70],
                        "rank": [1, 3, 2],
                        "rank_change": [0, 0, 0],
                        "laps_complete": [3, 3, 3],
                        "total_time": [30.0, 33.0, 36.0],
                        "penalty": [0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0],
                        "fastest_lap_time": [10.0, 11.0, 12.0],
                        "num_incidents": [0, 0, 0],
                        "fastest_lap": [True, False, False],
                        "cleanest_driver": [True, True, True],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "points_race_1": [10, 5, 3, 0],
                        "drop_race_1": [False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None],
                        "cleanest_driver_race_1": [True, True, True, None],
                        "points_total": [10, 5, 3, 0],
                        "points_rank": [1, 2, 3, 4],
                        "points_rank_change": [None, None, None, None],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [None, None, None, None],
                        "num_races": [1, 1, 1, 1],
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": ["Pro", "Am", "Am", "Am"],
                        "class_symbol": ["P", "A", "A", "A"],
                        "class_color": ["#ff0000", "#0000ff", "#0000ff", "#0000ff"],
                        "class_rank": [1, 1, 2, 3],
                        "class_rank_change": [None, None, None, None],
                    }
                ),
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "points_race_1": [10, 5, 3, 0],
                        "points_race_2": [10, 3, 5, None],
                        "drop_race_1": [False, False, False, False],
                        "drop_race_2": [False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None],
                        "fastest_lap_race_2": [True, False, False, None],
                        "cleanest_driver_race_1": [True, True, True, None],
                        "cleanest_driver_race_2": [True, True, True, None],
                        "points_total": [20, 8, 8, 0],
                        "points_rank": [1, 2, 2, 4],
                        "points_rank_change": [0, 0, -1, 0],
                        "rating": [1772, 1509, 1489, 1231],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [0, 0, 0, 0],
                        "num_races": [2, 2, 2, 1],
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": ["Pro", "Am", "Am", "Am"],
                        "class_symbol": ["P", "A", "A", "A"],
                        "class_color": ["#ff0000", "#0000ff", "#0000ff", "#0000ff"],
                        "class_rank": [1, 1, 1, 3],
                        "class_rank_change": [0, 0, -1, 0],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False, "check_row_order": False})


class _TestComputeRatings:
    def test_single_race(self, client):
        config = RatingConfig(**{"races": [{"subsession_id": 64681712}]})

        result = core.compute_ratings(config, client=client)

        assert_frame_equal(
            result,
            pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
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

        assert_frame_equal(result0, result1)


class TestPenaltyByNameIntegration:
    def test_name_based_penalty_pipeline(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "races": [{"subsession_id": 0, "penalties": [{"name": "b", "time": 30.0}]}],
                "points": {"points": [10, 5, 3]},
            }
        )
        summary_data = [
            {
                "subsession_id": 0,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40_0000, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44_0000, "laps_complete": 3},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42_0000, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            }
        ]
        generate_data(summary_data, fake_client)
        outputs = core.compute_ratings(config, client=fake_client)
        race_result = outputs["race_results"][0]
        # driver "b" (user_id=1) should have a 30s penalty, placing them last
        b_row = race_result.filter(pl.col("display_name") == "b")
        assert b_row["penalty"][0] == 30.0
        assert b_row["finish_position"][0] == 3
