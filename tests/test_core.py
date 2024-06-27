#!/usr/bin/env python3

import pandas as pd

from gpt_racing import core
from gpt_racing.config import RatingConfig


class TestComputeRatings:
    def test_single_race_fake(self, fake_client):
        config = RatingConfig.model_validate(
            {"races": [{"subsession_id": 0}]},
        )
        lap_data_0 = pd.DataFrame(
            [
                {"cust_id": 0, "display_name": "a", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 1,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 2,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 3,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                #
                {"cust_id": 1, "display_name": "b", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 1,
                    "lap_time": 110000,
                    "interval": 1000,
                    "lap_events": [],
                },
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 2,
                    "lap_time": 110000,
                    "interval": 2000,
                    "lap_events": [],
                },
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 3,
                    "lap_time": 110000,
                    "interval": 3000,
                    "lap_events": [],
                },
                #
                {"cust_id": 2, "display_name": "c", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 2,
                    "display_name": "c",
                    "lap_number": 1,
                    "lap_time": 120000,
                    "interval": 2000,
                    "lap_events": [],
                },
                {
                    "cust_id": 2,
                    "display_name": "c",
                    "lap_number": 2,
                    "lap_time": 120000,
                    "interval": 4000,
                    "lap_events": [],
                },
                {
                    "cust_id": 2,
                    "display_name": "c",
                    "lap_number": 3,
                    "lap_time": 120000,
                    "interval": 6000,
                    "lap_events": [],
                },
            ]
        )

        qualy_data_0 = pd.DataFrame(
            [
                {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "finish_position": 0, "laps_complete": 3},
                {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "finish_position": 3, "laps_complete": 4},
                {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "finish_position": 1, "laps_complete": 3},
                {"cust_id": 3, "display_name": "d", "best_lap_time": 43, "finish_position": 2, "laps_complete": 3},
            ]
        )

        fake_client._set_lap_data(0, lap_data_0)
        fake_client._set_qualy_result(0, qualy_data_0)

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

    def test_multi_race_fake(self, fake_client, tmp_path):
        config = RatingConfig.model_validate(
            {
                "races": [
                    {"subsession_id": 0},
                    {"subsession_id": 1},
                ]
            },
        )
        lap_data_0 = pd.DataFrame(
            [
                {"cust_id": 0, "display_name": "a", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 1,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 2,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 3,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                #
                {"cust_id": 1, "display_name": "b", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 1,
                    "lap_time": 110000,
                    "interval": 1000,
                    "lap_events": [],
                },
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 2,
                    "lap_time": 110000,
                    "interval": 2000,
                    "lap_events": [],
                },
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 3,
                    "lap_time": 110000,
                    "interval": 3000,
                    "lap_events": [],
                },
                #
                {"cust_id": 2, "display_name": "c", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 2,
                    "display_name": "c",
                    "lap_number": 1,
                    "lap_time": 120000,
                    "interval": 2000,
                    "lap_events": [],
                },
                {
                    "cust_id": 2,
                    "display_name": "c",
                    "lap_number": 2,
                    "lap_time": 120000,
                    "interval": 4000,
                    "lap_events": [],
                },
                {
                    "cust_id": 2,
                    "display_name": "c",
                    "lap_number": 3,
                    "lap_time": 120000,
                    "interval": 6000,
                    "lap_events": [],
                },
            ]
        )
        fake_client._set_lap_data(0, lap_data_0)

        lap_data_1 = pd.DataFrame(
            [
                {"cust_id": 0, "display_name": "a", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 1,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 2,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                {
                    "cust_id": 0,
                    "display_name": "a",
                    "lap_number": 3,
                    "lap_time": 100000,
                    "interval": 0,
                    "lap_events": [],
                },
                #
                {"cust_id": 1, "display_name": "b", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 1,
                    "lap_time": 110000,
                    "interval": 1000,
                    "lap_events": [],
                },
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 2,
                    "lap_time": 110000,
                    "interval": 1000,
                    "lap_events": [],
                },
                {
                    "cust_id": 1,
                    "display_name": "b",
                    "lap_number": 3,
                    "lap_time": 110000,
                    "interval": 1000,
                    "lap_events": [],
                },
                #
                {"cust_id": 4, "display_name": "d", "lap_number": 0, "lap_time": -1, "interval": 0, "lap_events": []},
                {
                    "cust_id": 4,
                    "display_name": "d",
                    "lap_number": 1,
                    "lap_time": 120000,
                    "interval": 2000,
                    "lap_events": [],
                },
                {
                    "cust_id": 4,
                    "display_name": "d",
                    "lap_number": 2,
                    "lap_time": 120000,
                    "interval": 2000,
                    "lap_events": [],
                },
                {
                    "cust_id": 4,
                    "display_name": "d",
                    "lap_number": 3,
                    "lap_time": 120000,
                    "interval": 2000,
                    "lap_events": [],
                },
            ]
        )
        fake_client._set_lap_data(1, lap_data_1)

        qualy_data_0 = pd.DataFrame(
            [
                {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "finish_position": 0, "laps_complete": 3},
                {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "finish_position": 3, "laps_complete": 4},
                {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "finish_position": 1, "laps_complete": 3},
                {"cust_id": 3, "display_name": "d", "best_lap_time": 43, "finish_position": 2, "laps_complete": 3},
            ]
        )
        qualy_data_1 = pd.DataFrame(
            [
                {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "finish_position": 0, "laps_complete": 3},
                {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "finish_position": 2, "laps_complete": 4},
                {"cust_id": 4, "display_name": "e", "best_lap_time": 43, "finish_position": 1, "laps_complete": 3},
            ]
        )

        fake_client._set_qualy_result(0, qualy_data_0)
        fake_client._set_qualy_result(1, qualy_data_1)

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
                    "interval": ["0.000", "3.000", "6.000", "-3L", "0.000", "1.000", "2.000"],
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

        # Check output artifacts


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
