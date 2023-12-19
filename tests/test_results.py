#!/usr/bin/env python3

import pandas as pd

from gpt_racing.results import compute_results
from gpt_racing.iracing_data import IracingDataClient


class TestComputeResults:
    def test_basic(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        result_df = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
            ]
        )

        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 0, "time": 100},
                {"user_id": 0, "lap": 1, "time": 100},
                {"user_id": 0, "lap": 2, "time": 100},
                {"user_id": 1, "lap": 2, "time": 100},
                {"user_id": 1, "lap": 1, "time": 100},
                {"user_id": 1, "lap": 0, "time": 110},
                {"user_id": 2, "lap": 1, "time": 100},
                {"user_id": 2, "lap": 0, "time": 120},
                {"user_id": 2, "lap": 2, "time": 100.123456},
            ]
        )

        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 15},
            ]
        )

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {
                        "user_id": 0,
                        "laps_complete": 2,
                        "total_time": 300.0,
                        "penalty": 0.0,
                        "finish_position": 0,
                        "interval": "0.000",
                    },
                    {
                        "user_id": 2,
                        "laps_complete": 2,
                        "total_time": 320.12345600000003,
                        "penalty": 0.0,
                        "finish_position": 1,
                        "interval": "-20.123",
                    },
                    {
                        "user_id": 1,
                        "laps_complete": 2,
                        "total_time": 325.0,
                        "penalty": 15.0,
                        "finish_position": 2,
                        "interval": "-25.000",
                    },
                ]
            ),
        )

    def test_no_penalties(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        result_df = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
            ]
        )

        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 0, "time": 100},
                {"user_id": 0, "lap": 1, "time": 100},
                {"user_id": 0, "lap": 2, "time": 100},
                {"user_id": 1, "lap": 2, "time": 100},
                {"user_id": 1, "lap": 1, "time": 100},
                {"user_id": 1, "lap": 0, "time": 110},
                {"user_id": 2, "lap": 1, "time": 100},
                {"user_id": 2, "lap": 0, "time": 120},
                {"user_id": 2, "lap": 2, "time": 100},
            ]
        )

        penalties = pd.DataFrame()

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {
                        "user_id": 0,
                        "laps_complete": 2,
                        "total_time": 300,
                        "penalty": 0,
                        "finish_position": 0,
                        "interval": "0.000",
                    },
                    {
                        "user_id": 1,
                        "laps_complete": 2,
                        "total_time": 310,
                        "penalty": 0,
                        "finish_position": 1,
                        "interval": "-10.000",
                    },
                    {
                        "user_id": 2,
                        "laps_complete": 2,
                        "total_time": 320,
                        "penalty": 0,
                        "finish_position": 2,
                        "interval": "-20.000",
                    },
                ]
            ),
        )

    def test_penalize_leader(self):
        result_df = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
            ]
        )

        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 0, "time": 100},
                {"user_id": 0, "lap": 1, "time": 100},
                {"user_id": 0, "lap": 2, "time": 100},
                {"user_id": 1, "lap": 2, "time": 100},
                {"user_id": 1, "lap": 1, "time": 100},
                {"user_id": 1, "lap": 0, "time": 110},
                {"user_id": 2, "lap": 1, "time": 100},
                {"user_id": 2, "lap": 0, "time": 120},
                {"user_id": 2, "lap": 2, "time": 100},
            ]
        )

        penalties = pd.DataFrame(
            [
                {"user_id": 0, "time": 15},
            ]
        )

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {
                        "user_id": 1,
                        "laps_complete": 2,
                        "total_time": 310.0,
                        "penalty": 0.0,
                        "finish_position": 0,
                        "interval": "0.000",
                    },
                    {
                        "user_id": 0,
                        "laps_complete": 2,
                        "total_time": 315.0,
                        "penalty": 15.0,
                        "finish_position": 1,
                        "interval": "-5.000",
                    },
                    {
                        "user_id": 2,
                        "laps_complete": 2,
                        "total_time": 320.0,
                        "penalty": 0.0,
                        "finish_position": 2,
                        "interval": "-10.000",
                    },
                ]
            ),
        )

    def test_penalize_across_final_lap(self):
        result_df = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
            ]
        )

        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 0, "time": 100},
                {"user_id": 0, "lap": 1, "time": 100},
                {"user_id": 0, "lap": 2, "time": 100},
                # User 1 finishes on lead lap, but they started their last lap within penalty range
                {"user_id": 1, "lap": 0, "time": 200},
                {"user_id": 1, "lap": 1, "time": 90},
                {"user_id": 1, "lap": 2, "time": 100},
                # User 2 finishes one lap down, 10s behind leader
                {"user_id": 2, "lap": 0, "time": 200},
                {"user_id": 2, "lap": 1, "time": 110},
            ]
        )

        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 30},
            ]
        )

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {
                        "user_id": 0,
                        "laps_complete": 2,
                        "total_time": 300.0,
                        "penalty": 0.0,
                        "finish_position": 0,
                        "interval": "0.000",
                    },
                    {
                        "user_id": 2,
                        "laps_complete": 1,
                        "total_time": 310.0,
                        "penalty": 0.0,
                        "finish_position": 1,
                        "interval": "-1L",
                    },
                    {
                        "user_id": 1,
                        "laps_complete": 1,
                        "total_time": 320.0,
                        "penalty": 30.0,
                        "finish_position": 2,
                        "interval": "-1L",
                    },
                ]
            ),
        )

    def test_disconnected_drivers(self):
        pass

    def test_invalid_lap(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "time": -1, "interval": None},
                {"user_id": 0, "lap": 2, "time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "time": -1, "interval": None},
                #
                {"user_id": 1, "lap": 1, "time": 101, "interval": -1},
                {"user_id": 1, "lap": 2, "time": -1, "interval": -2},
                {"user_id": 1, "lap": 3, "time": 101, "interval": -3},
                #
                {"user_id": 2, "lap": 1, "time": 110, "interval": -10},
                {"user_id": 2, "lap": 2, "time": 110, "interval": -20},
                {"user_id": 2, "lap": 3, "time": 110, "interval": -30},
            ]
        )

        result = compute_results(lap_df, None)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2],
                    "laps_complete": [3, 3, 3],
                    "total_time": [300, 303, 330],
                    "penalty": [0, 0, 0],
                    "finish_position": [0, 1, 2],
                    "interval": ["0.000", "-3.000", "-30.000"],
                }
            ),
        )

    def test_no_valid_lap(self):
        """
        Test what happens when one lap has all invalid across all drivers
        """
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "time": -1, "interval": None},
                {"user_id": 0, "lap": 2, "time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "time": 100, "interval": None},
                #
                {"user_id": 1, "lap": 1, "time": 101, "interval": -1},
                {"user_id": 1, "lap": 2, "time": -1, "interval": -2},
                {"user_id": 1, "lap": 3, "time": 101, "interval": -3},
                #
                {"user_id": 2, "lap": 1, "time": 110, "interval": -10},
                {"user_id": 2, "lap": 2, "time": -1, "interval": -20},
                {"user_id": 2, "lap": 3, "time": 110, "interval": -30},
            ]
        )
        # The missing laps will be filled with the fastest lap. This is an ok approximation, and is unlikely to break
        # In this case, we should infer a 100s lap time

        result = compute_results(lap_df, None)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2],
                    "laps_complete": [3, 3, 3],
                    "total_time": [300.0, 303.0, 330.0],
                    "penalty": [0, 0, 0],
                    "finish_position": [0, 1, 2],
                    "interval": ["0.000", "-3.000", "-30.000"],
                }
            ),
        )

    def test_lead_change_invalid_laps(self):
        """
        Test that we can properly handle a lead change across an invalid lap. Might get weird
        """
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "time": -2, "interval": -10},  # Interval should put this lap time at 110,
                {"user_id": 0, "lap": 3, "time": 100, "interval": -10},
                #
                {"user_id": 1, "lap": 1, "time": 102, "interval": -2},
                {"user_id": 1, "lap": 2, "time": 98, "interval": None},  # total time: 200
                {"user_id": 1, "lap": 3, "time": 100, "interval": None},
            ]
        )

        result = compute_results(lap_df, None)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [1, 0],
                    "laps_complete": [3, 3],
                    "total_time": [300, 310],
                    "penalty": [0, 0],
                    "finish_position": [0, 1],
                    "interval": ["0.000", "-10.000"],
                }
            ),
        )

    def test_real_data(self):
        client = IracingDataClient()

        lap_df = client.get_lap_data(65426723)
        lap_df = lap_df.rename(
            columns={
                "cust_id": "user_id",
                "lap_number": "lap",
            }
        )
        lap_df["time"] = lap_df["lap_time"] / 10000
        lap_df["interval"] = lap_df["interval"] / 10000

        result_df = compute_results(lap_df, penalty_df=None)

        result_df = result_df.merge(lap_df[["user_id", "display_name"]].drop_duplicates(), on="user_id")

        print("NEED TO ALSO INCLUDE ALL DRIVERS THAT DISCONNECTED!")
        # Anyone who completed a qualify lap OR any race lap is considered to have competed
        import ipdb

        ipdb.set_trace()
        pass

        pd.testing.assert_frame_equal(
            result_df,
            pd.DataFrame(
                [
                    {
                        "user_id": 407068,
                        "laps_complete": 14,
                        "total_time": 1262.3206,
                        "penalty": 0,
                        "finish_position": 0,
                        "interval": "0.000",
                        "display_name": "Christian Youngwall",
                    },
                    {
                        "user_id": 483350,
                        "laps_complete": 14,
                        "total_time": 1267.0601,
                        "penalty": 0,
                        "finish_position": 1,
                        "interval": "-4.739",
                        "display_name": "Reed Gibson",
                    },
                    {
                        "user_id": 412321,
                        "laps_complete": 14,
                        "total_time": 1267.5307,
                        "penalty": 0,
                        "finish_position": 2,
                        "interval": "-5.210",
                        "display_name": "John Sedlak",
                    },
                    {
                        "user_id": 260862,
                        "laps_complete": 14,
                        "total_time": 1268.1958,
                        "penalty": 0,
                        "finish_position": 3,
                        "interval": "-5.875",
                        "display_name": "Rafael Amorim",
                    },
                    {
                        "user_id": 586629,
                        "laps_complete": 14,
                        "total_time": 1271.2425,
                        "penalty": 0,
                        "finish_position": 4,
                        "interval": "-8.922",
                        "display_name": "Enrico Gregoratto",
                    },
                    {
                        "user_id": 663448,
                        "laps_complete": 14,
                        "total_time": 1274.3785,
                        "penalty": 0,
                        "finish_position": 5,
                        "interval": "-12.058",
                        "display_name": "Scott Kessel",
                    },
                    {
                        "user_id": 346566,
                        "laps_complete": 14,
                        "total_time": 1275.1662,
                        "penalty": 0,
                        "finish_position": 6,
                        "interval": "-12.846",
                        "display_name": "Derek M Cyphers",
                    },
                    {
                        "user_id": 538346,
                        "laps_complete": 14,
                        "total_time": 1275.9868,
                        "penalty": 0,
                        "finish_position": 7,
                        "interval": "-13.666",
                        "display_name": "Ariel Julian",
                    },
                    {
                        "user_id": 282802,
                        "laps_complete": 14,
                        "total_time": 1278.3515,
                        "penalty": 0,
                        "finish_position": 8,
                        "interval": "-16.031",
                        "display_name": "Ross Yost",
                    },
                    {
                        "user_id": 414448,
                        "laps_complete": 14,
                        "total_time": 1279.7396,
                        "penalty": 0,
                        "finish_position": 9,
                        "interval": "-17.419",
                        "display_name": "Justin SW Tsang",
                    },
                    {
                        "user_id": 577153,
                        "laps_complete": 14,
                        "total_time": 1289.5283,
                        "penalty": 0,
                        "finish_position": 10,
                        "interval": "-27.208",
                        "display_name": "Matt Scherff",
                    },
                    {
                        "user_id": 622340,
                        "laps_complete": 14,
                        "total_time": 1289.7335,
                        "penalty": 0,
                        "finish_position": 11,
                        "interval": "-27.413",
                        "display_name": "Logan Grado",
                    },
                    {
                        "user_id": 696009,
                        "laps_complete": 14,
                        "total_time": 1293.9454,
                        "penalty": 0,
                        "finish_position": 12,
                        "interval": "-31.625",
                        "display_name": "Jochem Bakker",
                    },
                    {
                        "user_id": 686056,
                        "laps_complete": 14,
                        "total_time": 1301.5767,
                        "penalty": 0,
                        "finish_position": 13,
                        "interval": "-39.256",
                        "display_name": "Chad Henson",
                    },
                    {
                        "user_id": 367874,
                        "laps_complete": 14,
                        "total_time": 1302.1102,
                        "penalty": 0,
                        "finish_position": 14,
                        "interval": "-39.790",
                        "display_name": "Matthew Siddall",
                    },
                    {
                        "user_id": 384356,
                        "laps_complete": 6,
                        "total_time": 599.7201,
                        "penalty": 0,
                        "finish_position": 15,
                        "interval": "-8L",
                        "display_name": "Chris Walsh",
                    },
                    {
                        "user_id": 759324,
                        "laps_complete": 5,
                        "total_time": 466.88210000000004,
                        "penalty": 0,
                        "finish_position": 16,
                        "interval": "-9L",
                        "display_name": "Evan Smogor",
                    },
                    {
                        "user_id": 719884,
                        "laps_complete": 2,
                        "total_time": 252.8147,
                        "penalty": 0,
                        "finish_position": 17,
                        "interval": "-12L",
                        "display_name": "Robby Prescott",
                    },
                ]
            ),
        )
