#!/usr/bin/env python3

import pandas as pd

from gpt_racing.penalties import apply_penalties


class TestApplyPenalties:
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

        result = apply_penalties(lap_df, penalties)

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

        result = apply_penalties(lap_df, penalties)

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

        result = apply_penalties(lap_df, penalties)

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

        result = apply_penalties(lap_df, penalties)

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
