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
                {"user_id": 2, "lap": 2, "time": 100},
            ]
        )

        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time_s": 15},
            ]
        )

        result = apply_penalties(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {"user_id": 0, "laps_complete": 3, "total_time": 300.0, "penalty": 0.0, "finish_position": 0},
                    {"user_id": 2, "laps_complete": 3, "total_time": 320.0, "penalty": 0.0, "finish_position": 1},
                    {"user_id": 1, "laps_complete": 3, "total_time": 325.0, "penalty": 15.0, "finish_position": 2},
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
                    {"user_id": 0, "laps_complete": 3, "total_time": 300.0, "penalty": 0.0, "finish_position": 0},
                    {"user_id": 1, "laps_complete": 3, "total_time": 310.0, "penalty": 0.0, "finish_position": 1},
                    {"user_id": 2, "laps_complete": 3, "total_time": 320.0, "penalty": 0.0, "finish_position": 2},
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
                {"user_id": 0, "time_s": 15},
            ]
        )

        result = apply_penalties(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {"user_id": 1, "laps_complete": 3, "total_time": 310.0, "penalty": 0.0, "finish_position": 0},
                    {"user_id": 0, "laps_complete": 3, "total_time": 315.0, "penalty": 15.0, "finish_position": 1},
                    {"user_id": 2, "laps_complete": 3, "total_time": 320.0, "penalty": 0.0, "finish_position": 2},
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
                {"user_id": 1, "time_s": 30},
            ]
        )

        result = apply_penalties(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    {"user_id": 0, "laps_complete": 3, "total_time": 300.0, "penalty": 0.0, "finish_position": 0},
                    {"user_id": 2, "laps_complete": 2, "total_time": 310.0, "penalty": 0.0, "finish_position": 1},
                    {"user_id": 1, "laps_complete": 2, "total_time": 320.0, "penalty": 30.0, "finish_position": 2},
                ]
            ),
        )

    def test_disconnected_drivers(self):
        pass
