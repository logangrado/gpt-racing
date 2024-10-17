#!/usr/bin/env python3

import pandas as pd
import pytest

from gpt_racing.results import compute_results, infer_invalid_laps


class TestInferInvalidLapTimes:
    """
    Test that we can infer invalid laps
    """

    def test_basic(self):
        """Basic test without invalid laps"""
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                {"user_id": 1, "lap": 1, "lap_time": 110, "interval": -10},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": -10},
                {"user_id": 1, "lap": 3, "lap_time": 100, "interval": -10},
                {"user_id": 2, "lap": 1, "lap_time": 120, "interval": -20},
                {"user_id": 2, "lap": 2, "lap_time": 100, "interval": -20},
                {"user_id": 2, "lap": 3, "lap_time": 100, "interval": -20},
            ]
        )

        result = infer_invalid_laps(lap_df)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1, 2, 2, 2],
                    "lap": [1, 2, 3, 1, 2, 3, 1, 2, 3],
                    "lap_time": [100, 100, 100, 110, 100, 100, 120, 100, 100],
                    "interval": [0.0, 0.0, 0.0, -10.0, -10.0, -10.0, -20.0, -20.0, -20.0],
                }
            ),
        )

    def test_single_invalid(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                #
                {"user_id": 1, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 1, "lap": 2, "lap_time": -1, "interval": -2},
                {"user_id": 1, "lap": 3, "lap_time": 101, "interval": -3},
            ]
        )

        result = infer_invalid_laps(lap_df)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1],
                    "lap": [1, 2, 3, 1, 2, 3],
                    "lap_time": [100, 100, 100, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, -1.0, -2.0, -3.0],
                }
            ),
        )

    def test_single_invalid_leader(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                #
                {"user_id": 1, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 1, "lap": 2, "lap_time": 101, "interval": -2},
                {"user_id": 1, "lap": 3, "lap_time": 101, "interval": -3},
            ]
        )

        result = infer_invalid_laps(lap_df)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1],
                    "lap": [1, 2, 3, 1, 2, 3],
                    "lap_time": [100, 100, 100, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, -1.0, -2.0, -3.0],
                }
            ),
        )

    def test_all_invalid_for_a_lap(self):
        """
        Here, we don't have enough information to determine the missing lap time.

        We will instead infer that the lead driver matched their fastest lap
        """
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 99, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                #
                {"user_id": 1, "lap": 1, "lap_time": 101, "interval": -2},
                {"user_id": 1, "lap": 2, "lap_time": -1, "interval": -3},
                {"user_id": 1, "lap": 3, "lap_time": 101, "interval": -4},
            ]
        )

        result = infer_invalid_laps(lap_df)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1],
                    "lap": [1, 2, 3, 1, 2, 3],
                    "lap_time": [99.0, 99.0, 100.0, 101.0, 100.0, 101.0],
                    "interval": [0.0, 0.0, 0.0, -2.0, -3.0, -4.0],
                }
            ),
        )

    def test_sequential_invalid(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 4, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 5, "lap_time": 100, "interval": None},
                #
                {"user_id": 1, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 1, "lap": 2, "lap_time": -1, "interval": -2},
                {"user_id": 1, "lap": 3, "lap_time": -1, "interval": -3},
                {"user_id": 1, "lap": 4, "lap_time": -1, "interval": -4},
                {"user_id": 1, "lap": 5, "lap_time": 101, "interval": -5},
            ]
        )

        result = infer_invalid_laps(lap_df)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
                    "lap": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
                    "lap_time": [100, 100, 100, 100, 100, 101, 101, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, 0.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0],
                }
            ),
        )

    def test_sequential_invalid_leader(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 4, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 5, "lap_time": 100, "interval": None},
                #
                {"user_id": 1, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 1, "lap": 2, "lap_time": 101, "interval": -2},
                {"user_id": 1, "lap": 3, "lap_time": 101, "interval": -3},
                {"user_id": 1, "lap": 4, "lap_time": 101, "interval": -4},
                {"user_id": 1, "lap": 5, "lap_time": 101, "interval": -5},
            ]
        )

        result = infer_invalid_laps(lap_df)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
                    "lap": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
                    "lap_time": [100, 100, 100, 100, 100, 101, 101, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, 0.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0],
                }
            ),
        )


class TestComputeResults:
    def test_basic(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 0, "lap": 2, "lap_time": 101, "interval": -2},
                {"user_id": 1, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 2, "lap": 1, "lap_time": 102, "interval": -2},
                {"user_id": 2, "lap": 2, "lap_time": 102, "interval": -4},
            ]
        )
        lap_df["incident"] = False

        penalties = pd.DataFrame()

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [1, 0, 2],
                    "laps_complete": [2, 2, 2],
                    "total_time": [200, 202, 204],
                    "penalty": [0, 0, 0],
                    "interval": ["0.000", "-2.000", "-4.000"],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 101.0, 102.0],
                    "fastest_lap_time": [100, 101, 102],
                }
            ),
        )

    def test_basic_penalty(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 1, "lap": 1, "lap_time": 110, "interval": -10},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": -10},
                {"user_id": 2, "lap": 1, "lap_time": 120, "interval": -20},
                {"user_id": 2, "lap": 2, "lap_time": 100, "interval": -20},
            ]
        )
        lap_df["incident"] = False

        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 15},
            ]
        )

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 2, 1],
                    "laps_complete": [2, 2, 2],
                    "total_time": [200.0, 220.0, 225.0],
                    "penalty": [0.0, 0.0, 15.0],
                    "interval": ["0.000", "-20.000", "-25.000"],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 110.0, 112.5],
                    "fastest_lap_time": [100, 100, 100],
                }
            ),
        )

    def test_penalize_leader(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": 0},
                {"user_id": 1, "lap": 1, "lap_time": 110, "interval": -10},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": -10},
                {"user_id": 1, "lap": 3, "lap_time": 100, "interval": -10},
                {"user_id": 2, "lap": 1, "lap_time": 120, "interval": -20},
                {"user_id": 2, "lap": 2, "lap_time": 100, "interval": -20},
                {"user_id": 2, "lap": 3, "lap_time": 100, "interval": -20},
            ]
        )
        lap_df["incident"] = False
        penalties = pd.DataFrame(
            [
                {"user_id": 0, "time": 15},
            ]
        )

        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [1, 0, 2],
                    "laps_complete": [3, 3, 3],
                    "total_time": [310.0, 315.0, 320.0],
                    "penalty": [0.0, 15.0, 0.0],
                    "interval": ["0.000", "-5.000", "-10.000"],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [103.33333333333333, 105.0, 106.66666666666667],
                    "fastest_lap_time": [100, 100, 100],
                }
            ),
        )

    def test_penalize_across_final_lap(self):
        """Test what happens if we apply a penalty big enough to knock a driver down two laps!"""
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 10, "interval": 0},
                {"user_id": 0, "lap": 2, "lap_time": 10, "interval": 0},
                {"user_id": 0, "lap": 3, "lap_time": 10, "interval": 0},
                {"user_id": 0, "lap": 4, "lap_time": 10, "interval": 0},
                # {"user_id": 0, "lap": 5, "lap_time": 10, "interval": 0},
                # User 1 finishes on lead lap, but they started their last lap within penalty range
                {"user_id": 1, "lap": 1, "lap_time": 11, "interval": -1},
                {"user_id": 1, "lap": 2, "lap_time": 11, "interval": -2},
                {"user_id": 1, "lap": 3, "lap_time": 11, "interval": -3},
                {"user_id": 1, "lap": 4, "lap_time": 11, "interval": -4},
                # {"user_id": 1, "lap": 5, "lap_time": 11, "interval": -5},
                # User 2
                {"user_id": 2, "lap": 1, "lap_time": 13, "interval": -3},
                {"user_id": 2, "lap": 2, "lap_time": 13, "interval": -6},
                {"user_id": 2, "lap": 3, "lap_time": 13, "interval": -9},
                {"user_id": 2, "lap": 4, "lap_time": 13, "interval": -12},
                # User 3
                {"user_id": 3, "lap": 1, "lap_time": 14, "interval": -4},
                {"user_id": 3, "lap": 2, "lap_time": 14, "interval": -8},
                {"user_id": 3, "lap": 3, "lap_time": 14, "interval": -12},
            ]
        )
        lap_df["incident"] = False

        # 6s penalty, no change in lap/position
        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 6},
            ]
        )
        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2, 3],
                    "laps_complete": [4, 4, 4, 3],
                    "total_time": [40.0, 50.0, 52.0, 42.0],
                    "penalty": [0.0, 6.0, 0.0, 0.0],
                    "interval": ["0.000", "-10.000", "-12.000", "-1L"],
                    "finish_position": [0, 1, 2, 3],
                    "average_lap_time": [10.0, 12.5, 13.0, 14.0],
                    "fastest_lap_time": [10, 11, 13, 14],
                }
            ),
        )

        # 7s penalty, exactly on edge of being knocked down a lap
        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 7},
            ]
        )
        result = compute_results(lap_df, penalties)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2, 3],
                    "laps_complete": [4, 4, 4, 3],
                    "total_time": [40.0, 51.0, 52.0, 42.0],
                    "penalty": [0.0, 7.0, 0.0, 0.0],
                    "interval": ["0.000", "-11.000", "-12.000", "-1L"],
                    "finish_position": [0, 1, 2, 3],
                    "average_lap_time": [10.0, 12.75, 13.0, 14.0],
                    "fastest_lap_time": [10, 11, 13, 14],
                }
            ),
        )

        # Just over 7s to knock to next lap
        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 7.2},
            ]
        )
        result = compute_results(lap_df, penalties)
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 2, 1, 3],
                    "laps_complete": [4, 4, 3, 3],
                    "total_time": [40.0, 52.0, 40.2, 42.0],
                    "penalty": [0.0, 0.0, 7.2, 0.0],
                    "interval": ["0.000", "-12.000", "-1L", "-1L"],
                    "finish_position": [0, 1, 2, 3],
                    "average_lap_time": [10.0, 13.0, 13.4, 14.0],
                    "fastest_lap_time": [10, 13, 11, 14],
                }
            ),
        )

        # Just under 2 lap penalty
        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 15},
            ]
        )
        result = compute_results(lap_df, penalties)
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 2, 3, 1],
                    "laps_complete": [4, 4, 3, 3],
                    "total_time": [40.0, 52.0, 42.0, 48.0],
                    "penalty": [0.0, 0.0, 0.0, 15.0],
                    "interval": ["0.000", "-12.000", "-1L", "-1L"],
                    "finish_position": [0, 1, 2, 3],
                    "average_lap_time": [10.0, 13.0, 14.0, 16.0],
                    "fastest_lap_time": [10, 13, 14, 11],
                }
            ),
        )

        # Just over 2 lap penalty
        penalties = pd.DataFrame(
            [
                {"user_id": 1, "time": 15.2},
            ]
        )
        result = compute_results(lap_df, penalties)
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 2, 3, 1],
                    "laps_complete": [4, 4, 3, 2],
                    "total_time": [40.0, 52.0, 42.0, 37.2],
                    "penalty": [0.0, 0.0, 0.0, 15.2],
                    "interval": ["0.000", "-12.000", "-1L", "-2L"],
                    "finish_position": [0, 1, 2, 3],
                    "average_lap_time": [10.0, 13.0, 14.0, 18.6],
                    "fastest_lap_time": [10, 13, 14, 11],
                }
            ),
        )

    @pytest.mark.skip("Not implemented")
    def test_disconnected_drivers(self):
        """Test that disconnected drivers are placed"""
        pass

    @pytest.mark.skip("Test that lap 0 is always dropped")
    def test_lap_0_dropped(self):
        pass

    @pytest.mark.skip("Not implemented")
    def test_interval_sum_mismatch(self):
        """
        Sometimes (often?) the interval calculated from total time does not match perfectly the
        actual interval.

        Check a scinerio where this is true, and show that we use the interval (back calculated from last lap)

        Also check when a lap down?
        """
