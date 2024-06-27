#!/usr/bin/env python3

import pandas as pd
import pytest

from gpt_racing.results import compute_results, infer_invalid_laps
from gpt_racing.iracing_data import IracingDataClient


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
                    "interval": [0, 0, 0, -10, -10, -10, -20, -20, -20],
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
                    "interval": [0, 0, 0, -1, -2, -3],
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
                    "interval": [0, 0, 0, -1, -2, -3],
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
                    "lap_time": [99, 99, 100, 101, 100, 101],
                    "interval": [0, 0, 0, -2, -3, -4],
                }
            ),
        )

    def test_sequential_invalid(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
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
                    "lap": [1, 2, 3, 3, 3, 1, 2, 3, 4, 5],
                    "lap_time": [100, 100, 100, 100, 100, 101, 101, 101, 101, 101],
                    "interval": [0, 0, 0, 0, 0, -1, -2, -3, -4, -5],
                }
            ),
        )

    def test_sequential_invalid_leader(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": -1, "interval": None},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": None},
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
                    "lap": [1, 2, 3, 3, 3, 1, 2, 3, 4, 5],
                    "lap_time": [100, 100, 100, 100, 100, 101, 101, 101, 101, 101],
                    "interval": [0, 0, 0, 0, 0, -1, -2, -3, -4, -5],
                }
            ),
        )


class TestComputeResults:
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
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 101.0, 102.0],
                    "interval": ["0.000", "-2.000", "-4.000"],
                }
            ),
        )

    def test_basic_penalty(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": None},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": None},
                {"user_id": 1, "lap": 1, "lap_time": 110, "interval": -10},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": -10},
                {"user_id": 2, "lap": 1, "lap_time": 120, "interval": -20},
                {"user_id": 2, "lap": 2, "lap_time": 100, "interval": -20},
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
                {
                    "user_id": [0, 2, 1],
                    "laps_complete": [2, 2, 2],
                    "total_time": [200.0, 220.0, 225.0],
                    "penalty": [0.0, 0.0, 15.0],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 110.0, 112.5],
                    "interval": ["0.000", "-20.000", "-25.000"],
                }
            ),
        )

    def test_penalize_leader(self):
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
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [103.33333333333333, 105.0, 106.66666666666667],
                    "interval": ["0.000", "-5.000", "-10.000"],
                }
            ),
        )

    def test_penalize_across_final_lap(self):
        lap_df = pd.DataFrame(
            [
                {"user_id": 0, "lap": 0, "lap_time": 100},
                {"user_id": 0, "lap": 1, "lap_time": 100},
                {"user_id": 0, "lap": 2, "lap_time": 100},
                # User 1 finishes on lead lap, but they started their last lap within penalty range
                {"user_id": 1, "lap": 0, "lap_time": 200},
                {"user_id": 1, "lap": 1, "lap_time": 90},
                {"user_id": 1, "lap": 2, "lap_time": 100},
                # User 2 finishes one lap down, 10s behind leader
                {"user_id": 2, "lap": 0, "lap_time": 200},
                {"user_id": 2, "lap": 1, "lap_time": 110},
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
                {
                    "user_id": [0, 1, 2],
                    "laps_complete": [2, 2, 1],
                    "total_time": [200.0, 220.0, 110.0],
                    "penalty": [0.0, 30.0, 0.0],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 110.0, 110.0],
                    "interval": ["0.000", "-20.000", "-1L"],
                }
            ),
        )

    @pytest.mark.skip("Not implemented")
    def test_disconnected_drivers(self):
        """Test that disconnected drivers are placed"""
        pass
