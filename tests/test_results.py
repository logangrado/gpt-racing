#!/usr/bin/env python3

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from gpt_racing.results import compute_results, infer_invalid_laps


class TestInferInvalidLapTimes:
    """
    Test that we can infer invalid laps
    """

    def test_basic(self):
        """Basic test without invalid laps"""
        lap_df = pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1, 2, 2, 2],
                    "lap": [1, 2, 3, 1, 2, 3, 1, 2, 3],
                    "lap_time": [100, 100, 100, 110, 100, 100, 120, 100, 100],
                    "interval": [0.0, 0.0, 0.0, -10.0, -10.0, -10.0, -20.0, -20.0, -20.0],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_single_invalid(self):
        lap_df = pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1],
                    "lap": [1, 2, 3, 1, 2, 3],
                    "lap_time": [100, 100, 100, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, -1.0, -2.0, -3.0],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_single_invalid_leader(self):
        lap_df = pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1],
                    "lap": [1, 2, 3, 1, 2, 3],
                    "lap_time": [100, 100, 100, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, -1.0, -2.0, -3.0],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_all_invalid_for_a_lap(self):
        """
        Here, we don't have enough information to determine the missing lap time.

        We will instead infer that the lead driver matched their fastest lap
        """
        lap_df = pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 0, 0, 1, 1, 1],
                    "lap": [1, 2, 3, 1, 2, 3],
                    "lap_time": [99.0, 99.0, 100.0, 101.0, 100.0, 101.0],
                    "interval": [0.0, 0.0, 0.0, -2.0, -3.0, -4.0],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_sequential_invalid(self):
        lap_df = pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
                    "lap": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
                    "lap_time": [100, 100, 100, 100, 100, 101, 101, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, 0.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_sequential_invalid_leader(self):
        lap_df = pl.DataFrame(
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

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
                    "lap": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
                    "lap_time": [100, 100, 100, 100, 100, 101, 101, 101, 101, 101],
                    "interval": [0.0, 0.0, 0.0, 0.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )


class TestComputeResults:
    def test_basic(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        lap_df = pl.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 0, "lap": 2, "lap_time": 101, "interval": -2},
                {"user_id": 1, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 2, "lap": 1, "lap_time": 102, "interval": -2},
                {"user_id": 2, "lap": 2, "lap_time": 102, "interval": -4},
            ]
        )
        lap_df = lap_df.with_columns(pl.lit(False).alias("incident"))

        penalties = pl.DataFrame()

        result = compute_results(lap_df, penalties)

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [1, 0, 2],
                    "laps_complete": [2, 2, 2],
                    "total_time": [200.0, 202.0, 204.0],
                    "penalty": [0.0, 0.0, 0.0],
                    "interval": ["0.000", "-2.000", "-4.000"],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 101.0, 102.0],
                    "fastest_lap_time": [100, 101, 102],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_same_laps_tiebreaker(self):
        """
        When two drivers DNF on the same lap, the one who completed that lap
        sooner (lower total_time) should get the better (lower) finish_position.
        """
        lap_df = pl.DataFrame(
            [
                # Driver 0: winner, completes all 3 laps
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 0, "lap": 3, "lap_time": 100, "interval": 0},
                # Driver 1: DNF after lap 2, total_time=195 (faster on those laps)
                {"user_id": 1, "lap": 1, "lap_time": 95, "interval": -5},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": -10},
                # Driver 2: DNF after lap 2, total_time=220 (slower on those laps)
                {"user_id": 2, "lap": 1, "lap_time": 110, "interval": -10},
                {"user_id": 2, "lap": 2, "lap_time": 110, "interval": -20},
            ]
        )
        lap_df = lap_df.with_columns(pl.lit(False).alias("incident"))
        penalties = pl.DataFrame()

        result = compute_results(lap_df, penalties)

        # Driver 1 (total_time=195) should beat Driver 2 (total_time=220) even though
        # Driver 2 had a higher interval magnitude at the same lap count
        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 1, 2],
                    "laps_complete": [3, 2, 2],
                    "total_time": [300.0, 195.0, 220.0],
                    "penalty": [0.0, 0.0, 0.0],
                    "interval": ["0.000", "-1L", "-1L"],
                    "finish_position": [0, 1, 2],
                    "average_lap_time": [100.0, 97.5, 110.0],
                    "fastest_lap_time": [100, 95, 110],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
        )

    def test_basic_penalty(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        lap_df = pl.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 0, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 1, "lap": 1, "lap_time": 110, "interval": -10},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": -10},
                {"user_id": 2, "lap": 1, "lap_time": 120, "interval": -20},
                {"user_id": 2, "lap": 2, "lap_time": 100, "interval": -20},
            ]
        )
        lap_df = lap_df.with_columns(pl.lit(False).alias("incident"))

        penalties = pl.DataFrame(
            [
                {"user_id": 1, "time": 15},
            ]
        )

        result = compute_results(lap_df, penalties)

        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

    def test_penalize_leader(self):
        lap_df = pl.DataFrame(
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
        lap_df = lap_df.with_columns(pl.lit(False).alias("incident"))
        penalties = pl.DataFrame(
            [
                {"user_id": 0, "time": 15},
            ]
        )

        result = compute_results(lap_df, penalties)

        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

    def test_penalize_across_final_lap(self):
        """Test what happens if we apply a penalty big enough to knock a driver down two laps!"""
        lap_df = pl.DataFrame(
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
        lap_df = lap_df.with_columns(pl.lit(False).alias("incident"))

        # 6s penalty, no change in lap/position
        penalties = pl.DataFrame(
            [
                {"user_id": 1, "time": 6},
            ]
        )
        result = compute_results(lap_df, penalties)

        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

        # 7s penalty, exactly on edge of being knocked down a lap
        penalties = pl.DataFrame(
            [
                {"user_id": 1, "time": 7},
            ]
        )
        result = compute_results(lap_df, penalties)

        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

        # Just over 7s to knock to next lap
        penalties = pl.DataFrame(
            [
                {"user_id": 1, "time": 7.2},
            ]
        )
        result = compute_results(lap_df, penalties)
        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

        # Just under 2 lap penalty
        penalties = pl.DataFrame(
            [
                {"user_id": 1, "time": 15},
            ]
        )
        result = compute_results(lap_df, penalties)
        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

        # Just over 2 lap penalty
        penalties = pl.DataFrame(
            [
                {"user_id": 1, "time": 15.2},
            ]
        )
        result = compute_results(lap_df, penalties)
        assert_frame_equal(
            result,
            pl.DataFrame(
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
            check_dtypes=False,
            check_row_order=False,
        )

    def test_driver_missing_from_qual(self):
        """
        Basic penalty test, all drivers on lead lap
        """
        lap_df = pl.DataFrame(
            [
                {"user_id": 0, "lap": 1, "lap_time": 101, "interval": -1},
                {"user_id": 1, "lap": 1, "lap_time": 100, "interval": 0},
                {"user_id": 1, "lap": 2, "lap_time": 100, "interval": 0},
                {"user_id": 2, "lap": 1, "lap_time": 102, "interval": -2},
                {"user_id": 2, "lap": 2, "lap_time": 102, "interval": -4},
            ]
        )
        qualy_df = pl.DataFrame(
            [
                {"user_id": 1, "finish_position": 1, "best_lap_time": 12.345, "laps_complete": 1},
                {"user_id": 2, "finish_position": 2, "best_lap_time": 32.345, "laps_complete": 2},
            ]
        )

        lap_df = lap_df.with_columns(pl.lit(False).alias("incident"))

        penalties = pl.DataFrame()

        result = compute_results(lap_df, penalties, qualy_df)

        assert_frame_equal(
            result,
            pl.DataFrame(
                {
                    "user_id": [0, 1, 2],
                    "laps_complete": [1, 2, 2],
                    "total_time": [101.0, 200.0, 204.0],
                    "penalty": [0.0, 0.0, 0.0],
                    "interval": ["-1L", "0.000", "-4.000"],
                    "finish_position": [2, 0, 1],
                    "average_lap_time": [101.0, 100.0, 102.0],
                    "start_position": [None, 1.0, 2.0],
                    "qualify_lap_time": [None, 12.345, 32.345],
                    "fastest_lap_time": [101, 100, 102],
                }
            ),
            check_dtypes=False,
            check_row_order=False,
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
