#!/usr/bin/env python3

import pandas as pd

from gpt_racing import elo_mmr


class TestELOMMR:
    def test_single_contest(self):
        data = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
                {"user_id": 1, "finish_position": 1},
                {"user_id": 2, "finish_position": 2},
            ]
        )
        data["contest_id"] = 0
        data["contest_time"] = 0

        result = elo_mmr.compute_elo_mmr(data)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2],
                    "rating": [1715, 1500, 1285],
                    "rating_change": [215, 0, -215],
                    "num_contests": [1, 1, 1],
                    "participated": [True, True, True],
                    "rank": [1, 2, 3],
                    "contest_id": [0, 0, 0],
                    "rank_change": [pd.NA, pd.NA, pd.NA],
                }
            ),
            check_dtype=False,
        )

    def test_single_contest_tie(self):
        data = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
                {"user_id": 1, "finish_position": 1},
                {"user_id": 2, "finish_position": 2},
                {"user_id": 3, "finish_position": 2},
                {"user_id": 4, "finish_position": 4},
            ]
        )
        data["contest_id"] = 0
        data["contest_time"] = 0

        result = elo_mmr.compute_elo_mmr(data)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2, 3, 4],
                    "rating": [1815, 1636, 1444, 1444, 1185],
                    "rating_change": [315, 136, -56, -56, -315],
                    "num_contests": [1, 1, 1, 1, 1],
                    "participated": [True, True, True, True, True],
                    "rank": [1, 2, 3, 3, 5],
                    "contest_id": [0, 0, 0, 0, 0],
                    "rank_change": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
                }
            ),
            check_dtype=False,
        )

    def test_multi_contest(self):
        data = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0, "contest_id": 0, "contest_time": 0},
                {"user_id": 1, "finish_position": 1, "contest_id": 0, "contest_time": 0},
                {"user_id": 2, "finish_position": 2, "contest_id": 0, "contest_time": 0},
                {"user_id": 0, "finish_position": 2, "contest_id": 1, "contest_time": 1},
                {"user_id": 1, "finish_position": 0, "contest_id": 1, "contest_time": 1},
                {"user_id": 3, "finish_position": 1, "contest_id": 1, "contest_time": 1},
            ]
        )

        result = elo_mmr.compute_elo_mmr(data)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "user_id": [0, 1, 2, 1, 3, 0, 2],
                    "rating": [1715, 1500, 1285, 1664, 1519, 1454, 1285],
                    "rating_change": [215, 0, -215, 164, 19, -261, pd.NA],
                    "num_contests": [1, 1, 1, 2, 1, 2, 1],
                    "participated": [True, True, True, True, True, True, False],
                    "rank": [1, 2, 3, 1, 2, 3, 4],
                    "contest_id": [0, 0, 0, 1, 1, 1, 1],
                    "rank_change": [pd.NA, pd.NA, pd.NA, -1, pd.NA, 2, 1],
                }
            ),
            check_dtype=False,
        )

    def test_contest_order_invariance(self):
        contests = [
            pd.DataFrame(
                [
                    {"user_id": 0, "finish_position": 0, "contest_id": 0, "contest_time": 0},
                    {"user_id": 1, "finish_position": 1, "contest_id": 0, "contest_time": 0},
                    {"user_id": 2, "finish_position": 2, "contest_id": 0, "contest_time": 0},
                ]
            ),
            pd.DataFrame(
                [
                    {"user_id": 0, "finish_position": 0, "contest_id": 1, "contest_time": 2},
                    {"user_id": 1, "finish_position": 1, "contest_id": 1, "contest_time": 2},
                    {"user_id": 3, "finish_position": 2, "contest_id": 1, "contest_time": 2},
                ]
            ),
            pd.DataFrame(
                [
                    {"user_id": 1, "finish_position": 0, "contest_id": 2, "contest_time": 1},
                    {"user_id": 2, "finish_position": 1, "contest_id": 2, "contest_time": 1},
                    {"user_id": 3, "finish_position": 2, "contest_id": 2, "contest_time": 1},
                ]
            ),
        ]

        result0 = elo_mmr.compute_elo_mmr(pd.concat(contests))

        result1 = elo_mmr.compute_elo_mmr(pd.concat(contests[::-1]))

        pd.testing.assert_frame_equal(result0, result1)
