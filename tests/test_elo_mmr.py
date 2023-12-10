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

        result = elo_mmr.calculate(data)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame({"rank": [1, 2, 3], "user_id": [0, 1, 2], "rating": [1432, 1227, 1022]}),
        )

    def test_multi_contest(self):
        data = pd.DataFrame(
            [
                {"user_id": 0, "finish_position": 0, "contest_id": 0, "contest_time": 0},
                {"user_id": 1, "finish_position": 1, "contest_id": 0, "contest_time": 0},
                {"user_id": 2, "finish_position": 2, "contest_id": 0, "contest_time": 0},
                {"user_id": 0, "finish_position": 0, "contest_id": 1, "contest_time": 1},
                {"user_id": 1, "finish_position": 1, "contest_id": 1, "contest_time": 1},
                {"user_id": 3, "finish_position": 2, "contest_id": 1, "contest_time": 1},
            ]
        )

        result = elo_mmr.calculate(data)

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame({"rank": [1, 2, 3, 4], "user_id": [0, 1, 3, 2], "rating": [1592, 1362, 1064, 1022]}),
        )
