#!/usr/bin/env python3

import datetime

import polars as pl

from gpt_racing.elo_mmr import ELOMMR
from gpt_racing._testing import assert_frame_equal


class TestELOMMR:
    def test_single_contest(self):
        data = pl.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
                {"user_id": 1, "finish_position": 1},
                {"user_id": 2, "finish_position": 2},
            ]
        ).with_columns(pl.lit(0).alias("contest_id"), pl.lit(0).alias("contest_date").cast(pl.Datetime))

        elo = ELOMMR()
        elo.update(data)
        result = elo.collect_results()

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2],
                "rating": [1715, 1500, 1285],
                "num_contests": [1, 1, 1],
                "participated": [True, True, True],
                "contest_id": [0, 0, 0],
                "contest_date": [
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                ],
                "num_valid_contests": [1, 1, 1],
                "rank": [1, 2, 3],
                "rank_change": [None, None, None],
                "rating_change": [215, 0, -215],
            }
        )

        assert_frame_equal(result, expected, check_dtypes=False)

    def test_single_contest_tie(self):
        data = pl.DataFrame(
            [
                {"user_id": 0, "finish_position": 0},
                {"user_id": 1, "finish_position": 1},
                {"user_id": 2, "finish_position": 2},
                {"user_id": 3, "finish_position": 2},
                {"user_id": 4, "finish_position": 4},
            ]
        ).with_columns(pl.lit(0).alias("contest_id"), pl.lit(0).alias("contest_date").cast(pl.Datetime))

        elo = ELOMMR()
        elo.update(data)
        result = elo.collect_results()

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2, 3, 4],
                "rating": [1815, 1636, 1444, 1444, 1185],
                "num_contests": [1, 1, 1, 1, 1],
                "participated": [True, True, True, True, True],
                "contest_id": [0, 0, 0, 0, 0],
                "contest_date": [
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                ],
                "num_valid_contests": [1, 1, 1, 1, 1],
                "rank": [1, 2, 3, 3, 5],
                "rank_change": [None, None, None, None, None],
                "rating_change": [315, 136, -56, -56, -315],
            }
        )
        assert_frame_equal(result, expected, check_dtypes=False)

    def test_multi_contest(self):
        data = pl.DataFrame(
            [
                {"user_id": 0, "finish_position": 0, "contest_id": 0, "contest_date": 0},
                {"user_id": 1, "finish_position": 1, "contest_id": 0, "contest_date": 0},
                {"user_id": 2, "finish_position": 2, "contest_id": 0, "contest_date": 0},
                {"user_id": 0, "finish_position": 2, "contest_id": 1, "contest_date": 1},
                {"user_id": 1, "finish_position": 0, "contest_id": 1, "contest_date": 1},
                {"user_id": 3, "finish_position": 1, "contest_id": 1, "contest_date": 1},
            ]
        ).with_columns(pl.col("contest_date").cast(pl.Datetime))

        elo = ELOMMR()
        elo.update(data)
        result = elo.collect_results()

        expected = pl.DataFrame(
            {
                "user_id": [0, 1, 2, 1, 3, 0, 2],
                "rating": [1715, 1500, 1285, 1664, 1519, 1454, 1285],
                "num_contests": [1, 1, 1, 2, 1, 2, 1],
                "participated": [True, True, True, True, True, True, False],
                "contest_id": [0, 0, 0, 1, 1, 1, 1],
                "contest_date": [
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0),
                    datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                    datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                    datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                    datetime.datetime(1970, 1, 1, 0, 0, 0, 1),
                ],
                "num_valid_contests": [1, 1, 1, 2, 1, 2, 2],
                "rank": [1, 2, 3, 1, 2, 3, 4],
                "rank_change": [None, None, None, -1, None, 2, 1],
                "rating_change": [215, 0, -215, 164, 19, -261, None],
            }
        )
        assert_frame_equal(result, expected, check_dtypes=False)
