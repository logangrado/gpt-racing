#!/usr/bin/env python3

import datetime

import polars as pl
from polars.testing import assert_frame_equal

from gpt_racing import _testing, core
from gpt_racing._testing import assert_object_equal
from gpt_racing.config import RatingConfig


def _df_to_schema_str(df):
    out = "{" + ",".join([f'"{k}":pl.{v}' for k, v in df.schema.items()]) + "}"
    return out


def _print_result(result):
    """Print a full result in a friendly way that we can just copy/paste back in"""

    print("")
    print("ACTUAL")
    print("{")
    for key, value in result.items():
        print(f'"{key}":[')
        for item in value:
            print(f"pl.DataFrame({item.to_dict(as_series=False)}, schema={_df_to_schema_str(item)}),")
        print("],")

    print("}")


def _assert_result_equal(actual, expected):
    assert type(actual) == type(expected)
    if isinstance(actual, dict):
        assert set(actual.keys()) == set(expected.keys())
        for key in actual.keys():
            _assert_result_equal(actual[key], expected[key])

    elif isinstance(actual, list):
        assert len(actual) == len(expected)
        for a, b in zip(actual, expected):
            _assert_result_equal(a, b)

    elif isinstance(actual, pl.DataFrame):
        assert_frame_equal(actual, expected)

    else:
        raise NotImplementedError(f"Comparison not implemented for type: {type(actual)}")


class TestIntegration:
    def test_single_race(self, fake_client):
        config = RatingConfig.model_validate(
            {
                "races": [{"subsession_id": 0}],
                "elo": {},
                "points": {
                    "points": [3, 2, 1],
                },
            },
        )
        summary_data = [
            {
                "subsession_id": 0,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "laps_complete": 3},
                    {"cust_id": 3, "display_name": "d", "best_lap_time": 43, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            }
        ]

        _testing.generate_data(summary_data, fake_client)

        result = core.compute_ratings(config, client=fake_client)

        _print_result(result)

        expected = {
            "ELO": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "rating": [1769, 1579, 1421, 1231],
                        "num_contests": [1, 1, 1, 1],
                        "participated": [True, True, True, True],
                        "subsession_id": [0, 0, 0, 0],
                        "contest_date": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "num_valid_contests": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "rating_change": [269, 79, -79, -269],
                        "display_name": ["a", "b", "c", "d"],
                    }
                ),
            ],
            "points": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "finish_position": [0, 1, 2, 3],
                        "subsession_id": [0, 0, 0, 0],
                        "session_end_time": [
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                            datetime.datetime(1970, 1, 1, 0, 0),
                        ],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "laps_complete": [3, 3, 3, 0],
                        "points_type": ["default", "default", "default", "default"],
                        "points": [3, 2, 1, 0],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                        "cumulative_points": [3, 2, 1, 0],
                        "num_races": [1, 1, 1, 1],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "drop": [False, False, False, False],
                    }
                ),
            ],
            "race_results": [
                pl.DataFrame(
                    {
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "start_position": [1, 4, 2, 3],
                        "qualify_lap_time": [0.004, 0.0044, 0.004200000000000001, 0.0043],
                        "finish_position": [1, 2, 3, 4],
                        "class_position": [1, 2, 3, 4],
                        "interval": ["0.000", "-3.000", "-6.000", "-3L"],
                        "points": [3, 2, 1, 0],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_change": [269, 79, -79, -269],
                        "rank": [1, 2, 3, 4],
                        "rank_change": [None, None, None, None],
                        "laps_complete": [3, 3, 3, 0],
                        "total_time": [30.0, 33.0, 36.0, None],
                        "penalty": [0.0, 0.0, 0.0, 0.0],
                        "average_lap_time": [10.0, 11.0, 12.0, None],
                        "fastest_lap_time": [10.0, 11.0, 12.0, None],
                        "num_incidents": [0, 0, 0, None],
                        "fastest_lap": [True, False, False, None],
                        "cleanest_driver": [True, True, True, None],
                    }
                ),
            ],
            "standings": [
                pl.DataFrame(
                    {
                        "user_id": [0, 1, 2, 3],
                        "points_race_1": [3, 2, 1, 0],
                        "drop_race_1": [False, False, False, False],
                        "fastest_lap_race_1": [True, False, False, None],
                        "cleanest_driver_race_1": [True, True, True, None],
                        "points_total": [3, 2, 1, 0],
                        "points_rank": [1, 2, 3, 4],
                        "points_rank_change": [None, None, None, None],
                        "rating": [1769, 1579, 1421, 1231],
                        "rating_rank": [1, 2, 3, 4],
                        "rating_rank_change": [None, None, None, None],
                        "num_races": [1, 1, 1, 1],
                        "display_name": ["a", "b", "c", "d"],
                        "class_name": [None, None, None, None],
                        "class_symbol": [None, None, None, None],
                        "class_color": [None, None, None, None],
                        "class_rank": [1, 2, 3, 4],
                        "class_rank_change": [None, None, None, None],
                    }
                ),
            ],
        }

        assert_object_equal(result, expected, frame_kwargs={"check_dtypes": False})
        # _assert_result_equal(result, expected)

    def test_multi_race_with_classes(self, fake_client):
        """End-to-end: two races with Pro/Am classes.

        Verifies that class_position and class_rank reflect within-class rank,
        not overall rank, and that class_rank_change tracks movement across races.

        Race 1: a (Pro) 1st, b (Am) 2nd, c (Am) 3rd
        Race 2: a (Pro) 1st, c (Am) 2nd, b (Am) 3rd  ← b/c swap
        """
        config = RatingConfig.model_validate(
            {
                "races": [{"subsession_id": 0}, {"subsession_id": 1}],
                "elo": {},
                "points": {"points": [3, 2, 1]},
                "classes": [
                    {"name": "Pro", "symbol": "P", "color": "#ff0000", "drivers": [{"name": "a"}]},
                    {"name": "Am", "symbol": "A", "color": "#0000ff", "default": True},
                ],
            }
        )

        summary_data = [
            {
                "subsession_id": 0,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 42, "laps_complete": 3},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 44, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
            {
                "subsession_id": 1,
                "qualifying": [
                    {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                    {"cust_id": 1, "display_name": "b", "best_lap_time": 42, "laps_complete": 3},
                    {"cust_id": 2, "display_name": "c", "best_lap_time": 44, "laps_complete": 3},
                ],
                "race": [
                    {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                    {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 11},
                    {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 12},
                ],
            },
        ]

        _testing.generate_data(summary_data, fake_client)
        result = core.compute_ratings(config, client=fake_client)

        # ── Race 1 results ───────────────────────────────────────────────────
        rr1 = {row["display_name"]: row for row in result["race_results"][0].to_dicts()}
        assert rr1["a"]["class_name"] == "Pro"
        assert rr1["b"]["class_name"] == "Am"
        assert rr1["c"]["class_name"] == "Am"

        # a finishes 1st overall and 1st in Pro; b is 2nd overall but 1st Am
        assert rr1["a"]["finish_position"] == 1
        assert rr1["a"]["class_position"] == 1
        assert rr1["b"]["finish_position"] == 2
        assert rr1["b"]["class_position"] == 1
        assert rr1["c"]["finish_position"] == 3
        assert rr1["c"]["class_position"] == 2

        # ── Race 1 standings ─────────────────────────────────────────────────
        st1 = {row["display_name"]: row for row in result["standings"][0].to_dicts()}
        assert st1["a"]["class_rank"] == 1  # only Pro
        assert st1["b"]["class_rank"] == 1  # top Am
        assert st1["c"]["class_rank"] == 2  # second Am
        assert all(row["class_rank_change"] is None for row in st1.values())

        # ── Race 2 results ───────────────────────────────────────────────────
        rr2 = {row["display_name"]: row for row in result["race_results"][1].to_dicts()}
        # b and c swapped: c now finishes 2nd overall (1st Am), b 3rd (2nd Am)
        assert rr2["c"]["finish_position"] == 2
        assert rr2["c"]["class_position"] == 1
        assert rr2["b"]["finish_position"] == 3
        assert rr2["b"]["class_position"] == 2

        # ── Race 2 standings ─────────────────────────────────────────────────
        # After 2 races: b=2+1=3 pts, c=1+2=3 pts → tie at Am rank 1
        st2 = {row["display_name"]: row for row in result["standings"][1].to_dicts()}
        assert st2["b"]["class_rank"] == 1
        assert st2["c"]["class_rank"] == 1
        # b was rank 1, still 1 → no change; c was rank 2, now 1 → improved by 1
        assert st2["b"]["class_rank_change"] == 0
        assert st2["c"]["class_rank_change"] == -1
