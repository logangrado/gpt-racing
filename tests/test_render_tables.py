#!/usr/bin/env python3

import shutil
import subprocess
import tempfile
import webbrowser
from pathlib import Path

import polars as pl
import pytest
from gpt_racing import render_tables

REF_BASE_PATH = Path(__file__).parent / "data" / "ref"


def _compare_files(actual, expected):
    # just compare literally

    with open(actual, "r") as f:
        actual_contents = f.readlines()

    with open(expected, "r") as f:
        expected_contents = f.readlines()

    assert actual_contents == expected_contents


@pytest.fixture
def compare_html(request, test_id):
    def _compare_html(html):
        ref_path = REF_BASE_PATH / f"{test_id}.html"

        if request.config.getoption("show"):
            with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False) as f:
                f.write(html)
                path = Path(f.name)

                webbrowser.open(path.as_uri())

        if request.config.getoption("update_refs"):
            ref_path.parent.mkdir(exist_ok=True, parents=True)
            with open(ref_path, "w") as f:
                f.write(html)

        with open(ref_path, "r") as f:
            ref_contents = f.read()

        assert ref_contents == html

    return _compare_html


class TestRaceResults:
    def test_basic(self, compare_html):
        df = pl.DataFrame(
            {
                "finish_position": [1, 2, 3],
                "class_position": [1, 1, 2],
                "qualify_lap_time": [60.1234, 55.1234, 65.1234],
                "start_position": [2, 1, 3],
                "average_lap_time": [123.123, 124.123, 124.123],
                "fastest_lap_time": [100.123, 101.123, 102.123],
                "fastest_lap": [True, False, False],
                "penalty": [0.0, 0.0, 0.0],
                "num_incidents": [5, 2, 6],
                "cleanest_driver": [False, True, False],
                "rank": [1, 2, 3],
                "rating": [60, 50, 40],
                "rating_change": [10, 0, -10],
                "display_name": ["a", "b", "c"],
                "class_name": ["Pro", "Am", "Am"],
                "class_symbol": ["P", "A", "A"],
                "class_color": ["#ff0000", "#0000ff", "#0000ff"],
                "interval": ["0.000", "10.111", "1:10.888"],
                "points": [50, 40, 30],
            }
        )

        result = render_tables.render_race_results(df)

        compare_html(result)

    def test_penalty(self, compare_html):
        df = pl.DataFrame(
            {
                "finish_position": [1, 2, 3],
                "class_position": [1, 1, 2],
                "qualify_lap_time": [60.1234, 55.1234, 65.1234],
                "start_position": [2, 1, 3],
                "average_lap_time": [123.123, 124.123, 124.123],
                "fastest_lap_time": [100.123, 101.123, 102.123],
                "fastest_lap": [True, False, False],
                "penalty": [0.0, 5, 0.0],
                "num_incidents": [5, 2, 6],
                "cleanest_driver": [False, True, False],
                "rank": [1, 2, 3],
                "rating": [60, 50, 40],
                "rating_change": [10, 0, -10],
                "display_name": ["a", "b", "c"],
                "class_name": ["Pro", "Am", "Am"],
                "class_symbol": ["P", "A", "A"],
                "class_color": ["#ff0000", "#0000ff", "#0000ff"],
                "interval": ["0.000", "10.111", "1:10.888"],
                "points": [50, 40, 30],
            }
        )

        result = render_tables.render_race_results(df)

        compare_html(result)

    def test_no_classes(self, compare_html):
        """When config.classes is None, all drivers have class_symbol='Overall' and class_color='#ffffff'."""
        df = pl.DataFrame(
            {
                "finish_position": [1, 2, 3],
                "class_position": [1, 2, 3],
                "qualify_lap_time": [60.1234, 55.1234, 65.1234],
                "start_position": [2, 1, 3],
                "average_lap_time": [123.123, 124.123, 124.123],
                "fastest_lap_time": [100.123, 101.123, 102.123],
                "fastest_lap": [True, False, False],
                "penalty": [0.0, 0.0, 0.0],
                "num_incidents": [5, 2, 6],
                "cleanest_driver": [False, True, False],
                "rank": [1, 2, 3],
                "rating": [60, 50, 40],
                "rating_change": [10, 0, -10],
                "display_name": ["a", "b", "c"],
                "class_name": ["Overall", "Overall", "Overall"],
                "class_symbol": ["Overall", "Overall", "Overall"],
                "class_color": ["#ffffff", "#ffffff", "#ffffff"],
                "interval": ["0.000", "10.111", "1:10.888"],
                "points": [50, 40, 30],
            }
        )

        result = render_tables.render_race_results(df)

        compare_html(result)


class TestStandings:
    def test_basic(self):
        pass


class TestRatings:
    def test_basic(self):
        pass
