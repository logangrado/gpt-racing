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
                "qualify_lap_time": [60.1234, 55.1234, 65.1234],
                "start_position": [2, 1, 3],
                "average_lap_time": [123.123, 124.123, 124.123],
                "fastest_lap_time": [100.123, 101.123, 102.123],
                "fastest_lap": [True, False, False],
                "num_incidents": [5, 2, 6],
                "cleanest_driver": [False, True, False],
                "rank": [1, 2, 3],
                "rating": [60, 50, 40],
                "rating_change": [10, 0, -10],
                "display_name": ["a", "b", "c"],
                "interval": [0.000, 10.1234, 70.1234],
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
