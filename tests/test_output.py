#!/usr/bin/env python3

import polars as pl
import pytest

from gpt_racing import output
from gpt_racing.config import RenderConfig

# ---------------------------------------------------------------------------
# Minimal DataFrames sufficient to exercise the dispatch logic.
# render_standings / render_race_results are already unit-tested in
# test_render_tables.py; here we only care that the right tables are
# produced and labelled correctly.
# ---------------------------------------------------------------------------

_RACE_RESULT_BASE = {
    "finish_position": [1, 2, 3],
    "class_position": [1, 1, 2],
    "qualify_lap_time": [60.0, 61.0, 62.0],
    "start_position": [1, 2, 3],
    "average_lap_time": [60.0, 61.0, 62.0],
    "fastest_lap_time": [60.0, 61.0, 62.0],
    "fastest_lap": [True, False, False],
    "penalty": [0.0, 0.0, 0.0],
    "num_incidents": [0, 1, 2],
    "cleanest_driver": [True, False, False],
    "rank": [1, 2, 3],
    "rating": [1800, 1600, 1400],
    "rating_change": [100, 0, -100],
    "display_name": ["a", "b", "c"],
    "interval": ["0.000", "1.000", "2.000"],
    "points": [10, 5, 3],
    "laps_complete": [10, 10, 10],
    "total_time": [600.0, 610.0, 620.0],
    "rank_change": [None, None, None],
}

_STANDINGS_BASE = {
    "user_id": [0, 1, 2],
    "points_race_1": [10, 5, 3],
    "drop_race_1": [False, False, False],
    "fastest_lap_race_1": [True, False, False],
    "cleanest_driver_race_1": [True, False, False],
    "points_total": [10, 5, 3],
    "points_rank": [1, 2, 3],
    "points_rank_change": [None, None, None],
    "rating": [1800, 1600, 1400],
    "rating_rank": [1, 2, 3],
    "rating_rank_change": [None, None, None],
    "num_races": [1, 1, 1],
    "display_name": ["a", "b", "c"],
    "class_rank": [1, 2, 3],
    "class_rank_change": [None, None, None],
}


def _race_result_df(class_names):
    return pl.DataFrame(
        {
            **_RACE_RESULT_BASE,
            "class_name": class_names,
            "class_symbol": class_names,
            "class_color": ["#ff0000" if c == "Pro" else "#0000ff" if c == "Am" else "#ffffff" for c in class_names],
        }
    )


def _standings_df(class_names):
    return pl.DataFrame(
        {
            **_STANDINGS_BASE,
            "class_name": class_names,
            "class_symbol": class_names,
            "class_color": ["#ff0000" if c == "Pro" else "#0000ff" if c == "Am" else "#ffffff" for c in class_names],
        }
    )


# ---------------------------------------------------------------------------
# race_result_htmls
# ---------------------------------------------------------------------------


class TestRaceResultHtmls:
    def test_combined_only(self):
        cfg = RenderConfig(combined_table=True, per_class_tables=False)
        df = _race_result_df(["Pro", "Am", "Am"])
        result = output.race_result_htmls(df, cfg)
        assert len(result) == 1
        label, html = result[0]
        assert label == ""
        assert isinstance(html, str)

    def test_per_class_only(self):
        cfg = RenderConfig(combined_table=False, per_class_tables=True)
        df = _race_result_df(["Pro", "Am", "Am"])
        result = output.race_result_htmls(df, cfg)
        assert len(result) == 2
        labels = [label for label, _ in result]
        assert "Am" in labels
        assert "Pro" in labels

    def test_both(self):
        cfg = RenderConfig(combined_table=True, per_class_tables=True)
        df = _race_result_df(["Pro", "Am", "Am"])
        result = output.race_result_htmls(df, cfg)
        assert len(result) == 3
        labels = [label for label, _ in result]
        assert "" in labels
        assert "Am" in labels
        assert "Pro" in labels

    def test_per_class_suppressed_when_single_class(self):
        """per_class_tables=True is a no-op when all drivers share one class."""
        cfg = RenderConfig(combined_table=True, per_class_tables=True)
        df = _race_result_df(["Overall", "Overall", "Overall"])
        result = output.race_result_htmls(df, cfg)
        assert len(result) == 1
        assert result[0][0] == ""

    def test_nothing_when_both_false(self):
        cfg = RenderConfig(combined_table=False, per_class_tables=False)
        df = _race_result_df(["Pro", "Am", "Am"])
        assert output.race_result_htmls(df, cfg) == []

    def test_per_class_html_contains_only_that_class(self):
        """The HTML for a per-class table must not include drivers from other classes."""
        cfg = RenderConfig(combined_table=False, per_class_tables=True)
        df = _race_result_df(["Pro", "Am", "Am"]).with_columns(
            pl.Series("display_name", ["ProDriver", "AmDriver1", "AmDriver2"])
        )
        result = output.race_result_htmls(df, cfg)
        by_label = {label: html for label, html in result}
        assert "ProDriver" in by_label["Pro"]
        assert "AmDriver1" not in by_label["Pro"]
        assert "AmDriver1" in by_label["Am"]
        assert "ProDriver" not in by_label["Am"]


# ---------------------------------------------------------------------------
# standings_htmls
# ---------------------------------------------------------------------------


class TestStandingsHtmls:
    def test_combined_only(self):
        cfg = RenderConfig(combined_table=True, per_class_tables=False)
        df = _standings_df(["Pro", "Am", "Am"])
        result = output.standings_htmls(df, cfg)
        assert len(result) == 1
        assert result[0][0] == ""

    def test_per_class_only(self):
        cfg = RenderConfig(combined_table=False, per_class_tables=True)
        df = _standings_df(["Pro", "Am", "Am"])
        result = output.standings_htmls(df, cfg)
        assert len(result) == 2
        labels = {label for label, _ in result}
        assert labels == {"Pro", "Am"}

    def test_both(self):
        cfg = RenderConfig(combined_table=True, per_class_tables=True)
        df = _standings_df(["Pro", "Am", "Am"])
        result = output.standings_htmls(df, cfg)
        assert len(result) == 3

    def test_per_class_suppressed_when_single_class(self):
        cfg = RenderConfig(combined_table=True, per_class_tables=True)
        df = _standings_df(["Overall", "Overall", "Overall"])
        result = output.standings_htmls(df, cfg)
        assert len(result) == 1

    def test_nothing_when_both_false(self):
        cfg = RenderConfig(combined_table=False, per_class_tables=False)
        df = _standings_df(["Pro", "Am", "Am"])
        assert output.standings_htmls(df, cfg) == []


# ---------------------------------------------------------------------------
# ratings_htmls
# ---------------------------------------------------------------------------

import datetime


def _ratings_df():
    return pl.DataFrame(
        {
            "user_id": [0, 1, 2],
            "rating": [1800, 1600, 1400],
            "num_contests": [3, 3, 3],
            "participated": [True, True, True],
            "subsession_id": [2, 2, 2],
            "contest_date": [datetime.datetime(1970, 1, 1)] * 3,
            "num_valid_contests": [3, 3, 3],
            "rank": [1, 2, 3],
            "rank_change": [None, None, None],
            "rating_change": [100, 0, -100],
            "display_name": ["a", "b", "c"],
        }
    )


class TestRatingsHtmls:
    def test_combined_produces_one_table(self):
        cfg = RenderConfig(combined_table=True, per_class_tables=False)
        result = output.ratings_htmls(_ratings_df(), cfg)
        assert len(result) == 1
        assert result[0][0] == ""
        assert isinstance(result[0][1], str)

    def test_per_class_is_noop(self):
        """per_class_tables has no effect on ratings (no class_name column)."""
        cfg = RenderConfig(combined_table=True, per_class_tables=True)
        result = output.ratings_htmls(_ratings_df(), cfg)
        assert len(result) == 1

    def test_nothing_when_combined_false(self):
        cfg = RenderConfig(combined_table=False, per_class_tables=True)
        assert output.ratings_htmls(_ratings_df(), cfg) == []
