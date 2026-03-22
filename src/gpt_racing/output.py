#!/usr/bin/env python3
"""
Dispatch layer between computed DataFrames and rendered HTML.

Each function returns a list of (label, html) pairs, where `label` is:
  - ""            for a combined (all-class) table
  - "<ClassName>" for a per-class table (e.g. "Pro", "Am")

The label is used by the caller to construct output filenames.
"""

import polars as pl

from gpt_racing import render_tables
from gpt_racing.config import RenderConfig


def race_result_htmls(df: pl.DataFrame, render_config: RenderConfig) -> list[tuple[str, str]]:
    out = []
    if render_config.combined_table:
        out.append(("", render_tables.render_race_results(df)))
    if render_config.per_class_tables and df["class_name"].n_unique() > 1:
        for cls in df["class_name"].unique().sort():
            out.append((cls, render_tables.render_race_results(df.filter(pl.col("class_name") == cls))))
    return out


def ratings_htmls(df: pl.DataFrame, render_config: RenderConfig) -> list[tuple[str, str]]:
    # ELO DataFrame does not currently carry class_name, so per_class_tables
    # is always a no-op here. The combined table is produced unconditionally
    # (matching the previous behaviour) when combined_table is True.
    out = []
    if render_config.combined_table:
        out.append(("", render_tables.render_ratings(df)))
    return out


def standings_htmls(df: pl.DataFrame, render_config: RenderConfig) -> list[tuple[str, str]]:
    out = []
    if render_config.combined_table:
        out.append(("", render_tables.render_standings(df)))
    if render_config.per_class_tables and df["class_name"].n_unique() > 1:
        for cls in df["class_name"].unique().sort():
            out.append((cls, render_tables.render_standings(df.filter(pl.col("class_name") == cls))))
    return out
