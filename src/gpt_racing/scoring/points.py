#!/usr/bin/env python3

import pandas as pd
import polars as pl

from gpt_racing.config import PointsScoringConfig


def _compute_drop_races(df: pl.DataFrame, num_drop_races: int) -> pl.DataFrame:
    out = (
        df.sort(["user_id", "points", "contest_time"], descending=[False, False, False])
        .group_by("user_id")
        .agg(
            pl.col("contest_id"),
            pl.arange(0, pl.len()).alias("drop_order"),
            pl.len().cast(pl.Int32).alias("num_contests"),
        )
        .explode("contest_id", "drop_order")
    )

    out = df.join(
        out["user_id", "contest_id", "drop_order", "num_contests"],
        on=["user_id", "contest_id"],
        how="inner",
        coalesce=True,
    )

    # n_drop_races = min((n_contests - num_drop_races), num_drop_races)
    # Drop a race when drop_order < n_drop_races

    # out = out.with_columns((pl.min(pl.col("num_contests") - num_drop_races), num_drop_races).alias("num_drop_races"))
    out = out.with_columns(
        (pl.col("drop_order") < (pl.min_horizontal(pl.col("num_contests") - num_drop_races, num_drop_races))).alias(
            "drop"
        )
    )

    out = out.drop("drop_order", "num_contests")

    return out


def compute_points_score(data: pl.DataFrame, config: PointsScoringConfig) -> pl.DataFrame:
    """
    Compute points scoring from a results dataframe

    Parameters
    ----------
    data : Dataframe with the following columns:
        user_id
        finish_position
        contest_id
        contest_time
    config : Points scoring config

    Returns
    -------
    """
    # Create the points df
    points_df = pl.DataFrame({"finish_position": range(len(config.points)), "points": config.points})

    # Join points!
    out = data.join(points_df, on="finish_position", how="full", coalesce=True)
    out = out.with_columns(pl.col("points").fill_null(0))

    # Compute drop races
    out = _compute_drop_races(out, config.drop_races)

    return out
