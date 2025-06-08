#!/usr/bin/env python3

from typing import Tuple

import polars as pl

from gpt_racing.config import PointsConfig


def _compute_drop_races(df: pl.DataFrame, num_drop_races: int) -> pl.DataFrame:
    out = (
        df.sort(["user_id", "points", "subsession_id"], descending=[False, False, False])
        .group_by("user_id")
        .agg(
            pl.col("subsession_id"),
            pl.arange(0, pl.len()).alias("drop_order"),
            pl.len().cast(pl.Int32).alias("num_contests"),
        )
        .explode("subsession_id", "drop_order")
    )

    out = df.join(
        out["user_id", "subsession_id", "drop_order", "num_contests"],
        on=["user_id", "subsession_id"],
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


def _add_fastest_lap(df, config):
    """Always add fastest lap, even if we don't score on it"""
    if "fastest_lap_time" not in df:
        return df

    if config is None:
        must_be_on_lead_lap = True
        points = 0
    else:
        must_be_on_lead_lap = config.must_be_on_lead_lap
        points = config.points

    df = df.with_columns((pl.col("fastest_lap_time") == pl.col("fastest_lap_time").min()).alias("fastest_lap"))

    lead_lap_expr = True
    if must_be_on_lead_lap:
        lead_lap_expr = pl.col("laps_complete") == pl.col("laps_complete").max()
    df = df.with_columns((pl.col("points") + (pl.col("fastest_lap") & lead_lap_expr) * points).alias("points"))

    return df


def _add_cleanest_driver(df, config):
    """Always cleanest driver, even if we don't score on it"""
    if "num_incidents" not in df:
        return df

    if config is None:
        must_be_on_lead_lap = True
        points = 0
    else:
        must_be_on_lead_lap = config.must_be_on_lead_lap
        points = config.points

    df = df.with_columns(
        (
            pl.col("num_incidents")
            == df.filter(pl.col("laps_complete") == pl.col("laps_complete").max())["num_incidents"].min()
        ).alias("cleanest_driver")
    )

    lead_lap_expr = True
    if must_be_on_lead_lap:
        lead_lap_expr = pl.col("laps_complete") == pl.col("laps_complete").max()
    df = df.with_columns((pl.col("points") + (pl.col("cleanest_driver") & lead_lap_expr) * points).alias("points"))
    return df


def compute_points_score(data: pl.DataFrame, config: PointsConfig, default_points_type="default") -> pl.DataFrame:
    """
    Compute points scoring from a results dataframe

    Parameters
    ----------
    data : Dataframe with the following columns:
        user_id
        finish_position
        subsession_id
    config : Points scoring config

    Returns
    -------
    """
    points_dict = config.points
    if not isinstance(points_dict, dict):
        points_dict = {"default": points_dict}

    points_df = pl.concat(
        [
            pl.DataFrame({"finish_position": range(len(v)), "points": v}).with_columns(pl.lit(k).alias("points_type"))
            for k, v in points_dict.items()
        ]
    )

    if "points_type" not in data:
        data = data.with_columns(pl.lit(default_points_type).alias("points_type"))
    else:
        data = data.with_columns(pl.col("points_type").fill_null(default_points_type))

    # Join points!
    out = data.join(points_df, on=["finish_position", "points_type"], how="left", coalesce=True)
    out = out.with_columns(pl.col("points").fill_null(0))

    # Compute fastest lap, cleanest driver
    out = _add_fastest_lap(out, config.fastest_lap)
    out = _add_cleanest_driver(out, config.cleanest_driver)

    # Ensure each user has an entry per contest. If they didn't participate, they get null points/finish position
    out = out.join(
        out.select("user_id").unique().join(out.select("subsession_id").unique(), how="cross"),
        on=("user_id", "subsession_id"),
        how="full",
        coalesce=True,
    )

    out = out.sort("user_id", "subsession_id")
    # Compute drop races
    out = _compute_drop_races(out, config.drop_races)

    out = (
        out.sort("subsession_id")
        # Compute cumulative points
        .with_columns(
            (pl.when(pl.col("drop")).then(0).otherwise(pl.col("points")))
            .cum_sum()
            .over("user_id")
            .alias("cumulative_points")
        )
        # Number of races
        .with_columns(
            (pl.when(pl.col("finish_position").is_not_null()).then(1).otherwise(0))
            .cum_sum()
            .over("user_id")
            .alias("num_races")
        )
        # Compute rank
        .with_columns(
            pl.when(pl.col("num_races") > 0)
            .then(pl.col("cumulative_points").rank("min", descending=True).over("subsession_id"))
            .otherwise(None)  # Fill non-matching rows with None
            .cast(pl.Int32)
            .alias("rank")
        )
        # Compute rank change
        .with_columns((pl.col("rank") - pl.col("rank").shift(1).over("user_id")).alias("rank_change"))
    )

    return out
