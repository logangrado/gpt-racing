#!/usr/bin/env python3

import polars as pl

from gpt_racing import utils


def _compute_interval(result_df):
    lead_lap = result_df["laps_complete"].max()

    result_df = result_df.with_columns((pl.col("laps_complete") - lead_lap).alias("laps_down"))

    # Subtract min interval, in case we penalized the leader
    # INTERVALS ARE NEGATIVE! Therefore, the "lowest" interval is the max
    result_df = result_df.with_columns((pl.col("interval") - pl.col("interval").max()).alias("interval"))

    result_df = result_df.with_columns(
        pl.struct(["interval", "laps_down"])
        .map_elements(
            lambda x: utils.seconds_to_str(x["interval"]) if x["laps_down"] == 0 else f"{int(x['laps_down'])}L",
            return_dtype=pl.String,
        )
        .alias("interval")
    )
    result_df = result_df.with_columns(pl.col("interval").fill_null("-"))

    result_df = result_df.drop("laps_down")

    return result_df


def infer_invalid_laps(lap_df):
    """
    Infer lap times for any lap marked as invalid.
    """
    original_df = lap_df
    # PREPARE LAP DATAFRAME
    lap_df = lap_df.select(["user_id", "lap", "lap_time", "interval"]).clone()
    lap_df = lap_df.with_columns(pl.col("interval").fill_null(0))

    lap_df = lap_df.with_columns(
        pl.col("interval").shift(1).over("user_id", order_by="lap").fill_null(0).alias("interval_previous")
    )

    valid_laps = lap_df.filter(pl.col("lap_time") > 0)

    # Get the single valid lap per lap_number with the lowest interval
    zero_int_lap_times = valid_laps.sort("interval", descending=True).group_by("lap").first()

    # Merge zero int lap times onto lap dataframe, by lap. Suffix zero int lap data with `0`
    lap_df = lap_df.join(
        zero_int_lap_times.select(["lap", "lap_time", "interval", "interval_previous"]).rename(
            {
                "lap_time": "lap_time0",
                "interval": "interval0",
                "interval_previous": "interval_previous0",
            }
        ),
        on="lap",
        how="left",
    )

    # Compute this_lap_interval
    lap_df = lap_df.with_columns((pl.col("interval") - pl.col("interval_previous")).alias("interval_change"))

    # Infer all lap times
    lap_df = lap_df.with_columns(
        (
            pl.col("lap_time0")
            - pl.col("interval_change").fill_null(0)
            + pl.col("interval0")
            - pl.col("interval_previous0")
        ).alias("lap_time_inferred")
    )

    # Fill -1 lap times with inferred
    lap_df = lap_df.with_columns(
        pl.when(pl.col("lap_time") <= 0)
        .then(pl.col("lap_time_inferred"))
        .otherwise(pl.col("lap_time"))
        .alias("lap_time")
    )

    # In the rare case where we have all invalid across all laps, we will get Nones in `time`
    # We can just fill with the fastest lap + interval.
    # We use the fastest lap because it is unlikely that all laps are invalid (so we should have at least 1),
    # and it probably won't break any of our assumptions
    fastest_lap = lap_df.filter(pl.col("lap_time").is_not_null())["lap_time"].min()
    lap_df = lap_df.with_columns(
        pl.when(pl.col("lap_time").is_null())
        .then(fastest_lap - pl.col("interval_change").fill_null(0))
        .otherwise(pl.col("lap_time"))
        .alias("lap_time")
    )

    # Check for any remaining -1 laps
    if (lap_df["lap_time"] < 0).sum() > 0:
        raise ValueError("Error inferring lap times!")

    # Downselect cols and cast
    lap_df = lap_df.select(["user_id", "lap", "lap_time", "interval"])

    # Join back on original
    lap_df = original_df.drop(["lap_time", "interval"]).join(lap_df, on=["user_id", "lap"])

    return lap_df


def _join_qualy_data(result_df, qualy_df):
    if qualy_df is None:
        return result_df

    qualy_df = qualy_df.select(["user_id", "finish_position", "best_lap_time", "laps_complete"]).rename(
        {
            "finish_position": "start_position",
            "best_lap_time": "qualify_lap_time",
        }
    )

    qualy_df = qualy_df.filter(pl.col("laps_complete") > 0).drop("laps_complete")

    result_df = result_df.join(qualy_df, on="user_id", how="full", coalesce=True)

    # Fill finish position for anyone that didn't compete
    result_df = result_df.with_columns(
        pl.col("finish_position").fill_null(pl.col("finish_position").max() + 1).cast(pl.Int64)
    )

    result_df = result_df.with_columns(pl.col("laps_complete").cast(pl.Int64).fill_null(0))

    result_df = result_df.with_columns(pl.col("penalty").fill_null(0))

    return result_df


def _check_lap_df(lap_df):
    if len(lap_df.select(["user_id", "lap"]).unique()) < len(lap_df):
        raise ValueError("Lap dataframe has duplicate laps per driver")


def _drop_penalized_laps(driver_df):
    """Drop any extra laps which wouldn't occur if the driver was penalized in-race"""
    driver_df = driver_df.sort("lap", descending=True)

    # Basically, we are "exhausting" the penalty applied by subtracting out the interval at the end of each lap.
    # Once we have <=0 penalty remaining, we've found the last lap for the driver
    penalty_remaining = driver_df[0, "penalty"]
    for i in range(len(driver_df)):
        row = driver_df.row(i, named=True)
        penalty_remaining = -1 * (row["interval"] - penalty_remaining) - row["lap_time"]
        if penalty_remaining <= 0:
            break

    driver_df = driver_df.slice(i)
    return driver_df


def compute_results(lap_df: pl.DataFrame, penalty_df: pl.DataFrame, qualy_df: pl.DataFrame = None) -> pl.DataFrame:
    """
    Compute the race result with penalties applied

    Parameters
    ----------
    lap_df : Dataframe of lap times, must have the following columns:
        user_id : User identifier
        lap : Lap index (starts at 0)
        lap_time : Time per lap (in seconds)
    penalty_df : Dataframe of penalties. Can have multiple entries per user_id
        user_id : User identifier
        time_s : Penalty time in seconds
    """
    # Make some assertions on the lap dataframe
    _check_lap_df(lap_df)

    lap_df = lap_df.clone()

    # Compute cumulative lap time
    lap_df = lap_df.sort(["user_id", "lap"])

    # Drop formation lap
    lap_df = lap_df.filter(pl.col("lap") > 0)

    lap_df = lap_df.with_columns(pl.col("lap_time").cum_sum().over("user_id", order_by="lap").alias("total_time"))
    lap_df = lap_df.with_columns(
        (pl.col("lap_time").cum_sum(reverse=True).over("user_id", order_by="lap") * -1).alias("time_from_finish")
    )

    # Compute each drivers total penalty
    if penalty_df is None or len(penalty_df) == 0:
        penalty_df = pl.DataFrame(schema={"user_id": pl.Int64, "penalty": pl.Float64})
    else:
        penalty_df = penalty_df.rename({"time": "penalty"})
        penalty_df = penalty_df.group_by("user_id").agg(pl.col("penalty").sum())

    # Add penalties for each driver to all laps
    unmatched_users = set(penalty_df["user_id"].to_list()) - set(lap_df["user_id"].to_list())
    if unmatched_users:
        raise ValueError(
            f"Found {len(unmatched_users)} user_ids in penalty_df not present in lap_df: {unmatched_users}"
        )
    lap_df = lap_df.join(penalty_df, on="user_id", how="left")
    lap_df = lap_df.with_columns(pl.col("penalty").cast(pl.Float64, strict=False).fill_null(0))

    # Drop penalized laps, if required
    lap_df = lap_df.group_by("user_id").map_groups(_drop_penalized_laps)

    # Get finish lap df per driver
    finish_lap_df = lap_df.sort("lap").group_by("user_id").last()

    finish_lap_df = finish_lap_df.with_columns(
        (pl.col("interval") - pl.col("penalty")).alias("interval"),
        (pl.col("total_time") + pl.col("penalty")).alias("total_time"),
    )

    # Finally, create the finish order dataframe
    result_df = (
        finish_lap_df.select(["user_id", "lap", "total_time", "penalty", "interval"])
        .sort(by=["lap", "total_time"], descending=[True, False])
        .rename({"lap": "laps_complete"})
    )

    result_df = result_df.with_columns(pl.int_range(pl.len()).alias("finish_position"))

    # Compute average lap
    time_df = lap_df.group_by("user_id").agg(
        pl.col("lap_time").sum(),
        pl.col("lap").max(),
    )

    time_df = time_df.join(penalty_df, on="user_id", how="left")
    time_df = time_df.with_columns(pl.col("penalty").cast(pl.Float64, strict=False).fill_null(0))
    time_df = time_df.with_columns(((pl.col("lap_time") + pl.col("penalty")) / pl.col("lap")).alias("average_lap_time"))

    result_df = result_df.join(time_df.select(["user_id", "average_lap_time"]), on="user_id")

    result_df = _join_qualy_data(result_df, qualy_df)

    # Compute interval column
    result_df = _compute_interval(result_df)

    fastest_lap_df = (
        lap_df.filter(~pl.col("incident"))
        .sort("lap_time")
        .group_by("user_id")
        .first()
        .select(["user_id", "lap_time"])
        .rename({"lap_time": "fastest_lap_time"})
    )

    result_df = result_df.join(fastest_lap_df, on="user_id", how="left")

    # Restore finish position order (polars joins don't preserve row order)
    result_df = result_df.sort("finish_position")

    return result_df
