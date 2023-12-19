#!/usr/bin/env python3

import pandas as pd
import numpy as np

from gpt_racing import utils


def _compute_interval(result_df):
    result_df = result_df.copy()

    # WE NEED TO USE IRACING PROVIDED INTERVALS!!!
    # The iracing we calculate using all lap times has some drift!
    result_df["interval"] = result_df.iloc[0]["total_time"] - result_df["total_time"]

    result_df["laps_down"] = result_df["laps_complete"] - result_df["laps_complete"][0]

    result_df["interval"] = result_df.apply(
        lambda x: utils.seconds_to_str(x["interval"]) if x["laps_down"] == 0 else f"{int(x['laps_down'])}L", axis=1
    )
    result_df = result_df.drop(columns="laps_down")

    return result_df


def _infer_invalid_laps(lap_df):
    """
    Infer lap times for any lap marked as invalid.
    """
    lap_df = lap_df.copy()

    lap_df["interval_previous"] = (
        lap_df.sort_values(["user_id", "lap"]).groupby(["user_id"])["interval"].shift().fillna(0)
    )

    valid_laps = lap_df[lap_df["time"] != -1]
    valid_laps["interval"] = valid_laps["interval"].fillna(0)

    zero_int_lap_times = valid_laps.sort_values(["interval"], ascending=False).groupby("lap").first().reset_index()

    lap_df = lap_df.merge(
        zero_int_lap_times[["lap", "time", "interval", "interval_previous"]].rename(
            columns={"time": "time0", "interval": "interval0", "interval_previous": "interval_previous0"}
        ),
        on=["lap"],
        how="left",
    )

    # Compute this_lap_interval
    lap_df["interval_change"] = lap_df["interval"] - lap_df["interval_previous"]

    # Infer all lap times
    lap_df["time_inferred"] = (
        lap_df["time0"] - lap_df["interval_change"].fillna(0) + lap_df["interval0"] - lap_df["interval_previous0"]
    )

    # Fill -1 lap times with inferred
    lap_df["time"][lap_df["time"] < 0] = lap_df["time_inferred"]

    # In the rare case where we have all invalid across all laps, we will get Nones in `time`
    # We can just fill with the fastest lap + interval.
    # We use the fastest lap because it is unlikely that all laps are invalid (so we should have at least 1),
    # and it probably won't break any of our assumptions
    fastest_lap = lap_df["time"][~lap_df["time"].isna()].min()
    lap_df["time"][lap_df["time"].isna()] = lap_df["time"].fillna(fastest_lap) - lap_df["interval_change"].fillna(0)

    # Drop extra columns
    lap_df = lap_df.drop(columns=["time0", "interval0", "interval_previous", "interval_change", "time_inferred"])

    return lap_df


def compute_results(lap_df: pd.DataFrame, penalty_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the race result with penalties applied

    Parameters
    ----------
    lap_df : Dataframe of lap times, must have the following columns:
        user_id : User identifier
        lap : Lap index (starts at 0)
        time : Time per lap (in seconds)
    penalty_df : Dataframe of penalties. Can have multiple entries per user_id
        user_id : User identifier
        time_s : Penalty time in seconds
    """
    lap_df = lap_df.copy()

    # Compute cumulative lap time
    lap_df = lap_df.sort_values(["user_id", "lap"])

    # Drop formation lap
    lap_df = lap_df[lap_df["lap"] > 0]

    lap_df = _infer_invalid_laps(lap_df)

    lap_df["total_time"] = lap_df[["user_id", "time"]].groupby("user_id").cumsum()

    # Compute each drivers total penalty
    if penalty_df is None or len(penalty_df) == 0:
        penalty_df = pd.DataFrame(columns=["user_id", "time"])

    penalty_df = penalty_df.rename(columns={"time": "penalty"})
    penalty_df = penalty_df.groupby("user_id").sum().reset_index()

    # Add penalties for each driver to all laps
    lap_df = lap_df.merge(penalty_df, on="user_id", how="left")
    lap_df["penalty"] = lap_df["penalty"].fillna(0)

    # Compute penalized total_time
    lap_df["total_time"] = lap_df["total_time"] + lap_df["penalty"]

    # Compute race end time
    race_end_time = lap_df[lap_df["lap"] == lap_df["lap"].max()]["total_time"].min()

    # Find the last lap for each driver. That is the _first_ lap that ends ON or AFTER the race end time
    finish_lap_df = (
        lap_df[lap_df["total_time"] >= race_end_time]
        .sort_values(["user_id", "total_time"])
        .groupby("user_id")
        .first()
        .reset_index()
    )
    # We also need the last lap per driver, to catch disconnects
    last_lap_df = lap_df.sort_values(["user_id", "total_time"]).groupby("user_id").last().reset_index()

    last_lap_df = (
        pd.concat([finish_lap_df, last_lap_df]).sort_values("lap").drop_duplicates(subset="user_id", keep="first")
    )

    # Finally, create the finish order dataframe
    result_df = (
        last_lap_df[["user_id", "lap", "total_time", "penalty"]]
        .sort_values(by=["lap", "total_time"], ascending=[False, True])
        .rename(columns={"lap": "laps_complete"})
        .reset_index(drop=True)
    )

    result_df["finish_position"] = np.arange(len(result_df))

    # Compute interval column
    # result_df["interval"] = result_df.apply(_compute_interval, axis=1, winner=result_df.iloc[0])
    result_df = _compute_interval(result_df)

    # Compute fastest lap column
    return result_df
