#!/usr/bin/env python3

import pandas as pd
import numpy as np

from gpt_racing import utils


def _compute_interval(result_df):
    result_df = result_df.copy()

    result_df["interval"] = result_df.iloc[0]["total_time"] - result_df["total_time"]

    result_df["laps_down"] = result_df["laps_complete"] - result_df["laps_complete"][0]

    result_df["interval"] = result_df.apply(
        lambda x: utils.seconds_to_str(x["interval"]) if x["laps_down"] == 0 else f"{int(x['laps_down'])}L", axis=1
    )
    result_df = result_df.drop(columns="laps_down")

    return result_df


def apply_penalties(lap_df: pd.DataFrame, penalty_df: pd.DataFrame) -> pd.DataFrame:
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

    # Ensure all times are positive
    lap_df["time"] = lap_df["time"].apply(lambda x: max(x, 0))
    lap_df["total_time"] = lap_df[["user_id", "time"]].groupby("user_id").cumsum()

    # Compute each drivers total penalty
    if len(penalty_df) == 0 or penalty_df is None:
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
    last_lap_df = (
        lap_df[lap_df["total_time"] >= race_end_time]
        .sort_values(["user_id", "total_time"])
        .groupby("user_id")
        .first()
        .reset_index()
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
