#!/usr/bin/env python3

import pandas as pd
import numpy as np

from gpt_racing import utils


def _compute_interval(result_df):
    result_df = result_df.copy()

    result_df["laps_down"] = result_df["laps_complete"] - result_df["laps_complete"][0]

    # Subtract min interval, in case we penalized the leader
    result_df["interval"] = result_df["interval"] - result_df["interval"].max()

    result_df["interval"] = result_df.apply(
        lambda x: utils.seconds_to_str(x["interval"]) if x["laps_down"] == 0 else f"{int(x['laps_down'])}L",
        axis=1,
    )
    result_df["interval"] = result_df["interval"].fillna("-")

    result_df = result_df.drop(columns="laps_down")

    return result_df


def infer_invalid_laps(lap_df):
    """
    Infer lap times for any lap marked as invalid.
    """
    original_df = lap_df
    # PREPARE LAP DATAFRAME
    lap_df = lap_df.copy()[["user_id", "lap", "lap_time", "interval"]]
    lap_df.loc[:, "interval"] = lap_df["interval"].fillna(0)

    lap_df["interval_previous"] = (
        lap_df.sort_values(["user_id", "lap"]).groupby(["user_id"])["interval"].shift().fillna(0)
    )

    valid_laps = lap_df[lap_df["lap_time"] > 0]

    # Get the single valid lap per lap_number with the lowest interval
    zero_int_lap_times = valid_laps.sort_values(["interval"], ascending=False).groupby("lap").first().reset_index()

    # Merge zero int lap times onto lap dataframe, by lap. Suffix zero int lap data with `0`
    lap_df = lap_df.merge(
        zero_int_lap_times[["lap", "lap_time", "interval", "interval_previous"]].rename(
            columns={
                "lap_time": "lap_time0",
                "interval": "interval0",
                "interval_previous": "interval_previous0",
            }
        ),
        on=["lap"],
        how="left",
    )

    # Compute this_lap_interval
    lap_df["interval_change"] = lap_df["interval"] - lap_df["interval_previous"]

    # Infer all lap times
    lap_df["lap_time_inferred"] = (
        lap_df["lap_time0"] - lap_df["interval_change"].fillna(0) + lap_df["interval0"] - lap_df["interval_previous0"]
    )

    # Fill -1 lap times with inferred
    lap_df.loc[lap_df["lap_time"] <= 0, "lap_time"] = lap_df["lap_time_inferred"]

    # In the rare case where we have all invalid across all laps, we will get Nones in `time`
    # We can just fill with the fastest lap + interval.
    # We use the fastest lap because it is unlikely that all laps are invalid (so we should have at least 1),
    # and it probably won't break any of our assumptions
    fastest_lap = lap_df["lap_time"][~lap_df["lap_time"].isna()].min()
    lap_df.loc[lap_df["lap_time"].isna(), "lap_time"] = lap_df["lap_time"].fillna(fastest_lap) - lap_df[
        "interval_change"
    ].fillna(0)

    # Check for any remaining -1 laps
    if sum(lap_df["lap_time"] < 0) > 0:
        raise ValueError("Error inferring lap times!")

    # Downselect cols and cast
    lap_df = lap_df[["user_id", "lap", "lap_time", "interval"]]

    # Join back on original
    lap_df = original_df.drop(columns=["lap_time", "interval"]).merge(lap_df, on=["user_id", "lap"])

    return lap_df


def _join_qualy_data(result_df, qualy_df):
    if qualy_df is None:
        return result_df

    qualy_df = qualy_df[["user_id", "finish_position", "best_lap_time", "laps_complete"]].rename(
        columns={
            "finish_position": "start_position",
            "best_lap_time": "qualify_lap_time",
        }
    )

    qualy_df = qualy_df[qualy_df["laps_complete"] > 0].drop(columns="laps_complete")

    result_df = result_df.merge(qualy_df, on="user_id", how="outer")

    # Fill finish position for anyone that didn't compete
    result_df["finish_position"] = (
        result_df["finish_position"].fillna(result_df["finish_position"].max() + 1).astype("Int64")
    )

    result_df["laps_complete"] = result_df["laps_complete"].astype("Int64").fillna(0)

    result_df["penalty"] = result_df["penalty"].fillna(0)

    return result_df


def _check_lap_df(lap_df):
    if len(lap_df[["user_id", "lap"]].drop_duplicates()) < len(lap_df):
        raise ValueError("Lap dataframe has duplicate laps per driver")


def _drop_penalized_laps(driver_df):
    """Drop any extra laps which wouldn't occur if the driver was penalized in-race"""
    driver_df = driver_df.sort_values("lap", ascending=False).reset_index(drop=True)

    # Basically, we are "exhausting" the penalty applied by subtracting out the interval at the end of each lap.
    # Once we have <=0 penalty remaining, we've found the last lap for the driver
    penalty_remaining = driver_df.iloc[0]["penalty"]
    for i, lap in driver_df.iterrows():
        penalty_remaining = -1 * (lap["interval"] - penalty_remaining) - lap["lap_time"]
        if penalty_remaining <= 0:
            break

    driver_df = driver_df.iloc[i:]
    return driver_df


def compute_results(lap_df: pd.DataFrame, penalty_df: pd.DataFrame, qualy_df: pd.DataFrame = None) -> pd.DataFrame:
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

    lap_df = lap_df.copy()

    # Compute cumulative lap time
    lap_df = lap_df.sort_values(["user_id", "lap"])

    # Drop formation lap
    lap_df = lap_df[lap_df["lap"] > 0]

    lap_df["total_time"] = lap_df.sort_values("lap")[["user_id", "lap_time"]].groupby("user_id").cumsum()
    lap_df["time_from_finish"] = (
        lap_df.sort_values("lap", ascending=False)[["user_id", "lap_time"]].groupby("user_id").cumsum() * -1
    )

    # Compute each drivers total penalty
    if penalty_df is None or len(penalty_df) == 0:
        penalty_df = pd.DataFrame(columns=["user_id", "time"])

    penalty_df = penalty_df.rename(columns={"time": "penalty"})
    penalty_df = penalty_df.groupby("user_id").sum().reset_index()

    # Add penalties for each driver to all laps
    lap_df = lap_df.merge(penalty_df, on="user_id", how="left")
    lap_df["penalty"] = pd.to_numeric(lap_df["penalty"], errors="coerce")  # Fix fillna warning

    lap_df["penalty"] = lap_df["penalty"].fillna(0)

    # Drop penalized laps, if required
    lap_df = lap_df.groupby("user_id").apply(_drop_penalized_laps).reset_index(drop=True)

    # Get finish lap df per driver
    finish_lap_df = lap_df.sort_values("lap").groupby("user_id").last().reset_index()

    finish_lap_df["interval"] = finish_lap_df["interval"] - finish_lap_df["penalty"]
    finish_lap_df["total_time"] = finish_lap_df["total_time"] + finish_lap_df["penalty"]

    # We also need the last lap per driver, to catch disconnects
    # NOTE: We probably don't need this anymore with the new finish_lap_calc, which always finds the last lap per driver!!
    # last_lap_df = lap_df.sort_values(["user_id", "lap"]).groupby("user_id").last().reset_index()
    # last_lap_df = (
    #     pd.concat([finish_lap_df, last_lap_df]).sort_values("lap").drop_duplicates(subset="user_id", keep="first")
    # )

    # Finally, create the finish order dataframe
    result_df = (
        finish_lap_df[["user_id", "lap", "total_time", "penalty", "interval"]]
        .sort_values(by=["lap", "interval"], ascending=False)
        .rename(columns={"lap": "laps_complete"})
        .reset_index(drop=True)
    )

    result_df["finish_position"] = np.arange(len(result_df))

    # Compute average lap
    total_time = lap_df[["user_id", "lap_time"]].groupby("user_id").sum()
    total_laps = lap_df[["user_id", "lap"]].groupby("user_id").max()

    time_df = total_time.join(total_laps)
    time_df = time_df.reset_index().merge(penalty_df, how="left")

    time_df["penalty"] = pd.to_numeric(time_df["penalty"], errors="coerce")  # Fix fillna warning
    time_df["average_lap_time"] = (time_df["lap_time"] + time_df["penalty"].fillna(0)) / time_df["lap"]

    result_df = result_df.merge(time_df[["user_id", "average_lap_time"]], on="user_id")

    result_df = _join_qualy_data(result_df, qualy_df)

    # Compute interval column
    result_df = _compute_interval(result_df)

    fastest_lap_df = (
        lap_df[~lap_df["incident"]]
        .sort_values("lap_time")
        .groupby("user_id")
        .first()
        .reset_index()[["user_id", "lap_time"]]
        .rename(columns={"lap_time": "fastest_lap_time"})
    )

    result_df = result_df.merge(fastest_lap_df, on="user_id", how="left")

    # Compute fastest lap column
    return result_df
