#!/usr/bin/env python3

import pandas as pd
import numpy as np

from gpt_racing import utils


def _compute_interval(result_df):
    result_df = result_df.copy()

    # The iracing we calculate using all lap times has some drift!
    # result_df["interval"] = result_df.iloc[0]["total_time"] - result_df["total_time"]

    # We need to use iRacing's interval, rather than calculating from total time, as there is some drift.
    # The source of the drift is unknown, but likely a precision issue

    result_df["laps_down"] = result_df["laps_complete"] - result_df["laps_complete"][0]

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
    lap_df["penalty"] = lap_df["penalty"].fillna(0)

    # Compute penalized total_time and interval
    lap_df["total_time"] = lap_df["total_time"] + lap_df["penalty"]
    lap_df["interval"] = lap_df["interval"] - lap_df["penalty"]

    # If the final interval with penalty is greather than lap time, that driver would have been penalized across the final lap??
    finish_lap_df = lap_df.sort_values("lap").groupby("user_id").last().reset_index()
    # Identify any drivers where (interval - penalty) < lap_time
    drivers_penalized_across_last_lap = finish_lap_df[finish_lap_df["interval"] * -1 > finish_lap_df["lap_time"]][
        "user_id"
    ]
    if len(drivers_penalized_across_last_lap) > 0:
        x = drivers_penalized_across_last_lap.to_list()

        y = finish_lap_df[finish_lap_df["user_id"].isin(x)][["user_id", "lap"]]
        y["lap"] = y["lap"] - 1
        y.loc[y["lap"] < 1, "lap"] = 1

        penalized_lap_df = lap_df.merge(y, on=["user_id", "lap"], how="inner")

        finish_lap_df = finish_lap_df[~finish_lap_df["user_id"].isin(x)]

        finish_lap_df = pd.concat([finish_lap_df, penalized_lap_df])

    # # Compute race end time
    # race_end_time = lap_df[lap_df["lap"] == lap_df["lap"].max()]["total_time"].min()

    # # Find the last lap for each driver. That is the _first_ lap that ends ON or AFTER the race end time
    # finish_lap_df = (
    #     lap_df[lap_df["total_time"] >= race_end_time]
    #     .sort_values(["user_id", "total_time"])
    #     .groupby("user_id")
    #     .first()
    #     .reset_index()
    # )

    # We also need the last lap per driver, to catch disconnects
    last_lap_df = lap_df.sort_values(["user_id", "lap"]).groupby("user_id").last().reset_index()

    last_lap_df = (
        pd.concat([finish_lap_df, last_lap_df]).sort_values("lap").drop_duplicates(subset="user_id", keep="first")
    )

    # Finally, create the finish order dataframe
    result_df = (
        last_lap_df[["user_id", "lap", "total_time", "penalty", "interval"]]
        .sort_values(by=["lap", "total_time"], ascending=[False, True])
        .rename(columns={"lap": "laps_complete"})
        .reset_index(drop=True)
    )

    result_df["finish_position"] = np.arange(len(result_df))
    result_df["average_lap_time"] = result_df["total_time"] / result_df["laps_complete"]

    result_df = _join_qualy_data(result_df, qualy_df)

    # Compute interval column
    # result_df["interval"] = result_df.apply(_compute_interval, axis=1, winner=result_df.iloc[0])
    result_df = _compute_interval(result_df)

    result_df["average_lap_time"] = result_df["total_time"] / result_df["laps_complete"]

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
