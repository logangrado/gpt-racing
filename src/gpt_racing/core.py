#!/usr/bin/env python3


import pandas as pd

from gpt_racing.iracing_data import IracingDataClient
from gpt_racing.elo_mmr import compute_elo_mmr
from gpt_racing.results import compute_results
from gpt_racing import utils


def _load_race_data(config, client):
    # Load lap data
    result_dfs = []
    name_data = []

    for race_config in config.races:
        # Load lap time DF
        lap_df = client.get_lap_data(race_config.subsession_id)
        lap_df = lap_df.rename(
            columns={
                "cust_id": "user_id",
                "lap_number": "lap",
            }
        )
        lap_df["time"] = lap_df["lap_time"] / 10000
        lap_df["interval"] = lap_df["interval"] / 10000

        # Compute results
        penalty_df = pd.DataFrame([dict(x) for x in race_config.penalties])
        qualy_df = client.get_qualy_result(race_config.subsession_id)
        qualy_df["best_lap_time"] = qualy_df["best_lap_time"] / 10000

        result_df = compute_results(lap_df, penalty_df, qualy_df)
        result_df["subsession_id"] = race_config.subsession_id

        result_dfs.append(result_df)
        name_data.append(qualy_df[["user_id", "display_name"]].drop_duplicates())

    result_df = pd.concat(result_dfs).reset_index(drop=True)
    name_df = pd.concat(name_data).drop_duplicates().reset_index(drop=True)

    return result_df, name_df


def _write_outputs(rating_df, result_df, output_path):
    rating_df = rating_df.copy()
    result_df = result_df.copy()

    # Format results, select columns
    rating_df["rating"] = utils.format_value_with_delta(rating_df, "rating", "rating_change")
    rating_df["rank"] = utils.format_value_with_delta(rating_df, "rank", "rank_change")
    rating_df = rating_df[["subsession_id", "rank", "display_name", "rating"]].rename(
        columns={"display_name": "Name", "rating": "Rating", "rank": "Rank"}
    )

    result_df["rating"] = utils.format_value_with_delta(result_df, "rating", "rating_change")
    result_df["rank"] = utils.format_value_with_delta(result_df, "rank", "rank_change")
    result_df = result_df[
        [
            "subsession_id",
            "finish_position",
            "display_name",
            "interval",
            "rating",
            "rank",
        ]
    ]


def compute_ratings(config, client):
    """
    Compute ratings given a config
    """
    result_df, name_df = _load_race_data(config, client)

    contest_df = result_df[["user_id", "finish_position", "subsession_id"]].rename(
        columns={"subsession_id": "contest_id"}
    )

    rating_df = compute_elo_mmr(contest_df).rename(columns={"contest_id": "subsession_id"})

    # Join ratings and names onto result_df
    result_df = (
        result_df.merge(rating_df, on=["subsession_id", "user_id"], how="inner")
        .merge(name_df, on="user_id", how="left")
        .sort_values(["subsession_id", "finish_position"])
    )

    # Join names onto rating df, select/rename
    rating_df = (
        rating_df.merge(name_df, on="user_id")[
            ["display_name", "user_id", "rating", "rating_change", "rank", "rank_change", "subsession_id"]
        ]
        .sort_values(["subsession_id", "rank"])
        .reset_index(drop=True)
    )

    # Increment columns that start at zero
    inc_cols = ["start_position", "finish_position"]
    for col in inc_cols:
        result_df[col] += 1

    # Format output columns
    time_cols = ["total_time", "qualify_lap_time", "average_lap_time"]
    for col in time_cols:
        result_df[col] = result_df[col].apply(utils.seconds_to_str)

    return rating_df, result_df
