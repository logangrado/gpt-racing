#!/usr/bin/env python3

from typing import Tuple

import pandas as pd
import polars as pl

# from great_tables import GT

from gpt_racing.iracing_data import IracingDataClient
from gpt_racing.elo_mmr import compute_elo_mmr
from gpt_racing.results import compute_results, infer_invalid_laps
from gpt_racing.scoring.points import compute_points_score
from gpt_racing import utils


def _load_race_data(config, client) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Load lap data
    result_dfs = []
    name_data = []

    for i, race_config in enumerate(config.races):
        # Load lap time DF
        lap_df = client.get_lap_data(race_config.subsession_id)
        lap_df = lap_df.rename(
            columns={
                "cust_id": "user_id",
                "lap_number": "lap",
            }
        )

        # For some really stupid reason, the `lap_time` is in 10,000th's of a second, while `interval` is in thousandths
        lap_df["lap_time"] = lap_df["lap_time"] / 10000
        lap_df["interval"] = lap_df["interval"] / 1000

        # Infer invalid laps
        lap_df = infer_invalid_laps(lap_df)

        # Compute results
        if race_config.penalties:
            penalty_df = pd.DataFrame([dict(x) for x in race_config.penalties])
        else:
            penalty_df = None
        qualy_df = client.get_qualy_result(race_config.subsession_id)
        qualy_df = qualy_df.rename(
            columns={
                "cust_id": "user_id",
            }
        )

        qualy_df["best_lap_time"] = qualy_df["best_lap_time"] / 10000

        result_df = compute_results(lap_df, penalty_df, qualy_df)
        result_df["subsession_id"] = race_config.subsession_id

        race_name = f"race_{i+1}"
        if race_config.race_name:
            race_name += f"_{race_config.race_name}"
        result_df["race_name"] = race_name

        result_dfs.append(result_df)
        name_data.append(qualy_df[["user_id", "display_name"]].drop_duplicates())

    result_df = pd.concat(result_dfs).reset_index(drop=True)
    name_df = pd.concat(name_data).drop_duplicates().reset_index(drop=True)

    return result_df, name_df


def compute_ratings(config, client):
    """
    Compute ratings given a config
    """
    result_df, name_df = _load_race_data(config, client)
    result_df = pl.DataFrame(result_df)
    name_df = pl.DataFrame(name_df)

    contest_df = result_df[["user_id", "finish_position", "subsession_id"]].rename({"subsession_id": "contest_id"})

    outputs = {
        "race_results": [],
        "standings": [],
        "points": [],
        "ELO": [],
    }
    for contest_id in contest_df["contest_id"].unique():
        sub_contest_df = contest_df.filter(pl.col("contest_id") <= contest_id)

        # if config.scoring.type == "POINTS":
        points_df, points_summary_df = compute_points_score(sub_contest_df, config.points)
        points_df = points_df.rename({"contest_id": "subsession_id"})

        # if config.scoring.type == "ELO":
        elo_df = compute_elo_mmr(sub_contest_df.to_pandas()).rename(columns={"contest_id": "subsession_id"})
        elo_df = pl.DataFrame(elo_df)
        current_elo_df = elo_df.filter(pl.col("subsession_id") == contest_id)

        # JOIN POINTS
        race_result_df = (
            result_df.filter(pl.col("subsession_id") == contest_id)
            .join(name_df, on="user_id")
            .join(
                points_df[["user_id", "subsession_id", "points"]],
                on=["user_id", "subsession_id"],
            )
        )

        # JOIN ELO RATING
        race_result_df = race_result_df.join(
            elo_df[["user_id", "subsession_id", "rating", "rating_change", "rank", "rank_change"]],
            on=["user_id", "subsession_id"],
        )

        race_result_df = race_result_df.select(
            "display_name",
            "start_position",
            "qualify_lap_time",
            "finish_position",
            "interval",
            "points",
            "rating",
            "rating_change",
            "laps_complete",
            "total_time",
            "penalty",
            "average_lap_time",
            "fastest_lap_time",
        ).with_columns(pl.col("start_position") + 1, pl.col("finish_position") + 1)

        # CREATE SERIES STANDINGs DATAFRAME
        standings_df = (
            points_df.join(result_df.select("subsession_id", "race_name").unique(), on="subsession_id")
            .sort("subsession_id")
            .pivot(on="race_name", index="user_id", values=["points", "drop"])
            .join(points_summary_df.rename({"points": "points_total", "rank": "points_rank"}), on="user_id")
            .join(
                current_elo_df.select("user_id", "rating", "rank", "num_contests").rename(
                    {"rank": "rating_rank", "num_contests": "num_races"}
                ),
                on="user_id",
            )
            .join(name_df, on="user_id")
            .sort("points_rank")
        )

        outputs["race_results"].append(race_result_df)
        outputs["standings"].append(standings_df)
        outputs["points"].append(points_df)
        outputs["ELO"].append(elo_df)

    return outputs
    # ============================================

    _render_standings(standings_df)
    import ipdb

    ipdb.set_trace()
    pass

    # Join ratings and names onto result_df
    result_df = (
        result_df.merge(rating_df, on=["subsession_id", "user_id"], how="inner")
        .merge(name_df, on="user_id", how="left")
        .sort_values(["subsession_id", "finish_position"])
    )

    # Join names onto rating df, select/rename
    rating_df = (
        rating_df.merge(name_df, on="user_id")[
            [
                "display_name",
                "user_id",
                "rating",
                "rating_change",
                "rank",
                "rank_change",
                "subsession_id",
            ]
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
