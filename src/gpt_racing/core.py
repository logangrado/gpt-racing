#!/usr/bin/env python3

from typing import Tuple
import functools

import pandas as pd
import polars as pl

# from great_tables import GT

from gpt_racing.iracing_data import IracingDataClient
from gpt_racing.elo_mmr import compute_elo_mmr, ELOMMR
from gpt_racing.results import compute_results, infer_invalid_laps
from gpt_racing.scoring.points import compute_points_score
from gpt_racing import utils


def _load_race_data(race_configs, client) -> Tuple[pl.DataFrame, pl.DataFrame]:
    # Load lap data
    result_dfs = []
    name_data = []
    session_metadata = []

    for i, race_config in enumerate(race_configs):
        race_result_df = client.get_race_result(race_config.subsession_id)
        session_metadata += [
            {
                "subsession_id": race_config.subsession_id,
                "session_end_time": race_result_df.iloc[0]["session_end_time"],
            }
        ]

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

        result_df = result_df.merge(
            race_result_df[["cust_id", "incidents"]].rename(
                columns={"cust_id": "user_id", "incidents": "num_incidents"}
            )
        )

        result_dfs.append(result_df)
        name_data.append(qualy_df[["user_id", "display_name"]].drop_duplicates())

    result_df = pd.concat(result_dfs).reset_index(drop=True)
    name_df = pd.concat(name_data).drop_duplicates().reset_index(drop=True)

    result_df = pl.DataFrame(result_df)
    name_df = pl.DataFrame(name_df)

    session_mdf = pl.DataFrame(session_metadata)
    # session_mdf = session_mdf.with_columns(pl.col("session_end_time").cast(pl.Date).alias("session_end_time"))

    result_df = result_df.join(session_mdf["subsession_id", "session_end_time"], how="left", on="subsession_id")

    return result_df, name_df


def _compute_elo_previous_seasons(elo_config, client, elo_mmr):
    all_races = functools.reduce(lambda x, y: x + y, [x.races for x in elo_config.previous_seasons])

    result_df, _ = _load_race_data(all_races, client)

    for subsession_id in result_df["subsession_id"].unique():
        this_contest_df = result_df.filter(pl.col("subsession_id") == subsession_id)
        elo_mmr = _compute_elo_from_result_df(this_contest_df, elo_config, elo_mmr)

    return elo_mmr


def _compute_elo_from_result_df(result_df: pl.DataFrame, elo_config, elo_mmr):
    contest_df = result_df[["user_id", "finish_position", "subsession_id", "session_end_time"]].rename(
        {"subsession_id": "contest_id", "session_end_time": "contest_date"}
    )

    elo_mmr.update(contest_df)
    # elo_df, elo_state = compute_elo_mmr(contest_df, elo_config, elo_state)
    # elo_df = pl.DataFrame(elo_df).rename({"contest_id": "subsession_id"})

    # elo_mmr.update(contest_df)

    return elo_mmr


def compute_ratings(config, client):
    """
    Compute ratings given a config
    """
    # players = None
    # past_elo_df = None
    elo_mmr = ELOMMR(config.elo)

    if config.elo.previous_seasons:
        elo_mmr = _compute_elo_previous_seasons(config.elo, client, elo_mmr)

    result_df, name_df = _load_race_data(config.races, client)
    result_df = pl.DataFrame(result_df)
    name_df = pl.DataFrame(name_df)

    race_points_type_df = pl.DataFrame(
        [
            {
                "points_type": race.points_type if race.points_type else config.points.default,
                "subsession_id": race.subsession_id,
            }
            for race in config.races
        ]
    )
    contest_df = result_df[
        [
            "user_id",
            "finish_position",
            "subsession_id",
            "session_end_time",
            "fastest_lap_time",
            "num_incidents",
            "laps_complete",
        ]
    ].join(race_points_type_df, on="subsession_id")

    outputs = {
        "race_results": [],
        "standings": [],
        "points": [],
        "ELO": [],
    }
    for subsession_id in contest_df["subsession_id"].unique():
        sub_contest_df = contest_df.filter(pl.col("subsession_id") <= subsession_id)
        this_contest_df = contest_df.filter(pl.col("subsession_id") == subsession_id)

        # POINTS
        points_df = compute_points_score(sub_contest_df, config.points)
        current_points_df = points_df.filter(pl.col("subsession_id") == subsession_id).sort("finish_position")

        # ELO
        elo_mmr = _compute_elo_from_result_df(this_contest_df, config.elo, elo_mmr)
        elo_df = elo_mmr.collect_results()
        elo_df = elo_df.rename({"contest_id": "subsession_id"})
        current_elo_df = elo_df.filter(pl.col("subsession_id") == subsession_id)

        # JOIN POINTS
        race_result_df = (
            result_df.filter(pl.col("subsession_id") == subsession_id)
            .join(name_df, on="user_id")
            .join(
                points_df[["user_id", "subsession_id", "points", "fastest_lap", "cleanest_driver"]],
                on=["user_id", "subsession_id"],
            )
        )

        # JOIN ELO RATING
        race_result_df = race_result_df.join(
            current_elo_df[["user_id", "subsession_id", "rating", "rating_change", "rank", "rank_change"]],
            on=["user_id", "subsession_id"],
        )

        race_result_df = (
            race_result_df.select(
                "display_name",
                "start_position",
                "qualify_lap_time",
                "finish_position",
                "interval",
                "points",
                "rating",
                "rating_change",
                "rank",
                "rank_change",
                "laps_complete",
                "total_time",
                "penalty",
                "average_lap_time",
                "fastest_lap_time",
                "num_incidents",
                "fastest_lap",
                "cleanest_driver",
            )
            .with_columns(pl.col("start_position") + 1, pl.col("finish_position") + 1)
            .sort("finish_position")
        )

        # CREATE SERIES STANDINGs DATAFRAME
        standings_df = (
            points_df.join(result_df.select("subsession_id", "race_name").unique(), on="subsession_id")
            .sort("subsession_id")
            .pivot(on="race_name", index="user_id", values=["points", "drop", "fastest_lap", "cleanest_driver"])
            .join(
                current_points_df["user_id", "cumulative_points", "rank", "rank_change"].rename(
                    {"cumulative_points": "points_total", "rank": "points_rank", "rank_change": "points_rank_change"}
                ),
                on="user_id",
            )
            # .join(points_summary_df.rename({"points": "points_total", "rank": "points_rank"}), on="user_id")
            .join(
                current_elo_df.select("user_id", "rating", "rank", "rank_change", "num_contests").rename(
                    {"rank": "rating_rank", "num_contests": "num_races", "rank_change": "rating_rank_change"}
                ),
                on="user_id",
            )
            .join(name_df, on="user_id")
            .sort("points_rank")
        )

        outputs["race_results"].append(race_result_df)
        outputs["standings"].append(standings_df)
        outputs["points"].append(current_points_df)
        outputs["ELO"].append(current_elo_df)

    return outputs
