#!/usr/bin/env python3

import tempfile
from pathlib import Path
import json
import shutil
from typing import Tuple

import subprocess

import elommr
import numpy as np
import polars as pl
import pandas as pd

from gpt_racing import ELO_MMR_PATH, ELO_MMR_CACHE_PATH, ELO_MMR_RESULT_PATH


def _write_data(data, path):
    path.mkdir(parents=True)

    contest_df = data[["contest_id", "contest_time"]].drop_duplicates().sort_values("contest_time")
    contest_df["contest_index"] = np.arange(len(contest_df))

    data = data.merge(contest_df[["contest_id", "contest_index"]], on="contest_id")

    for (contest_index, contest_id), contest_df in data.groupby(["contest_index", "contest_id"]):
        contest_dest = path / f"{contest_index}.json"
        contest_data = {}

        contest_data["name"] = str(contest_id)
        contest_data["time_seconds"] = 1
        contest_data["standings"] = []
        for i, row in contest_df.iterrows():
            contest_data["standings"].append(
                [str(row["user_id"]), int(row["finish_position"]), int(row["finish_position"])]
            )

        with open(contest_dest, "w") as f:
            json.dump(contest_data, f, indent=2)


def _read_result(path):
    # Read all_players result
    csv_path = path / "all_players.csv"
    result = pd.read_csv(csv_path)
    result = result.rename(
        columns={
            "handle": "user_id",
            "display_rating": "rating",
        }
    )

    import ipdb

    ipdb.set_trace()
    pass
    result = result[["rank", "user_id", "rating", "num_contests"]]

    players_path = path / "players"
    player_dfs = []
    for player_path in players_path.glob("*.csv"):
        player_df = pd.read_csv(player_path)
        player_df["user_id"] = player_path.stem
        player_dfs.append(player_df)

    player_df = pd.concat(player_dfs)

    import ipdb

    ipdb.set_trace()
    pass

    return result


def _write_config(config, config_path):
    with open(config_path, "w") as f:
        json.dump(config, f)


class ELOMMR:
    def __init__(self, elo_config):
        self._elo_mmr = elommr.EloMMR(
            drift_per_sec=0,
            weight_limit=1,
        )
        self._players = {}
        self._config = elo_config
        self._participation_df = None
        self._history = []

    def _get_or_create_player(self, player_id):
        player = self._players.get(player_id)

        if player is None:
            player = elommr.Player()
            self._players[player_id] = player

        return player

    def _update_participation_record(self, contest_df):
        participation_df = contest_df["contest_id", "contest_date", "user_id"]

        if self._participation_df is None:
            self._participation_df = participation_df
        else:
            self._participation_df = pl.concat([self._participation_df, participation_df])

    def update(self, df: pl.DataFrame):
        """
        Update ratings with a dataframe of contests
        """
        for (contest_id, contest_date), contest_df in df.sort("contest_id").group_by(
            "contest_id", "contest_date", maintain_order=True
        ):
            standings = []
            self._update_participation_record(contest_df)

            for row in contest_df.iter_rows(named=True):
                player = self._get_or_create_player(row["user_id"])

                standings.append(
                    (
                        player,
                        row["finish_position"],
                        row["finish_position"] + (contest_df["finish_position"] == row["finish_position"]).sum() - 1,
                    )
                )

            self._elo_mmr.round_update(standings=standings, contest_time=contest_date.timestamp())

            # Need to record all ratings each time
            round_data = []
            for user_id, player in self._players.items():
                # Collect for all users, regardless of participation
                num_contests = len(player.event_history)
                current_rating = player.event_history[-1].mu
                last_rating = 1500 if num_contests < 2 else player.event_history[-2].mu
                participated = any(contest_df["user_id"] == user_id)

                round_data.append(
                    {
                        "user_id": user_id,
                        "rating": current_rating,
                        "last_rating": last_rating,
                        "num_contests": num_contests,
                        "participated": participated,
                    }
                )
            round_df = pl.DataFrame(round_data).with_columns(
                pl.lit(contest_id).alias("contest_id").cast(df["contest_id"].dtype),
                pl.lit(contest_date).alias("contest_date"),
            )
            self._history.append(round_df)

    def collect_results(self):
        # Collect results. Collect for all existing players, even if they didn't compete in this round
        all_rounds_df = pl.concat(self._history)

        if self._config.time_window:
            contest_counts = (
                all_rounds_df.join(all_rounds_df["user_id", "contest_date", "participated"], on="user_id", suffix="_y")
                .filter(
                    (pl.col("contest_date_y") >= pl.col("contest_date") - self._config.time_window)
                    & (pl.col("contest_date_y") <= pl.col("contest_date"))
                    & pl.col("participated_y")
                )
                .group_by("user_id", "contest_id")
                .agg(pl.len().alias("num_valid_contests"))
            )
        else:
            contest_counts = all_rounds_df.select(["user_id", "contest_id"]).with_columns(pl.lit(1).alias("_temp"))
            contest_counts = contest_counts.with_columns(
                pl.col("_temp").cum_sum().over("user_id").alias("num_valid_contests")
            )
            contest_counts = contest_counts.drop("_temp")

        all_rounds_df = all_rounds_df.join(contest_counts, on=["user_id", "contest_id"], how="left")

        # COMPUTE RANK
        all_rounds_df = all_rounds_df.with_columns(
            pl.when(pl.col("num_valid_contests") >= self._config.min_races)
            .then(pl.col("rating").rank("min", descending=True).over("contest_id"))
            .otherwise(None)  # Fill non-matching rows with None
            .cast(pl.Int32)
            .alias("rank")
        )

        # COMPUTE RANK CHANGE
        all_rounds_df = all_rounds_df.with_columns(
            (pl.col("rank") - pl.col("rank").shift(1).over("user_id")).alias("rank_change")
        )

        all_rounds_df = all_rounds_df.sort(
            ["contest_id", "rank", "rating"], nulls_last=True, descending=[False, False, True]
        )

        all_rounds_df = all_rounds_df.with_columns(
            pl.when(pl.col("participated") == True)
            .then(pl.col("rating") - pl.col("last_rating"))
            .otherwise(pl.lit(None))
            .alias("rating_change")
        ).drop("last_rating")

        return all_rounds_df
