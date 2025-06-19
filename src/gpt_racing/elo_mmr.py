#!/usr/bin/env python3

import tempfile
from pathlib import Path
import json
import shutil
from typing import Tuple, Optional

import subprocess

import elommr
import numpy as np
import polars as pl

from gpt_racing.config import ELOConfig


class ELOMMR:
    def __init__(self, elo_config: Optional[ELOConfig] = None):
        if elo_config is None:
            elo_config = ELOConfig()

        self._elo_mmr = elommr.EloMMR(
            drift_per_sec=0,
            weight_limit=1,
        )
        self._players = {}
        self._config = elo_config
        self._participation_df = None
        self._name_df = None
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

    def _update_name_df(self, name_df: pl.DataFrame):
        if self._name_df is None:
            self._name_df = name_df
        else:
            self._name_df = pl.concat([self._name_df, name_df]).unique()

    def update(self, df: pl.DataFrame, name_df: pl.DataFrame):
        """
        Update ratings with a dataframe of contests
        """
        self._update_name_df(name_df)

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
        rank_df = (
            all_rounds_df.filter(pl.col("num_valid_contests") >= self._config.min_races)
            .with_columns(pl.col("rating").rank("min", descending=True).over("contest_id").cast(pl.Int32).alias("rank"))
            .select(["user_id", "contest_id", "rank"])
        )

        all_rounds_df = all_rounds_df.join(rank_df, on=["user_id", "contest_id"], how="left")

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

        # Join in names
        all_rounds_df = all_rounds_df.join(self._name_df, on="user_id", how="left")

        return all_rounds_df
