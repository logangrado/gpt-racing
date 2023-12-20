#!/usr/bin/env python3

import tempfile
from pathlib import Path
import json
import shutil

import subprocess

import elommr
import numpy as np
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


def compute_elo_mmr(data: pd.DataFrame) -> pd.DataFrame:
    elo_mmr = elommr.EloMMR(
        drift_per_sec=0,
        weight_limit=1,
    )

    players = {}

    round_dfs = []
    for contest_id, contest_df in data.sort_values("contest_id").groupby("contest_id"):
        standings = []
        for i, row in contest_df.iterrows():
            player = players.get(row["user_id"], None)
            if player is None:
                player = elommr.Player()
                players[row["user_id"]] = player

            standings.append(
                (
                    player,
                    row["finish_position"],
                    row["finish_position"] + (contest_df["finish_position"] == row["finish_position"]).sum() - 1,
                )
            )

        elo_mmr.round_update(standings)

        # Collect results. Collect for all existing players, even if they didn't compete in this round
        round_data = []
        for user_id, player in players.items():
            num_contests = len(player.event_history)
            current_rating = player.event_history[-1].mu
            last_rating = 1500 if num_contests < 2 else player.event_history[-2].mu
            rating_change = current_rating - last_rating
            participated = any(contest_df["user_id"] == user_id)
            # User didn't compete this round
            if not participated:
                rating_change = None

            round_data.append(
                {
                    "user_id": user_id,
                    "rating": current_rating,
                    "rating_change": rating_change,
                    "num_contests": num_contests,
                    "participated": participated,
                }
            )
        round_df = pd.DataFrame(round_data).sort_values("rating", ascending=False)
        round_df["rank"] = round_df["rating"].rank(method="min", ascending=False).astype("Int64")
        round_df["rating_change"] = round_df["rating_change"].astype("Int64")
        round_df["contest_id"] = contest_id
        round_dfs.append(round_df)

    all_rounds_df = pd.concat(round_dfs).reset_index(drop=True)

    all_rounds_df = all_rounds_df.sort_values(["user_id", "contest_id"])
    all_rounds_df["rank_previous"] = all_rounds_df.groupby("user_id")["rank"].shift().astype("Int64")
    all_rounds_df["rank_change"] = all_rounds_df["rank"] - all_rounds_df["rank_previous"]
    all_rounds_df = (
        all_rounds_df.sort_values(["contest_id", "rank"]).drop(columns="rank_previous").reset_index(drop=True)
    )

    return all_rounds_df


def _compute_elo_mmr_rust(data: pd.DataFrame) -> pd.DataFrame:
    # Check required data columns
    req_cols = {"contest_id", "contest_time", "user_id", "finish_position"}
    missing_cols = req_cols - set(data.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        config_path = tmpdir / "config.json"

        dataset_name = "gpt_dataset"
        dataset_path = ELO_MMR_CACHE_PATH / dataset_name
        result_path = ELO_MMR_RESULT_PATH / dataset_name
        if dataset_path.exists():
            shutil.rmtree(dataset_path)
        if result_path.exists():
            shutil.rmtree(result_path)

        # Write data for elo-mmr to consume
        _write_data(data, dataset_path)

        config = {
            "mu_noob": 1500,
            "sig_noob": 350,
            "contest_source": dataset_name,
            "system": {
                "method": "mmr",
                "params": [0.23, 54.0, 0, 100.0, 2.0, 0.04],
            },
        }
        _write_config(config, config_path)

        # Run elo-mmr
        command = ["cargo", "run", "--bin", "rate_from_configs", config_path]

        command = ["cargo", "run", "--bin", "rate", "mmr", dataset_name]
        subprocess.run(command, cwd=ELO_MMR_PATH)  # , stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        import ipdb

        ipdb.set_trace()
        pass
        # Read back results
        result = _read_result(result_path)

    return result
