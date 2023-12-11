#!/usr/bin/env python3

import tempfile
from pathlib import Path
import json
import shutil

import subprocess

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
    csv_path = path / "all_players.csv"

    result = pd.read_csv(csv_path)

    result = result.rename(
        columns={
            "handle": "user_id",
            "display_rating": "rating",
        }
    )

    result = result[["rank", "user_id", "rating"]]

    return result


def compute_elo_mmr(data: pd.DataFrame) -> pd.DataFrame:
    # Check required data columns
    req_cols = {"contest_id", "contest_time", "user_id", "finish_position"}
    missing_cols = req_cols - set(data.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    dataset_name = "gtp_dataset"
    dataset_path = ELO_MMR_CACHE_PATH / dataset_name
    result_path = ELO_MMR_RESULT_PATH / dataset_name
    if dataset_path.exists():
        shutil.rmtree(dataset_path)
    if result_path.exists():
        shutil.rmtree(result_path)

    # Write data for elo-mmr to consume
    _write_data(data, dataset_path)

    # Run elo-mmr
    command = ["cargo", "run", "--bin", "rate", "mmr-fast", dataset_name]
    subprocess.run(command, cwd=ELO_MMR_PATH, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Read back results
    result = _read_result(result_path)

    return result
