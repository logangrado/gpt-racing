#!/usr/bin/env python3


import pandas as pd

from gpt_racing.iracing_data import IracingDataClient
from gpt_racing.elo_mmr import compute_elo_mmr


def _load_race_data(config, client):
    data = pd.concat([client.get_race_result(race_config.subsession_id) for race_config in config.races])

    data = data.rename(
        columns={
            "subsession_id": "contest_id",
            "session_end_time": "contest_time",
        }
    )

    data = data.reset_index(drop=True)

    return data


def compute_ratings(config, client):
    """
    Compute ratings given a config
    """
    race_data = _load_race_data(config, client)

    elo_df = compute_elo_mmr(race_data)

    name_df = race_data[["user_id", "display_name"]].drop_duplicates(subset="user_id")

    # Join display name
    elo_df = elo_df.merge(name_df, on="user_id", how="left")

    return elo_df
