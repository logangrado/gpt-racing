#!/usr/bin/env python3

from pathlib import Path
from io import StringIO
import json

import numpy as np
import pandas as pd
import click


def _norm_columns(df):
    return df.rename(columns={c: c.lower().replace(" ", "_") for c in df.columns})


def _parse_header_lines(lines):
    keys = ",".join(lines[::2])
    values = ",".join(lines[1::2])

    csv_str = "\n".join([keys, values])
    meta_df = pd.read_csv(StringIO(csv_str))
    meta_df = _norm_columns(meta_df)

    # Convert dt col
    meta_df["start_time"] = pd.to_datetime(meta_df["start_time"]).dt.tz_localize(None)

    return meta_df


def _parse_pca_csv(path):
    lines = []
    with open(path, "r") as f:
        lines = f.readlines()

    lines = [l.strip() for l in lines]
    lines = [l for l in lines if l]

    race_meta_df = _parse_header_lines(lines[:4])
    data = "\n".join(lines[4:])
    series_df = pd.read_csv(StringIO(data))
    series_df = _norm_columns(series_df)

    series_df["start_time"] = race_meta_df["start_time"].iloc[0]
    series_df["track"] = race_meta_df["track"].iloc[0]

    return race_meta_df, series_df


def _import_data(path):
    path = Path(path)

    series_dfs = []
    meta_dfs = []
    for p in path.glob("**/*.csv"):
        meta_df, series_df = _parse_pca_csv(p)
        series_dfs.append(series_df)
        meta_dfs.append(meta_df)

    series_df = pd.concat(series_dfs)
    meta_df = pd.concat(meta_dfs)

    # Fix/update data
    meta_df = meta_df.sort_values("start_time").reset_index(drop=True)
    meta_df["race_id"] = np.arange(len(meta_df))

    series_df = series_df.merge(meta_df[["start_time", "track", "race_id"]], on=["start_time", "track"])
    return meta_df, series_df


def _export_to_json(series_df, dest):
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    for (race_id, track), race_df in series_df.groupby(["race_id", "track"]):
        race_dest = dest / f"{race_id}.json"
        race_data = {}

        race_data["name"] = track
        race_data["time_seconds"] = 1
        race_data["standings"] = []
        for i, row in race_df[["name", "fin_pos"]].iterrows():
            race_data["standings"].append([row["name"], row["fin_pos"], row["fin_pos"]])

        with open(race_dest, "w") as f:
            json.dump(race_data, f)


@click.command
@click.argument("SRC")
@click.argument("DEST")
def main(src, dest):
    """
    Import PCA results from csv, and write out to json for consumption by ELO-MMR
    """
    print(f"{src} -> {dest}")
    meta_df, series_df = _import_data(src)
    _export_to_json(series_df, dest)


if __name__ == "__main__":
    main()
