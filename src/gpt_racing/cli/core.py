#!/usr/bin/env python3

import click
from pathlib import Path
import json


def _load_config(path):
    import _jsonnet

    path = Path(path)

    config = json.loads(_jsonnet.evaluate_file(str(path)))

    return config


@click.command
@click.argument("config")
def core(config):
    from gpt_racing.config import RatingConfig
    from gpt_racing import core
    from gpt_racing.iracing_data import IracingDataClient

    config = RatingConfig(**_load_config(config))
    client = IracingDataClient()

    rating_df, result_df = core.compute_ratings(config, client)

    out = Path("out")
    out.mkdir(parents=True, exist_ok=True)
    rating_df.to_csv(out / "ranking.csv", index=False)
    result_df.to_csv(out / "results.csv", index=False)


if __name__ == "__main__":
    core()
