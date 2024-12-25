#!/usr/bin/env python3

import click
from pathlib import Path
import json
import shutil


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

    from gpt_racing import render_tables

    config = RatingConfig(**_load_config(config))
    client = IracingDataClient()

    out = core.compute_ratings(config, client)

    # render_tables.render_standings(out["standings"][-1]).show()

    # Write outputs
    out_path = Path("out")
    sentinel = out_path / ".sentinel"
    if out_path.is_dir():
        if sentinel.is_file():
            shutil.rmtree(out_path)
        else:
            print(
                f"Path exists, and is not an output dir: {out_path}\nPlease manually remove this dir or select a new one"
            )
            return

    out_path.mkdir(parents=True)
    sentinel.touch()

    csv_out_path = out_path / "csv"
    csv_out_path.mkdir(parents=True, exist_ok=True)
    for key, tables in out.items():
        for i, table in enumerate(tables):
            table.write_csv(csv_out_path / f"{key}_race_{i+1}.csv")

    pdf_out_path = out_path / "pdf"
    pdf_out_path.mkdir(parents=True, exist_ok=True)
    for i, table in enumerate(out["standings"]):
        gt = render_tables.render_standings(table)
        path = pdf_out_path / f"standings_race_{i+1}.html"

        with open(path, "w") as f:
            if isinstance(gt, str):
                f.write(gt)
            else:
                f.write(gt.as_raw_html())

    for i, table in enumerate(out["race_results"]):
        gt = render_tables.render_race_results(table)
        path = pdf_out_path / f"result_race_{i+1}.html"

        with open(path, "w") as f:
            if isinstance(gt, str):
                f.write(gt)
            else:
                f.write(gt.as_raw_html())


if __name__ == "__main__":
    core()
