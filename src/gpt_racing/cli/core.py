#!/usr/bin/env python3

import json
import shutil
from pathlib import Path

import click
import polars as pl


def _load_config(path):
    import _jsonnet

    path = Path(path)

    config = json.loads(_jsonnet.evaluate_file(str(path)))

    return config


def _render_and_write_table(gt, path):
    with open(path, "w") as f:
        if isinstance(gt, str):
            f.write(gt)
        else:
            f.write(gt.as_raw_html())


def core_entrypoint(config, out_path, _overwrite=False, client=None):
    from gpt_racing import core, render_tables
    from gpt_racing.config import RatingConfig
    from gpt_racing.iracing_data import IracingDataClient

    config = RatingConfig(**_load_config(config))
    if client is None:
        client = IracingDataClient()

    # render_tables.render_standings(out["standings"][-1]).show()

    # Write outputs
    out_path = Path(out_path)
    sentinel = out_path / ".sentinel"
    if out_path.is_dir():
        if sentinel.is_file() or _overwrite:
            shutil.rmtree(out_path)
        else:
            print(
                f"Path exists, and is not an output dir: {out_path}\nPlease manually remove this dir or select a new one"
            )
            return

    out = core.compute_ratings(config, client)
    out_path.mkdir(parents=True)
    sentinel.touch()

    csv_out_path = out_path / "csv"
    csv_out_path.mkdir(parents=True, exist_ok=True)
    for key, tables in out.items():
        for i, table in enumerate(tables):
            # float_cols = [c for c, t in zip(table.columns, table.dtypes) if t in (pl.Float32, pl.Float64)]
            table.write_csv(csv_out_path / f"{key}_race_{i + 1}.csv", float_precision=4)

    pdf_out_path = out_path / "pdf"
    pdf_out_path.mkdir(parents=True, exist_ok=True)
    for i, table in enumerate(out["standings"]):
        _render_and_write_table(
            gt=render_tables.render_standings(table), path=pdf_out_path / f"standings_race_{i + 1}.html"
        )

    for i, table in enumerate(out["race_results"]):
        _render_and_write_table(
            gt=render_tables.render_race_results(table), path=pdf_out_path / f"results_race_{i + 1}.html"
        )

    for i, table in enumerate(out["ELO"]):
        _render_and_write_table(gt=render_tables.render_ratings(table), path=pdf_out_path / f"elo_race_{i + 1}.html")


def list_drivers_entrypoint(config):
    from gpt_racing.config import RatingConfig
    from gpt_racing.core import _load_race_data
    from gpt_racing.iracing_data import IracingDataClient

    config = RatingConfig(**_load_config(config))
    client = IracingDataClient()

    _, name_df = _load_race_data(config.races, client)

    for row in name_df.sort("display_name").iter_rows(named=True):
        print(f"{{ name: '{row['display_name']}' }},")


@click.group
def cli():
    pass


@cli.command("run")
@click.argument("config")
@click.argument("out_path")
def core(config, out_path):
    core_entrypoint(config, out_path)


@cli.command("list-drivers")
@click.argument("config")
def list_drivers(config):
    list_drivers_entrypoint(config)


if __name__ == "__main__":
    cli()
