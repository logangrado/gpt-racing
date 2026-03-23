#!/usr/bin/env python3

import json
import re
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
    from gpt_racing import core, output, render_tables
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
        for label, html in output.standings_htmls(table, config.render):
            suffix = f"_{label.lower()}" if label else ""
            _render_and_write_table(gt=html, path=pdf_out_path / f"standings_race_{i + 1}{suffix}.html")

    for i, table in enumerate(out["race_results"]):
        for label, html in output.race_result_htmls(table, config.render):
            suffix = f"_{label.lower()}" if label else ""
            _render_and_write_table(gt=html, path=pdf_out_path / f"results_race_{i + 1}{suffix}.html")

    for i, table in enumerate(out["ELO"]):
        for label, html in output.ratings_htmls(table, config.render):
            suffix = f"_{label.lower()}" if label else ""
            _render_and_write_table(gt=html, path=pdf_out_path / f"elo_race_{i + 1}{suffix}.html")


def append_races_to_config(config_path: Path, new_races: list[dict]) -> None:
    """Insert new race entries before the closing ] of the races array in a jsonnet config."""
    text = config_path.read_text()
    match = re.search(r"\braces:\s*\[", text)
    if match is None:
        raise ValueError("Could not find 'races: [' in config")
    bracket_start = match.end() - 1  # position of '['
    depth = 0
    bracket_end = None
    for i, ch in enumerate(text[bracket_start:], bracket_start):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                bracket_end = i
                break
    if bracket_end is None:
        raise ValueError("Could not find closing ] for races array")
    line_start = text.rfind("\n", 0, bracket_end) + 1
    closing_indent = text[line_start:bracket_end]  # whitespace before ']'
    entry_indent = closing_indent + "  "
    entries = "".join(
        f"{entry_indent}{{ subsession_id: {r['subsession_id']}, race_name: '{r['track_name']}' }},\n" for r in new_races
    )
    config_path.write_text(text[:line_start] + entries + text[line_start:])


def update_season_entrypoint(config_path, league_id: int, season_id: int) -> None:
    from gpt_racing.iracing_data import IracingDataClient
    from gpt_racing.league import fetch_new_races

    config_dict = _load_config(config_path)
    existing_ids = {r["subsession_id"] for r in config_dict["races"]}
    client = IracingDataClient()
    new_races = fetch_new_races(client, league_id, season_id, existing_ids)
    if not new_races:
        print("No new races found.")
        return
    append_races_to_config(Path(config_path), new_races)
    for r in new_races:
        print(f"Added: subsession_id={r['subsession_id']} ({r['track_name']})")


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


@cli.command("update-season")
@click.argument("config")
@click.argument("league_id", type=int)
@click.argument("season_id", type=int)
def update_season(config, league_id, season_id):
    update_season_entrypoint(config, league_id, season_id)


if __name__ == "__main__":
    cli()
