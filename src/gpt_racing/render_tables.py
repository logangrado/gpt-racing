#!/usr/bin/env python3

import re
import great_tables as GT
import polars as pl

from gpt_racing import utils


def render_standings(standings_df: pl.DataFrame):
    race_cols = [col.replace("points_", "") for col in standings_df.columns if col.startswith("points_race_")]
    race_col_map = {"points_" + col: col.replace("_", " ").title() for col in race_cols}

    select_cols = {
        **{
            "points_rank": "Rank",
            "display_name": "Name",
        },
        **race_col_map,
        **{
            "points_total": "Total Points",
            "rating": "Rating",
            "rating_rank": "Rating Rank",
        },
    }

    standings_df = standings_df.sort("points_rank")
    df = standings_df.select(select_cols.keys()).rename(select_cols)

    # Format None values in race cols
    for col in race_col_map.values():
        df = df.with_columns(pl.col(col).fill_null(""))

    gt = GT.GT(df).tab_header(title="Standings")

    gt = gt.tab_style(
        style=GT.style.borders(sides="right", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="Name"),
    )

    gt = gt.tab_style(
        style=GT.style.borders(sides="left", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="Total Points"),
    )

    # Format drop races
    for race_col in race_cols:
        gt_col = race_col_map["points_" + race_col]
        drop_col = "drop_" + race_col

        gt = gt.tab_style(
            style=GT.style.fill("#bbbbbb"),
            locations=GT.loc.body(columns=gt_col, rows=standings_df[drop_col].arg_true().to_list()),
        )

    # Format race col names
    formatted_race_cols = {
        value: GT.html(re.sub("(Race \d+) (.*)", r"\1<br>\2", value)) for value in race_col_map.values()
    }
    gt = gt.cols_label(**formatted_race_cols)

    gt = gt.tab_options(
        data_row_padding="1px",
    )
    #     .tab_options(
    # source_notes_font_size='x-small',
    # source_notes_padding=3,
    # table_font_names=system_fonts("humanist"),
    # data_row_padding='1px',
    # heading_background_color='antiquewhite',
    # source_notes_background_color='antiquewhite',
    # column_labels_background_color='antiquewhite',
    # table_background_color='snow',
    # data_row_padding_horizontal=3,
    # column_labels_padding_horizontal=58
    # ) \

    return gt


def _format_rating(row) -> str:
    out = f"{row['rating']} ("
    if row["rating_change"] > 0:
        out += "+"
    out = out + f"{row['rating_change']})"
    return out


def render_race_results(df: pl.DataFrame):
    df = df.sort("finish_position").with_columns(
        pl.col("qualify_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.col("average_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.col("fastest_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.struct(["rating", "rating_change"]).map_elements(_format_rating, return_dtype=str).alias("rating"),
        pl.col("start_position").cast(pl.Int32),
    )

    select_cols = {
        "finish_position": "Pos",
        "display_name": "Name",
        "qualify_lap_time": "Qual. Lap",
        "start_position": "Start",
        "interval": "Interval",
        "average_lap_time": "Avg. Lap",
        "fastest_lap_time": "Best Lap",
        "points": "Points",
        "rating": "Rating",
        # Qual lap time
        # Average lap time
    }

    df = df.select(select_cols.keys()).rename(select_cols)
    gt = GT.GT(df)

    gt = gt.tab_options(
        data_row_padding="1px",
    )
    gt = gt.tab_style(
        style=GT.style.borders(sides="right", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="Name"),
    )

    return gt
