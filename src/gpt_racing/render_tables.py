#!/usr/bin/env python3

import functools
import re
import great_tables as GT
import polars as pl

from gpt_racing import utils


def _add_row_striping(table):
    return table.tab_options(row_striping_include_table_body=True, row_striping_background_color="#eeeeee")


def _format_change(x, dx, rank=False):
    if dx is None:
        dx = ""
    else:
        if rank:
            if dx > 0:
                dx = f"(<span style='color:red'>↓{abs(dx)}</span>)"
            elif dx == 0:
                dx = "(-)"
            else:
                dx = f"(<span style='color:green'>↑{abs(dx)}</span>)"
        else:
            if dx > 0:
                dx = f"(<span style='color:green'>+{abs(dx)}</span>)"
            elif dx == 0:
                dx = "(-)"
            else:
                dx = f"(<span style='color:red'>-{abs(dx)}</span>)"

    # Center on space
    out = f"""<div style='display: flex; align-items: center; line-height: 1; width: auto;'><div style='flex: 1; text-align: right; padding-right: 4px;'>{x}</div><div style='flex: 1; text-align: left; padding-left: 4px;'>{dx}</div></div>"""

    out = f"""
    <div class="progress-ww">
      <div><span>{x}</span> - <span>{dx}</span></div>
    </div>
    """

    return out


def _format_change_only(dx, rank=False):
    if dx is None:
        dx = ""
    else:
        if rank:
            if dx > 0:
                dx = f"(<span style='color:red'>↓{abs(dx)}</span>)"
            elif dx == 0:
                dx = "(-)"
            else:
                dx = f"(<span style='color:green'>↑{abs(dx)}</span>)"
        else:
            if dx > 0:
                dx = f"(<span style='color:green'>+{abs(dx)}</span>)"
            elif dx == 0:
                dx = "(-)"
            else:
                dx = f"(<span style='color:red'>-{abs(dx)}</span>)"

    return dx


def _combine_column_headers(html, label, columns):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html)
    # Find the header row with class 'gt_col_headings'
    header_row = soup.find("tr", class_="gt_col_headings")
    if not header_row:
        raise ValueError("Header row with class 'gt_col_headings' not found.")

    col_headers = header_row.find_all("th")

    # Get col header indices
    col_header_idx = [i for i, c in enumerate(col_headers) if c["id"] in (columns)]
    if len(col_header_idx) != len(columns):
        raise ValueError("Missing some columns")

    if len([True for x, y in zip(col_header_idx[1:], col_header_idx[:-1]) if (x - y) > 1]) > 0:
        raise ValueError("Some columns are not next to eachother")

    col_headers[col_header_idx[0]]["id"] = label
    col_headers[col_header_idx[0]]["colspan"] = str(len(columns))
    col_headers[col_header_idx[0]]["class"][-1] = "gt_center"

    for i in col_header_idx[1:][::-1]:
        col_headers[i].decompose()
    return str(soup)


def _render_html(html):
    import tempfile
    import webbrowser
    from pathlib import Path
    import time

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "page.html"
        with open(path, "w") as f:
            f.write(html)

        webbrowser.open("file://" + str(path))
        time.sleep(5)


def render_standings(standings_df: pl.DataFrame):
    race_cols = [col.replace("points_", "") for col in standings_df.columns if col.startswith("points_race_")]
    race_col_map = {"points_" + col: col.replace("_", " ").title() for col in race_cols}

    standings_df = standings_df.sort("points_rank").with_columns(
        pl.col("points_rank_change").map_elements(
            functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        ),
        pl.when(pl.col("rating_rank").is_not_null())
        .then(pl.col("rating_rank"))
        .otherwise(pl.lit(""))
        .alias("rating_rank"),
        pl.when(pl.col("rating_rank").is_not_null()).then(pl.col("rating")).otherwise(pl.lit("")).alias("rating"),
        pl.col("rating_rank_change").map_elements(
            functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        ),
    )

    select_cols = {
        **{
            "points_rank": "Rank",
            "points_rank_change": "points_rank_change",
            "display_name": "Name",
            "rating": "Rating",
            "rating_rank": "Rating Rank",
            "rating_rank_change": "rating_rank_change",
        },
        **race_col_map,
        **{
            "points_total": "Total Points",
        },
    }

    df = standings_df.select(select_cols.keys()).rename(select_cols)

    # Format None values in race cols
    for col in race_col_map.values():
        df = df.with_columns(pl.col(col).fill_null(""))

    gt = GT.GT(df).tab_header(title="Standings")

    gt = gt.tab_style(
        style=GT.style.borders(sides="right", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="rating_rank_change"),
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

    gt = _add_row_striping(gt)
    gt = gt.tab_style(style=GT.style.text(align="right"), locations=GT.loc.body(columns="Rating Rank"))

    table_html = gt.render("html")
    table_html = _combine_column_headers(table_html, "Rank", ["Rank", "points_rank_change"])
    table_html = _combine_column_headers(table_html, "Rating Rank", ["Rating Rank", "rating_rank_change"])

    return table_html


def _format_rating(row) -> str:
    if row["rating_change"] > 0:
        rating_change = f"<span style='color:green'>+{row['rating_change']}</span>"
    else:
        rating_change = f"<span style='color:red'>{row['rating_change']}</span>"

    # Center on space
    out = f"""
        <div style='display: flex; align-items: center; line-height: 1; width: 120px;'>
            <div style='flex: 1; text-align: right; padding-right: 4px;'>{row['rating']}</div>
            <div style='flex: 1; text-align: left; padding-left: 4px;'>({rating_change})</div>
        </div>
    """
    return out


def _format_fastest_lap(row) -> str:
    out = utils.seconds_to_str(row["fastest_lap_time"])
    if row["fastest_lap"]:
        out = f"<span style='color:DarkViolet; font-weight:bold'>{out}</span>"
    return out


def _format_incidents(row) -> str:
    out = str(row["num_incidents"])
    if row["cleanest_driver"]:
        out = f"<span style='color:#007BFF; font-weight:bold'>{out}</span>"
    return out


def render_race_results(df: pl.DataFrame):
    df = df.sort("finish_position").with_columns(
        pl.col("qualify_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.col("average_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.struct(["fastest_lap_time", "fastest_lap"]).map_elements(_format_fastest_lap, return_dtype=str),
        pl.struct(["num_incidents", "cleanest_driver"]).map_elements(_format_incidents, return_dtype=str),
        # pl.struct(["rating", "rating_change"]).map_elements(_format_rating, return_dtype=str).alias("rating"),
        pl.when(pl.col("rank").is_not_null()).then(pl.col("rank")).otherwise(pl.lit("")).alias("rank"),
        # Render RATING column - None if we aren't rated
        pl.when(pl.col("rank").is_not_null()).then(pl.col("rating")).otherwise(pl.lit("")).alias("rating"),
        # Render rating change column
        pl.when(pl.col("rank").is_not_null())
        .then(
            pl.col("rating_change").map_elements(
                functools.partial(_format_change_only), return_dtype=str, skip_nulls=False
            )
        )
        .otherwise(pl.lit(""))
        .alias("rating_change"),
        pl.col("start_position").cast(pl.Int32),
    )

    select_cols = {
        "finish_position": "Pos",
        "display_name": "Name",
        "rating": "Rating",
        "rating_change": "rating_change",
        "qualify_lap_time": "Qual. Lap",
        "start_position": "Start",
        "interval": "Interval",
        "average_lap_time": "Avg. Lap",
        "fastest_lap_time": "Best Lap",
        "num_incidents": "Inc",
        "points": "Points",
    }

    df = df.select(select_cols.keys()).rename(select_cols)
    gt = GT.GT(df)

    gt = gt.tab_options(
        data_row_padding="1px",
    )
    gt = gt.tab_style(
        style=GT.style.borders(sides="right", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="rating_change"),
    )
    gt = _add_row_striping(gt)

    table_html = gt.render("html")
    table_html = _combine_column_headers(table_html, "Rating", ["Rating", "rating_change"])

    return table_html
