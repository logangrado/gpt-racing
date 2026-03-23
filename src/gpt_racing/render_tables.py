#!/usr/bin/env python3

import functools
import re

import great_tables as GT
import polars as pl
from bs4 import BeautifulSoup

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


def _format_change_only(
    dx: float,
    rank: bool = False,
    skip_zero: bool = False,
    precision: int = 0,
    invert_color: bool = False,
    is_time: bool = False,
) -> str:
    if dx is None or (skip_zero and dx == 0):
        out = ""
    else:
        val_fmt = abs(dx)
        if is_time:
            val_fmt = utils.seconds_to_str(val_fmt, precision=precision)

        if dx == 0:
            out = "(-)"
        else:
            # Determine base color based on whether higher or lower is better
            if rank:
                # For ranks, lower is better
                if dx > 0:
                    base_color = "red"  # Rank increased (worse)
                    symbol = "↓"
                else:
                    base_color = "green"  # Rank decreased (better)
                    symbol = "↑"
            else:
                # For normal values, higher is better
                if dx > 0:
                    base_color = "green"  # Value increased (better)
                    symbol = "+"
                else:
                    base_color = "red"  # Value decreased (worse)
                    symbol = "-"

            # Apply color inversion if needed
            if invert_color:
                color = "red" if base_color == "green" else "green"
            else:
                color = base_color

            out = f"(<span style='color:{color}'>{symbol}{val_fmt}</span>)"

    return out


def _combine_column_headers(html, label, columns):
    soup = BeautifulSoup(html, "html.parser")
    # Find the header row with class 'gt_col_headings'
    header_row = soup.find("tr", class_="gt_col_headings")
    if not header_row:
        raise ValueError("Header row with class 'gt_col_headings' not found.")

    col_headers = header_row.find_all("th")

    # Get col header indices
    col_header_idx = [i for i, c in enumerate(col_headers) if c["id"] in (columns)]
    if len(col_header_idx) != len(columns):
        found_ids = {c["id"] for c in col_headers}
        missing = [col for col in columns if col not in found_ids]
        raise ValueError(f"Missing columns: {missing}")

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
    import time
    import webbrowser
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "page.html"
        with open(path, "w") as f:
            f.write(html)

        webbrowser.open("file://" + str(path))
        time.sleep(5)


def _format_class(row) -> str:
    symbol = row["class_symbol"]
    color = row["class_color"]
    return f"<span style='background-color:{color}; padding: 2px 6px; border-radius: 3px;'>{symbol}</span>"


def _format_rating_value(value, change):
    out = str(value)
    color = "DimGrey"
    color = "coral"
    color = "LightCoral"
    if change is None:
        out = f"<span style='color:{color}'>{out}*</span>"

    return out


def _format_rank_value(value):
    if value is None:
        return ""
    return str(value)


def _format_rating(row) -> str:
    if row["rating_change"] > 0:
        rating_change = f"<span style='color:green'>+{row['rating_change']}</span>"
    else:
        rating_change = f"<span style='color:red'>{row['rating_change']}</span>"

    # Center on space
    out = f"""
        <div style='display: flex; align-items: center; line-height: 1; width: 120px;'>
            <div style='flex: 1; text-align: right; padding-right: 4px;'>{row["rating"]}</div>
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


def _fix_gt_id(html: str) -> str:
    """
    Replace the randomly generated gt HTML ID with a constant '000000'.

    This modifies both the ID attribute on the <div> and all CSS selectors
    that reference it (e.g., '#randomid table', etc).
    """
    # Match the first id="..." in the <div>
    match = re.search(r'<div id="([^"]+)"', html)
    if not match:
        return html  # No ID found, return unchanged

    original_id = match.group(1)
    fixed_id = "abc123"

    # Replace all occurrences of the original ID in the HTML
    html = html.replace(f'id="{original_id}"', f'id="{fixed_id}"')
    html = html.replace(f"#{original_id} ", f"#{fixed_id} ")
    html = html.replace(f"#{original_id}\n", f"#{fixed_id}\n")  # in case of newline
    html = html.replace(f"#{original_id}.", f"#{fixed_id}.")  # in case of classes
    html = html.replace(f"#{original_id}{{", f"#{fixed_id}{{")  # in case of direct open

    return html


def render_standings(standings_df: pl.DataFrame, per_class: bool = False):
    race_cols = [col.replace("points_", "") for col in standings_df.columns if col.startswith("points_race_")]
    race_col_map = {"points_" + col: col.replace("_", " ").title() for col in race_cols}

    show_provisional_rating = True
    if show_provisional_rating:
        standings_df = standings_df.with_columns(
            pl.struct(["rating", "rating_rank"])
            .map_elements(
                lambda s: _format_rating_value(s["rating"], s["rating_rank"]), return_dtype=str, skip_nulls=False
            )
            .alias("rating"),
            pl.col("rating_rank").map_elements(_format_rank_value, return_dtype=str, skip_nulls=False),
        )
    else:
        standings_df = standings_df.with_columns(
            pl.when(pl.col("rating_rank").is_not_null())
            .then(pl.col("rating_rank"))
            .otherwise(pl.lit(""))
            .alias("rating_rank"),
            pl.when(pl.col("rating_rank").is_not_null()).then(pl.col("rating")).otherwise(pl.lit("")).alias("rating"),
        )

    has_classes = standings_df["class_name"].is_not_null().any()

    sort_col = "class_rank" if per_class else "points_rank"
    standings_df = standings_df.sort([sort_col, "user_id"]).with_columns(
        pl.col("points_rank_change").map_elements(
            functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        ),
        pl.col("rating_rank_change").map_elements(
            functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        ),
        pl.col("class_rank_change").map_elements(
            functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        ),
    )

    standings_df = standings_df.with_columns(
        pl.struct(["class_symbol", "class_color"]).map_elements(_format_class, return_dtype=str).alias("class_display")
    )

    _tail_cols = {
        "rating": "Rating",
        "rating_rank": "Rating Rank",
        "rating_rank_change": "rating_rank_change",
        **race_col_map,
        "points_total": "Total Points",
    }
    if per_class:
        select_cols = {
            "class_display": "Class",
            "class_rank": "Rank",
            "class_rank_change": "rank_change",
            "display_name": "Name",
            **_tail_cols,
        }
    elif has_classes:
        select_cols = {
            "points_rank": "Rank",
            "points_rank_change": "points_rank_change",
            "class_display": "Class",
            "class_rank": "Class Rank",
            "class_rank_change": "class_rank_change",
            "display_name": "Name",
            **_tail_cols,
        }
    else:
        select_cols = {
            "points_rank": "Rank",
            "points_rank_change": "points_rank_change",
            "display_name": "Name",
            **_tail_cols,
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
        value: GT.html(re.sub(r"(Race \d+) (.*)", r"\1<br>\2", value)) for value in race_col_map.values()
    }
    gt = gt.cols_label(**formatted_race_cols)

    gt = gt.tab_options(data_row_padding="1px")
    gt = _add_row_striping(gt)
    gt = gt.tab_style(style=GT.style.text(align="right"), locations=GT.loc.body(columns="Rating Rank"))

    table_html = _fix_gt_id(gt.render("html"))
    if per_class:
        table_html = _combine_column_headers(table_html, "Rank", ["Rank", "rank_change"])
    else:
        table_html = _combine_column_headers(table_html, "Rank", ["Rank", "points_rank_change"])
        if has_classes:
            table_html = _combine_column_headers(table_html, "Class Rank", ["Class-Rank", "class_rank_change"])
    table_html = _combine_column_headers(table_html, "Rating Rank", ["Rating-Rank", "rating_rank_change"])

    return table_html


def render_race_results(df: pl.DataFrame, per_class: bool = False):
    show_provisional_rating = True
    has_classes = df["class_name"].is_not_null().any()

    sort_col = "class_position" if per_class else "finish_position"
    df = df.sort(sort_col).with_columns(
        pl.col("qualify_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.col("average_lap_time").map_elements(utils.seconds_to_str, return_dtype=str),
        pl.struct(["fastest_lap_time", "fastest_lap"]).map_elements(_format_fastest_lap, return_dtype=str),
        pl.struct(["num_incidents", "cleanest_driver"]).map_elements(_format_incidents, return_dtype=str),
        pl.col("start_position").cast(pl.Int32),
    )

    # Format penalty column
    df = df.with_columns(
        pl.col("penalty").map_elements(
            lambda x: _format_change_only(x, rank=False, skip_zero=True, invert_color=True, is_time=True),
            return_dtype=str,
        )
    )

    if show_provisional_rating:
        df = df.with_columns(
            pl.struct(["rating", "rank"])
            .map_elements(lambda s: _format_rating_value(s["rating"], s["rank"]), return_dtype=str, skip_nulls=False)
            .alias("rating"),
            pl.col("rank").map_elements(_format_rank_value, return_dtype=str, skip_nulls=False),
            pl.col("rating_change").map_elements(
                functools.partial(_format_change_only), return_dtype=str, skip_nulls=False
            ),
        )

    else:
        df = df.with_columns(
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
        )

    df = df.with_columns(
        pl.struct(["class_symbol", "class_color"]).map_elements(_format_class, return_dtype=str).alias("class_display")
    )

    _tail_cols = {
        "rating": "Rating",
        "rating_change": "rating_change",
        "qualify_lap_time": "Qual. Lap",
        "start_position": "Start",
        "interval": "Interval",
        "penalty": "penalty",
        "average_lap_time": "Avg. Lap",
        "fastest_lap_time": "Best Lap",
        "num_incidents": "Inc",
        "points": "Points",
    }
    if per_class:
        select_cols = {
            "class_display": "Class",
            "class_position": "Pos",
            "display_name": "Name",
            **_tail_cols,
        }
    elif has_classes:
        select_cols = {
            "finish_position": "Pos",
            "class_display": "Class",
            "class_position": "Class Pos",
            "display_name": "Name",
            **_tail_cols,
        }
    else:
        select_cols = {
            "finish_position": "Pos",
            "display_name": "Name",
            **_tail_cols,
        }

    df = df.select(select_cols.keys()).rename(select_cols)
    gt = (
        GT.GT(df)
        .tab_header(title="Race Result")
        .tab_style(
            style=[
                GT.style.css("font-variant-numeric: tabular-nums; font-feature-settings: 'tnum';"),
                GT.style.text(align="right"),
            ],
            locations=GT.loc.body(columns=["Interval", "Qual. Lap", "Avg. Lap", "Best Lap"]),
        )
    )

    gt = gt.tab_options(
        data_row_padding="1px",
    )
    gt = gt.tab_style(
        style=GT.style.borders(sides="right", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="rating_change"),
    )
    gt = _add_row_striping(gt)

    table_html = _fix_gt_id(gt.render("html"))
    table_html = _combine_column_headers(
        table_html,
        "Rating",
        ["Rating", "rating_change"],
    )
    table_html = _combine_column_headers(table_html, "Interval", ["Interval", "penalty"])

    return table_html


def render_ratings(df):
    show_provisional_rating = True

    df = df.sort(["rank", "rating", "user_id"], descending=[False, True, False], nulls_last=True)

    # Drop those with 0 contests
    df = df.filter(pl.col("num_valid_contests") > 0)

    if show_provisional_rating:
        df = df.with_columns(
            pl.struct(["rating", "rank"])
            .map_elements(lambda s: _format_rating_value(s["rating"], s["rank"]), return_dtype=str, skip_nulls=False)
            .alias("rating"),
            pl.col("rank").map_elements(_format_rank_value, return_dtype=str, skip_nulls=False),
            pl.col("rating_change").map_elements(
                functools.partial(_format_change_only), return_dtype=str, skip_nulls=False
            ),
        )

    else:
        df = df.with_columns(
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
        )

    df = df.with_columns(
        # pl.col("points_rank_change").map_elements(
        #     functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        # ),
        pl.col("rank_change").map_elements(
            functools.partial(_format_change_only, rank=True), return_dtype=str, skip_nulls=False
        ),
    )

    select_cols = {
        "rank": "Rank",
        "rank_change": "rank_change",
        "display_name": "Name",
        "rating": "Rating",
        "rating_change": "rating_change",
        "num_valid_contests": "Races",
    }

    df = df.select(select_cols.keys()).rename(select_cols)
    gt = GT.GT(df).tab_header(title="ELO")

    gt = gt.tab_options(
        data_row_padding="1px",
    )
    gt = gt.tab_style(
        style=GT.style.borders(sides="right", color="#000000", style="solid", weight="1px"),
        locations=GT.loc.body(columns="Name"),
    )
    gt = _add_row_striping(gt)

    table_html = _fix_gt_id(gt.render("html"))
    table_html = _combine_column_headers(table_html, "Rating", ["Rating", "rating_change"])
    table_html = _combine_column_headers(table_html, "Rank", ["Rank", "rank_change"])

    return table_html
