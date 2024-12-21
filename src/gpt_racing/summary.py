#!/usr/bin/env python3

import polars as pl
from great_tables import GT


def render_output(df):
    gtdf = GT(df)

    gtdf.show()


def _to_str(x):
    return str(x) if x is not None else ""


def summarize_results(df, race_name_df, driver_info_df):
    # Compute final points

    total_points = (
        df.filter(~pl.col("drop"))
        .group_by("user_id")
        .agg(pl.sum("points"))
        .with_columns(pl.col("points").rank("min", descending=True).alias("rank"))
    )

    df = df.join(race_name_df, on="contest_id")

    race_summary_df = df[["user_id", "race_name", "points"]].pivot(on="race_name", index="user_id", values="points")
    race_summary_df = race_summary_df.with_columns(pl.all().exclude("user_id").map_elements(_to_str, skip_nulls=False))

    out = race_summary_df.join(total_points, on="user_id").sort("rank", "user_id")

    import ipdb

    ipdb.set_trace()
    pass
    render_output(out)
