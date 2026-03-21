#!/usr/bin/env python3

import polars as pl


def _format_value_with_delta_helper(value, delta):
    out = str(value)
    if delta is not None:
        if delta < 0:
            sign = "-"
        elif delta > 0:
            sign = "+"
        else:
            sign = ""
        out += f" ({sign}{abs(delta)})"
    return out


def format_value_with_delta(df, value_col, delta_col):
    return df.select(
        pl.struct([value_col, delta_col]).map_elements(
            lambda x: _format_value_with_delta_helper(x[value_col], x[delta_col]),
            return_dtype=pl.String,
        )
    ).to_series()


def require_columns(df: pl.DataFrame, cols: list[str]):
    """
    Raise error if dataframe is missing required columns
    """
    missing_cols = [col for col in cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")


def seconds_to_str(seconds, precision=3):
    if seconds is None:
        return "-"

    negative = False
    if seconds < 0:
        seconds *= -1
        negative = True

    seconds = round(seconds, precision)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    # seconds, remainder = divmod(remainder, 1)
    hours = int(round(hours))
    minutes = int(round(minutes))

    if hours == 0 and minutes == 0:
        out = f"{seconds:.{precision}f}"
    elif hours == 0:
        out = f"{minutes}:{seconds:0{precision + 3}.{precision}f}"
    else:
        out = f"{hours}:{minutes:02d}:{seconds:0{precision + 3}.{precision}f}"

    if negative:
        out = "-" + out

    return out
