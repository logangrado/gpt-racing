#!/usr/bin/env python3

import json
import textwrap

import polars as pl
from polars.testing import assert_frame_equal as _assert_frame_equal


def _generate_qualy_data(session_data):
    df = pl.DataFrame(session_data)
    df = df.sort("best_lap_time").with_columns(pl.int_range(pl.len()).alias("finish_position"))

    return df.to_dicts()


def _generate_lap_data(session_data):
    df = pl.DataFrame(session_data)

    df = df.rename({"average_lap_time": "lap_time"})

    df = df.with_columns(
        pl.col("laps_complete").map_elements(lambda x: range(x + 1), return_dtype=list[int]).alias("lap_number")
    )

    df = df.explode("lap_number")

    df = df.with_columns(pl.col("lap_time") * 10000)

    # Compute interval
    df = df.with_columns((pl.col("lap_time") * pl.col("lap_number")).alias("total_time"))
    leader_df = (
        df.sort("total_time")
        .group_by("lap_number")
        .first()["lap_number", "total_time"]
        .rename({"total_time": "total_time_leader"})
    )
    df = df.join(leader_df, on="lap_number")
    df = df.with_columns(((pl.col("total_time") - pl.col("total_time_leader")) / 10 * -1).alias("interval"))

    # Make lap_number - lap_time = -1
    df = df.with_columns(
        [pl.when(pl.col("lap_number") == 0).then(-1).otherwise(pl.col("interval")).alias("interval")],
    )

    df = df.with_columns(pl.lit([]).alias("lap_events"))

    # Also need incidents column
    df = df.with_columns(pl.lit(False).alias("incident"))

    # Select columns of interest
    df = df["cust_id", "display_name", "lap_number", "lap_time", "interval", "lap_events", "incident"]
    return df.to_dicts()


def _generate_race_result(subsession_id, session_data):
    """
    Race data takes the following form:

    Returns one row per driver, in the following format:

    cust_id                                          346566
    display_name                            Derek M Cyphers
    finish_position                                       0
    finish_position_in_class                              0
    interval                                              0
    average_lap                                      896126
    laps_complete                                        41
    class_interval                                        0
    qual_lap_time                                        -1
    starting_position                                     0
    starting_position_in_class                            0
    incidents                                             0
    car_class_name                    Hosted All Cars Class
    reason_out                                      Running
    subsession_id                                  66010063
    session_end_time              2024-01-09 02:34:35+00:00

    ...
    cust_id                                          367874
    display_name                            Matthew Siddall
    finish_position                                       1
    finish_position_in_class                              1
    interval                                         136641
    average_lap                                      899458
    laps_complete                                        41
    class_interval                                   136641
    qual_lap_time                                        -1
    starting_position                                     2
    starting_position_in_class                            2
    incidents                                             0
    car_class_name                    Hosted All Cars Class
    reason_out                                      Running
    subsession_id                                  66010063
    session_end_time              2024-01-09 02:34:35+00:00
    Name: 1, dtype: object


    dtypes:

    cust_id                                    int64
    display_name                              object
    finish_position                            int64
    finish_position_in_class                   int64
    interval                                   int64
    average_lap                                int64
    laps_complete                              int64
    class_interval                             int64
    qual_lap_time                              int64
    starting_position                          int64
    starting_position_in_class                 int64
    incidents                                  int64
    car_class_name                            object
    reason_out                                object
    subsession_id                              int64
    session_end_time              datetime64[s, UTC]

    """
    # Create dataframe directly from inputs. Explicitly select required columns
    df = pl.DataFrame(session_data).select(["cust_id", "display_name", "laps_complete", "average_lap_time"])

    # Compute values

    df = df.with_columns(
        pl.lit(subsession_id).alias("subsession_id"),
        pl.lit(0).alias("incidents"),
        (pl.col("laps_complete") * pl.col("average_lap_time")).alias("_total_time"),
    )

    df = df.with_columns(
        pl.col("_total_time").rank().alias("finish_position").cast(pl.Int64),
        ((pl.col("_total_time").min() - pl.col("_total_time")) * 10000).alias(
            "interval"
        ),  # Factor might be 1000, unsure
        pl.col("subsession_id").cast(pl.Datetime).alias("session_end_time"),
    )

    return df


def generate_data(summary_data, fake_client):
    """Generate and insert race data into a fake client

    summary_data looks like:

    summary_data = [
        {
            "subsession_id": 0,
            "qualifying": [
                {"cust_id": 0, "display_name": "a", "best_lap_time": 40, "laps_complete": 3},
                {"cust_id": 1, "display_name": "b", "best_lap_time": 44, "laps_complete": 4},
                {"cust_id": 2, "display_name": "c", "best_lap_time": 42, "laps_complete": 3},
                {"cust_id": 3, "display_name": "d", "best_lap_time": 43, "laps_complete": 3},
            ],
            "race": [
                {"cust_id": 0, "display_name": "a", "laps_complete": 3, "average_lap_time": 10},
                {"cust_id": 1, "display_name": "b", "laps_complete": 3, "average_lap_time": 11},
                {"cust_id": 2, "display_name": "c", "laps_complete": 3, "average_lap_time": 12},
            ],
        }
    ]
    """
    for session in summary_data:
        fake_client._set_qualy_result(
            session["subsession_id"], pl.DataFrame(_generate_qualy_data(session["qualifying"]))
        )
        fake_client._set_lap_data(session["subsession_id"], pl.DataFrame(_generate_lap_data(session["race"])))

        fake_client._set_race_result(
            session["subsession_id"], _generate_race_result(session["subsession_id"], session["race"])
        )


def assert_frame_equal(actual, expected, *args, **kwargs):
    try:
        _assert_frame_equal(actual, expected, *args, **kwargs)
    except Exception as e:
        raise AssertionError(f"Dataframes do not match\nActual data:\n{actual.to_dict(as_series=False)}") from e


def _compare_objects(actual, expected, frame_kwargs, path="root"):
    errors = []

    if type(actual) != type(expected):
        errors.append(f"{path}: Types do not match: {type(actual)} != {type(expected)}")
        return errors

    if isinstance(actual, dict):
        actual_keys = set(actual.keys())
        expected_keys = set(expected.keys())
        for key in sorted(actual_keys | expected_keys):
            sub_path = f"{path}.{key}"
            if key not in actual:
                errors.append(f"{sub_path}: Key missing in actual")
            elif key not in expected:
                errors.append(f"{sub_path}: Key missing in expected")
            else:
                errors.extend(_compare_objects(actual[key], expected[key], frame_kwargs, path=sub_path))

    elif isinstance(actual, list):
        if len(actual) != len(expected):
            errors.append(f"{path}: List length mismatch: {len(actual)} != {len(expected)}")
        for i, (a, b) in enumerate(zip(actual, expected)):
            sub_path = f"{path}[{i}]"
            errors.extend(_compare_objects(a, b, frame_kwargs, path=sub_path))

    elif isinstance(actual, pl.DataFrame):
        try:
            assert_frame_equal(actual, expected, **frame_kwargs)
        except AssertionError as e:
            errors.append(f"{path}: DataFrame mismatch:\n{textwrap.indent(str(e), '  ')}")

    else:
        if actual != expected:
            errors.append(f"{path}: Value mismatch: {actual!r} != {expected!r}")

    return errors


def _serialize(object: any):
    if isinstance(object, dict):
        out = "{"
        for key in sorted(object.keys()):
            out += f"'{key}': {_serialize(object[key])},"
        out += "}"
        return out
    elif isinstance(object, list):
        out = "["
        for item in object:
            out += f"{_serialize(item)},"
        out += "]"
        return out
    elif isinstance(object, pl.DataFrame):
        return f"pl.DataFrame({object.to_dict(as_series=False)})"

    else:
        return str(object)


def assert_object_equal(actual, expected, frame_kwargs={}):
    errors = _compare_objects(actual, expected, frame_kwargs)
    if errors:
        raise AssertionError(
            "Objects do not match:\n"
            + "\n".join(textwrap.indent(e, "  ") for e in errors)
            + f"\n\nFull object: \n\n{_serialize(actual)}"
        )
