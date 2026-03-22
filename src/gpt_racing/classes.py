#!/usr/bin/env python3

import polars as pl


def resolve_driver_classes(config, name_df: pl.DataFrame) -> pl.DataFrame:
    """
    Given a RatingConfig and a name_df (columns: user_id, display_name),
    return a DataFrame with columns: user_id, class_name, class_symbol, class_color.

    If config.classes is None, all class columns are set to null.

    Matching priority:
    1. If DriverEntry has user_id → match by user_id
    2. If DriverEntry has only name → exact match on display_name; raises ValueError if not found
    3. Unmatched drivers → assigned the class with default=True, or raises ValueError if none
    """
    if config.classes is None:
        return name_df.select("user_id").with_columns(
            pl.lit(None).cast(pl.String).alias("class_name"),
            pl.lit(None).cast(pl.String).alias("class_symbol"),
            pl.lit(None).cast(pl.String).alias("class_color"),
        )

    class_lookup: dict[str, tuple[str, str]] = {dc.name: (dc.symbol, dc.color) for dc in config.classes}

    assignments: dict[int, str] = {}
    duplicates: dict[int, list[str]] = {}  # user_id → [class names]

    for driver_class in config.classes:
        for entry in driver_class.drivers:
            matched = name_df.filter(pl.col("display_name") == entry.name)
            if matched.is_empty():
                raise ValueError(
                    f"Driver name '{entry.name}' not found in race data. "
                    f"Available names: {name_df['display_name'].to_list()}"
                )
            for user_id in matched["user_id"].to_list():
                if user_id in assignments:
                    duplicates.setdefault(user_id, [assignments[user_id]]).append(driver_class.name)
                assignments[user_id] = driver_class.name

    uid_to_name = dict(zip(name_df["user_id"].to_list(), name_df["display_name"].to_list()))

    if duplicates:
        detail = "; ".join(f"{uid_to_name[uid]} in {classes}" for uid, classes in duplicates.items())
        raise ValueError(f"Drivers assigned to multiple classes: {detail}")

    default_classes = [dc for dc in config.classes if dc.default]
    default_class_name = default_classes[0].name if default_classes else None

    rows = []
    missing: list[str] = []
    for user_id in name_df["user_id"].to_list():
        class_name = assignments.get(user_id, default_class_name)
        if class_name is None:
            missing.append(uid_to_name[user_id])
            continue
        symbol, color = class_lookup[class_name]
        rows.append({"user_id": user_id, "class_name": class_name, "class_symbol": symbol, "class_color": color})

    if missing:
        raise ValueError(f"Drivers not assigned to any class (no default class set): {missing}")

    return pl.DataFrame(
        rows,
        schema={"user_id": pl.Int64, "class_name": pl.String, "class_symbol": pl.String, "class_color": pl.String},
    )
