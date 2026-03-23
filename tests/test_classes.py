#!/usr/bin/env python3

import pytest
import polars as pl

from gpt_racing.classes import resolve_driver_classes
from gpt_racing.config import DriverClass, DriverEntry, RatingConfig


def _make_config(classes=None):
    return RatingConfig(
        races=[{"subsession_id": 1}],
        classes=classes,
    )


NAME_DF = pl.DataFrame(
    {
        "user_id": [100, 200, 300],
        "display_name": ["Alice", "Bob", "Charlie"],
    }
)


def test_match_by_name():
    config = _make_config(
        classes=[
            DriverClass(name="Pro", symbol="P", color="#ff0000", drivers=[DriverEntry(name="Alice")]),
            DriverClass(name="Am", symbol="A", color="#0000ff", default=True),
        ],
    )
    result = resolve_driver_classes(config, NAME_DF)
    alice = result.filter(pl.col("user_id") == 100)
    assert alice["class_name"][0] == "Pro"
    assert alice["class_symbol"][0] == "P"
    assert alice["class_color"][0] == "#ff0000"


def test_name_not_found_raises():
    config = _make_config(
        classes=[
            DriverClass(name="Pro", symbol="P", color="#ff0000", drivers=[DriverEntry(name="Nonexistent")]),
        ],
    )
    with pytest.raises(ValueError, match="Nonexistent"):
        resolve_driver_classes(config, NAME_DF)


def test_default_class_assigned():
    config = _make_config(
        classes=[
            DriverClass(name="Pro", symbol="P", color="#ff0000", drivers=[DriverEntry(name="Alice")]),
            DriverClass(name="Am", symbol="A", color="#0000ff", default=True),
        ],
    )
    result = resolve_driver_classes(config, NAME_DF)
    # Bob and Charlie not listed → default "Am"
    for uid in [200, 300]:
        row = result.filter(pl.col("user_id") == uid)
        assert row["class_name"][0] == "Am"
        assert row["class_symbol"][0] == "A"
        assert row["class_color"][0] == "#0000ff"


def test_driver_in_multiple_classes_raises():
    config = _make_config(
        classes=[
            DriverClass(name="Pro", symbol="P", color="#ff0000", drivers=[DriverEntry(name="Alice")]),
            DriverClass(name="Am", symbol="A", color="#0000ff", default=True, drivers=[DriverEntry(name="Alice")]),
        ],
    )
    with pytest.raises(ValueError, match="multiple classes"):
        resolve_driver_classes(config, NAME_DF)


def test_missing_driver_no_default_raises():
    config = _make_config(
        classes=[
            DriverClass(name="Pro", symbol="P", color="#ff0000", drivers=[DriverEntry(name="Alice")]),
            DriverClass(name="Am", symbol="A", color="#0000ff"),
        ],
    )
    with pytest.raises(ValueError, match="not assigned to any class"):
        resolve_driver_classes(config, NAME_DF)


def test_multiple_defaults_raises():
    with pytest.raises(ValueError, match="At most one class"):
        _make_config(
            classes=[
                DriverClass(name="Pro", symbol="P", color="#ff0000", default=True),
                DriverClass(name="Am", symbol="A", color="#0000ff", default=True),
            ],
        )


def test_no_classes_config():
    config = _make_config(classes=None)
    result = resolve_driver_classes(config, NAME_DF)
    assert result["class_name"].is_null().all()
    assert result["class_symbol"].is_null().all()
    assert result["class_color"].is_null().all()
    assert len(result) == 3
