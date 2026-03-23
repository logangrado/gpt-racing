import textwrap
from pathlib import Path

import pytest

from gpt_racing.cli.core import append_races_to_config


SAMPLE_CONFIG = textwrap.dedent("""\
    // championship config
    local lib = import 'lib.libsonnet';
    {
      name: 'Test Series',
      races: [
        { subsession_id: 100, race_name: 'Spa' },
      ],
    }
""")


def test_appends_entry(tmp_path):
    cfg = tmp_path / "config.jsonnet"
    cfg.write_text(SAMPLE_CONFIG)
    new_races = [{"subsession_id": 200, "track_name": "Monza"}]
    append_races_to_config(cfg, new_races)
    result = cfg.read_text()
    # Entry indentation should match existing entries (4 spaces, same as closing ] + 2)
    assert "    { subsession_id: 200, race_name: 'Monza' }," in result


def test_preserves_surrounding_text(tmp_path):
    cfg = tmp_path / "config.jsonnet"
    cfg.write_text(SAMPLE_CONFIG)
    append_races_to_config(cfg, [{"subsession_id": 200, "track_name": "Monza"}])
    result = cfg.read_text()
    assert "// championship config" in result
    assert "local lib = import 'lib.libsonnet';" in result
    assert "name: 'Test Series'" in result
    assert "subsession_id: 100" in result


def test_appended_before_closing_bracket(tmp_path):
    cfg = tmp_path / "config.jsonnet"
    cfg.write_text(SAMPLE_CONFIG)
    append_races_to_config(cfg, [{"subsession_id": 200, "track_name": "Monza"}])
    result = cfg.read_text()
    new_pos = result.index("subsession_id: 200")
    closing = result.index("]", result.index("races:"))
    assert new_pos < closing


def test_multiple_entries(tmp_path):
    cfg = tmp_path / "config.jsonnet"
    cfg.write_text(SAMPLE_CONFIG)
    new_races = [
        {"subsession_id": 201, "track_name": "Monza"},
        {"subsession_id": 202, "track_name": "Silverstone"},
    ]
    append_races_to_config(cfg, new_races)
    result = cfg.read_text()
    assert "subsession_id: 201" in result
    assert "subsession_id: 202" in result


def test_ignores_drop_races_key(tmp_path):
    """'drop_races:' contains 'races:' as a substring — must not match it."""
    cfg = tmp_path / "config.jsonnet"
    cfg.write_text(
        textwrap.dedent("""\
        {
          points: { drop_races: 2, default: [50, 41, 36] },
          races: [
            { subsession_id: 100, race_name: 'Spa' },
          ],
        }
    """)
    )
    append_races_to_config(cfg, [{"subsession_id": 200, "track_name": "Monza"}])
    result = cfg.read_text()
    assert "subsession_id: 200" in result
    # points array must be untouched
    assert "default: [50, 41, 36]" in result


def test_no_races_no_change(tmp_path):
    cfg = tmp_path / "config.jsonnet"
    cfg.write_text(SAMPLE_CONFIG)
    append_races_to_config(cfg, [])
    assert cfg.read_text() == SAMPLE_CONFIG
