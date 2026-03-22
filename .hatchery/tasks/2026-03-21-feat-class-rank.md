# Task: feat-class-rank

**Status**: complete
**Branch**: hatchery/feat-class-rank
**Created**: 2026-03-21 23:25

## Objective

We recently added `classes`. We should update outputs to reflect rank by class:

- `standings` table (CSV and HTML):
  - Add `class rank` column when class is present (in addition to overall rank)
- `results` table (CSV and HTML)
  - Add `class position` when class is present (in addition to overall position)

## Context

Classes assign drivers to named groups (e.g. Pro/Am). When `config.classes` is
`None`, all drivers share the `"Overall"` class (`n_unique == 1`), and no class
columns are shown. Multi-class detection: `df["class_name"].n_unique() > 1`.

## Summary

### Changes made

**`src/gpt_racing/core.py`**
- Added `class_position` (rank within class by finish position) to `race_result_df`
  using `pl.col("finish_position").rank("min").over("class_name")`.
- Added `class_rank` (rank within class by cumulative points) and `class_rank_change`
  (delta vs previous race) to `standings_df`.

**`src/gpt_racing/render_tables.py`**
- `render_race_results`: detects `has_classes = df["class_name"].n_unique() > 1`;
  shows `"Cls Pos"` column only when true.
- `render_standings`: same detection; shows `"Class Rank"` + change arrow column
  only when classes are present, and calls `_combine_column_headers` for it.

**`src/gpt_racing/config.py`**
- Added `RenderConfig(combined_table: bool, per_class_tables: bool)` to control output.
- Added `render: RenderConfig` field to `RatingConfig` (defaults: combined=True, per_class=False).

**`src/gpt_racing/output.py`** (new)
- Dispatch layer between computed DataFrames and rendered HTML.
- `race_result_htmls`, `standings_htmls`, `ratings_htmls` each return `list[tuple[str, str]]`
  where label `""` = combined table, `"<ClassName>"` = per-class table.
- Per-class tables auto-suppressed when `n_unique() == 1`.
- `ratings_htmls` is a stub (ELO df has no class_name; `per_class_tables` always no-ops).

**`src/gpt_racing/cli/core.py`**
- Updated all three render loops to call `output.*_htmls()` dispatchers.

**Tests**
- `test_core.py`, `test_integration.py`: updated all expected standings/race_results
  DataFrames to include `class_position`, `class_rank`, `class_rank_change`.
  Added `test_multi_race_with_classes_fake` covering full Pro/Am two-race output.
- `test_render_tables.py`: added `class_position` to test input DataFrames for
  multi-class tests; regenerated HTML refs via `--update-refs`.
- `test_output.py` (new): covers `race_result_htmls`, `standings_htmls`, `ratings_htmls`
  dispatch logic including combined-only, per-class-only, both, single-class suppression.

### Key decisions / patterns
- `class_position` is always computed in `core.py` (even for single-class races)
  but only rendered when multi-class is detected.
- `render_tables.py` remains config-free — dispatch/config logic lives in `output.py`.
- `per_class_tables` defaults to `False` (opt-in) to avoid silently generating new files
  for existing seasons that already have classes configured.
- `test_iracing_data.py` has 2 integration tests that error without credentials;
  they are marked `@pytest.mark.integration` and skip under `CI=true`.
- The uv lockfile pins `pyqt5` which has no aarch64 wheel; the test environment
  was built manually with `uv pip install` omitting pyqt5/selenium (not needed
  for any unit/render tests).
