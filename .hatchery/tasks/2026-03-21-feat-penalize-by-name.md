# Task: feat-penalize-by-name

**Status**: complete
**Branch**: hatchery/feat-penalize-by-name
**Created**: 2026-03-21 23:22

## Objective

Update the Penalties config to be specified by either driver display name or numeric user ID.

## Context

`Penalty` in `config.py` only accepted `user_id: int`. Config files worked around this by embedding
comments next to each numeric ID (e.g. `// Facciolo`). The goal was to allow penalties to be
specified by driver display name (e.g. `{ name: "Facciolo", time: 5 }`) as an alternative to
numeric ID, making configs more readable and author-friendly.

The codebase already had a pattern for name→user_id resolution in `classes.py::resolve_driver_classes()`,
which matches `display_name` from race data. We followed the same pattern here.

## Summary

### Files changed

- **`src/gpt_racing/config.py`** — `Penalty.user_id` changed from `int` to `Optional[int] = None`;
  added `name: Optional[str] = None`; added `@model_validator` enforcing exactly one of the two.
  Fully backward-compatible: existing configs with `{ user_id: ..., time: ... }` still validate.

- **`src/gpt_racing/core.py`** — Added module-level `_resolve_penalties(penalties, name_df)` helper.
  Moved `penalty_df` construction in `_load_race_data` to *after* `qualy_df` is loaded (so
  `display_name` is available for name lookup). The old `pl.DataFrame([dict(x) for x in ...])` call
  is replaced with `_resolve_penalties(...)`.

- **`tests/test_core.py`** — Added `TestResolvePenalties` (unit tests for the helper),
  `TestPenaltyValidation` (pydantic validation tests), and `TestPenaltyByNameIntegration`
  (end-to-end via `FakeClient`/`generate_data`).

### Key decisions

- **Collect all missing names before raising** — `_resolve_penalties` accumulates every unrecognised
  name and raises a single `ValueError` listing all of them plus the available names. Avoids the
  frustration of fixing typos one at a time.

- **Resolution against qualifying data** — name lookup uses the `qualy_df` that is already loaded
  per race, consistent with how `resolve_driver_classes` works. The `name_df` slice passed in is
  `qualy_df[["user_id", "display_name"]]`.

- **Schema enforcement** — `_resolve_penalties` always returns a `pl.DataFrame` with an explicit
  `{"user_id": pl.Int64, "time": pl.Float64}` schema, even for an empty result, so downstream
  joins remain type-stable.
