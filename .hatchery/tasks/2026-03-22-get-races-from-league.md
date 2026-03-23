# Task: get-races-from-league

**Status**: complete
**Branch**: hatchery/get-races-from-league
**Created**: 2026-03-22 21:58

## Objective

Add a CLI command to fetch completed sessions from an iRacing league season and automatically append new ones to a jsonnet config — enabling scheduled scoring runs.

## Context

Configs are jsonnet files with a `races` list of `{ subsession_id: N, race_name: '...' }` entries, manually maintained. The `iracingdataapi` client exposes `league_season_sessions(league_id, season_id)`. A real API call confirmed the response shape: each session has `has_results: bool`, `subsession_id: int | None`, and `track.track_name: str`.

## Summary

### Key decisions

**`_no_cache` on `CachedIRClient`**: `league_season_sessions` must not be cached because the list grows as races complete. Added a `_no_cache: frozenset` class attribute; `__getattr__` skips the cache wrapper for methods in this set and calls the raw client directly. Adding future no-cache methods is a one-liner.

**`get_league_sessions` on `IracingDataClient`**: Filters to sessions where `has_results=True` and `subsession_id is not None`. Returns `[{subsession_id, track_name, launch_at}]`. Handles both dict and pydantic-model response shapes from the upstream library.

**`fetch_new_races` in `league.py`**: Pure function — takes a client, IDs, returns filtered list. Easy to unit test by injecting a mock client.

**`append_races_to_config` in `cli/core.py`**: Bracket-counting text insertion — walks the text to find the closing `]` of the `races` array, inserts before it. Preserves all comments and imports in the jsonnet file. `race_name` is set to `track_name` from the API; user can edit manually afterward.

### Files changed

- `src/gpt_racing/iracing_data.py` — `_no_cache` on `CachedIRClient`, `get_league_sessions` on `IracingDataClient`
- `src/gpt_racing/league.py` (new) — `fetch_new_races`
- `src/gpt_racing/cli/core.py` — `append_races_to_config`, `update_season_entrypoint`, `update-season` command
- `tests/test_iracing_data.py` — 4 unit tests for `get_league_sessions`
- `tests/test_league.py` (new) — 4 unit tests for `fetch_new_races`
- `tests/test_cli.py` (new) — 5 unit tests for `append_races_to_config`

### Usage

```bash
gpt-scoring update-season configs/gpt/26-1.jsonnet 10301 128672
# Prints "No new races found." or "Added: subsession_id=... (track)"
```
