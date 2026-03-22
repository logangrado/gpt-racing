# Task: fix-warnings

**Status**: complete
**Branch**: hatchery/fix-warnings
**Created**: 2026-03-21 21:57

## Objective

Address deprecation/parser warnings surfacing during test runs.

## Context

Two warnings appeared during test runs:

1. **DeprecationWarning** from `iracing_data.py:62` — `irDataClient` constructed without `use_pydantic=True`. Migrating to Pydantic models would break the entire codebase: `CachedIRClient` serializes with `json.dump` (incompatible with Pydantic v2 models) and all downstream code uses dict-style access. Suppressing the warning explicitly is the correct minimal fix.

2. **GuessedAtParserWarning** from `render_tables.py:96` — `BeautifulSoup(html)` called without an explicit parser.

## Summary

### Changes made

- **`src/gpt_racing/iracing_data.py`**: Added `import warnings` and wrapped the `irDataClient(...)` constructor call in `_load_client()` with `warnings.catch_warnings()` / `warnings.simplefilter("ignore", DeprecationWarning)` to suppress the `iracingdataapi` deprecation warning without doing a large Pydantic migration.

- **`src/gpt_racing/render_tables.py:96`**: Changed `BeautifulSoup(html)` → `BeautifulSoup(html, "html.parser")` to eliminate the `GuessedAtParserWarning`.

### Key decisions

- Did **not** set `use_pydantic=True` on `irDataClient` — the caching layer (`CachedIRClient`) uses `json.dump`/`json.load` which is incompatible with Pydantic v2 models, and all callers use dict-style access. A full migration would be a large separate task.
- Used `warnings.catch_warnings()` context manager (scoped suppression) rather than a global `warnings.filterwarnings` call, keeping the suppression contained to the constructor call site.
