# Task: remove-extra-deps

**Status**: complete
**Branch**: hatchery/remove-extra-deps
**Created**: 2026-03-22 09:27

## Objective

Remove dependencies and submodules that are no longer needed from the `gpt-racing` project.

## Context

The project had accumulated unused prod dependencies and a stale git submodule. The initial hypothesis was that `iracing-oauth-client` and the `Elo-MMR` submodule were unused — exploration found a more nuanced picture.

**What was kept (still actively used):**
- `iracing-oauth-client` — used in `src/gpt_racing/iracing_data.py` for OAuth auth with iRacing API
- `elommr` (PyPI package) — used in `src/gpt_racing/elo_mmr.py` and tests

**What was removed:**

| Item | Type | Reason |
|------|------|--------|
| `selenium>=4.25.0` | prod dep | No imports anywhere in codebase |
| `pyqt5>=5.15.11` | prod dep | No imports anywhere in codebase |
| `ipympl>=0.9.4` | prod dep | No imports anywhere in codebase |
| `submodules/Elo-MMR` | git submodule | Empty/never used; code uses `elommr` PyPI package instead |
| `ELO_MMR_PATH` constants | `__init__.py` | Defined but never imported/used anywhere |

## Summary

- Removed 3 unused prod deps (`selenium`, `pyqt5`, `ipympl`) from `pyproject.toml`
- Removed the `Elo-MMR` git submodule from `.gitmodules` and the git index
- Removed stale `ELO_MMR_PATH`, `ELO_MMR_CACHE_PATH`, `ELO_MMR_RESULT_PATH` constants from `src/gpt_racing/__init__.py` — these pointed at the now-removed submodule and were never imported elsewhere
- Regenerated `uv.lock` (removed 11 packages including selenium, pyqt5, and their transitive deps)
- Note: `ipympl>=0.9.7` in dev dependencies is intentionally kept (used for Jupyter plotting in dev)
- Tests could not be verified in sandbox due to missing C++ compiler (`jsonnet` build fails) — this is a pre-existing environment constraint unrelated to these changes
