# Agent Guidelines

## Changelog

This project uses [conventional commits](https://www.conventionalcommits.org/) to produce the changelog. Each PR is squashed to a small number of commits before merging.

When your PR contains multiple distinct user-facing changes, add one **empty commit** per additional changelog entry after finishing your implementation:

```bash
git commit --allow-empty -m "feat: <describe the second change>"
git commit --allow-empty -m "fix: <describe a fix>"
```

- `feat:` — new feature (minor version bump)
- `fix:` — bug fix (patch version bump)
- `no-bump:` — explicitly suppresses a changelog entry

The squashed PR commit counts as the first entry; add empty commits only for *additional* entries beyond the first.

## Task files

Browse `.hatchery/tasks/` for context on past decisions, patterns, and gotchas before starting new work. When completing a task, update its file to a clean ADR (Status → Objective → Context → Summary) before committing.
