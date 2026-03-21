#!/usr/bin/env python3

from invoke import task


@task
def format(c, check=False):
    """Run ruff formatter. Pass --check to verify without modifying files."""
    args = "--check" if check else ""
    c.run(f"ruff format ./src ./tests {args}".strip())
