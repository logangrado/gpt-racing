#!/usr/bin/env sh

uv run src/gpt_racing/cli/core.py configs/gpt/26-1.jsonnet ./out && tar -czf ./out/out.tar.gz out && open ./out
