#!/usr/bin/env python3

from pathlib import Path
import shutil
import subprocess

import pytest

from gpt_racing.cli.core import core_entrypoint


@pytest.fixture
def config_dir():
    return Path(__file__).parent / "configs"


@pytest.fixture
def test_id(request):
    unique_id = request.node.nodeid
    unique_id = "/".join(unique_id.split("::", 1))
    return unique_id


REF_BASE_PATH = Path(__file__).parent / "data" / "ref"


def _compare_files(actual, expected):
    # just compare literally

    with open(actual, "r") as f:
        actual_contents = f.readlines()

    with open(expected, "r") as f:
        expected_contents = f.readlines()

    assert actual_contents == expected_contents


@pytest.fixture
def compare_output(request, test_id):
    def _compare_output(out_dir):
        ref_dir = REF_BASE_PATH / test_id

        if request.config.getoption("show"):
            subprocess.run(["open", out_dir])

        if request.config.getoption("update_refs"):
            if ref_dir.is_dir():
                shutil.rmtree(ref_dir)
            shutil.copytree(out_dir, ref_dir)

        # compare contents
        ref_contents = set([str(f.relative_to(ref_dir)) for f in ref_dir.glob("**/*") if f.is_file()])
        out_contents = set([str(f.relative_to(out_dir)) for f in out_dir.glob("**/*") if f.is_file()])

        assert ref_contents == out_contents

        for f_path in ref_contents:
            try:
                _compare_files(actual=out_dir / f_path, expected=ref_dir / f_path)
            except Exception as e:
                raise AssertionError(f"Files do not match: {f_path}\n{e}") from e

    return _compare_output


class TestFullIntegration:
    def test_single_season(self, client, config_dir, tmp_path, compare_output):
        config_path = config_dir / "season_1.jsonnet"

        core_entrypoint(config_path, tmp_path, _overwrite=True, client=client)

        compare_output(tmp_path)

    def test_multi_season(self, client, config_dir, tmp_path, compare_output):
        config_path = config_dir / "season_2.jsonnet"

        core_entrypoint(config_path, tmp_path, _overwrite=True, client=client)

        compare_output(tmp_path)


# THINGS TO TEST
# time_window
#   test null
#   test not null
