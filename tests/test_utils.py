#!/usr/bin/env python3

import pytest

from gpt_racing import utils


class TestSecondsToStr:
    @pytest.mark.parametrize(
        "seconds,precision,expected",
        [
            [1.2347, 3, "1.235"],
            [61.2347, 3, "1:01.235"],
            [3662.2347, 3, "1:01:02.235"],
            [-1.2347, 3, "-1.235"],
            [-61.2347, 3, "-1:01.235"],
            [-3662.2347, 3, "-1:01:02.235"],
        ],
    )
    def test_basic(self, seconds, precision, expected):
        assert utils.seconds_to_str(seconds, precision) == expected
