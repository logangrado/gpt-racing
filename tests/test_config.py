#!/usr/bin/env python3

import polars as pl
import pytest

from gpt_racing.config import Penalty
from gpt_racing.core import resolve_penalties


class TestResolvePenalties:
    def _make_name_df(self, rows):
        return pl.DataFrame(rows, schema={"user_id": pl.Int64, "display_name": pl.String})

    def test_by_user_id(self):
        name_df = self._make_name_df([{"user_id": 1, "display_name": "Alice"}])
        result = resolve_penalties([Penalty(user_id=1, time=5.0)], name_df)
        assert result["user_id"].to_list() == [1]
        assert result["time"].to_list() == [5.0]

    def test_by_name(self):
        name_df = self._make_name_df([{"user_id": 42, "display_name": "Bob"}])
        result = resolve_penalties([Penalty(name="Bob", time=10.0)], name_df)
        assert result["user_id"].to_list() == [42]
        assert result["time"].to_list() == [10.0]

    def test_mixed(self):
        name_df = self._make_name_df(
            [
                {"user_id": 1, "display_name": "Alice"},
                {"user_id": 2, "display_name": "Bob"},
            ]
        )
        result = resolve_penalties([Penalty(user_id=1, time=5.0), Penalty(name="Bob", time=3.0)], name_df)
        assert result["user_id"].to_list() == [1, 2]
        assert result["time"].to_list() == [5.0, 3.0]

    def test_name_not_found_raises(self):
        name_df = self._make_name_df([{"user_id": 1, "display_name": "Alice"}])
        with pytest.raises(ValueError, match="Nobody") as exc_info:
            resolve_penalties([Penalty(name="Nobody", time=5.0), Penalty(name="Ghost", time=3.0)], name_df)
        assert "Ghost" in str(exc_info.value)


class TestPenaltyValidation:
    def test_user_id_only_valid(self):
        p = Penalty(user_id=1, time=5)
        assert p.user_id == 1

    def test_name_only_valid(self):
        p = Penalty(name="Alice", time=5)
        assert p.name == "Alice"

    def test_both_raises(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Penalty(user_id=1, name="Alice", time=5)

    def test_neither_raises(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Penalty(time=5)
