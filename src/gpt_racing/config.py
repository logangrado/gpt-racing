#!/usr/bin/env python3

from typing import List, Optional
import pydantic


class Penalty(pydantic.BaseModel):
    user_id: int
    time_s: float


class Race(pydantic.BaseModel):
    subsession_id: int
    penalties: Optional[List[Penalty]] = None


class RatingConfig(pydantic.BaseModel):
    races: List[Race]
