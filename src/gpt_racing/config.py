#!/usr/bin/env python3

from typing import List, Optional, Literal
import pydantic


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")


class Penalty(BaseModel):
    user_id: int
    time: float


class Race(BaseModel):
    subsession_id: int
    penalties: Optional[List[Penalty]] = None


# Scoring Types
class PointsScoringConfig(BaseModel):
    type: Literal["POINTS"] = "POINTS"
    drop_races: int = 0
    points: list[int]


class RatingConfig(BaseModel):
    races: List[Race]
