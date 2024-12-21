#!/usr/bin/env python3

from typing import List, Optional, Literal, Annotated, Union
import pydantic


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")


class Penalty(BaseModel):
    user_id: int
    time: float


class Race(BaseModel):
    subsession_id: int
    race_name: Optional[str] = None
    penalties: Optional[List[Penalty]] = None


# Scoring Types
class PointsConfig(BaseModel):
    # type: Literal["POINTS"] = "POINTS"
    drop_races: int = 0
    points: list[int]


class ELOConfig(BaseModel):
    # type: Literal["ELO"] = "ELO"
    key: int = 0


class RatingConfig(BaseModel):
    races: List[Race]
    points: Optional[PointsConfig] = None
    elo: Optional[ELOConfig] = None
