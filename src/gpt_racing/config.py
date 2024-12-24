#!/usr/bin/env python3

from typing import List, Optional, Literal, Annotated, Union
import datetime
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
    points_type: Optional[str] = None


class PointsConfig(BaseModel):
    class FastestLapConfig(BaseModel):
        points: int = 1
        must_be_on_lead_lap: bool = True

    class CleanestDriverConfig(BaseModel):
        points: int = 1
        must_be_on_lead_lap: bool = True

    drop_races: int = 0
    points: Union[list[int], dict[str, list[int]]]
    default: str = "default"
    fastest_lap: Optional[FastestLapConfig] = None
    cleanest_driver: Optional[CleanestDriverConfig] = None


class ELOConfig(BaseModel):
    previous_seasons: Optional[List["RatingConfig"]] = None
    min_races: int = 1
    time_window: datetime.timedelta


class RatingConfig(BaseModel):
    races: List[Race]
    points: Optional[PointsConfig] = None
    elo: Optional[ELOConfig] = None
