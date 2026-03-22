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


class DriverEntry(BaseModel):
    name: str


class DriverClass(BaseModel):
    name: str
    symbol: str
    color: str
    default: bool = False
    drivers: List[DriverEntry] = []


class RatingConfig(BaseModel):
    races: List[Race]
    elo: Optional["ELOConfig"] = pydantic.Field(default_factory=lambda: ELOConfig())  # forward ref as default
    points: Optional[PointsConfig] = None
    classes: Optional[List[DriverClass]] = None

    @pydantic.model_validator(mode="after")
    def _check_default_class(self):
        if self.classes is not None:
            defaults = [c for c in self.classes if c.default]
            if len(defaults) > 1:
                raise ValueError(f"At most one class may be marked default, got: {[c.name for c in defaults]}")
        return self


class ELOConfig(BaseModel):
    previous_seasons: Optional[List[RatingConfig]] = None
    min_races: int = 1
    time_window: Optional[datetime.timedelta] = None
    weight: float = 1


# Resolve forward refs
ELOConfig.model_rebuild()
RatingConfig.model_rebuild()
