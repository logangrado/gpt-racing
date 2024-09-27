#!/usr/bin/env python3

from typing import List, Optional
import pydantic


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")


class Penalty(BaseModel):
    user_id: int
    time: float


class Race(BaseModel):
    subsession_id: int
    penalties: Optional[List[Penalty]] = None


class RatingConfig(BaseModel):
    races: List[Race]
