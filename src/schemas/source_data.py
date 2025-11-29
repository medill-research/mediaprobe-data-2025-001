
import polars as pl
from pydantic import BaseModel, ConfigDict


class SessionMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    session_id: pl.Int64
    country: pl.Utf8
    channel: pl.Utf8
    format: pl.Utf8
    genre: pl.Utf8
    subgenre: pl.Utf8
    program: pl.Utf8
    age_average: pl.Float64
    age18_age34: pl.Float64
    age35_age64: pl.Float64
    age65: pl.Float64
    females_pct: pl.Float64
    males_pct: pl.Float64
    non_binary_pct: pl.Float64
    start_time: pl.Datetime


class TimelineMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    time_id: pl.Int64
    session_id: pl.Int64
    time_stamp: pl.Datetime
    phasic_program: pl.Float64
    ars_program: pl.Float64


class TimelineMetadata11272025(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    session_id: pl.Int64
    time_stamp: pl.Time
    phasic_program: pl.Float64
    ars_program: pl.Float64


class AdsMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    session_id: pl.Int64
    description: pl.Utf8
    category: pl.Utf8
    time_in: pl.Time
    time_out: pl.Time
    phasic_ads: pl.Float64
    ars_ads: pl.Float64


class AdsMetadata11272025(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    session_id: pl.Int64
    description: pl.Utf8
    time_in: pl.Time
    time_out: pl.Time
    phasic_ads: pl.Float64
    ars_ads: pl.Float64
    ads_comparison: pl.Utf8
