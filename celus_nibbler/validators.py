import datetime
from typing import Optional, Union

from dateutil import parser as datetimes_parser
from pydantic import BaseModel, validator

from .errors import NibblerValidation


def non_empty(name: str) -> str:
    if not name:
        raise NibblerValidation("cant-be-empty")
    return name


def stripped(name: str) -> str:
    return name.strip()


class Value(BaseModel):
    value: int

    @validator("value")
    def non_negative(cls, value: int) -> int:
        if value < 0:
            raise NibblerValidation("cant-be-negative")
        return value


class Platform(BaseModel):
    platform: str

    _stripped_platform = validator('platform', allow_reuse=True)(stripped)
    _non_empty_platform = validator('platform', allow_reuse=True)(non_empty)


class Metric(BaseModel):
    metric: str

    _stripped_metric = validator('metric', allow_reuse=True)(stripped)
    _non_empty_metric = validator('metric', allow_reuse=True)(non_empty)

    @validator("metric")
    def not_digit(cls, metric: str) -> str:
        if metric.isdigit():
            raise NibblerValidation("cant-be-digit")
        return metric


class Title(BaseModel):
    title: Optional[str]

    _stripped_title = validator('title', allow_reuse=True)(stripped)
    _non_empty_title = validator('title', allow_reuse=True)(non_empty)

    @validator("title")
    def not_digit(cls, title: str) -> str:
        if title.isdigit():
            raise NibblerValidation("cant-be-digit")
        return title


class DimensionData(BaseModel):
    key: str
    value: Union[str, int]
    # ToAsk: is it possible that the `vaule` can be iteger in some cases? not sure how can all these various dimension_data look like

    _non_empty_key = validator('key', allow_reuse=True)(non_empty)
    _non_empty_value = validator('value', allow_reuse=True)(non_empty)
    # _stripped_value = validator('value', allow_reuse=True)(stripped)

    @validator("key", allow_reuse=True)
    @validator("value", allow_reuse=True)
    def stripped(cls, v):
        if isinstance(v, str):
            v = v.strip()
        return v

    @validator("key")
    def not_digit(cls, key: str) -> str:
        if key.isdigit():
            raise NibblerValidation("cant-be-digit")
        return key

    @validator("value")
    def non_negative(cls, value):
        if isinstance(value, int):
            if value < 0:
                raise NibblerValidation("cant-be-negative")
        return value


class Date(BaseModel):
    date: datetime.date

    _stripped_date = validator('date', allow_reuse=True, pre=True)(stripped)
    _non_empty_date = validator('date', allow_reuse=True, pre=True)(non_empty)

    @validator("date", pre=True)
    def to_datetime(cls, date: str) -> str:
        try:
            return datetimes_parser.parse(date)
        except datetimes_parser.ParserError:
            raise NibblerValidation("cant-parse-date")


class DateInString(BaseModel):
    date: datetime.date

    _stripped_date = validator('date', allow_reuse=True, pre=True)(stripped)
    _non_empty_date = validator('date', allow_reuse=True, pre=True)(non_empty)

    @validator("date", pre=True)
    def to_datetime(cls, date: str) -> str:
        # TODO add some validation whether the date in the 'date' variable is really on the last index
        try:
            return datetimes_parser.parse(date.split(' ')[-1])
        except datetimes_parser.ParserError:
            raise NibblerValidation("cant-parse-date")
