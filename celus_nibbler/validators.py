import datetime
from typing import Optional

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

    @validator("title")
    def not_digit(cls, title: str) -> str:
        if title.isdigit():
            raise NibblerValidation("cant-be-digit")
        return title


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
