import datetime
import re
from typing import Optional, Union

from dateutil import parser as datetimes_parser
from pydantic import BaseModel, ValidationError, validator


def non_empty(name: str) -> str:
    if not name:
        raise ValidationError("cant-be-empty")
    return name


def stripped(name: str) -> str:
    return name.strip()


def issn(issn: str) -> str:
    if not issn:
        return ""
    if not re.match(r"^[0-9]{4}-[0-9]{3}[0-9X]$", issn):
        raise ValidationError("not-issn")
    return issn


class Value(BaseModel):
    value: Union[int, float]

    @validator("value")
    def non_negative(cls, value: Union[int, float]) -> int:
        if value < 0:
            raise ValidationError("cant-be-negative")
        return round(value)


class Platform(BaseModel):
    platform: str

    _stripped_platform = validator('platform', allow_reuse=True)(stripped)
    _non_empty_platform = validator('platform', allow_reuse=True)(non_empty)


class Dimension(BaseModel):
    dimension: str

    _stripped_dimension = validator('dimension', allow_reuse=True)(stripped)


class Metric(BaseModel):
    metric: str

    _stripped_metric = validator('metric', allow_reuse=True)(stripped)
    _non_empty_metric = validator('metric', allow_reuse=True)(non_empty)

    @validator("metric")
    def not_digit(cls, metric: str) -> str:
        if metric.isdigit():
            raise ValidationError("cant-be-digit")
        return metric


class Title(BaseModel):
    title: Optional[str]

    @validator("title")
    def not_digit(cls, title: str) -> str:
        if title.isdigit():
            raise ValidationError("cant-be-digit")
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
            raise ValidationError("cant-parse-date")


class DOI(BaseModel):
    doi: str

    @validator("doi")
    def check_doi(cls, doi: str) -> str:
        if not doi:
            return ""
        if not re.match(r"^10\.[\d\.]+\/[^\s]+$", doi):
            raise ValidationError("not-doi")

        return doi


class ISBN(BaseModel):
    isbn: str

    @validator("isbn")
    def check_isbn(cls, isbn: str) -> str:
        if not isbn:
            return ""

        if not re.match(r"^[0-9\-]{9,}$", isbn):
            raise ValidationError("not-isbn")

        return isbn


class ISSN(BaseModel):
    issn: str

    _issn_format = validator('issn', allow_reuse=True)(issn)


class EISSN(BaseModel):
    eissn: str

    _issn_format = validator('eissn', allow_reuse=True)(issn)


class ProprietaryID(BaseModel):
    proprietary_id: str


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
            raise ValidationError("cant-parse-date")
