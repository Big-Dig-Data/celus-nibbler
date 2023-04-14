import datetime
from functools import lru_cache
from typing import Any, Optional, Tuple, Type, Union

from dateutil import parser as datetimes_parser
from pydantic import BaseModel, validator

from .utils import COMMON_DATE_FORMATS


class BaseValueModel(BaseModel):
    value: Any


def non_empty(name: str) -> str:
    if not name:
        raise ValueError("cant-be-empty")
    return name


def stripped(name: Optional[str]) -> Optional[str]:
    if name:
        return name.strip()
    return name


def not_none(value: Any) -> Any:
    if value is None:
        raise ValueError("cant-be-none")
    return value


def issn(issn: str) -> str:
    return issn.strip() or ""


class Value(BaseValueModel):
    value: Union[int, float]

    @validator("value")
    def non_negative(cls, value: Union[int, float]) -> Union[int, float]:
        if value < 0:
            raise ValueError("cant-be-negative")
        return round(value)


@lru_cache
def gen_default_validator(
    orig_validator: Type[BaseValueModel], default_value: Any, blank_values: Tuple[Any]
) -> Type[BaseValueModel]:
    class Validator(BaseValueModel):
        value: Any

        @validator("value", allow_reuse=True)
        def default(cls, value: Any) -> Any:
            if value in blank_values:
                return default_value
            else:
                return orig_validator(value=value).value

    return Validator


class CommaSeparatedNumberValidator(BaseValueModel):
    value: Any

    @validator("value", allow_reuse=True, pre=True)
    def comma_separeted_number(cls, value: str) -> str:
        return Value(value=value.replace(",", "")).value


class Organization(BaseValueModel):
    value: str

    _not_none_organization = validator('value', allow_reuse=True)(not_none)
    _stripped_organization = validator('value', allow_reuse=True)(stripped)
    _non_empty_organization = validator('value', allow_reuse=True)(non_empty)


class Platform(BaseValueModel):
    value: str

    _stripped_platform = validator('value', allow_reuse=True)(stripped)
    _non_empty_platform = validator('value', allow_reuse=True)(non_empty)


class Dimension(BaseValueModel):
    value: str

    _stripped_dimension = validator('value', allow_reuse=True)(stripped)


class Metric(BaseValueModel):
    value: str

    _not_none_metic = validator('value', allow_reuse=True)(not_none)
    _stripped_metric = validator('value', allow_reuse=True)(stripped)
    _non_empty_metric = validator('value', allow_reuse=True)(non_empty)

    @validator("value")
    def not_digit(cls, metric: str) -> str:
        if metric.isdigit():
            raise ValueError("cant-be-digit")
        return metric


class Title(BaseValueModel):
    value: Optional[str]

    _stripped_title = validator('value', allow_reuse=True)(stripped)


class Date(BaseValueModel):
    value: datetime.date

    _not_none_date = validator('value', allow_reuse=True, pre=True)(not_none)
    _stripped_date = validator('value', allow_reuse=True, pre=True)(stripped)
    _non_empty_date = validator('value', allow_reuse=True, pre=True)(non_empty)

    _parserinfo = datetimes_parser.parserinfo(dayfirst=False)  # prefer US variant

    @validator("value", pre=True)
    def to_datetime(cls, date: str) -> datetime.datetime:
        # Check for common formats (faster that dateutil)
        for fmt in COMMON_DATE_FORMATS:
            try:
                return datetime.datetime.strptime(date, fmt).replace(day=1)
            except ValueError:
                pass

        # Try to parse date using dateutil for more obscure date formats
        try:
            return datetimes_parser.parse(date, parserinfo=cls._parserinfo).replace(day=1)
        except datetimes_parser.ParserError:
            raise ValueError("cant-parse-date")


class DateEU(Date):
    _parserinfo = datetimes_parser.parserinfo(dayfirst=True)  # prefer EU variant


class DOI(BaseValueModel):
    value: str

    @validator("value")
    def check_doi(cls, doi: str) -> str:
        return doi.strip() or ""


class URI(BaseValueModel):
    value: str

    @validator("value")
    def check_uri(cls, uri: str) -> str:
        return uri.strip() or ""


class ISBN(BaseValueModel):
    value: str

    @validator("value")
    def check_isbn(cls, isbn: str) -> str:
        return isbn.strip() or ""


class ISSN(BaseValueModel):
    value: str

    _issn_format = validator('value', allow_reuse=True)(issn)


class EISSN(BaseValueModel):
    value: str

    _issn_format = validator('value', allow_reuse=True)(issn)


class ProprietaryID(BaseValueModel):
    value: str
