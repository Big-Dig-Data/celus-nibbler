import datetime
import inspect
import re
from functools import lru_cache
from typing import Any, List, Optional, Tuple, Type, Union

from celus_nigiri.record import Author
from dateutil import parser as datetimes_parser
from isbnlib import get_isbnlike, is_isbn10, is_isbn13
from pydantic import NonNegativeFloat, NonNegativeInt, field_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass

from .utils import COMMON_DATE_FORMATS, PydanticConfig

issn_matcher = re.compile(r"(\d{4})-?(\d{3}[\dXx])")
issn_number_matcher = re.compile(r"^\d{0,7}[\dXx]$")


@pydantic_dataclass(config=PydanticConfig)
class BaseValueModel:
    model_config = PydanticConfig
    name = "base"

    value: Any


def non_empty(name: str) -> str:
    if not name:
        raise ValueError("cant-be-empty")
    return name


def stripped(name: Optional[str]) -> Optional[str]:
    if name and isinstance(name, str):
        return name.strip()
    return name


def not_none(value: Any) -> Any:
    if value is None:
        raise ValueError("cant-be-none")
    return value


def issn(issn: str) -> str:
    return issn.strip() or ""


def issn_strict(issn: str) -> str:
    clean = "".join(issn.split())  # remove all whitespace

    if m := issn_matcher.search(clean):
        # upper() because 'X' can be also lowercase
        return m.group(1) + "-" + m.group(2).upper()

    # sometimes the leading zeros are missing, so we add them
    if issn_number_matcher.match(clean):
        clean = (8 - len(clean)) * "0" + clean
        return clean[:4] + "-" + clean[4:].upper()

    raise ValueError(f'Invalid ISSN: "{issn}"')


def date_aligned(value: datetime.datetime) -> datetime.datetime:
    if value.day != 1:
        raise ValueError("date-not-aligned")
    return value


def remove_multiple_spaces(value: Optional[str]) -> str:
    return value and re.sub(r"\s+", " ", value)


@pydantic_dataclass(config=PydanticConfig)
class Value(BaseValueModel):
    name = "value"
    value: Union[NonNegativeInt, NonNegativeFloat]

    _stripped_value = field_validator("value", mode="before")(stripped)

    @field_validator("value")
    def non_negative(cls, value: Union[NonNegativeInt, NonNegativeFloat]) -> int:
        return round(value)


@pydantic_dataclass(config=PydanticConfig)
class ValueNegative(BaseValueModel):
    name = "value_negative"
    value: Union[int, float]

    @field_validator("value")
    def non_negative(cls, value: Union[int, float]) -> Union[int, float]:
        return round(value)


@lru_cache
def gen_default_validator(
    orig_validator: Type[BaseValueModel], default_value: Any, blank_values: Tuple[Any]
) -> Type[BaseValueModel]:
    @pydantic_dataclass(config=PydanticConfig)
    class Validator(BaseValueModel):
        name = f"{orig_validator.name}_default"
        value: Any

        @field_validator("value")
        def default(cls, value: Any) -> Any:
            if value in blank_values:
                return default_value
            else:
                return orig_validator(value=value).value

    return Validator


@pydantic_dataclass(config=PydanticConfig)
class CommaSeparatedNumberValidator(BaseValueModel):
    name = "comma_separeted_number"
    value: Any

    @field_validator("value", mode="before")
    def comma_separeted_number(cls, value: str) -> str:
        return Value(value=value.replace(",", "")).value


@pydantic_dataclass(config=PydanticConfig)
class MinutesToSecondsValidator(BaseValueModel):
    name = "minutes_to_seconds"
    value: Union[NonNegativeInt, NonNegativeFloat]

    @field_validator("value", mode="after")
    def minutes_to_seconds(cls, value: Union[float, int]) -> int:
        return round(value * 60)


@pydantic_dataclass(config=PydanticConfig)
class HoursToSecondsValidator(BaseValueModel):
    name = "hours_to_seconds"
    value: Union[NonNegativeInt, NonNegativeFloat]

    @field_validator("value", mode="after")
    def hours_to_seconds(cls, value: Union[float, int]) -> int:
        return round(value * 60 * 60)


@pydantic_dataclass(config=PydanticConfig)
class DurationToSecondsValidator(BaseValueModel):
    """Converts HH:MM:SS to format to seconds"""

    name = "duration_to_seconds"
    value: Union[NonNegativeInt, NonNegativeFloat]

    @field_validator("value", mode="before")
    def duration_to_seconds(cls, value: str) -> int:
        try:
            hours, minutes, seconds = value.split(":")
            return 60 * 60 * int(hours) + 60 * int(minutes) + int(seconds)
        except ValueError:
            raise ValueError("cant-parse-duration")


@pydantic_dataclass(config=PydanticConfig)
class Organization(BaseValueModel):
    name = "organization"
    value: str

    _not_none_organization = field_validator("value")(not_none)
    _stripped_organization = field_validator("value")(stripped)
    _non_empty_organization = field_validator("value")(non_empty)
    _remove_multiple_spaces = field_validator("value")(remove_multiple_spaces)


@pydantic_dataclass(config=PydanticConfig)
class Platform(BaseValueModel):
    name = "platform"
    value: str

    _stripped_platform = field_validator("value")(stripped)
    _non_empty_platform = field_validator("value")(non_empty)


@pydantic_dataclass(config=PydanticConfig)
class Dimension(BaseValueModel):
    name = "dimension"
    value: str

    _stripped_dimension = field_validator("value")(stripped)
    _remove_multiple_spaces = field_validator("value")(remove_multiple_spaces)


@pydantic_dataclass(config=PydanticConfig)
class Metric(BaseValueModel):
    name = "metric"
    value: str

    _not_none_metic = field_validator("value")(not_none)
    _stripped_metric = field_validator("value")(stripped)
    _non_empty_metric = field_validator("value")(non_empty)
    _remove_multiple_spaces = field_validator("value")(remove_multiple_spaces)

    @field_validator("value")
    def not_digit(cls, metric: str) -> str:
        if metric.isdigit():
            raise ValueError("cant-be-digit")
        return metric


@pydantic_dataclass(config=PydanticConfig)
class Title(BaseValueModel):
    name = "title"
    value: Optional[str]

    _stripped_title = field_validator("value")(stripped)
    _remove_multiple_spaces = field_validator("value")(remove_multiple_spaces)


parserinfo_us = datetimes_parser.parserinfo(dayfirst=False)  # prefer US variant
parserinfo_eu = datetimes_parser.parserinfo(dayfirst=True)  # prefer EU variant


@pydantic_dataclass(config=PydanticConfig)
class Date(BaseValueModel):
    name = "date"
    value: datetime.date

    _not_none_date = field_validator("value", mode="before")(not_none)
    _stripped_date = field_validator("value", mode="before")(stripped)
    _non_empty_date = field_validator("value", mode="before")(non_empty)

    @classmethod
    def parserinfo(cls):
        return parserinfo_us

    @classmethod
    def align_date(cls, input: datetime.datetime) -> datetime.datetime:
        return input.replace(day=1)

    @field_validator("value", mode="before")
    def to_datetime(cls, date: str) -> datetime.datetime:
        if not date:
            raise ValueError("no-date-provided")

        # Check for common formats (faster that dateutil)
        for fmt in COMMON_DATE_FORMATS:
            try:
                return cls.align_date(datetime.datetime.strptime(date, fmt))
            except ValueError:
                pass

        # Try to parse date using dateutil for more obscure date formats
        try:
            return cls.align_date(datetimes_parser.parse(date, parserinfo=cls.parserinfo()))
        except datetimes_parser.ParserError:
            raise ValueError("cant-parse-date")


@pydantic_dataclass(config=PydanticConfig)
class DateEU(Date):
    name = "date_eu"

    @classmethod
    def parserinfo(cls):
        return parserinfo_eu


@pydantic_dataclass(config=PydanticConfig)
class DateAligned(Date):
    name = "date_aligned"
    _date_aligned = field_validator("value", mode="after")(date_aligned)

    @classmethod
    def align_date(cls, input: datetime.datetime) -> datetime.datetime:
        # Don't perfrom auto aligment
        return input


@pydantic_dataclass(config=PydanticConfig)
class DateEUAligned(DateEU):
    name = "date_eu_aligned"
    _date_aligned = field_validator("value", mode="after")(date_aligned)

    @classmethod
    def align_date(cls, input: datetime.datetime) -> datetime.datetime:
        # Don't perfrom auto aligment
        return input


@lru_cache
def gen_date_format_validator(pattern: str) -> Type[BaseValueModel]:
    @pydantic_dataclass(config=PydanticConfig)
    class DateFormat(BaseValueModel):
        name = "date_format"
        value: datetime.date

        _not_none_date = field_validator("value", mode="before")(not_none)
        _stripped_date = field_validator("value", mode="before")(stripped)
        _non_empty_date = field_validator("value", mode="before")(non_empty)

        @field_validator("value", mode="before")
        def to_datetime(cls, date: str) -> datetime.date:
            if not date:
                raise ValueError("no-date-provided")
            try:
                parsed = datetime.datetime.strptime(date, pattern)
                return datetime.date(year=parsed.year, month=parsed.month, day=1)
            except ValueError:
                raise ValueError("cant-parse-date")

    return DateFormat


@pydantic_dataclass(config=PydanticConfig)
class DOI(BaseValueModel):
    name = "doi"
    value: str

    @field_validator("value")
    def check_doi(cls, doi: str) -> str:
        return doi.strip() or ""


@pydantic_dataclass(config=PydanticConfig)
class URI(BaseValueModel):
    name = "uri"
    value: str

    @field_validator("value")
    def check_uri(cls, uri: str) -> str:
        return uri.strip() or ""


@pydantic_dataclass(config=PydanticConfig)
class ISBN(BaseValueModel):
    name = "isbn"
    value: str

    @field_validator("value")
    def check_isbn(cls, isbn: str) -> str:
        return isbn.strip() or ""


@pydantic_dataclass(config=PydanticConfig)
class StrictISBN(BaseValueModel):
    name = "isbn_strict"
    value: str

    @field_validator("value")
    def check_isbn(cls, isbn: str) -> str:
        isbns = get_isbnlike(isbn, level="strict")

        if not isbns:
            raise ValueError("isbn-not-valid")

        # return the first isbn the rest is omitted
        isbn = isbns[0]

        # check isbns including checksums
        if not (is_isbn10(isbn) or is_isbn13(isbn)):
            raise ValueError("isbn-not-valid")

        return isbn


@pydantic_dataclass(config=PydanticConfig)
class StrictISBN13(BaseValueModel):
    name = "isbn13"
    value: str

    @field_validator("value")
    def check_isbn(cls, isbn: str) -> str:
        isbns = get_isbnlike(isbn, level="strict")

        if not isbns:
            raise ValueError("isbn13-not-valid")

        # return the first isbn the rest is omitted
        isbn = isbns[0]

        # check isbns including checksums
        if not is_isbn13(isbn):
            raise ValueError("isbn13-not-valid")

        return isbn


@pydantic_dataclass(config=PydanticConfig)
class StrictISBN10(BaseValueModel):
    name = "isbn10"
    value: str

    @field_validator("value")
    def check_isbn(cls, isbn: str) -> str:
        isbns = get_isbnlike(isbn, level="strict")

        if not isbns:
            raise ValueError("isbn10-not-valid")

        # return the first isbn the rest is omitted
        isbn = isbns[0]

        # check isbns including checksums
        if not is_isbn10(isbn):
            raise ValueError("isbn10-not-valid")

        return isbn


@pydantic_dataclass(config=PydanticConfig)
class ISSN(BaseValueModel):
    name = "issn"
    value: str

    _issn_format = field_validator("value")(issn)


@pydantic_dataclass(config=PydanticConfig)
class StrictISSN(BaseValueModel):
    name = "issn_strict"
    value: str

    _issn_format = field_validator("value")(issn_strict)


@pydantic_dataclass(config=PydanticConfig)
class EISSN(BaseValueModel):
    name = "eissn"
    value: str

    _issn_format = field_validator("value")(issn)


@pydantic_dataclass(config=PydanticConfig)
class StrictEISSN(BaseValueModel):
    name = "eissn_strict"
    value: str

    _issn_format = field_validator("value")(issn_strict)


@pydantic_dataclass(config=PydanticConfig)
class ProprietaryID(BaseValueModel):
    name = "proprietary_id"
    value: str


@pydantic_dataclass(config=PydanticConfig)
class AuthorsValidator(BaseValueModel):
    name = "authors"
    value: str

    AUTHOR_REGEX = re.compile(r"^([^\(]+)\s+\(([a-zA-Z]+):([^\)]+)\)$")

    @field_validator("value")
    def authors(cls, authors: str) -> List[Author]:
        res = []
        splitted_authors = [e.strip() for e in authors.split(";")]
        for sa in splitted_authors:
            if match := cls.AUTHOR_REGEX.match(sa):
                name, kind, code = match.groups()
                if kind == "ISNI":
                    res.append(Author(name=name, ISNI=code))
                elif kind == "ORCID":
                    res.append(Author(name=name, ORCID=code))
                else:
                    res.append(Author(name=name))
            else:
                res.append(Author(name=sa))

        return res


@pydantic_dataclass(config=PydanticConfig)
class YOPValidator(BaseValueModel):
    name = "yop"
    value: str

    YOP_REGEX = re.compile(r"^\s*(\d{4})\s*$")

    @field_validator("value")
    def yop(cls, yop: str) -> List[Author]:
        if not cls.YOP_REGEX.match(yop):
            raise ValueError("not-yop")

        return yop


validators = [e for e in locals().values() if inspect.isclass(e) and issubclass(e, BaseValueModel)]
