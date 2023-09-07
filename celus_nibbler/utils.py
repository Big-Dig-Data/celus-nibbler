import contextlib
import cProfile
import functools
import typing
from datetime import date, timedelta

from pydantic import ConfigDict, TypeAdapter

COMMON_DATE_FORMATS = [
    "%Y-%m",
    "%Y-%m-%d",
    "%b-%Y",
    "%Y-%b",
    "%b-%y",
    "%y-%b",
]


@contextlib.contextmanager
def profile(*args, **kwargs):
    profile = cProfile.Profile(*args, **kwargs)
    profile.enable()
    yield
    profile.disable()

    print()
    profile.print_stats()


PydanticConfig = ConfigDict(
    extra="forbid",
    frozen=True,
    undefined_types_warning=False,
    defer_build=True,
)


class JsonEncorder:
    @classmethod
    @functools.lru_cache
    def adapter(cls):
        return TypeAdapter(cls)

    def dict(self):
        return self.adapter().dump_python(self)

    def json(self):
        return self.adapter().dump_json(self).decode()

    def model_dump(self):
        """To become more compatible with basic BaseModel"""
        return self.dict()

    @classmethod
    def parse(cls, obj: typing.Dict[str, typing.Any]):
        return cls.adapter().validate_python(obj)


def start_month(in_date: date) -> date:
    return in_date.replace(day=1)


def end_month(in_date: date) -> date:
    if in_date.month == 12:
        return date(year=in_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        return date(year=in_date.year, month=in_date.month + 1, day=1) - timedelta(days=1)


def colnum_to_colletters(colnum: int) -> str:
    colletters = ""
    while colnum > 0:
        colnum, remainder = divmod(colnum - 1, 26)
        colletters = chr(65 + remainder) + colletters
    return colletters
