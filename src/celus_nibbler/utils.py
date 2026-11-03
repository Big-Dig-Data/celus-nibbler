import contextlib
import cProfile
import functools
import re
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
    frozen=False,
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


# Matches either a run of whitespaces or a single non-space whitespace (tab, nbsp, newline, ...)
# Replacing both with a space is equivalent to converting all whitespaces to spaces first
# and then collapsing runs of spaces, but done in a single pass
_MULTI_WHITESPACE_RE = re.compile(r"\s{2,}|[^\S ]")


def _may_need_normalization(text: str) -> bool:
    # Fast check without regex: plain space is the only printable whitespace character,
    # so a printable text without doubled spaces can't contain a run of whitespaces
    return "  " in text or not text.isprintable()


def normalize_spaces(text: str) -> str:
    """Convert whitespaces to spaces and collapse runs into a single space

    e.g. "a \\t a  c\\td" -> "a a c d"
    """
    if not _may_need_normalization(text):
        return text
    return _MULTI_WHITESPACE_RE.sub(" ", text)


def normalize_row(row: typing.List[str]) -> typing.List[str]:
    """Normalize whitespaces in the row in place

    Only the cells which contain whitespaces to be normalized are replaced,
    the others stay untouched and the row itself is not copied.
    """
    for idx, cell in enumerate(row):
        if _may_need_normalization(cell):
            row[idx] = _MULTI_WHITESPACE_RE.sub(" ", cell)
    return row


def colnum_to_colletters(colnum: int) -> str:
    colletters = ""
    while colnum > 0:
        colnum, remainder = divmod(colnum - 1, 26)
        colletters = chr(65 + remainder) + colletters
    return colletters
