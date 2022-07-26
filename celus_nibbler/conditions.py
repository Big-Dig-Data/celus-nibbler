import typing
from abc import ABCMeta, abstractmethod

from jellyfish import porter_stem
from pydantic import Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated
from unidecode import unidecode

from .coordinates import Coord
from .errors import TableException
from .reader import SheetReader
from .utils import JsonEncorder, PydanticConfig


class BaseCondition(metaclass=ABCMeta):
    @abstractmethod
    def check(self, sheet: SheetReader):
        pass


class ArithmeticsMixin:
    def __invert__(self):
        return NegCondition(self)

    def __or__(self, other):
        return OrCondition([self, other])

    def __and__(self, other):
        return AndCondition([self, other])


@dataclass(config=PydanticConfig)
class NegCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    cond: 'Condition'

    kind: typing.Literal["neg"] = "neg"

    def check(self, sheet: SheetReader):
        return not self.cond.check(sheet)


@dataclass(config=PydanticConfig)
class AndCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    conds: typing.List['Condition']

    kind: typing.Literal["and"] = "and"

    def check(self, sheet: SheetReader):
        return all(e.check(sheet) for e in self.conds)


@dataclass(config=PydanticConfig)
class OrCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    conds: typing.List['Condition']

    kind: typing.Literal["or"] = "or"

    def check(self, sheet: SheetReader):
        return any(e.check(sheet) for e in self.conds)


@dataclass(config=PydanticConfig)
class RegexCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    pattern: typing.Pattern
    coord: Coord

    kind: typing.Literal["regex"] = "regex"

    def check(self, sheet: SheetReader):
        try:
            return bool(self.pattern.match(self.coord.content(sheet)))
        except TableException:
            return False


@dataclass(config=PydanticConfig)
class StemmerCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Compare content based on Porter Stemming algorithm"""

    content: str
    coord: Coord

    kind: typing.Literal["stemmer"] = "stemmer"

    def _convert(self, text: str) -> str:
        return porter_stem(unidecode(text.strip()).lower())

    def __post_init__(self):
        self.content = self._convert(self.content)

    def check(self, sheet: SheetReader):
        try:
            return self._convert(self.coord.content(sheet)) == self.content
        except TableException:
            return False


@dataclass(config=PydanticConfig)
class SheetNameRegexCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Compare sheet name against the regex"""

    pattern: typing.Pattern

    kind: typing.Literal["sheet_name"] = "sheet_name"

    def check(self, sheet: SheetReader):
        if sheet.name is not None:
            return bool(self.pattern.match(sheet.name))
        else:
            return False


@dataclass(config=PydanticConfig)
class SheetIdxCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Limit sheet by its index"""

    min: typing.Optional[int] = None
    max: typing.Optional[int] = None

    kind: typing.Literal["sheet_idx"] = "sheet_idx"

    def check(self, sheet: SheetReader):
        if self.min is not None and sheet.sheet_idx < self.min:
            return False

        if self.max is not None and sheet.sheet_idx > self.max:
            return False

        return True


Condition = Annotated[
    typing.Union[
        NegCondition,
        OrCondition,
        AndCondition,
        RegexCondition,
        StemmerCondition,
        SheetNameRegexCondition,
        SheetIdxCondition,
    ],
    Field(discriminator='kind'),
]


@dataclass(config=PydanticConfig)
class SheetExtraCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Checks whether sheet extra attribute matches"""

    field_name: str
    value: typing.Any

    kind: typing.Literal["sheet_extra"] = "sheet_extra"

    def check(self, sheet: SheetReader) -> bool:
        if sheet.extra is None:
            return False
        if self.field_name in sheet.extra:
            return sheet.extra[self.field_name] == self.value
        else:
            return False


# Need to update forward refs
NegCondition.__pydantic_model__.update_forward_refs()
AndCondition.__pydantic_model__.update_forward_refs()
OrCondition.__pydantic_model__.update_forward_refs()
