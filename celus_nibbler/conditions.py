import typing
from abc import ABCMeta, abstractmethod

from nltk import stem
from pydantic import Field, ValidationError
from pydantic.dataclasses import dataclass, rebuild_dataclass
from typing_extensions import Annotated
from unidecode import unidecode

from . import validators
from .coordinates import Coord, CoordRange
from .errors import TableException
from .reader import SheetReader
from .utils import JsonEncorder, PydanticConfig

stemmer = stem.PorterStemmer()


class BaseCondition(metaclass=ABCMeta):
    @abstractmethod
    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
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
    cond: "Condition"

    kind: typing.Literal["neg"] = "neg"

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        return not self.cond.check(sheet, row_offset)


@dataclass(config=PydanticConfig)
class AndCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    conds: typing.List["Condition"]

    kind: typing.Literal["and"] = "and"

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        return all(e.check(sheet, row_offset) for e in self.conds)


@dataclass(config=PydanticConfig)
class OrCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    conds: typing.List["Condition"]

    kind: typing.Literal["or"] = "or"

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        return any(e.check(sheet, row_offset) for e in self.conds)


@dataclass(config=PydanticConfig)
class RegexCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    pattern: typing.Pattern
    coord: typing.Union[Coord, CoordRange]

    kind: typing.Literal["regex"] = "regex"

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        try:
            coord = self.coord
            if row_offset:
                coord = coord.with_row_offset(row_offset)

            if isinstance(coord, CoordRange):
                return any(bool(self.pattern.match(coord.content(sheet))) for coord in coord)

            return bool(self.pattern.match(coord.content(sheet)))
        except TableException:
            return False


@dataclass(config=PydanticConfig)
class IsDateCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    coord: typing.Union[Coord, CoordRange]
    date_format: typing.Optional[str] = None

    kind: typing.Literal["is_date"] = "is_date"

    @property
    def validator_class(self) -> typing.Type[validators.BaseValueModel]:
        if not self.date_format:
            return validators.Date
        else:
            return validators.gen_date_format_validator(self.date_format)

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        # Deal with offsets
        coord = self.coord
        if row_offset:
            coord = coord.with_row_offset(row_offset)

        # Handle coord ranges
        if isinstance(coord, CoordRange):
            for c in coord:
                try:
                    content = c.content(sheet)
                    content = content and content.strip()
                    self.validator_class(value=content)
                except ValidationError:
                    continue
                except (TableException, IndexError):
                    return False  # end was reached

                return True

        # Handle single coord
        try:
            content = coord.content(sheet)
            content = content and content.strip()
            self.validator_class(value=content)
        except (TableException, ValidationError, IndexError):
            return False

        return True


@dataclass(config=PydanticConfig)
class StemmerCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Compare content based on Porter Stemming algorithm"""

    content: str
    coord: typing.Union[Coord, CoordRange]

    kind: typing.Literal["stemmer"] = "stemmer"

    def _convert(self, text: str) -> str:
        return stemmer.stem(unidecode(text.strip()).lower())

    def __post_init__(self):
        self.content = self._convert(self.content)

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        try:
            if isinstance(self.coord, CoordRange):
                return any(
                    self._convert(coord.content(sheet)) == self.content for coord in self.coord
                )
            return self._convert(self.coord.content(sheet)) == self.content
        except TableException:
            return False


@dataclass(config=PydanticConfig)
class SheetNameRegexCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Compare sheet name against the regex"""

    pattern: typing.Pattern

    kind: typing.Literal["sheet_name"] = "sheet_name"

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
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

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
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
    Field(discriminator="kind"),
]


@dataclass(config=PydanticConfig)
class SheetExtraCondition(ArithmeticsMixin, BaseCondition, JsonEncorder):
    """Checks whether sheet extra attribute matches"""

    field_name: str
    value: typing.Any

    kind: typing.Literal["sheet_extra"] = "sheet_extra"

    def check(self, sheet: SheetReader, row_offset: typing.Optional[int] = None) -> bool:
        if sheet.extra is None:
            return False
        if self.field_name in sheet.extra:
            return sheet.extra[self.field_name] == self.value
        else:
            return False


# Need to update forward refs
rebuild_dataclass(NegCondition, force=True)
rebuild_dataclass(AndCondition, force=True)
rebuild_dataclass(OrCondition, force=True)
