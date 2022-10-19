import datetime
import typing
from enum import Enum

from pydantic import ValidationError
from pydantic.dataclasses import dataclass

from celus_nibbler import validators
from celus_nibbler.coordinates import Coord, CoordRange, SheetAttr, Value
from celus_nibbler.errors import TableException
from celus_nibbler.reader import SheetReader
from celus_nibbler.utils import JsonEncorder, PydanticConfig

Source = typing.Union[Coord, CoordRange, SheetAttr, Value]

IDS_VALIDATORS = {
    "DOI": validators.DOI,
    "ISBN": validators.ISBN,
    "Print_ISSN": validators.ISSN,
    "Online_ISSN": validators.EISSN,
    "Proprietary": validators.ProprietaryID,
}


class Role(str, Enum):
    VALUE = "value"
    DATE = "date"
    TITLE = "title"
    TITLE_ID = "title_id"
    DIMENSION = "dimension"
    METRIC = "metric"
    ORGANIZATION = "organization"


class ContentExtractorMixin:
    source: Source
    regex: typing.Optional[typing.Pattern]
    default: typing.Optional[typing.Any]
    last_value_as_default: bool
    role: Role

    def content(self, sheet: SheetReader, idx: int) -> typing.Any:
        content = self.source[idx].content(sheet)
        if self.regex:
            if extracted := self.regex.search(content):
                return extracted.group(1)
            else:
                # Unable to extract data
                return None
        return content

    def extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
    ) -> typing.Any:
        value = self._extract(sheet, idx, validator)
        self.last_value = value
        return value

    def _extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
    ) -> typing.Any:
        coord = self.coord(idx)
        try:
            content = self.content(sheet, idx)
            validator = validator or self.validator
            if validator:
                if self.default is not None:
                    return validators.gen_default_validator(validator, self.default)(
                        value=content
                    ).value
                elif self.last_value_as_default:
                    last_value = getattr(self, 'last_value', None)
                    if last_value:
                        return validators.gen_default_validator(validator, last_value)(
                            value=content
                        ).value

                return validator(value=content).value
            else:
                return content
        except ValidationError as e:
            if isinstance(self.source, Value):
                raise TableException(
                    value=self.source.value, sheet=sheet.sheet_idx, reason="wrong-value"
                )
            elif isinstance(self.source, SheetAttr):
                raise TableException(
                    value=self.source.sheet_attr, sheet=sheet.sheet_idx, reason="wrong-sheet-attr"
                )
            else:
                raise TableException(
                    content,
                    row=coord.row if coord else None,
                    col=coord.col if coord else None,
                    sheet=sheet.sheet_idx,
                    reason=e.model.__name__.lower() if content else "empty",
                ) from e
        except IndexError as e:
            raise TableException(
                row=coord.row if coord else None,
                col=coord.col if coord else None,
                sheet=sheet.sheet_idx,
                reason='out-of-bounds',
            ) from e

    def coord(self, idx: int) -> typing.Optional[Coord]:
        if isinstance(self.source, Coord):
            return Coord(self.source.row, self.source.col)
        elif isinstance(self.source, CoordRange):
            return self.source[idx]
        return None

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return None


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder, ContentExtractorMixin):
    name: str
    source: Source
    required: bool = True
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[str] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.DIMENSION] = Role.DIMENSION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Dimension


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[str] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.METRIC] = Role.METRIC

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Metric


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[str] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.ORGANIZATION] = Role.ORGANIZATION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Organization


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[str] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.TITLE] = Role.TITLE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Title


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder, ContentExtractorMixin):
    name: str
    source: Source
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[str] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.TITLE_ID] = Role.TITLE_ID

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return IDS_VALIDATORS.get(self.name)


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[datetime.date] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.DATE] = Role.DATE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Date


@dataclass(config=PydanticConfig)
class ValueSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[int] = None
    last_value_as_default: bool = False
    role: typing.Literal[Role.VALUE] = Role.VALUE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Value
