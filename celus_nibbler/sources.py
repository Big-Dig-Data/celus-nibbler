import typing
from dataclasses import field
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
    "URI": validators.URI,
}


class Role(str, Enum):
    VALUE = "value"
    DATE = "date"
    TITLE = "title"
    TITLE_ID = "title_id"
    DIMENSION = "dimension"
    METRIC = "metric"
    ORGANIZATION = "organization"


@dataclass(config=PydanticConfig)
class ExtractParams(JsonEncorder):
    regex: typing.Optional[typing.Pattern] = None
    default: typing.Optional[typing.Any] = None
    last_value_as_default: bool = False
    blank_values: typing.List[typing.Any] = field(default_factory=lambda: [None, ""])
    skip_validation: bool = False


class ContentExtractorMixin:
    source: Source
    extract_params: ExtractParams
    role: Role
    _last_sheet = None
    _last_coord = None
    _last_extracted = None

    def content(self, sheet: SheetReader, idx: int) -> typing.Any:
        content = self.source[idx].content(sheet)
        if regex := self.extract_params.regex:
            if extracted := regex.search(content):
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
        if self.extract_params.last_value_as_default:
            self.extract_params.default = value
        return value

    def _extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
    ) -> typing.Any:
        coord = self.coord(idx)
        if coord == self._last_coord and self._last_sheet == sheet and self._last_extracted:
            # Same value will be extracted from the same coord
            return self._last_extracted
        else:
            self._last_coord = coord

        try:
            content = self.content(sheet, idx)
            validator = validator or self.validator
            if validator:
                if self.extract_params.default is not None:
                    res = validators.gen_default_validator(
                        validator, self.extract_params.default, self.extract_params.blank_values
                    )(value=content).value
                elif self.extract_params.skip_validation:
                    res = (content or "").strip()
                else:
                    res = validator(value=content).value

            else:
                res = content
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

        # update last extracted
        self._last_extracted = res
        self._last_sheet = sheet
        return res

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
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.DIMENSION] = Role.DIMENSION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Dimension


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.METRIC] = Role.METRIC

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Metric


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.ORGANIZATION] = Role.ORGANIZATION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Organization


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.TITLE] = Role.TITLE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Title


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder, ContentExtractorMixin):
    name: str
    source: Source
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.TITLE_ID] = Role.TITLE_ID

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return IDS_VALIDATORS.get(self.name)


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.DATE] = Role.DATE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Date


@dataclass(config=PydanticConfig)
class ValueSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = ExtractParams()
    role: typing.Literal[Role.VALUE] = Role.VALUE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Value
