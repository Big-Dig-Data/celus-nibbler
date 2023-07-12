import typing
from dataclasses import field
from datetime import date
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

IDS_VALIDATORS_STRICT = {
    "DOI": validators.DOI,  # TODO strict validator for DOI
    "ISBN": validators.StrictISBN,
    "Print_ISSN": validators.StrictISSN,
    "Online_ISSN": validators.StrictEISSN,
    "Proprietary": validators.ProprietaryID,
    "URI": validators.URI,  # TODO strict validator for URI
}


class DateFormat(str, Enum):
    US = "us"  # US format 12/31/2022 (more common)
    EU = "eu"  # EU format 31/12/2022


class SpecialExtraction(str, Enum):
    NO = "no"
    COMMA_SEPARATED_NUMBER = "comma_separated_number"


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
    blank_values: typing.Tuple[typing.Any, ...] = field(default_factory=lambda: (None, ""))
    skip_validation: bool = False
    prefix: str = ""
    suffix: str = ""
    special_extraction: SpecialExtraction = SpecialExtraction.NO
    on_validation_error: TableException.Action = TableException.Action.FAIL


class ContentExtractorMixin:
    source: Source
    extract_params: ExtractParams
    cleanup_during_header_processing: bool
    role: Role
    _last_sheet_idx = None
    _last_source = None
    _last_extracted = None

    def content(self, sheet: SheetReader, source: Source) -> typing.Any:
        content = source.content(sheet)
        if regex := self.extract_params.regex:
            if extracted := regex.search(content):
                content = extracted.group(1)
            else:
                # Unable to extract data
                return None

        if prefix := self.extract_params.prefix:
            content = prefix + content

        if suffix := self.extract_params.suffix:
            content = content + suffix

        return content

    def extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseModel]] = None,
        row_offset: typing.Optional[int] = None,
    ) -> typing.Any:
        value = self._extract(sheet, idx, validator, row_offset)
        if self.extract_params.last_value_as_default:
            self.extract_params.default = value
        return value

    def get_validator(
        self, validator: typing.Optional[typing.Type[validators.BaseModel]]
    ) -> typing.Optional[typing.Type[validators.BaseModel]]:
        if self.extract_params.special_extraction == SpecialExtraction.NO:
            return validator or self.validator
        elif self.extract_params.special_extraction == SpecialExtraction.COMMA_SEPARATED_NUMBER:
            return validators.CommaSeparatedNumberValidator
        else:
            raise NotImplementedError()

    def _extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseModel]] = None,
        row_offset: typing.Optional[int] = None,
    ) -> typing.Any:

        if row_offset:
            source = self.source.with_row_offset(row_offset)
        else:
            source = self.source

        source = source[idx]
        if (
            source == self._last_source
            and self._last_sheet_idx == sheet.sheet_idx
            and self._last_extracted
        ):
            # Same value will be extracted from the same coord
            return self._last_extracted
        else:
            self._last_source = source

        try:
            content = self.content(sheet, source)
            if validator := self.get_validator(validator):
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
                    value=self.source.value,
                    sheet=sheet.sheet_idx,
                    reason="wrong-value",
                    action=self.extract_params.on_validation_error,
                )
            elif isinstance(self.source, SheetAttr):
                raise TableException(
                    value=self.source.sheet_attr,
                    sheet=sheet.sheet_idx,
                    reason="wrong-sheet-attr",
                    action=self.extract_params.on_validation_error,
                )
            else:
                raise TableException(
                    content,
                    row=getattr(source, 'row', None),
                    col=getattr(source, 'col', None),
                    sheet=sheet.sheet_idx,
                    reason=e.model.__name__.lower(),
                    action=self.extract_params.on_validation_error,
                ) from e
        except IndexError as e:
            raise TableException(
                row=getattr(source, 'row', None),
                col=getattr(source, 'col', None),
                sheet=sheet.sheet_idx,
                reason='out-of-bounds',
                action=TableException.Action.STOP,
            ) from e

        # update last extracted
        self._last_extracted = res
        self._last_sheet_idx = sheet.sheet_idx
        return res

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return None


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder, ContentExtractorMixin):
    name: str
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.DIMENSION] = Role.DIMENSION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return validators.Dimension


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.METRIC] = Role.METRIC

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return validators.Metric


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.ORGANIZATION] = Role.ORGANIZATION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return validators.Organization


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.TITLE] = Role.TITLE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return validators.Title


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder, ContentExtractorMixin):
    name: str
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    fallback: typing.Optional['TitleIdSource'] = None
    strict: bool = False
    role: typing.Literal[Role.TITLE_ID] = Role.TITLE_ID

    @property
    def last_key(self):
        return getattr(self, '_last_key', self.name)

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        if self.strict:
            return IDS_VALIDATORS_STRICT.get(self.name)
        else:
            return IDS_VALIDATORS.get(self.name)

    def extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseModel]] = None,
        row_offset: typing.Optional[int] = None,
    ) -> typing.Any:

        self._last_key = None

        try:
            res = super().extract(sheet, idx, validator, row_offset)
            self._last_key = self.name

        except TableException as e:
            if e.action == TableException.Action.PASS and self.fallback:
                res = self.fallback.extract(sheet, idx, validator, row_offset)
                self._last_key = self.fallback.last_key
            else:
                raise

        return res


@dataclass(config=PydanticConfig)
class ComposedDate(JsonEncorder):
    year: 'DateSource'
    month: 'DateSource'


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    preferred_date_format: DateFormat = DateFormat.US
    composed: typing.Optional[ComposedDate] = None
    date_pattern: typing.Optional[str] = None
    role: typing.Literal[Role.DATE] = Role.DATE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return validators.Date

    def get_validator(
        self, validator: typing.Optional[typing.Type[validators.BaseModel]]
    ) -> typing.Optional[typing.Type[validators.BaseModel]]:

        if date_pattern := self.date_pattern:
            return validators.gen_date_format_validator(date_pattern)

        if self.preferred_date_format == DateFormat.EU:
            return validators.DateEU
        else:
            return validators.Date

    def extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseModel]] = None,
        row_offset: typing.Optional[int] = None,
    ) -> typing.Any:
        if self.composed:
            year_date = self.composed.year.extract(sheet, idx, row_offset)
            month_date = self.composed.month.extract(sheet, idx, row_offset)
            return date(year_date.year, month_date.month, 1)
        else:
            return super().extract(sheet, idx, validator, row_offset)


ComposedDate.__pydantic_model__.update_forward_refs()


@dataclass(config=PydanticConfig)
class ValueSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.VALUE] = Role.VALUE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseModel]]:
        return validators.Value
