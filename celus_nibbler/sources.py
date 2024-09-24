import typing
from dataclasses import field
from datetime import date
from enum import Enum

from pydantic import Field, ValidationError
from pydantic.dataclasses import dataclass, rebuild_dataclass
from typing_extensions import Annotated

from celus_nibbler import validators
from celus_nibbler.coordinates import Coord, CoordRange, SheetAttr, Value
from celus_nibbler.errors import TableException
from celus_nibbler.reader import SheetReader
from celus_nibbler.utils import JsonEncorder, PydanticConfig

Source = typing.Union[Coord, CoordRange, SheetAttr, Value]


class KindISBN(str, Enum):
    ISBN13 = "isbn13"
    ISBN10 = "isbn10"


class TitleIdKind(str, Enum):
    ISBN = "ISBN"
    Print_ISSN = "Print_ISSN"
    Online_ISSN = "Online_ISSN"
    Proprietary = "Proprietary"
    DOI = "DOI"
    URI = "URI"

    def __str__(self):
        return self.value

    def validator_class(
        self, opts: typing.Optional["IdValidatorOpts"] = None
    ) -> typing.Type[validators.BaseValueModel]:
        if self == TitleIdKind.ISBN:
            if not isinstance(opts, IdValidatorOptsISBN):
                opts = IdValidatorOptsISBN()
            return opts.validator_class()
        elif self == TitleIdKind.Print_ISSN:
            if not isinstance(opts, IdValidatorOptsISSN):
                opts = IdValidatorOptsISSN()
            return opts.validator_class()
        elif self == TitleIdKind.Online_ISSN:
            if not isinstance(opts, IdValidatorOptsEISSN):
                opts = IdValidatorOptsEISSN()
            return opts.validator_class()
        elif self == TitleIdKind.Proprietary:
            return validators.ProprietaryID
        elif self == TitleIdKind.DOI:
            return validators.DOI
        elif self == TitleIdKind.URI:
            return validators.URI
        else:
            raise NotImplementedError()


@dataclass(config=PydanticConfig)
class IdValidatorOptsISSN:
    type: typing.Literal[TitleIdKind.Print_ISSN] = TitleIdKind.Print_ISSN

    strict: bool = False

    def validator_class(self) -> typing.Type[validators.BaseValueModel]:
        return validators.StrictISSN if self.strict else validators.ISSN


@dataclass(config=PydanticConfig)
class IdValidatorOptsEISSN:
    type: typing.Literal[TitleIdKind.Online_ISSN] = TitleIdKind.Online_ISSN
    strict: bool = False

    def validator_class(self) -> typing.Type[validators.BaseValueModel]:
        return validators.StrictEISSN if self.strict else validators.EISSN


@dataclass(config=PydanticConfig)
class IdValidatorOptsISBN:
    type: typing.Literal[TitleIdKind.ISBN] = TitleIdKind.ISBN
    strict: bool = False
    isbn_kind: typing.Optional[KindISBN] = None

    def validator_class(self) -> typing.Type[validators.BaseValueModel]:
        if not self.strict:
            return validators.ISBN
        if not self.isbn_kind:
            return validators.StrictISBN
        elif self.isbn_kind == KindISBN.ISBN13:
            return validators.StrictISBN13
        elif self.isbn_kind == KindISBN.ISBN10:
            return validators.StrictISBN10

        raise NotImplementedError()


IdValidatorOpts = Annotated[
    typing.Union[
        IdValidatorOptsISBN,
        IdValidatorOptsISSN,
        IdValidatorOptsEISSN,
    ],
    Field(discriminator="type"),
]


class DateFormat(str, Enum):
    US = "us"  # US format 12/31/2022 (more common)
    EU = "eu"  # EU format 31/12/2022


class SpecialExtraction(str, Enum):
    NO = "no"
    COMMA_SEPARATED_NUMBER = "comma_separated_number"
    MINUTES_TO_SECONDS = "minutes_to_seconds"
    DURATION_TO_SECONDS = "duration_to_seconds"

    def get_validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        if self == SpecialExtraction.NO:
            return None
        elif self == SpecialExtraction.COMMA_SEPARATED_NUMBER:
            return validators.CommaSeparatedNumberValidator
        elif self == SpecialExtraction.MINUTES_TO_SECONDS:
            return validators.MinutesToSecondsValidator
        elif self == SpecialExtraction.DURATION_TO_SECONDS:
            return validators.DurationToSecondsValidator
        else:
            raise NotImplementedError()


class Role(str, Enum):
    VALUE = "value"
    DATE = "date"
    TITLE = "title"
    TITLE_ID = "title_id"
    DIMENSION = "dimension"
    METRIC = "metric"
    ORGANIZATION = "organization"
    VOID = "void"
    AUTHORS = "authors"
    PUBLICATION_DATE = "publication_date"


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
    max_idx: typing.Optional[int] = None


class ContentExtractorMixin:
    source: Source
    extract_params: ExtractParams
    cleanup_during_header_processing: bool
    fallback: typing.Optional["ContentExtractorMixin"]
    role: Role
    _last_sheet_idx = None
    _last_source = None
    _last_extracted = None

    def content(
        self,
        sheet: SheetReader,
        source: Source,
        parser_row_offset: typing.Optional[int],
        area_row_offset: typing.Optional[int],
    ) -> typing.Any:
        content = source.content(sheet, parser_row_offset, area_row_offset)
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
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ) -> typing.Any:
        try:
            value = self._extract(sheet, idx, validator, parser_row_offset, area_row_offset)
            if self.extract_params.last_value_as_default:
                self.extract_params.default = value

        except TableException as e:
            if e.action == TableException.Action.PASS and self.fallback:
                return self.fallback.extract(
                    sheet, idx, validator, parser_row_offset, area_row_offset
                )
            else:
                raise
        return value

    def get_validator(
        self, validator: typing.Optional[typing.Type[validators.BaseValueModel]]
    ) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validator or self.extract_params.special_extraction.get_validator() or self.validator

    def _extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ) -> typing.Any:
        source = self.source[idx]

        if self.extract_params.max_idx is not None:
            if idx > self.extract_params.max_idx:
                raise TableException(
                    row=getattr(source, "row", None),
                    col=getattr(source, "col", None),
                    sheet=sheet.sheet_idx,
                    reason="out-of-bounds",
                    action=TableException.Action.STOP,
                )

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
            content = self.content(sheet, source, parser_row_offset, area_row_offset)
            if validator := self.get_validator(validator):
                if self.extract_params.default is not None:
                    res = validators.gen_default_validator(
                        validator,
                        self.extract_params.default,
                        self.extract_params.blank_values,
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
                reason = e.title.lower()
                # Try to Extract reason from exception args if present
                if ctx := e.errors()[0].get("ctx"):
                    if error := ctx.get("error"):
                        if args := error.args:
                            reason = ",".join(args)
                raise TableException(
                    content,
                    row=getattr(source, "row", None),
                    col=getattr(source, "col", None),
                    sheet=sheet.sheet_idx,
                    reason=reason,
                    action=self.extract_params.on_validation_error,
                ) from e
        except IndexError as e:
            raise TableException(
                row=getattr(source, "row", None),
                col=getattr(source, "col", None),
                sheet=sheet.sheet_idx,
                reason="out-of-bounds",
                action=TableException.Action.STOP,
            ) from e

        # update last extracted
        self._last_extracted = res
        self._last_sheet_idx = sheet.sheet_idx
        return res

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return None


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder, ContentExtractorMixin):
    name: str
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["DimensionSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.DIMENSION] = Role.DIMENSION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Dimension


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["MetricSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.METRIC] = Role.METRIC

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Metric


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["OrganizationSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.ORGANIZATION] = Role.ORGANIZATION

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Organization


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["TitleSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.TITLE] = Role.TITLE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Title


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder, ContentExtractorMixin):
    name: TitleIdKind
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    fallback: typing.Optional["TitleIdSource"] = None
    validator_opts: typing.Optional[IdValidatorOpts] = None
    role: typing.Literal[Role.TITLE_ID] = Role.TITLE_ID

    def __post_init__(self):
        self._last_key = None

    @property
    def last_key(self) -> typing.Optional[str]:
        return self._last_key

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return self.name.validator_class(self.validator_opts)

    def extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ) -> typing.Any:
        self._last_key = None
        res = super().extract(sheet, idx, validator, parser_row_offset, area_row_offset)
        # Update last key from fallback
        if fallback := self.fallback:
            if last_key := fallback._last_key:
                self._last_key = last_key
                fallback._last_key = None  # clean fallback
                return res

        self._last_key = self.name
        return res


@dataclass(config=PydanticConfig)
class ItemSource(TitleSource):
    pass


@dataclass(config=PydanticConfig)
class ItemIdSource(TitleIdSource):
    pass


@dataclass(config=PydanticConfig)
class ComposedDate(JsonEncorder):
    year: "DateSource"
    month: "DateSource"


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["DateSource"] = None
    cleanup_during_header_processing: bool = True
    preferred_date_format: DateFormat = DateFormat.US
    composed: typing.Optional[ComposedDate] = None
    date_pattern: typing.Optional[str] = None
    force_aligned: bool = False
    role: typing.Literal[Role.DATE] = Role.DATE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Date

    def get_validator(
        self, validator: typing.Optional[typing.Type[validators.BaseValueModel]]
    ) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        if date_pattern := self.date_pattern:
            return validators.gen_date_format_validator(date_pattern)

        if self.preferred_date_format == DateFormat.EU:
            if self.force_aligned:
                return validators.DateEUAligned
            else:
                return validators.DateEU
        else:
            if self.force_aligned:
                return validators.DateAligned
            else:
                return validators.Date

    def extract(
        self,
        sheet: SheetReader,
        idx: int,
        validator: typing.Optional[typing.Type[validators.BaseValueModel]] = None,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ) -> typing.Any:
        if self.composed:
            year_date = self.composed.year.extract(
                sheet, idx, parser_row_offset=parser_row_offset, area_row_offset=area_row_offset
            )
            month_date = self.composed.month.extract(
                sheet, idx, parser_row_offset=parser_row_offset, area_row_offset=area_row_offset
            )
            return date(year_date.year, month_date.month, 1)
        else:
            return super().extract(sheet, idx, validator, parser_row_offset, area_row_offset)


rebuild_dataclass(ComposedDate, force=True)


@dataclass(config=PydanticConfig)
class ValueSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    fallback: typing.Optional["ValueSource"] = None
    allow_negative: bool = False
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.VALUE] = Role.VALUE

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        if self.allow_negative:
            return validators.ValueNegative
        else:
            return validators.Value


@dataclass(config=PydanticConfig)
class VoidSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["VoidSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.VOID] = Role.VOID


@dataclass(config=PydanticConfig)
class AuthorsSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["AuthorsSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.AUTHORS] = Role.AUTHORS

    def get_validator(
        self, validator: typing.Optional[typing.Type[validators.BaseValueModel]]
    ) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.AuthorsValidator


@dataclass(config=PydanticConfig)
class PublicationDateSource(JsonEncorder, ContentExtractorMixin):
    source: Source
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    fallback: typing.Optional["PublicationDateSource"] = None
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.PUBLICATION_DATE] = Role.PUBLICATION_DATE
