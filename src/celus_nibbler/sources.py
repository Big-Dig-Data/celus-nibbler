import typing
from abc import ABCMeta, abstractmethod
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
    """ISBN variant required by strict ISBN validation."""

    ISBN13 = "isbn13"
    ISBN10 = "isbn10"


class TitleIdKind(str, Enum):
    """Kind of title or item identifier. Identifiers which aren't ISBN, ISSN, DOI or URI
    are `Proprietary`."""

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
    """Validation options for Print ISSN."""

    type: typing.Literal[TitleIdKind.Print_ISSN] = TitleIdKind.Print_ISSN

    strict: bool = False
    """Reject values which aren't valid ISSNs instead of only normalizing them."""

    def validator_class(self) -> typing.Type[validators.BaseValueModel]:
        return validators.StrictISSN if self.strict else validators.ISSN


@dataclass(config=PydanticConfig)
class IdValidatorOptsEISSN:
    """Validation options for Online ISSN."""

    type: typing.Literal[TitleIdKind.Online_ISSN] = TitleIdKind.Online_ISSN
    strict: bool = False
    """Reject values which aren't valid ISSNs instead of only normalizing them."""

    def validator_class(self) -> typing.Type[validators.BaseValueModel]:
        return validators.StrictEISSN if self.strict else validators.EISSN


@dataclass(config=PydanticConfig)
class IdValidatorOptsISBN:
    """Validation options for ISBN."""

    type: typing.Literal[TitleIdKind.ISBN] = TitleIdKind.ISBN
    strict: bool = False
    """Reject values which aren't valid ISBNs instead of only normalizing them."""
    isbn_kind: typing.Optional[KindISBN] = None
    """With `strict`, accept only this ISBN variant."""

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
    """Order of day and month used for ambiguous dates such as 01/02/2022:
    `us` is month first, `eu` is day first."""

    US = "us"  # US format 12/31/2022 (more common)
    EU = "eu"  # EU format 31/12/2022


class SpecialExtraction(str, Enum):
    """Conversion applied to numeric values.

    - `no`: plain number.
    - `comma_separated_number`: drops thousands separators, "1,234" -> 1234.
    - `minutes_to_seconds`, `hours_to_seconds`: converts the number to seconds.
    - `duration_to_seconds`: converts "HH:MM:SS" to seconds.
    """

    NO = "no"
    COMMA_SEPARATED_NUMBER = "comma_separated_number"
    MINUTES_TO_SECONDS = "minutes_to_seconds"
    HOURS_TO_SECONDS = "hours_to_seconds"
    DURATION_TO_SECONDS = "duration_to_seconds"

    def get_validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        if self == SpecialExtraction.NO:
            return None
        elif self == SpecialExtraction.COMMA_SEPARATED_NUMBER:
            return validators.CommaSeparatedNumberValidator
        elif self == SpecialExtraction.MINUTES_TO_SECONDS:
            return validators.MinutesToSecondsValidator
        elif self == SpecialExtraction.HOURS_TO_SECONDS:
            return validators.HoursToSecondsValidator
        elif self == SpecialExtraction.DURATION_TO_SECONDS:
            return validators.DurationToSecondsValidator
        else:
            raise NotImplementedError()


class Role(str, Enum):
    """Record field which a source fills."""

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


class BaseMatch(metaclass=ABCMeta):
    @abstractmethod
    def check(self, value: typing.Any) -> bool:
        pass


class MatchKind(str, Enum):
    NEG = "neg"
    AND = "and"
    OR = "or"
    EXACT = "exact"
    IEXACT = "iexact"
    CONTAINS = "contains"
    ICONTAINS = "icontains"
    CONTAINED = "contained"
    ICONTAINED = "icontained"
    REGEX = "regex"


class MatchArithmeticsMixin:
    def __invert__(self):
        return MatchNeg(self)

    def __or__(self, other):
        return MatchOr([self, other])

    def __and__(self, other):
        return MatchAnd([self, other])


@dataclass(config=PydanticConfig)
class MatchAnd(MatchArithmeticsMixin, JsonEncorder):
    """Matches when all of `matches` match."""

    matches: typing.List["Match"]

    kind: typing.Literal[MatchKind.AND] = MatchKind.AND

    @abstractmethod
    def check(self, value: typing.Any) -> bool:
        return all(e.check(value) for e in self.matches)


@dataclass(config=PydanticConfig)
class MatchOr(MatchArithmeticsMixin, JsonEncorder):
    """Matches when any of `matches` matches."""

    matches: typing.List["Match"]

    kind: typing.Literal[MatchKind.OR] = MatchKind.OR

    @abstractmethod
    def check(self, value: typing.Any) -> bool:
        return any(e.check(value) for e in self.matches)


@dataclass(config=PydanticConfig)
class MatchNeg(MatchArithmeticsMixin, JsonEncorder):
    """Matches when `match` doesn't."""

    match: "Match"

    kind: typing.Literal[MatchKind.NEG] = MatchKind.NEG

    @abstractmethod
    def check(self, value: typing.Any) -> bool:
        return not self.match.check(value)


@dataclass(config=PydanticConfig)
class MatchExact(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings equal to `value`."""

    value: str

    kind: typing.Literal[MatchKind.EXACT] = MatchKind.EXACT

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return value == self.value


@dataclass(config=PydanticConfig)
class MatchIexact(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings equal to `value`, ignoring case."""

    value: str

    kind: typing.Literal[MatchKind.IEXACT] = MatchKind.IEXACT

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return value.lower() == self.value.lower()


@dataclass(config=PydanticConfig)
class MatchContains(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings containing `value`."""

    value: str

    kind: typing.Literal[MatchKind.CONTAINS] = MatchKind.CONTAINS

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return self.value in value


@dataclass(config=PydanticConfig)
class MatchIcontains(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings containing `value`, ignoring case."""

    value: str

    kind: typing.Literal[MatchKind.ICONTAINS] = MatchKind.ICONTAINS

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return self.value.lower() in value.lower()


@dataclass(config=PydanticConfig)
class MatchContained(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings which are a substring of `value`, e.g. `value` "views,downloads"
    matches "views"."""

    value: str

    kind: typing.Literal[MatchKind.CONTAINED] = MatchKind.CONTAINED

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return value in self.value


@dataclass(config=PydanticConfig)
class MatchIcontained(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings which are a substring of `value`, ignoring case."""

    value: str

    kind: typing.Literal[MatchKind.ICONTAINED] = MatchKind.ICONTAINED

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return value.lower() in self.value.lower()


@dataclass(config=PydanticConfig)
class MatchRegex(MatchArithmeticsMixin, JsonEncorder):
    """Matches strings matching `regex`. Uses `re.match`, so the pattern is anchored at
    the start of the string; prefix it with `.*` to match anywhere."""

    regex: typing.Pattern

    kind: typing.Literal[MatchKind.REGEX] = MatchKind.REGEX

    def check(self, value: typing.Any) -> bool:
        if not isinstance(value, str):
            return False
        return bool(self.regex.match(value))


Match = Annotated[
    typing.Union[
        MatchOr,
        MatchAnd,
        MatchNeg,
        MatchExact,
        MatchIexact,
        MatchContains,
        MatchIcontains,
        MatchContained,
        MatchIcontained,
        MatchRegex,
    ],
    Field(discriminator="kind"),
]


@dataclass(config=PydanticConfig)
class ValueOverride(JsonEncorder):
    """Replaces an extracted value with `target` when it satisfies `match`, e.g.
    `{"match": {"kind": "iexact", "value": "download"}, "target": "Downloads"}`."""

    match: Match
    """Condition on the extracted value. Non-string values never match."""
    target: str
    """Replacement value."""

    def override(self, value: typing.Any) -> str:
        if self.match.check(value):
            return self.target
        return value


@dataclass(config=PydanticConfig)
class ExtractParams(JsonEncorder):
    """How values are read from a source and what happens when they are invalid.

    A value is processed in this order: `regex`, `prefix` and `suffix`, `value_overrides`,
    blank handling (`default`), validation, `skip_condition`.
    """

    regex: typing.Optional[typing.Pattern] = None
    """Pattern applied with `re.search`. The value becomes the first capture group, so the
    pattern needs one, e.g. `"Year: ([0-9]{4})"`. A value which doesn't match is empty."""
    default: typing.Optional[typing.Any] = None
    """Value used for blank cells (see `blank_values`), e.g. `0` for empty number cells.
    Without it, blank cells are validated as they are, which usually fails."""
    last_value_as_default: bool = False
    """Blank cells take the previous extracted value. Useful for merged cells, e.g. a
    month header spanning several columns."""
    blank_values: typing.Tuple[typing.Any, ...] = field(default_factory=lambda: (None, ""))
    """Values regarded as blank by `default` and `last_value_as_default`. Add e.g. `"-"`
    to treat dashes as blank."""
    skip_validation: bool = False
    """Use the value without validation, only stripped of whitespace. Ignored when
    `default` is set."""
    prefix: str = ""
    """Prepended to the value (after `regex`)."""
    suffix: str = ""
    """Appended to the value (after `regex`)."""
    special_extraction: SpecialExtraction = SpecialExtraction.NO
    """Conversion of numeric values."""
    on_validation_error: TableException.Action = TableException.Action.FAIL
    """What to do when a value fails validation. Blank cells replaced by `default`
    don't get here."""
    max_idx: typing.Optional[int] = None
    """Highest index read from the source; the area stops after it."""
    skip_condition: typing.Optional[Match] = None
    """Records whose value matches are skipped, e.g. "Total" rows."""
    value_overrides: typing.List[ValueOverride] = field(default_factory=list)
    """Replacements of extracted values, applied in order. Use them to fix values at
    one source; `metric_aliases` and `dimension_aliases` rename values in the whole
    parser."""


class ContentExtractorMixin:
    source: Source
    extract_params: ExtractParams
    cleanup_during_header_processing: bool
    fallback: typing.Optional["ContentExtractorMixin"]
    role: Role
    _last_sheet_idx = None
    _last_source = None
    _last_extracted = None
    _last_area_row_offset = None

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

        if skip_condition := self.extract_params.skip_condition:
            if skip_condition.check(value):
                raise TableException(
                    value=value, reason="skip-extracted", action=TableException.Action.SKIP
                )

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
        try:
            source = self.source[idx]
        except IndexError:
            raise TableException(
                sheet=sheet.sheet_idx,
                reason="max-count-of-source-reached",
                action=TableException.Action.STOP,
                value=type(self.source),
            )

        if self.extract_params.max_idx is not None:
            if idx > self.extract_params.max_idx:
                raise TableException(
                    row=getattr(source, "row_absolute", lambda x, y: None)(
                        parser_row_offset,
                        area_row_offset,
                    ),
                    col=getattr(source, "col", None),
                    sheet=sheet.sheet_idx,
                    reason="out-of-bounds",
                    action=TableException.Action.STOP,
                )

        if (
            source == self._last_source
            and self._last_sheet_idx == sheet.sheet_idx
            and self._last_area_row_offset == area_row_offset  # Should be in same area
            and self._last_extracted
        ):
            # Same value will be extracted from the same coord
            return self._last_extracted
        else:
            self._last_source = source

        try:
            content = self.content(sheet, source, parser_row_offset, area_row_offset)
            for override in self.extract_params.value_overrides:
                content = override.override(content)
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
                    row=getattr(source, "row_absolute", lambda x, y: None)(
                        parser_row_offset,
                        area_row_offset,
                    ),
                    col=getattr(source, "col", None),
                    sheet=sheet.sheet_idx,
                    reason=reason,
                    action=self.extract_params.on_validation_error,
                ) from e
        except IndexError as e:
            raise TableException(
                row=getattr(source, "row_absolute", lambda x, y: None)(
                    parser_row_offset,
                    area_row_offset,
                ),
                col=getattr(source, "col", None),
                sheet=sheet.sheet_idx,
                reason="out-of-bounds",
                action=TableException.Action.STOP,
            ) from e

        # update last extracted
        self._last_extracted = res
        self._last_sheet_idx = sheet.sheet_idx
        self._last_area_row_offset = area_row_offset
        return res

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return None


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder, ContentExtractorMixin):
    """Value of the dimension `name` (e.g. "Platform" or "Access Type")."""

    name: str
    """Name of the dimension."""
    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["DimensionSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.DIMENSION] = Role.DIMENSION
    """Discriminator, always `"dimension"`."""

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Dimension


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder, ContentExtractorMixin):
    """Metric name, e.g. "Searches" or "Downloads".

    Metrics which differ per data column belong to `DataHeaders.roles`, metrics in a
    body column to `GenericAreaDefinition.metrics`. A report with a single metric uses a
    constant, `{"value": "<metric>"}`.
    """

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["MetricSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.METRIC] = Role.METRIC
    """Discriminator, always `"metric"`."""

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Metric


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder, ContentExtractorMixin):
    """Organization (institution) the usage belongs to. Only needed when one file
    contains data of several organizations."""

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["OrganizationSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.ORGANIZATION] = Role.ORGANIZATION
    """Discriminator, always `"organization"`."""

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Organization


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder, ContentExtractorMixin):
    """Title name. A title is meant broadly, e.g. a journal, a book or a database."""

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["TitleSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.TITLE] = Role.TITLE
    """Discriminator, always `"title"`."""

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.Title


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder, ContentExtractorMixin):
    """Identifier of the title, e.g. ISSN or DOI."""

    name: TitleIdKind
    """Kind of the identifier."""
    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    cleanup_during_header_processing: bool = True
    """Use a fresh copy of the source for each header position when used in
    `DataHeaders.roles`. Keep the default."""
    fallback: typing.Optional["TitleIdSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    validator_opts: typing.Optional[IdValidatorOpts] = None
    """Stricter validation of ISBN and ISSN identifiers."""
    role: typing.Literal[Role.TITLE_ID] = Role.TITLE_ID
    """Discriminator, always `"title_id"`."""

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
    """Item name, e.g. an article or a book chapter."""


@dataclass(config=PydanticConfig)
class ItemIdSource(TitleIdSource):
    """Identifier of the item, e.g. DOI."""


@dataclass(config=PydanticConfig)
class ComposedDate(JsonEncorder):
    """Date assembled from two sources, e.g. year and month in separate header rows.
    Each part usually needs its own `date_pattern`, such as `"%Y"` and `"%b"`."""

    year: "DateSource"
    """Source of the year."""
    month: "DateSource"
    """Source of the month."""


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder, ContentExtractorMixin):
    """Month the usage belongs to. Dates are reduced to whole months.

    When the year and the month are in different places, either use `composed` (both
    parts on the same axis) or combine a date in `DataHeaders.roles` with one in
    `GenericAreaDefinition.dates` via `DataHeaders.data_cells_options`.
    """

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["DateSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    preferred_date_format: DateFormat = DateFormat.US
    """Order of day and month in ambiguous dates when `date_pattern` isn't set."""
    composed: typing.Optional[ComposedDate] = None
    """Read the year and the month from separate sources; `source` isn't used then."""
    date_pattern: typing.Optional[str] = None
    """`strptime` pattern of string dates, e.g. `"%b-%y"` for "Jan-24". Common formats
    are detected when omitted."""
    force_aligned: bool = False
    """Reject dates which aren't the first day of a month instead of rounding them."""
    role: typing.Literal[Role.DATE] = Role.DATE
    """Discriminator, always `"date"`."""

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
    """Usage count of the record.

    Values are normally read from `DataHeaders.data_cells`. A ValueSource placed in
    `DataHeaders.roles` replaces the data cells: its `source` provides the value of each
    record, e.g. `{"coord": {"row": 1, "col": 4}, "direction": "down"}` reads column E
    and the constant `{"value": 1}` counts each row as 1. Use it when nothing varies
    per data column, i.e. dates and metrics are in body columns.
    """

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    fallback: typing.Optional["ValueSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    allow_negative: bool = False
    """Accept negative values."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.VALUE] = Role.VALUE
    """Discriminator, always `"value"`."""

    @property
    def validator(self) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        if self.allow_negative:
            return validators.ValueNegative
        else:
            return validators.Value


@dataclass(config=PydanticConfig)
class VoidSource(JsonEncorder, ContentExtractorMixin):
    """Header role which is read (e.g. for `DataHeaders.rules`) but not stored."""

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["VoidSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.VOID] = Role.VOID
    """Discriminator, always `"void"`."""


@dataclass(config=PydanticConfig)
class AuthorsSource(JsonEncorder, ContentExtractorMixin):
    """Authors of the item."""

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["AuthorsSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.AUTHORS] = Role.AUTHORS
    """Discriminator, always `"authors"`."""

    def get_validator(
        self, validator: typing.Optional[typing.Type[validators.BaseValueModel]]
    ) -> typing.Optional[typing.Type[validators.BaseValueModel]]:
        return validators.AuthorsValidator


@dataclass(config=PydanticConfig)
class PublicationDateSource(JsonEncorder, ContentExtractorMixin):
    """Publication date of the item."""

    source: Source
    """Location of the values. A CoordRange yields its n-th cell for the n-th record;
    Coord, Value and SheetAttr yield the same value for every record."""
    extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    """How to read and validate the values."""
    fallback: typing.Optional["PublicationDateSource"] = None
    """Source tried instead when extraction fails with the `pass` action."""
    cleanup_during_header_processing: bool = True
    role: typing.Literal[Role.PUBLICATION_DATE] = Role.PUBLICATION_DATE
    """Discriminator, always `"publication_date"`."""
