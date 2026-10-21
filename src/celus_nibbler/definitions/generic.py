import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseArea, BaseTabularParser
from celus_nibbler.parsers.non_counter.generic import BaseGenericArea
from celus_nibbler.sources import (
    AuthorsSource,
    DateSource,
    DimensionSource,
    ItemIdSource,
    ItemSource,
    MetricSource,
    OrganizationSource,
    PublicationDateSource,
    SpecialExtraction,
    TitleIdSource,
    TitleSource,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig
from celus_nibbler.validators import validators

from .base import BaseAreaDefinition, BaseNonCounterParserDefinition, ValidatorChoices


@dataclass(config=PydanticConfig)
class GenericAreaDefinition(JsonEncorder, BaseAreaDefinition):
    """One table of a sheet.

    The table is read record by record (row by row in a standard table) and every source
    provides its n-th value to the n-th record. Reading ends when a source runs out of
    cells or a value fails validation with the `stop` action.

    A record field which differs per data column is read by `data_headers.roles`, a field
    which differs per row or is constant by the corresponding field of the area.
    """

    data_headers: DataHeaders
    """Header of the table and the location of the data cells."""
    metrics: typing.Optional[MetricSource] = None
    """Metric of the records, when it isn't in `data_headers.roles`."""
    dates: typing.Optional[DateSource] = None
    """Date of the records, when it (or a part of it) isn't in `data_headers.roles`."""
    titles: typing.Optional[TitleSource] = None
    """Title of the records; omit for reports without titles."""
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    """Title identifiers, at most one source per kind."""
    items: typing.Optional[ItemSource] = None
    """Item of the records; omit for reports without items."""
    item_ids: typing.List[ItemIdSource] = field(default_factory=lambda: [])
    """Item identifiers, at most one source per kind."""
    item_publication_date: typing.Optional[PublicationDateSource] = None
    """Publication date of the items."""
    item_authors: typing.Optional[AuthorsSource] = None
    """Authors of the items."""
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])
    """Dimensions of the records, one source per dimension."""
    organizations: typing.Optional[OrganizationSource] = None
    """Organization of the records; omit when the file covers a single organization."""
    aggregate_same_records: bool = False
    """Sum records which differ only in value, e.g. daily rows of the same month."""
    max_areas_generated: typing.Optional[int] = 1
    """How many times the table may repeat in a sheet; `null` means unlimited. Each
    repetition is located by `data_headers.condition`, searching from the row after the
    start of the previous one."""
    min_valid_areas: typing.Optional[int] = 1
    """Minimal number of repetitions; finding fewer is an error."""

    kind: typing.Literal["non_counter.generic"] = "non_counter.generic"

    def make_area(self) -> typing.Type[BaseArea]:
        headers = self.data_headers
        titles = self.titles
        title_ids = self.title_ids
        items = self.items
        item_ids = self.item_ids
        item_publication_date = self.item_publication_date
        item_authors = self.item_authors
        dimensions = self.dimensions
        metrics = self.metrics
        organizations = self.organizations
        dates = self.dates
        self_max_areas_generated = self.max_areas_generated
        self_min_valid_areas = self.min_valid_areas

        class Area(BaseGenericArea):
            data_headers = headers
            date_source = dates
            organization_source = organizations
            title_source = titles
            item_source = items
            item_publication_date_source = item_publication_date
            item_authors_source = item_authors
            metric_source = metrics
            aggregator = self.make_aggregator()
            max_areas_generated = self_max_areas_generated
            min_valid_areas = self_min_valid_areas

            @property
            def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
                return {e.name: e for e in title_ids}

            @property
            def item_ids_sources(self) -> typing.Dict[str, ItemIdSource]:
                return {e.name: e for e in item_ids}

            @property
            def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
                return {e.name: e for e in dimensions}

            @property
            def dimensions(self) -> typing.List[str]:
                return list([e.name for e in dimensions])

        return Area


@dataclass(config=PydanticConfig)
class GenericDefinition(JsonEncorder, BaseNonCounterParserDefinition):
    """Parser of non-COUNTER tabular reports. `heuristics` decide whether the parser
    applies to a sheet, `areas` describe the tables to read."""

    parser_name: str
    """Name of the parser, usually derived from the platform."""
    data_format: DataFormatDefinition

    areas: typing.List[GenericAreaDefinition] = field(default_factory=lambda: [])
    """Tables to read from each sheet."""
    platforms: typing.List[str] = field(default_factory=lambda: [])
    """Platforms the parser may be used for."""
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    """Records with these metrics are skipped (case-insensitive)."""
    available_metrics: typing.Optional[typing.List[str]] = None
    """Allowed metrics, others trigger `on_metric_check_failed`. `null` allows all."""
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    """What to do with a record whose metric isn't in `available_metrics`."""
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    """Records with these titles are skipped (case-insensitive)."""
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    """Records with these dimension values are skipped (case-insensitive), as a mapping
    of dimension name to values."""
    dimensions_validators: typing.Dict[str, ValidatorChoices] = field(default_factory=lambda: {})
    """Validators of dimension values, as a mapping of dimension name to validator."""
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    """`[original, target]` pairs; metric `original` is renamed to `target`, e.g.
    `[["Download", "Downloads"]]`."""
    metric_value_extraction_overrides: typing.Dict[str, SpecialExtraction] = field(
        default_factory=lambda: {}
    )
    """Conversion of the values of given metrics, e.g.
    `{"Session Duration": "duration_to_seconds"}`."""
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    """`[original, target]` pairs; dimension `original` is renamed to `target`."""
    heuristics: typing.Optional[Condition] = None
    """Decides whether the parser applies to a sheet; `null` matches any sheet. Usually
    an `and` of a few `regex` conditions on stable cells, such as the report title or
    fixed header labels. Unlike `DataHeaders.condition`, it doesn't locate anything in
    the sheet."""
    possible_row_offsets: typing.List[int] = field(default_factory=lambda: [0])
    """Offsets at which the heuristics are tried; the first matching one becomes the
    parser offset."""

    uses_titles: bool | None = None
    """When set, require all records to have (`true`) or not to have (`false`) a title."""
    uses_items: bool | None = None
    """When set, require all records to have (`true`) or not to have (`false`) an item."""

    kind: typing.Literal["non_counter.generic"] = "non_counter.generic"
    version: typing.Literal[1] = 1

    def make_parser(self):
        validators_map = {e.name: e for e in validators}
        _dimensions_validators = {
            dimension_name: validators_map[validator_name.value]
            for dimension_name, validator_name in self.dimensions_validators.items()
        }

        class Parser(BaseTabularParser):
            _definition = self

            data_format = _definition.data_format
            platforms = _definition.platforms
            name = (
                f"dynamic.{_definition.group}.{_definition.data_format.name}"
                f".{_definition.parser_name}"
            )

            metrics_to_skip = _definition.metrics_to_skip
            available_metrics = _definition.available_metrics
            on_metric_check_failed = _definition.on_metric_check_failed
            titles_to_skip = _definition.titles_to_skip
            dimensions_to_skip = _definition.dimensions_to_skip
            dimensions_validators = _dimensions_validators

            metric_aliases = dict(_definition.metric_aliases)
            metric_value_extraction_overrides = dict(_definition.metric_value_extraction_overrides)
            dimension_aliases = dict(_definition.dimension_aliases)

            heuristics = _definition.heuristics
            possible_row_offsets = _definition.possible_row_offsets

            uses_titles = self.uses_titles
            uses_items = self.uses_items

            areas = [e.make_area() for e in _definition.areas]

        return Parser
