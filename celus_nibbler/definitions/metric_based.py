import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.non_counter.metric_based import (
    BaseMetricArea,
    MetricBasedParser,
)
from celus_nibbler.sources import (
    AuthorsSource,
    DateSource,
    DimensionSource,
    ItemIdSource,
    ItemSource,
    OrganizationSource,
    PublicationDateSource,
    SpecialExtraction,
    TitleIdSource,
    TitleSource,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseNonCounterParserDefinition


@dataclass(config=PydanticConfig)
class MetricBasedAreaDefinition(JsonEncorder, BaseAreaDefinition):
    data_headers: DataHeaders
    dates: DateSource
    titles: typing.Optional[TitleSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    items: typing.Optional[ItemSource] = None
    item_ids: typing.List[ItemIdSource] = field(default_factory=lambda: [])
    item_publication_date: typing.Optional[PublicationDateSource] = None
    item_authors: typing.Optional[AuthorsSource] = None
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])
    organizations: typing.Optional[OrganizationSource] = None
    aggregate_same_records: bool = True

    kind: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"

    def make_area(self):
        headers = self.data_headers
        dates = self.dates
        titles = self.titles
        title_ids = self.title_ids
        items = self.items
        item_ids = self.item_ids
        item_publication_date = self.item_publication_date
        item_authors = self.item_authors
        dimensions = self.dimensions
        organizations = self.organizations

        class Area(BaseMetricArea):
            data_headers = headers
            organization_source = organizations
            date_source = dates
            title_source = titles
            item_source = items
            item_publication_date_source = item_publication_date
            item_authors_source = item_authors

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

            aggregator = self.make_aggregator()

        return Area


@dataclass(config=PydanticConfig)
class MetricBasedDefinition(BaseNonCounterParserDefinition):
    parser_name: str
    data_format: DataFormatDefinition
    areas: typing.List[MetricBasedAreaDefinition]

    platforms: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    available_metrics: typing.Optional[typing.List[str]] = None
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    metric_value_extraction_overrides: typing.Dict[str, SpecialExtraction] = field(
        default_factory=lambda: {}
    )
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None
    possible_row_offsets: typing.List[int] = field(default_factory=lambda: [0])

    kind: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"
    version: typing.Literal[1] = 1

    def make_parser(self):
        class Parser(MetricBasedParser):
            _definition = self

            data_format = _definition.data_format
            name = (
                f"dynamic.{_definition.group}.{_definition.data_format.name}"
                f".{_definition.parser_name}"
            )
            platforms = self.platforms

            metrics_to_skip = self.metrics_to_skip
            available_metrics = _definition.available_metrics
            on_metric_check_failed = _definition.on_metric_check_failed
            titles_to_skip = self.titles_to_skip
            dimensions_to_skip = self.dimensions_to_skip

            metric_aliases = dict(self.metric_aliases)
            metric_value_extraction_overrides = dict(self.metric_value_extraction_overrides)
            dimension_aliases = dict(self.dimension_aliases)

            heuristics = self.heuristics
            possible_row_offsets = _definition.possible_row_offsets

            areas = [e.make_area() for e in self.areas]

        return Parser
