import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseArea, BaseTabularParser
from celus_nibbler.parsers.non_counter.generic import BaseGenericArea
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    OrganizationSource,
    TitleIdSource,
    TitleSource,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseNonCounterParserDefinition


@dataclass(config=PydanticConfig)
class GenericAreaDefinition(JsonEncorder, BaseAreaDefinition):

    data_headers: DataHeaders
    metrics: typing.Optional[MetricSource] = None
    dates: typing.Optional[DateSource] = None
    titles: typing.Optional[TitleSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])
    organizations: typing.Optional[OrganizationSource] = None
    aggregate_same_records: bool = False

    kind: typing.Literal["non_counter.generic"] = "non_counter.generic"
    version: typing.Literal[1] = 1

    def make_area(self) -> typing.Type[BaseArea]:
        headers = self.data_headers
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        metrics = self.metrics
        organizations = self.organizations
        dates = self.dates

        class Area(BaseGenericArea):
            data_headers = headers
            date_source = dates
            organization_source = organizations
            title_source = titles
            metric_source = metrics
            aggregator = self.make_aggregator()

            @property
            def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
                return {e.name: e for e in title_ids}

            @property
            def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
                return {e.name: e for e in dimensions}

            @property
            def dimensions(self) -> typing.List[str]:
                return list([e.name for e in dimensions])

        return Area


@dataclass(config=PydanticConfig)
class GenericDefinition(JsonEncorder, BaseNonCounterParserDefinition):

    parser_name: str
    data_format: DataFormatDefinition

    areas: typing.List[GenericAreaDefinition] = field(default_factory=lambda: [])
    platforms: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    available_metrics: typing.Optional[typing.List[str]] = None
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None
    possible_row_offsets: typing.List[int] = field(default_factory=lambda: [0])

    kind: typing.Literal["non_counter.generic"] = "non_counter.generic"
    version: typing.Literal[1] = 1

    def make_parser(self):
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

            metric_aliases = dict(_definition.metric_aliases)
            dimension_aliases = dict(_definition.dimension_aliases)

            heuristics = _definition.heuristics
            possible_row_offsets = _definition.possible_row_offsets

            areas = [e.make_area() for e in _definition.areas]

        return Parser