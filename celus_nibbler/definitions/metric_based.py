import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.parsers.non_counter.metric_based import BaseMetricArea, MetricBasedParser
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    OrganizationSource,
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
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])
    organizations: typing.Optional[OrganizationSource] = None

    kind: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"

    def make_area(self):
        headers = self.data_headers
        dates = self.dates
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        organizations = self.organizations

        class Area(BaseMetricArea):
            data_headers = headers
            organization_source = organizations
            date_source = dates
            title_source = titles

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
class MetricBasedDefinition(BaseNonCounterParserDefinition):
    parser_name: str
    data_format: DataFormatDefinition
    areas: typing.List[MetricBasedAreaDefinition]

    platforms: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    kind: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"
    version: typing.Literal[1] = 1

    def make_parser(self):
        class Parser(MetricBasedParser):
            _definition = self

            data_format = _definition.data_format
            name = f"dynamic.{_definition.group}.{_definition.data_format.name}.{_definition.parser_name}"
            platforms = self.platforms

            metrics_to_skip = self.metrics_to_skip
            titles_to_skip = self.titles_to_skip
            dimensions_to_skip = self.dimensions_to_skip

            metric_aliases = self.metric_aliases
            dimension_aliases = self.dimension_aliases

            heuristics = self.heuristics

            areas = [e.make_area() for e in self.areas]

        return Parser
