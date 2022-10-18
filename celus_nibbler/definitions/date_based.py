import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.parsers.base import BaseArea
from celus_nibbler.parsers.non_counter.date_based import BaseDateArea
from celus_nibbler.sources import (
    DimensionSource,
    MetricSource,
    OrganizationSource,
    Source,
    TitleIdSource,
    TitleSource,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseParserDefinition


@dataclass(config=PydanticConfig)
class DateBasedAreaDefinition(JsonEncorder, BaseAreaDefinition):

    data_headers: DataHeaders
    metrics: MetricSource
    titles: typing.Optional[TitleSource] = None
    organizations: typing.Optional[OrganizationSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])

    kind: typing.Literal["non_counter.date_based"] = "non_counter.date_based"

    def make_area(self) -> typing.Type[BaseArea]:
        headers = self.data_headers
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        metrics = self.metrics
        organizations = self.organizations

        class Area(BaseDateArea):
            data_headers = headers
            organization_source = organizations
            title_source = titles
            metric_source = metrics

            @property
            def title_ids_sources(self) -> typing.Dict[str, Source]:
                return {e.name: e for e in title_ids}

            @property
            def dimensions_sources(self) -> typing.Dict[str, Source]:
                return {e.name: e for e in dimensions}

        return Area


@dataclass(config=PydanticConfig)
class DateBasedDefinition(JsonEncorder, BaseParserDefinition):
    # name of nibbler parser
    parser_name: str
    areas: typing.List[DateBasedAreaDefinition]
    data_format: DataFormatDefinition
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    kind: typing.Literal["non_counter.date_based"] = "non_counter.date_based"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from celus_nibbler.parsers import BaseParser
        from celus_nibbler.parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, BaseParser):
            _definition = self

            data_format = _definition.data_format
            platforms = _definition.platforms

            metrics_to_skip = _definition.metrics_to_skip
            titles_to_skip = _definition.titles_to_skip
            dimensions_to_skip = _definition.dimensions_to_skip

            metric_aliases = _definition.metric_aliases
            dimension_aliases = _definition.dimension_aliases

            heuristics = _definition.heuristics
            areas = [e.make_area() for e in _definition.areas]

            @classmethod
            def name(cls):
                return f"{cls._definition.kind}.{cls._definition.parser_name}"

        return Parser
