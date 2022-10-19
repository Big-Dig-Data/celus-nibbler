import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.parsers.base import BaseArea, BaseParser
from celus_nibbler.parsers.non_counter.date_metric_based import BaseDateMetricArea
from celus_nibbler.sources import DimensionSource, OrganizationSource, TitleIdSource, TitleSource
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseNonCounterParserDefinition


@dataclass(config=PydanticConfig)
class DateMetricBasedAreaDefinition(JsonEncorder, BaseAreaDefinition):
    data_headers: DataHeaders
    titles: typing.Optional[TitleSource]
    organizations: typing.Optional[OrganizationSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])

    kind: typing.Literal["non_counter.date_metric_based"] = "non_counter.date_metric_based"

    def make_area(self) -> typing.Type[BaseArea]:
        headers = self.data_headers
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        organizations = self.organizations

        class Area(BaseDateMetricArea):
            data_headers = headers
            organization_source = organizations
            title_source = titles

            @property
            def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
                return {e.name: e for e in title_ids}

            @property
            def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
                return {e.name: e for e in dimensions}

        return Area


@dataclass(config=PydanticConfig)
class DateMetricBasedDefinition(JsonEncorder, BaseNonCounterParserDefinition):

    parser_name: str
    data_format: DataFormatDefinition

    areas: typing.List[DateMetricBasedAreaDefinition]
    data_format: DataFormatDefinition
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    kind: typing.Literal["non_counter.date_metric_based"] = "non_counter.date_metric_based"
    version: typing.Literal[1] = 1

    def make_parser(self):
        class Parser(BaseParser):
            _definition = self

            data_format = _definition.data_format
            platforms = _definition.platforms
            name = f"dynamic.{_definition.group}.{_definition.data_format.name}.{_definition.parser_name}"

            metrics_to_skip = _definition.metrics_to_skip
            titles_to_skip = _definition.titles_to_skip
            dimensions_to_skip = _definition.dimensions_to_skip

            metric_aliases = _definition.metric_aliases
            dimension_aliases = _definition.dimension_aliases

            heuristics = _definition.heuristics
            areas = [e.make_area() for e in _definition.areas]

        return Parser
