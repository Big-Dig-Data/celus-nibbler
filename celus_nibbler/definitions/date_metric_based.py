import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.parsers.base import BaseArea, MonthDataCells
from celus_nibbler.parsers.non_counter.date_metric_based import BaseDateMetricArea
from celus_nibbler.sources import DimensionSource, OrganizationSource, TitleIdSource, TitleSource
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseParserDefinition
from .common import AreaGeneratorMixin


@dataclass(config=PydanticConfig)
class DatesAndMetrics(JsonEncorder):
    range: CoordRange
    data_direction: Direction
    dates_offset: Coord = Coord(0, 0)
    dates_regex: typing.Optional[typing.Pattern] = None
    metrics_offset: Coord = Coord(0, 0)
    metrics_regex: typing.Optional[typing.Pattern] = None
    data_skip: int = 1


@dataclass(config=PydanticConfig)
class DateMetricBasedAreaDefinition(AreaGeneratorMixin, JsonEncorder):
    dates_and_metrics: DatesAndMetrics
    titles: typing.Optional[TitleSource]
    organizations: typing.Optional[OrganizationSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])

    name: typing.Literal["non_counter.date_metric_based"] = "non_counter.date_metric_based"

    def make_area(self) -> typing.Type[BaseArea]:
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        organizations = self.organizations
        data_direction = self.dates_and_metrics.data_direction
        header_range = self.dates_and_metrics.range
        dates_regex = self.dates_and_metrics.dates_regex
        dates_offset = self.dates_and_metrics.dates_offset
        metrics_regex = self.dates_and_metrics.metrics_regex
        metrics_offset = self.dates_and_metrics.metrics_offset
        data_skip = self.dates_and_metrics.data_skip

        class Area(BaseDateMetricArea):
            data_header_month_offset = dates_offset
            data_header_month_regex = dates_regex
            data_header_metric_offset = metrics_offset
            data_header_metric_regex = metrics_regex
            data_header_data_skip = data_skip

            @property
            def header_cells(self) -> CoordRange:
                return header_range

            organization_source = organizations
            title_source = titles

            def find_data_cells(self) -> typing.List[MonthDataCells]:
                return self.find_data_cells_in_direction(data_direction)

            @property
            def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
                return {e.name: e for e in title_ids}

            @property
            def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
                return {e.name: e for e in dimensions}

        return Area


@dataclass(config=PydanticConfig)
class DateMetricBasedDefinition(JsonEncorder, BaseParserDefinition):
    # name of nibbler parser
    parser_name: str
    format_name: str
    areas: typing.List[DateMetricBasedAreaDefinition]
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    name: typing.Literal["non_counter.date_metric_based"] = "non_counter.date_metric_based"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from celus_nibbler.parsers import BaseParser
        from celus_nibbler.parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, BaseParser):
            _definition = self

            format_name = _definition.format_name
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
                return cls._definition.parser_name

        return Parser
