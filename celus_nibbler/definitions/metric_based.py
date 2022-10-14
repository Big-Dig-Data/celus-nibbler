import abc
import typing
from dataclasses import field

from pydantic import ValidationError
from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.coordinates import CoordRange
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import MetricDataCells
from celus_nibbler.parsers.non_counter.metric_based import BaseMetricArea, MetricBasedParser
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    OrganizationSource,
    TitleIdSource,
    TitleSource,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseParserDefinition
from .common import AreaGeneratorMixin


@dataclass(config=PydanticConfig)
class MetricBasedAreaDefinition(AreaGeneratorMixin, JsonEncorder):
    dates: DateSource
    metrics: MetricSource
    titles: typing.Optional[TitleSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])
    organizations: typing.Optional[OrganizationSource] = None

    name: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"

    def make_area(self):
        dates = self.dates
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        metrics_range = self.metrics.source
        organizations = self.organizations
        data_direction = self.metrics.direction

        class Area(BaseMetricArea):
            header_cells = metrics_range

            organization_source = organizations

            def find_data_cells(self) -> typing.List[MetricDataCells]:
                res = []
                for cell in self.header_cells:
                    try:
                        metric = self.parse_metric(cell)
                        range = CoordRange(cell, data_direction).skip(1)
                        res.append(
                            MetricDataCells(
                                metric,
                                range,
                            )
                        )

                    except TableException as e:
                        if e.reason == "out-of-bounds":
                            # We reached the end of row
                            break
                        raise
                    except ValidationError:
                        # Found a content which can't be parse e.g. "Total"
                        # We can exit here
                        break

                return res

            date_source = dates
            title_source = titles

            @property
            def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
                return {e.name: e for e in title_ids}

            @property
            def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
                return {e.name: e for e in dimensions}

        return Area


@dataclass(config=PydanticConfig)
class MetricBasedDefinition(BaseParserDefinition, metaclass=abc.ABCMeta):
    parser_name: str
    format_name: str
    areas: typing.List[MetricBasedAreaDefinition]
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    name: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, MetricBasedParser):
            name = self.parser_name
            format_name = self.format_name
            platforms = self.platforms

            metrics_to_skip = self.metrics_to_skip
            titles_to_skip = self.titles_to_skip
            dimensions_to_skip = self.dimensions_to_skip

            metric_aliases = self.metric_aliases
            dimension_aliases = self.dimension_aliases

            heuristics = self.heuristics

            areas = [e.make_area() for e in self.areas]

        return Parser