import abc
import typing
from dataclasses import field

from pydantic import ValidationError
from pydantic.dataclasses import dataclass

from ..conditions import Condition
from ..coordinates import Coord, CoordRange, Direction
from ..errors import TableException
from ..parsers.base import MetricDataCells
from ..parsers.non_counter.metric_based import BaseMetricArea, MetricBasedParser
from ..utils import JsonEncorder, PydanticConfig
from .base import BaseParserDefinition
from .common import (
    AreaGeneratorMixin,
    DateSource,
    DimensionSource,
    MetricSource,
    OrganizationSource,
    Source,
    TitleIdSource,
    TitleSource,
)


@dataclass(config=PydanticConfig)
class MetricBasedAreaDefinition(AreaGeneratorMixin, JsonEncorder):
    dates: DateSource
    metrics: MetricSource
    titles: typing.Optional[TitleSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])
    organization: typing.Optional[OrganizationSource] = None

    name: typing.Literal["non_counter.metric_based"] = "non_counter.metric_based"

    def make_area(self):
        dates = self.dates
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        metrics_range = self.metrics.source
        organization = self.organization
        data_direction = self.metrics.direction

        class Area(BaseMetricArea):
            header_cells = metrics_range

            @property
            def organization_cells(self) -> typing.Optional[Source]:
                if organization:
                    return organization.source
                else:
                    return None

            def find_data_cells(self) -> typing.List[MetricDataCells]:
                res = []
                for cell in self.header_cells:
                    try:
                        metric = self.parse_metric(cell)
                        if data_direction == Direction.DOWN:
                            range = CoordRange(Coord(cell.row + 1, cell.col), data_direction)
                        elif data_direction == Direction.LEFT:
                            range = CoordRange(Coord(cell.row, cell.col - 1), data_direction)
                        elif data_direction == Direction.RIGHT:
                            range = CoordRange(Coord(cell.row, cell.col + 1), data_direction)
                        elif data_direction == Direction.UP:
                            range = CoordRange(Coord(cell.row - 1, cell.col), data_direction)
                        else:
                            raise NotImplementedError()

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

            @property
            def date_cells(self) -> typing.Optional[Source]:
                return dates.source

            @property
            def title_cells(self) -> typing.Optional[Source]:
                if titles:
                    return titles.source
                else:
                    return None

            @property
            def title_ids_cells(self) -> typing.Dict[str, Source]:
                return {e.name: e.source for e in title_ids}

            @property
            def dimensions_cells(self) -> typing.Dict[str, Source]:
                return {e.name: e.source for e in dimensions}

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
