import typing
from dataclasses import field

from pydantic import ValidationError
from pydantic.dataclasses import dataclass

from ..conditions import Condition
from ..coordinates import Coord, CoordRange, Direction
from ..errors import TableException
from ..parsers.base import BaseArea, BaseDateArea, MonthDataCells
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
class DateBasedAreaDefinition(AreaGeneratorMixin, JsonEncorder):

    dates: DateSource
    titles: TitleSource
    metrics: MetricSource
    organizations: typing.Optional[OrganizationSource] = None
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])

    name: typing.Literal["non_counter.date_based"] = "non_counter.date_based"

    def make_area(self) -> typing.Type[BaseArea]:
        dates_range = self.dates.source
        data_direction = self.dates.direction
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        metrics = self.metrics
        organizations = self.organizations

        class Area(BaseDateArea):
            @property
            def header_cells(self) -> CoordRange:
                return dates_range

            @property
            def organization_cells(self) -> typing.Optional[Source]:
                if organizations:
                    return organizations.source
                else:
                    return None

            def find_data_cells(self) -> typing.List[MonthDataCells]:
                res = []
                for cell in self.header_cells:
                    try:
                        date = self.parse_date(cell)
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
                            MonthDataCells(
                                date.replace(day=1),
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
            def title_cells(self) -> typing.Optional[Source]:
                return titles.source

            @property
            def title_ids_cells(self) -> typing.Dict[str, Source]:
                return {e.name: e.source for e in title_ids}

            @property
            def dimensions_cells(self) -> typing.Dict[str, Source]:
                return {e.name: e.source for e in dimensions}

            @property
            def metric_cells(self) -> typing.Dict[str, Source]:
                return metrics.source

        return Area


@dataclass(config=PydanticConfig)
class DateBasedDefinition(JsonEncorder, BaseParserDefinition):
    # name of nibbler parser
    parser_name: str
    format_name: str
    areas: typing.List[DateBasedAreaDefinition]
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    name: typing.Literal["non_counter.date_based"] = "non_counter.date_based"
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
