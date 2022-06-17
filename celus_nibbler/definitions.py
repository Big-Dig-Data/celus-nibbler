import typing
from abc import ABCMeta, abstractmethod
from dataclasses import field
from datetime import date

from pydantic import Field, ValidationError
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from .conditions import Condition
from .coordinates import Coord, CoordRange, Direction, SheetAttr, Value
from .errors import TableException
from .parsers.base import BaseArea, MonthDataCells
from .utils import JsonEncorder, PydanticConfig

Source = typing.Union[Coord, CoordRange, SheetAttr, Value]


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder):
    name: str
    source: Source
    required: bool = True


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder):
    source: Source


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder):
    source: Source


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder):
    name: str
    source: Source


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder):
    direction: Direction
    source: Source


class AreaGeneratorMixin(metaclass=ABCMeta):
    @abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass


@dataclass(config=PydanticConfig)
class FixedAreaDefinition(AreaGeneratorMixin, JsonEncorder):
    """All cell are fixed to particular position"""

    dates: DateSource
    titles: TitleSource
    metrics: MetricSource
    title_ids: typing.List[TitleIdSource] = field(default_factory=lambda: [])
    dimensions: typing.List[DimensionSource] = field(default_factory=lambda: [])

    name: typing.Literal["fixed"] = "fixed"

    def make_area(self) -> typing.Type[BaseArea]:
        dates_range = self.dates.source
        data_direction = self.dates.direction
        titles = self.titles
        title_ids = self.title_ids
        dimensions = self.dimensions
        metrics = self.metrics

        class Area(BaseArea):
            @property
            def date_header_cells(self) -> CoordRange:
                return dates_range

            def find_data_cells(self) -> typing.List[MonthDataCells]:
                res = []
                for cell in self.date_header_cells:
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
class DummyAreaDefinition(AreaGeneratorMixin, JsonEncorder):
    """Represents a dummy area definition (for testing purposes)"""

    name: typing.Literal["dummy"] = "dummy"

    def make_area(self) -> typing.Type[BaseArea]:
        class Area(BaseArea):
            def date_header_cells(self) -> CoordRange:
                return CoordRange(Coord(0, 0), Direction.LEFT)

            def find_data_cells(self) -> typing.List[MonthDataCells]:
                return [
                    MonthDataCells(date(2020, 1, 1), CoordRange(Coord(1, 0), Direction.DOWN)),
                    MonthDataCells(date(2020, 2, 1), CoordRange(Coord(1, 1), Direction.DOWN)),
                ]

        return Area


AreaDefinition = Annotated[
    typing.Union[FixedAreaDefinition, DummyAreaDefinition], Field(discriminator='name')
]


@dataclass(config=PydanticConfig)
class Definition(JsonEncorder):
    # name of nibbler parser
    parser_name: str
    format_name: str
    areas: typing.List[AreaDefinition]
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    version: typing.Literal[1] = 1
