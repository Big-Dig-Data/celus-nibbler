import typing
from datetime import date

from pydantic.dataclasses import dataclass

from ..coordinates import Coord, CoordRange, Direction
from ..parsers.base import BaseArea, MonthDataCells
from ..utils import JsonEncorder, PydanticConfig
from .common import AreaGeneratorMixin


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
