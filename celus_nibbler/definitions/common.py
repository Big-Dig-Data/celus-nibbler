import typing
from abc import ABCMeta, abstractmethod

from pydantic.dataclasses import dataclass

from ..coordinates import Coord, CoordRange, Direction, SheetAttr, Value
from ..parsers.base import BaseArea
from ..utils import JsonEncorder, PydanticConfig

Source = typing.Union[Coord, CoordRange, SheetAttr, Value]
Content = typing.Union[Coord, SheetAttr, Value]


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder):
    name: str
    source: Source
    required: bool = True


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder):
    source: Source


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder):
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
