import typing
from abc import ABCMeta, abstractmethod

from pydantic.dataclasses import dataclass

from ..coordinates import Coord, CoordRange, Direction, SheetAttr, Value
from ..parsers.base import BaseArea
from ..utils import JsonEncorder, PydanticConfig

Source = typing.Union[Coord, CoordRange, SheetAttr, Value]


@dataclass(config=PydanticConfig)
class DimensionSource(JsonEncorder):
    name: str
    source: Source
    required: bool = True


@dataclass(config=PydanticConfig)
class MetricSource(JsonEncorder):
    source: Source
    direction: typing.Optional[Direction] = None


@dataclass(config=PydanticConfig)
class OrganizationSource(JsonEncorder):
    source: Source
    regex: typing.Optional[typing.Pattern] = None


@dataclass(config=PydanticConfig)
class TitleSource(JsonEncorder):
    source: Source


@dataclass(config=PydanticConfig)
class TitleIdSource(JsonEncorder):
    name: str
    source: Source


@dataclass(config=PydanticConfig)
class DateSource(JsonEncorder):
    source: Source
    direction: typing.Optional[Direction] = None


class AreaGeneratorMixin(metaclass=ABCMeta):
    @abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass
