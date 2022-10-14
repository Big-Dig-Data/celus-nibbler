import typing

from pydantic.dataclasses import dataclass

from celus_nibbler.coordinates import Coord, CoordRange, Direction, SheetAttr, Value
from celus_nibbler.utils import JsonEncorder, PydanticConfig

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
