import typing
from dataclasses import field

from pydantic import Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from ..conditions import Condition
from ..utils import JsonEncorder, PydanticConfig
from .dummy import DummyAreaDefinition
from .fixed import FixedAreaDefinition

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


__all__ = ['Definition', 'AreaDefinition', 'FixedAreaDefinition']
