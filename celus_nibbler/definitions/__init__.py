import typing
from dataclasses import field

from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from ..conditions import Condition
from ..utils import JsonEncorder, PydanticConfig
from . import counter
from .counter import CounterDefinition
from .dummy import DummyAreaDefinition
from .fixed import FixedAreaDefinition
from .metric_based import MetricBasedAreaDefinition, MetricBasedDefinition

AreaDefinition = Annotated[
    typing.Union[
        FixedAreaDefinition,
        DummyAreaDefinition,
        MetricBasedAreaDefinition,
    ],
    Field(discriminator='name'),
]


@dataclass(config=PydanticConfig)
class FixedDefinition(JsonEncorder):
    # name of nibbler parser
    parser_name: str
    format_name: str
    areas: typing.List[FixedAreaDefinition]
    platforms: typing.List[str] = field(default_factory=lambda: [])
    dimensions: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    name: typing.Literal["fixed"] = "fixed"
    version: typing.Literal[1] = 1


DefinitionAnotation = Annotated[
    typing.Union[
        FixedDefinition,
        counter.BR1Definition,
        counter.BR2Definition,
        counter.BR3Definition,
        counter.DB1Definition,
        counter.DB2Definition,
        counter.JR1Definition,
        counter.JR1aDefinition,
        counter.JR1GOADefinition,
        counter.JR2Definition,
        counter.MR1Definition,
        counter.PR1Definition,
        counter.TRDefinition,
        counter.DRDefinition,
        counter.PRDefinition,
        MetricBasedDefinition,
    ],
    Field(discriminator='name'),
]


class Definition(JsonEncorder, BaseModel):
    __root__: DefinitionAnotation


__all__ = [
    'Definition',
    'AreaDefinition',
    'FixedAreaDefinition',
    'CounterDefinition',
    'MetricBasedDefinition',
]
