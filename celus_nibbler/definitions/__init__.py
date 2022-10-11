import typing

from pydantic import BaseModel, Field
from typing_extensions import Annotated

from ..utils import JsonEncorder
from . import counter
from .base import BaseParserDefinition
from .date_based import DateBasedAreaDefinition, DateBasedDefinition
from .dummy import DummyAreaDefinition
from .metric_based import MetricBasedAreaDefinition, MetricBasedDefinition

AreaDefinition = Annotated[
    typing.Union[
        DateBasedAreaDefinition,
        DummyAreaDefinition,
        MetricBasedAreaDefinition,
    ],
    Field(discriminator='name'),
]


DefinitionAnotation = Annotated[
    typing.Union[
        DateBasedDefinition,
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
    'AreaDefinition',
    'Definition',
    'AreaDefinition',
    'DateBasedAreaDefinition',
    'DateBasedDefinition',
    'MetricBasedAreaDefinition',
    'MetricBasedDefinition',
    'BaseParserDefinition',
]
