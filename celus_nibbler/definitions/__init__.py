import typing

from pydantic import Field, RootModel
from typing_extensions import Annotated

from ..utils import JsonEncorder
from . import counter
from .base import BaseParserDefinition
from .celus_format import CelusFormatAreaDefinition, CelusFormatParserDefinition
from .counter import BaseCounterAreaDefinition, CounterDefinition
from .generic import GenericAreaDefinition, GenericDefinition

AreaDefinition = Annotated[
    typing.Union[
        counter.BR1AreaDefinition,
        counter.BR2AreaDefinition,
        counter.BR3AreaDefinition,
        counter.DB1AreaDefinition,
        counter.DB2AreaDefinition,
        counter.JR1AreaDefinition,
        counter.JR1aAreaDefinition,
        counter.JR1GOAAreaDefinition,
        counter.JR2AreaDefinition,
        counter.MR1AreaDefinition,
        counter.PR1AreaDefinition,
        counter.TRAreaDefinition,
        counter.IR_M1AreaDefinition,
        counter.IRAreaDefinition,
        counter.DRAreaDefinition,
        counter.PRAreaDefinition,
        counter.TR51AreaDefinition,
        counter.IR51AreaDefinition,
        counter.DR51AreaDefinition,
        counter.PR51AreaDefinition,
        CelusFormatAreaDefinition,
        GenericAreaDefinition,
    ],
    Field(discriminator="kind"),
]


DefinitionAnotation = Annotated[
    typing.Union[
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
        counter.DRDefinition,
        counter.PRDefinition,
        counter.TRDefinition,
        counter.IR_M1Definition,
        counter.IRDefinition,
        counter.TR51Definition,
        counter.IR51Definition,
        counter.DR51Definition,
        counter.PR51Definition,
        CelusFormatParserDefinition,
        GenericDefinition,
    ],
    Field(discriminator="kind"),
]


def get_all_definitions() -> typing.List[BaseParserDefinition]:
    return list(DefinitionAnotation.__origin__.__args__)


class Definition(JsonEncorder, RootModel):
    root: DefinitionAnotation


__all__ = [
    "AreaDefinition",
    "BaseCounterAreaDefinition",
    "CounterDefinition",
    "Definition",
    "BaseParserDefinition",
    "GenericDefinition",
    "GenericAreaDefinition",
    "CelusFormatAreaDefinition",
    "CelusFormatParserDefinition",
]
