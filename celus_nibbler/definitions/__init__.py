import typing

from pydantic import Field, RootModel
from typing_extensions import Annotated

from ..utils import JsonEncorder
from . import counter
from .base import BaseParserDefinition
from .celus_format import CelusFormatAreaDefinition, CelusFormatParserDefinition
from .date_based import DateBasedAreaDefinition, DateBasedDefinition
from .date_metric_based import DateMetricBasedAreaDefinition, DateMetricBasedDefinition
from .generic import GenericAreaDefinition, GenericDefinition
from .metric_based import MetricBasedAreaDefinition, MetricBasedDefinition

AreaDefinition = Annotated[
    typing.Union[
        DateBasedAreaDefinition,
        DateMetricBasedAreaDefinition,
        MetricBasedAreaDefinition,
        CelusFormatAreaDefinition,
        GenericAreaDefinition,
    ],
    Field(discriminator="kind"),
]


DefinitionAnotation = Annotated[
    typing.Union[
        DateBasedDefinition,
        DateMetricBasedDefinition,
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
        counter.IR_M1Definition,
        counter.IRDefinition,
        MetricBasedDefinition,
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
    "Definition",
    "DateBasedAreaDefinition",
    "DateBasedDefinition",
    "DateMetricBasedAreaDefinition",
    "DateMetricBasedDefinition",
    "MetricBasedAreaDefinition",
    "MetricBasedDefinition",
    "BaseParserDefinition",
    "CelusFormatAreaDefinition",
    "CelusFormatParserDefinition",
]
