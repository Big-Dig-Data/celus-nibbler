import abc
import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.parsers.non_counter.celus_format import (
    BaseCelusFormatArea,
    BaseCelusFormatParser,
)
from celus_nibbler.sources import ExtractParams
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseNonCounterParserDefinition


@dataclass(config=PydanticConfig)
class CelusFormatAreaDefinition(JsonEncorder, BaseAreaDefinition):
    title_column_names: typing.List[str] = field(default_factory=lambda: [])
    organization_column_names: typing.List[str] = field(default_factory=lambda: [])
    metric_column_names: typing.List[str] = field(default_factory=lambda: [])
    default_metric: typing.Optional[str] = None
    title_ids_mapping: typing.Dict[str, str] = field(default_factory=lambda: {})
    dimension_mapping: typing.Dict[str, str] = field(default_factory=lambda: {})
    value_extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())

    kind: typing.Literal["non_counter.celus_format"] = "non_counter.celus_format"

    def make_area(self):
        class Area(BaseCelusFormatArea):
            title_column_names = self.title_column_names
            organization_column_names = self.organization_column_names
            metric_column_names = self.metric_column_names
            default_metric = self.default_metric
            title_ids_mapping = self.title_ids_mapping
            dimension_mapping = self.dimension_mapping
            value_extract_params = self.value_extract_params

        return Area


@dataclass(config=PydanticConfig)
class CelusFormatParserDefinition(BaseNonCounterParserDefinition, metaclass=abc.ABCMeta):
    parser_name: str
    data_format: DataFormatDefinition
    areas: typing.List[CelusFormatAreaDefinition]

    platforms: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None

    kind: typing.Literal["non_counter.celus_format"] = "non_counter.celus_format"
    version: typing.Literal[1] = 1

    def make_parser(self):
        class Parser(BaseCelusFormatParser):
            _definition = self
            data_format = _definition.data_format
            name = f"dynamic.{_definition.group}.{_definition.data_format.name}.{_definition.parser_name}"

            platforms = _definition.platforms
            metrics_to_skip = self.metrics_to_skip
            titles_to_skip = self.titles_to_skip
            dimensions_to_skip = self.dimensions_to_skip

            metric_aliases = self.metric_aliases
            dimension_aliases = self.dimension_aliases

            heuristics = self.heuristics

            areas = [e.make_area() for e in self.areas]

        return Parser

    kind: typing.Literal["non_counter.celus_format"] = "non_counter.celus_format"
