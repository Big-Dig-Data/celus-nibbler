import abc
import typing
from dataclasses import field

from pydantic.dataclasses import dataclass

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.non_counter.celus_format import (
    BaseCelusFormatArea,
    BaseCelusFormatParser,
)
from celus_nibbler.sources import ExtractParams, SpecialExtraction
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseNonCounterParserDefinition


@dataclass(config=PydanticConfig)
class CelusFormatAreaDefinition(JsonEncorder, BaseAreaDefinition):
    title_column_names: typing.List[str] = field(default_factory=lambda: [])
    item_column_names: typing.List[str] = field(default_factory=lambda: [])
    organization_column_names: typing.List[str] = field(default_factory=lambda: [])
    metric_column_names: typing.List[str] = field(default_factory=lambda: [])
    default_metric: typing.Optional[str] = None
    title_ids_mapping: typing.Dict[str, str] = field(default_factory=lambda: {})
    item_ids_mapping: typing.Dict[str, str] = field(default_factory=lambda: {})
    item_publication_date_column_names: typing.List[str] = field(default_factory=lambda: [])
    item_authors_column_names: typing.List[str] = field(default_factory=lambda: [])
    dimension_mapping: typing.Dict[str, str] = field(default_factory=lambda: {})
    value_extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())
    aggregate_same_records: bool = False

    kind: typing.Literal["non_counter.celus_format"] = "non_counter.celus_format"

    def make_area(self):
        class Area(BaseCelusFormatArea):
            title_column_names = self.title_column_names
            item_column_names = self.item_column_names
            organization_column_names = self.organization_column_names
            metric_column_names = self.metric_column_names
            default_metric = self.default_metric
            title_ids_mapping = self.title_ids_mapping
            item_ids_mapping = self.item_ids_mapping
            item_publication_date_column_names = self.item_publication_date_column_names
            item_authors_column_names = self.item_authors_column_names
            dimension_mapping = self.dimension_mapping
            value_extract_params = self.value_extract_params
            aggregator = self.make_aggregator()

        return Area


@dataclass(config=PydanticConfig)
class CelusFormatParserDefinition(BaseNonCounterParserDefinition, metaclass=abc.ABCMeta):
    parser_name: str
    data_format: DataFormatDefinition
    areas: typing.List[CelusFormatAreaDefinition]

    platforms: typing.List[str] = field(default_factory=lambda: [])
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    available_metrics: typing.Optional[typing.List[str]] = None
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    metric_value_extraction_overrides: typing.Dict[str, SpecialExtraction] = field(
        default_factory=lambda: {}
    )
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    heuristics: typing.Optional[Condition] = None
    possible_row_offsets: typing.List[int] = field(default_factory=lambda: [0])

    kind: typing.Literal["non_counter.celus_format"] = "non_counter.celus_format"
    version: typing.Literal[1] = 1

    def make_parser(self):
        class Parser(BaseCelusFormatParser):
            _definition = self
            data_format = _definition.data_format
            name = (
                f"dynamic.{_definition.group}.{_definition.data_format.name}"
                f".{_definition.parser_name}"
            )

            platforms = _definition.platforms
            metrics_to_skip = self.metrics_to_skip
            titles_to_skip = self.titles_to_skip
            dimensions_to_skip = self.dimensions_to_skip
            available_metrics = self.available_metrics
            on_metric_check_failed = self.on_metric_check_failed

            metric_aliases = dict(self.metric_aliases)
            metric_value_extraction_overrides = dict(self.metric_value_extraction_overrides)
            dimension_aliases = dict(self.dimension_aliases)

            heuristics = self.heuristics
            possible_row_offsets = self.possible_row_offsets

            areas = [e.make_area() for e in self.areas]

        return Parser

    kind: typing.Literal["non_counter.celus_format"] = "non_counter.celus_format"
