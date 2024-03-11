import abc
import typing
from dataclasses import field

from celus_nibbler.aggregator import BaseAggregator, NoAggregator, SameAggregator
from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseArea, BaseParser
from celus_nibbler.sources import SpecialExtraction


class BaseParserDefinition(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_parser(self) -> typing.Type[BaseParser]:
        pass


class BaseNonCounterParserDefinition(BaseParserDefinition):
    version: int
    kind: str
    parser_name: str
    data_format: DataFormatDefinition
    group: str = "non_counter"
    heuristics: typing.Optional[Condition] = None
    metrics_to_skip: typing.List[str] = []
    available_metrics: typing.Optional[typing.List[str]] = None
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}
    metric_aliases: typing.List[typing.Tuple[str, str]] = []
    metric_value_extraction_overrides: typing.Dict[str, SpecialExtraction] = field(
        default_factory=lambda: {}
    )
    dimension_aliases: typing.List[typing.Tuple[str, str]] = []


class BaseAreaDefinition(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def kind(self) -> str:
        pass

    @abc.abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass

    def make_aggregator(self) -> BaseAggregator:
        return (
            SameAggregator() if getattr(self, "aggregate_same_records", False) else NoAggregator()
        )
