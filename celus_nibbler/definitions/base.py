import abc
import typing

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.parsers.base import BaseArea, BaseParser


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
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}
    metric_aliases: typing.List[typing.Tuple[str, str]] = []
    dimension_aliases: typing.List[typing.Tuple[str, str]] = []


class BaseAreaDefinition(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def kind(self) -> str:
        pass

    @abc.abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass
