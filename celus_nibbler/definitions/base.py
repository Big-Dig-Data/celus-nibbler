import abc
import typing

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


class BaseAreaDefinition(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def kind(self) -> str:
        pass

    @abc.abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass
