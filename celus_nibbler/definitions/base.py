import abc
import typing

from pydantic.dataclasses import dataclass

from celus_nibbler.parsers.base import BaseArea, BaseParser
from celus_nibbler.utils import PydanticConfig


@dataclass(config=PydanticConfig)
class BaseParserDefinition(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_parser(self) -> typing.Type[BaseParser]:
        pass

    @property
    @abc.abstractmethod
    def kind(self) -> str:
        pass


class BaseAreaDefinition(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def kind(self) -> str:
        pass

    @abc.abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass
