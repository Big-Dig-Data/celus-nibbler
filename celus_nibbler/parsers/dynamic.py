import typing
from abc import ABCMeta, abstractmethod

from .base import BaseParser


class DynamicParserMixin(metaclass=ABCMeta):
    """Parser generated based on a definition"""

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @classmethod
    def get_name(cls) -> str:
        # Some dynamic parsers may have name as attribute some a method
        return cls.name() if callable(cls.name) else cls.name

    @classmethod
    def full_name(cls):
        return f"nibbler.dynamic.{cls.get_name()}"

    def __str__(self):
        return f"{self.__name__}({self.get_name()})"


def gen_parser(parser_definition) -> typing.Type[BaseParser]:
    return parser_definition.__root__.make_parser()
