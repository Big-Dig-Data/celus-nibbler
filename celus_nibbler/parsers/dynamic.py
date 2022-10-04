import typing
from abc import ABCMeta, abstractmethod

from celus_nibbler.definitions import (
    CounterDefinition,
    Definition,
    FixedDefinition,
    MetricBasedDefinition,
)

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


def gen_parser(definition: Definition) -> typing.Type[BaseParser]:

    if isinstance(definition.__root__, FixedDefinition):

        class Parser(DynamicParserMixin, BaseParser):
            _definition = definition.__root__

            format_name = _definition.format_name
            platforms = _definition.platforms

            metrics_to_skip = _definition.metrics_to_skip
            titles_to_skip = _definition.titles_to_skip
            dimensions_to_skip = _definition.dimensions_to_skip

            metric_aliases = _definition.metric_aliases
            dimension_aliases = _definition.dimension_aliases

            heuristics = _definition.heuristics
            areas = [e.make_area() for e in _definition.areas]

            @classmethod
            def name(cls):
                return cls._definition.parser_name

    elif isinstance(definition.__root__, CounterDefinition):
        return definition.__root__.make_parser()

    elif isinstance(definition.__root__, MetricBasedDefinition):
        return definition.__root__.make_parser()

    else:
        raise NotImplementedError("Missing parser for definition")

    return Parser
