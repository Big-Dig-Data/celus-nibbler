from abc import ABCMeta, abstractmethod

from celus_nibbler.definitions import Definition

from .base import BaseParser


class DynamicParser(BaseParser, metaclass=ABCMeta):
    """Parser generated based on a definition"""

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @classmethod
    def full_name(cls):
        return f"nibbler.dynamic.{cls.name()}"

    def __str__(self):
        return f"{self.__name__}({self.name()})"


def gen_parser(definition: Definition):
    class Parser(DynamicParser):
        _definition = definition

        format_name = definition.format_name
        platforms = definition.platforms

        metrics_to_skip = definition.metrics_to_skip
        titles_to_skip = definition.titles_to_skip
        dimensions_to_skip = definition.dimensions_to_skip

        metric_aliases = definition.metric_aliases
        dimension_aliases = definition.dimension_aliases

        heuristics = definition.heuristics
        areas = [e.make_area() for e in definition.areas]

        @classmethod
        def name(cls):
            return definition.parser_name

    return Parser
