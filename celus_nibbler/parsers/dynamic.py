from celus_nibbler.definitions import Definition

from .base import BaseParser


def gen_parser(definition: Definition):
    """Generates a parser based on definition"""

    class DynamicParser(BaseParser):
        _definition = definition

        name = definition.parser_name
        platforms = definition.platforms
        metrics_to_skip = definition.metrics_to_skip
        titles_to_skip = definition.titles_to_skip
        platforms_to_skip = definition.platforms_to_skip
        heuristics = definition.heuristics
        areas = [e.make_area() for e in definition.areas]

    return DynamicParser
