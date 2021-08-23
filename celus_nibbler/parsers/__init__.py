import itertools
import typing

from celus_nibbler.parsers.format_1_parsers import (
    Parser_1_3_1,
    Parser_1_3_2,
    Parser_1_5_1,
    Parser_1_5_2,
)

from .generalparser import GeneralParser, HorizontalDatesParser


def all_parsers() -> typing.List[typing.Type[GeneralParser]]:
    """ Lists all available parsers """
    return [
        Parser_1_3_1,
        Parser_1_3_2,
        Parser_1_5_1,
        Parser_1_5_2,
    ]


def get_supported_platforms():
    """ Lists all supported platforms"""
    return sorted(
        list(set(itertools.chain.from_iterable(parser.platforms for parser in all_parsers())))
    )


__all__ = [
    "all_parsers",
    "GeneralParser",
    "HorizontalDatesParser",
    "format_1_parsers",
    "Parser_1_3_1",
    "Parser_1_3_2",
    "Parser_1_5_1",
    "Parser_1_5_2",
]
