import collections
import itertools
import typing

from celus_nibbler.parsers.format_1_parsers import (
    Parser_1_2,
    Parser_1_3_1,
    Parser_1_3_2,
    Parser_1_3_3,
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
        Parser_1_3_3,
        Parser_1_2,
    ]


def get_supported_platforms() -> typing.List[str]:
    """ Lists all supported platforms"""
    return sorted(
        list(set(itertools.chain.from_iterable(parser.platforms for parser in all_parsers())))
    )


def get_supported_platforms_count() -> typing.List[typing.Tuple[str, int]]:
    counter: typing.Dict[str, int] = collections.Counter()
    for parser in all_parsers():
        for platform in parser.platforms:
            counter[platform] += 1

    res = []
    for key in sorted(counter.keys()):
        res.append((key, counter[key]))

    return res


__all__ = [
    "all_parsers",
    "GeneralParser",
    "HorizontalDatesParser",
    "format_1_parsers",
    "Parser_1_3_1",
    "Parser_1_3_2",
    "Parser_1_5_1",
    "Parser_1_5_2",
    "Parser_1_3_3",
    "Parser_1_2",
]
