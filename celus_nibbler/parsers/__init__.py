import collections
import itertools
import re
import sys
import typing
from importlib.metadata import entry_points

from .base import BaseParser

PYTHON_VERSION = sys.version_info

if PYTHON_VERSION[0] == 3 and PYTHON_VERSION[1] < 10:

    def get_entry_points():
        return entry_points().get("nibbler_parsers", [])

else:

    def get_entry_points():
        return entry_points(group="nibbler_parsers")


def get_parsers(
    parsers: typing.Optional[typing.List[str]] = None,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.List[typing.Tuple[str, typing.Type[BaseParser]]]:
    """Lists all (name, parser) tuples

    :param parsers: use only parsers which names are on this list
    :returns: List of tuples with (name, parser class)
    """
    return [
        (entry_point.name, entry_point.load())
        for entry_point in get_entry_points()
        if not parsers or any(re.match(e, entry_point.name) for e in parsers)
    ] + [
        (parser.name, parser)
        for parser in dynamic_parsers
        if not parsers or any(re.match(e, parser.name) for e in parsers)
    ]


def available_parsers(
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.List[str]:
    return [name for name, _ in get_parsers(dynamic_parsers=dynamic_parsers)]


def filter_parsers(
    parsers: typing.Optional[typing.List[str]] = None,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.List[typing.Type[BaseParser]]:
    """Lists all available parsers

    :param parsers: use only parsers which names are on this list
    :returns: List of parser classes
    """
    return [entry_point for _, entry_point in get_parsers(parsers, dynamic_parsers)]


def get_supported_platforms(
    parsers: typing.Optional[typing.List[str]] = None,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.List[str]:
    """Lists all supported platforms"""
    return sorted(
        list(
            set(
                itertools.chain.from_iterable(
                    parser.platforms for parser in filter_parsers(parsers, dynamic_parsers)
                )
            )
        )
    )


def get_supported_platforms_count(
    parsers: typing.Optional[typing.List[str]] = None,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.List[typing.Tuple[str, int]]:
    counter: typing.Dict[str, int] = collections.Counter()
    for parser in filter_parsers(parsers, dynamic_parsers):
        for platform in parser.platforms:
            counter[platform] += 1

    res = []
    for key in sorted(counter.keys()):
        res.append((key, counter[key]))

    return res


__all__ = [
    "filter_parsers",
    "get_supported_platforms",
    "get_supported_platforms_count",
]
