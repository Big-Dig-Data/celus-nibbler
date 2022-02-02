import collections
import itertools
import typing

import pkg_resources

from . import format_1
from .base import BaseParser


def all_parsers() -> typing.List[typing.Type[BaseParser]]:
    """Lists all available parsers"""
    return [
        entry_point.load() for entry_point in pkg_resources.iter_entry_points("nibbler_parsers")
    ]


def get_supported_platforms() -> typing.List[str]:
    """Lists all supported platforms"""
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


__all__ = ["all_parsers", "format_1", "get_supported_platforms", "get_supported_platforms_count"]
