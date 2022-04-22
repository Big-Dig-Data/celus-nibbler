import collections
import itertools
import typing

import pkg_resources

from .base import BaseParser


def all_parsers(
    parsers: typing.Optional[typing.List[str]] = None,
) -> typing.List[typing.Type[BaseParser]]:
    """Lists all available parsers

    :param parsers: use only parsers which names are on this list
    :returns: List of parser classes
    """
    return [
        entry_point.load()
        for entry_point in pkg_resources.iter_entry_points("nibbler_parsers")
        if not parsers or any(entry_point.name.startswith(e) for e in parsers)
    ]


def get_supported_platforms(
    parsers: typing.Optional[typing.List[str]] = None,
) -> typing.List[str]:
    """Lists all supported platforms"""
    return sorted(
        list(
            set(itertools.chain.from_iterable(parser.platforms for parser in all_parsers(parsers)))
        )
    )


def get_supported_platforms_count(
    parsers: typing.Optional[typing.List[str]] = None,
) -> typing.List[typing.Tuple[str, int]]:
    counter: typing.Dict[str, int] = collections.Counter()
    for parser in all_parsers(parsers):
        for platform in parser.platforms:
            counter[platform] += 1

    res = []
    for key in sorted(counter.keys()):
        res.append((key, counter[key]))

    return res


__all__ = ["all_parsers", "get_supported_platforms", "get_supported_platforms_count"]
