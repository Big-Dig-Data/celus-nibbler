from .eat_and_poop import Poop, PoopStats, eat
from .errors import (
    MultipleParsersFound,
    NibblerError,
    NoParserForPlatformFound,
    NoParserFound,
    NoParserMatchesHeuristics,
    RecordError,
    TableException,
    WrongFileFormatError,
)
from .parsers import get_supported_platforms

__all__ = [
    'eat',
    'get_supported_platforms',
    'Poop',
    'PoopStats',
    'MultipleParsersFound',
    'NibblerError',
    'NoParserFound',
    'NoParserForPlatformFound',
    'NoParserMatchesHeuristics',
    'RecordError',
    'TableException',
    'WrongFileFormatError',
]
