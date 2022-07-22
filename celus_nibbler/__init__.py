from .eat_and_poop import Poop, eat
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
    'MultipleParsersFound',
    'NibblerError',
    'NoParserFound',
    'NoParserForPlatformFound',
    'NoParserMatchesHeuristics',
    'RecordError',
    'TableException',
    'WrongFileFormatError',
]
