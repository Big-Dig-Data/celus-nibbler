from .Format_1_Parsers import Parser_1_3_1, Parser_1_3_2, Parser_1_5_1, Parser_1_5_2
from .GeneralParser import GeneralParser

all_parsers = [
    Parser_1_3_1,
    Parser_1_5_2,
    Parser_1_3_2,
    Parser_1_5_1,
]

__all__ = [
    "all_parsers",
    "GeneralParser",
    "Parser_1_3_1",
    "Parser_1_5_2",
    "Parser_1_3_2",
    "Parser_1_5_1",
]
