import typing

from .base import BaseParser


def gen_parser(parser_definition) -> typing.Type[BaseParser]:
    return parser_definition.__root__.make_parser()
