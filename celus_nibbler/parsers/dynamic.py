import typing

from .base import BaseParser


def gen_parser(parser_definition) -> typing.Type[BaseParser]:
    return parser_definition.root.make_parser()
