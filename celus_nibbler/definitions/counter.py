import abc
import typing

from pydantic.dataclasses import dataclass

from ..parsers.counter.c4 import BR1, BR2, BR3, DB1, DB2, JR1, JR1GOA, JR2, MR1, PR1, JR1a
from ..parsers.counter.c5 import DR, PR, TR
from ..utils import JsonEncorder, PydanticConfig


@dataclass(config=PydanticConfig)
class CounterDefinition(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_parser(self):
        pass


@dataclass(config=PydanticConfig)
class BR1Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.BR1"] = "counter4.BR1"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, BR1):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class BR2Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.BR2"] = "counter4.BR2"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, BR2):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class BR3Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.BR3"] = "counter4.BR3"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, BR3):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class DB1Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.DB1"] = "counter4.DB1"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, DB1):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class DB2Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.DB2"] = "counter4.DB2"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, DB2):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class JR1Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.JR1"] = "counter4.JR1"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, JR1):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class JR1aDefinition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.JR1a"] = "counter4.JR1a"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, JR1a):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class JR1GOADefinition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.JR1GOA"] = "counter4.JR1GOA"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, JR1GOA):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class JR2Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.JR2"] = "counter4.JR2"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, JR2):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class MR1Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.MR1"] = "counter4.MR1"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, MR1):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class PR1Definition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter4.PR1"] = "counter4.PR1"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, PR1):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class TRDefinition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter5.TR"] = "counter5.TR"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, TR):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class DRDefinition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter5.DR"] = "counter5.DR"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, DR):
            name = self.name

        return Parser


@dataclass(config=PydanticConfig)
class PRDefinition(JsonEncorder, CounterDefinition):
    name: typing.Literal["counter5.PR"] = "counter5.PR"
    version: typing.Literal[1] = 1

    def make_parser(self):
        from ..parsers.dynamic import DynamicParserMixin

        class Parser(DynamicParserMixin, PR):
            name = self.name

        return Parser
