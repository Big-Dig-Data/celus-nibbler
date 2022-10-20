import abc
import typing
from dataclasses import field

from pydantic import Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.parsers.base import BaseArea, BaseParser
from celus_nibbler.parsers.counter import CounterHeaderArea
from celus_nibbler.parsers.counter.c4 import (
    BR1,
    BR2,
    BR3,
    DB1,
    DB2,
    JR1,
    JR1GOA,
    JR2,
    MR1,
    PR1,
    BaseCounter4Parser,
    JR1a,
)
from celus_nibbler.parsers.counter.c5 import DR, PR, TR, BaseCounter5Parser
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseParserDefinition


@dataclass(config=PydanticConfig)
class BaseCounterAreaDefinition(BaseAreaDefinition):
    kind: str = NotImplementedError
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_area(self):
        raise NotImplementedError()


def gen_area(
    base: typing.Type[BaseArea], definition: BaseCounterAreaDefinition
) -> typing.Type[CounterHeaderArea]:
    class Area(base):
        if definition.organization_column is not None:
            ORGANIZATION_COLUMN_NAMES = definition.organization_column
        if definition.metric_column is not None:
            METRIC_COLUMN_NAMES = definition.metric_column
        if definition.title_column is not None:
            TITLE_COLUMN_NAMES = definition.title_column

    return Area


@dataclass(config=PydanticConfig)
class BR1AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.BR1"] = "counter4.BR1"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(BR1.Area, self)


@dataclass(config=PydanticConfig)
class BR2AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.BR2"] = "counter4.BR2"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(BR2.Area, self)


@dataclass(config=PydanticConfig)
class BR3AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.BR3"] = "counter4.BR3"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(BR3.Area, self)


@dataclass(config=PydanticConfig)
class DB1AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.DB1"] = "counter4.DB1"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(DB1.Area, self)


@dataclass(config=PydanticConfig)
class DB2AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.DB2"] = "counter4.DB2"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(DB2.Area, self)


@dataclass(config=PydanticConfig)
class JR1AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.JR1"] = "counter4.JR1"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(JR1.Area, self)


@dataclass(config=PydanticConfig)
class JR1aAreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.JR1a"] = "counter4.JR1a"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(JR1a.Area, self)


@dataclass(config=PydanticConfig)
class JR1GOAAreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.JR1GOA"] = "counter4.JR1GOA"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(JR1GOA.Area, self)


@dataclass(config=PydanticConfig)
class JR2AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.JR2"] = "counter4.JR2"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(JR2.Area, self)


@dataclass(config=PydanticConfig)
class MR1AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.MR1"] = "counter4.MR1"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(MR1.Area, self)


@dataclass(config=PydanticConfig)
class PR1AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter4.PR1"] = "counter4.PR1"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(PR1.Area, self)


@dataclass(config=PydanticConfig)
class TRAreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter5.TR"] = "counter5.TR"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(TR.Area, self)


@dataclass(config=PydanticConfig)
class DRAreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter5.DR"] = "counter5.DR"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(DR.Area, self)


@dataclass(config=PydanticConfig)
class PRAreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter5.PR"] = "counter5.PR"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(PR.Area, self)


CounterAreaDefinition = Annotated[
    typing.Union[
        BR1AreaDefinition,
        BR2AreaDefinition,
        BR3AreaDefinition,
        DB1AreaDefinition,
        DB2AreaDefinition,
        JR1AreaDefinition,
        JR1aAreaDefinition,
        JR1GOAAreaDefinition,
        JR2AreaDefinition,
        MR1AreaDefinition,
        PR1AreaDefinition,
        DRAreaDefinition,
        PRAreaDefinition,
        TRAreaDefinition,
    ],
    Field(discriminator='kind'),
]


@dataclass(config=PydanticConfig)
class BaseCounterParserDefinition(BaseParserDefinition, metaclass=abc.ABCMeta):
    parser_name: str
    kind: str
    group: str
    heuristics: typing.Optional[Condition] = None
    data_format: typing.Optional[DataFormatDefinition] = None
    platforms: typing.Optional[typing.List[str]] = None
    areas: typing.List[CounterAreaDefinition] = field(
        default_factory=lambda: [], metadata={"min_items": 1, "max_items": 1}
    )


@dataclass(config=PydanticConfig)
class BaseCounter4ParserDefinition(BaseCounterParserDefinition):
    group: str = "counter4"


@dataclass(config=PydanticConfig)
class BaseCounter5ParserDefinition(BaseCounterParserDefinition):
    group: str = "counter5"


def gen_parser(
    base: typing.Union[typing.Type[BaseCounter4Parser], typing.Type[BaseCounter5Parser]],
    definition: BaseCounterParserDefinition,
) -> typing.Type[BaseParser]:
    class Parser(base):
        data_format: DataFormatDefinition = (
            definition.data_format if definition.data_format else base.data_format
        )
        name = f"dynamic.{definition.group}.{data_format.name}.{definition.parser_name}"
        platforms = definition.platforms or base.platforms
        heuristics = definition.heuristics or base.heuristics

        areas = [definition.areas[0].make_area()]

    return Parser


@dataclass(config=PydanticConfig)
class BR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.BR1"] = "counter4.BR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(BR1, self)


@dataclass(config=PydanticConfig)
class BR2Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.BR2"] = "counter4.BR2"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(BR2, self)


@dataclass(config=PydanticConfig)
class BR3Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.BR3"] = "counter4.BR3"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(BR3, self)


@dataclass(config=PydanticConfig)
class DB1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.DB1"] = "counter4.DB1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(DB1, self)


@dataclass(config=PydanticConfig)
class DB2Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.DB2"] = "counter4.DB2"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(DB2, self)


@dataclass(config=PydanticConfig)
class JR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.JR1"] = "counter4.JR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(JR1, self)


@dataclass(config=PydanticConfig)
class JR1aDefinition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.JR1a"] = "counter4.JR1a"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(JR1a, self)


@dataclass(config=PydanticConfig)
class JR1GOADefinition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.JR1GOA"] = "counter4.JR1GOA"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(JR1GOA, self)


@dataclass(config=PydanticConfig)
class JR2Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.JR2"] = "counter4.JR2"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(JR2, self)


@dataclass(config=PydanticConfig)
class MR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.MR1"] = "counter4.MR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(MR1, self)


@dataclass(config=PydanticConfig)
class PR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    kind: typing.Literal["counter4.PR1"] = "counter4.PR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(PR1, self)


@dataclass(config=PydanticConfig)
class TRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    kind: typing.Literal["counter5.TR"] = "counter5.TR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(TR, self)


@dataclass(config=PydanticConfig)
class DRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    kind: typing.Literal["counter5.DR"] = "counter5.DR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(DR, self)


@dataclass(config=PydanticConfig)
class PRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    kind: typing.Literal["counter5.PR"] = "counter5.PR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(PR, self)
