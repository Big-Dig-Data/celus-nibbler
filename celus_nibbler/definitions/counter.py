import abc
import typing
from dataclasses import field

from pydantic import Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.parsers.base import BaseParser
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
    JR1a,
)
from celus_nibbler.parsers.counter.c5 import DR, PR, TR
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseParserDefinition


@dataclass(config=PydanticConfig)
class HeuristicsOverride(JsonEncorder):
    value: typing.Optional[Condition]
    name: typing.Literal["heuristics"] = "heuristics"


@dataclass(config=PydanticConfig)
class PlatformsOverride(JsonEncorder):
    value: typing.List[str]
    name: typing.Literal["platforms"] = "platforms"


Override = Annotated[
    typing.Union[
        HeuristicsOverride,
        PlatformsOverride,
    ],
    Field(discriminator='name'),
]


@dataclass(config=PydanticConfig)
class BaseCounterParserDefinition(BaseParserDefinition, metaclass=abc.ABCMeta):
    parser_name: str
    kind: str
    group: str
    data_format: typing.Optional[DataFormatDefinition] = None


@dataclass(config=PydanticConfig)
class BaseCounter4ParserDefinition(BaseCounterParserDefinition):
    group: str = "counter4"


@dataclass(config=PydanticConfig)
class BaseCounter5ParserDefinition(BaseCounterParserDefinition):
    group: str = "counter5"


def gen_parser(base: typing.Type[BaseParser], definition: BaseParserDefinition) -> BaseParser:
    class Area(base.Area):
        if definition.organization_column is not None:
            ORGANIZATION_COLUMN_NAMES = definition.organization_column
        if definition.metric_column is not None:
            METRIC_COLUMN_NAMES = definition.metric_column
        if definition.title_column is not None:
            TITLE_COLUMN_NAMES = definition.title_column

    class Parser(base):
        data_format = definition.data_format or base.data_format
        name = f"dynamic.{definition.group}.{data_format.name}.{definition.parser_name}"

        for override in definition.overrides:
            locals()[override.name] = override.value

        areas = [Area]

    return Parser


@dataclass(config=PydanticConfig)
class BR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.BR1"] = "counter4.BR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(BR1, self)


@dataclass(config=PydanticConfig)
class BR2Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.BR2"] = "counter4.BR2"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(BR2, self)


@dataclass(config=PydanticConfig)
class BR3Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.BR3"] = "counter4.BR3"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(BR3, self)


@dataclass(config=PydanticConfig)
class DB1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.DB1"] = "counter4.DB1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(DB1, self)


@dataclass(config=PydanticConfig)
class DB2Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.DB2"] = "counter4.DB2"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(DB2, self)


@dataclass(config=PydanticConfig)
class JR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.JR1"] = "counter4.JR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(JR1, self)


@dataclass(config=PydanticConfig)
class JR1aDefinition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.JR1a"] = "counter4.JR1a"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(JR1a, self)


@dataclass(config=PydanticConfig)
class JR1GOADefinition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.JR1GOA"] = "counter4.JR1GOA"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(JR1GOA, self)


@dataclass(config=PydanticConfig)
class JR2Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.JR2"] = "counter4.JR2"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(JR2, self)


@dataclass(config=PydanticConfig)
class MR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.MR1"] = "counter4.MR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(MR1, self)


@dataclass(config=PydanticConfig)
class PR1Definition(JsonEncorder, BaseCounter4ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter4.PR1"] = "counter4.PR1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(PR1, self)


@dataclass(config=PydanticConfig)
class TRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter5.TR"] = "counter5.TR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(TR, self)


@dataclass(config=PydanticConfig)
class DRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter5.DR"] = "counter5.DR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(DR, self)


@dataclass(config=PydanticConfig)
class PRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    parser_name: str
    kind: typing.Literal["counter5.PR"] = "counter5.PR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1
    overrides: typing.List[Override] = field(default_factory=lambda: [])
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None

    def make_parser(self):
        return gen_parser(PR, self)
