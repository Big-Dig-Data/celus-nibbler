import abc
import typing
from dataclasses import field

from pydantic import Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from celus_nibbler.conditions import Condition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import TableException
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
from celus_nibbler.parsers.counter.c5 import DR, IR, IR_M1, PR, TR, BaseCounter5Parser
from celus_nibbler.sources import ExtractParams, SpecialExtraction
from celus_nibbler.utils import JsonEncorder, PydanticConfig

from .base import BaseAreaDefinition, BaseParserDefinition


@dataclass(config=PydanticConfig)
class BaseCounterAreaDefinition(BaseAreaDefinition):
    kind: str = NotImplementedError
    organization_column: typing.Optional[typing.List[str]] = None
    metric_column: typing.Optional[typing.List[str]] = None
    title_column: typing.Optional[typing.List[str]] = None
    item_column: typing.Optional[typing.List[str]] = None
    organization_extract_params: typing.Optional[ExtractParams] = None
    metric_extract_params: typing.Optional[ExtractParams] = None
    date_extract_params: typing.Optional[ExtractParams] = None
    data_extract_params: typing.Optional[ExtractParams] = None
    title_extract_params: typing.Optional[ExtractParams] = None
    title_ids_extract_params: typing.Optional[typing.Dict[str, ExtractParams]] = None
    item_extract_params: typing.Optional[ExtractParams] = None
    item_ids_extract_params: typing.Optional[typing.Dict[str, ExtractParams]] = None
    dimensions_extract_params: typing.Optional[typing.Dict[str, ExtractParams]] = None
    aggregate_same_records: bool = False

    def make_area(self):
        raise NotImplementedError()


def gen_area(
    base: typing.Type[BaseArea], definition: BaseCounterAreaDefinition
) -> typing.Type[CounterHeaderArea]:
    class Area(base):
        # Column names
        if definition.organization_column is not None:
            ORGANIZATION_COLUMN_NAMES = definition.organization_column
        if definition.metric_column is not None:
            METRIC_COLUMN_NAMES = definition.metric_column
        if definition.title_column is not None:
            TITLE_COLUMN_NAMES = definition.title_column
        if definition.item_column is not None:
            ITEM_COLUMN_NAMES = definition.item_column

        # Extra params
        if definition.organization_extract_params is not None:
            ORGANIZATION_EXTRACT_PARAMS = definition.organization_extract_params
        if definition.metric_extract_params is not None:
            METRIC_EXTRACT_PARAMS = definition.metric_extract_params
        if definition.date_extract_params is not None:
            DATE_EXTRACT_PARAMS = definition.date_extract_params
        if definition.data_extract_params is not None:
            DATA_EXTRACT_PARAMS = definition.data_extract_params
        if definition.title_extract_params is not None:
            TITLE_EXTRACT_PARAMS = definition.title_extract_params
        if definition.title_ids_extract_params is not None:
            TITLE_IDS_EXTRACT_PARAMS = definition.title_ids_extract_params
        if definition.item_extract_params is not None:
            ITEM_EXTRACT_PARAMS = definition.item_extract_params
        if definition.item_ids_extract_params is not None:
            ITEM_IDS_EXTRACT_PARAMS = definition.item_ids_extract_params
        if definition.dimensions_extract_params is not None:
            DIMENSIONS_EXTRACT_PARAMS = definition.dimensions_extract_params

        aggregator = definition.make_aggregator()

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
class IR_M1AreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter5.IR_M1"] = "counter5.IR_M1"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(IR_M1.Area, self)


@dataclass(config=PydanticConfig)
class IRAreaDefinition(BaseCounterAreaDefinition):
    kind: typing.Literal["counter5.IR"] = "counter5.IR"

    def make_area(self) -> typing.Type[CounterHeaderArea]:
        return gen_area(IR.Area, self)


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
        IR_M1AreaDefinition,
    ],
    Field(discriminator="kind"),
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
    metrics_to_skip: typing.List[str] = field(default_factory=lambda: [])
    available_metrics: typing.Optional[typing.List[str]] = None
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    titles_to_skip: typing.List[str] = field(default_factory=lambda: [])
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = field(default_factory=lambda: {})
    metric_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    metric_value_extraction_overrides: typing.Dict[str, SpecialExtraction] = field(
        default_factory=lambda: {}
    )
    dimension_aliases: typing.List[typing.Tuple[str, str]] = field(default_factory=lambda: [])
    possible_row_offsets: typing.List[int] = field(default_factory=lambda: [0])


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
        metrics_to_skip = definition.metrics_to_skip or base.metrics_to_skip
        available_metrics = definition.available_metrics or base.available_metrics
        on_metric_check_failed = definition.on_metric_check_failed or base.on_metric_check_failed
        titles_to_skip = definition.titles_to_skip or base.titles_to_skip
        dimensions_to_skip = definition.dimensions_to_skip or base.dimensions_to_skip
        metric_aliases = dict(definition.metric_aliases) or dict(base.metric_aliases)
        metric_value_extraction_overrides = dict(definition.metric_value_extraction_overrides)
        dimension_aliases = dict(definition.dimension_aliases) or dict(base.dimension_aliases)
        possible_row_offsets = definition.possible_row_offsets or base.possible_row_offsets

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


@dataclass(config=PydanticConfig)
class IR_M1Definition(JsonEncorder, BaseCounter5ParserDefinition):
    kind: typing.Literal["counter5.IR_M1"] = "counter5.IR_M1"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(IR_M1, self)


@dataclass(config=PydanticConfig)
class IRDefinition(JsonEncorder, BaseCounter5ParserDefinition):
    kind: typing.Literal["counter5.IR"] = "counter5.IR"
    data_format: typing.Optional[DataFormatDefinition] = None
    version: typing.Literal[1] = 1

    def make_parser(self):
        return gen_parser(IR, self)
