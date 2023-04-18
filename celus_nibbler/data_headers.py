import itertools
import logging
import typing
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from dataclasses import asdict, dataclass, field, fields
from enum import Enum

from celus_nigiri import CounterRecord
from pydantic import Field
from pydantic.dataclasses import dataclass as pydantic_dataclass
from typing_extensions import Annotated

from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.reader import SheetReader
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    ExtractParams,
    MetricSource,
    OrganizationSource,
    TitleIdSource,
    TitleSource,
    ValueSource,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig, end_month, start_month

Role = Annotated[
    typing.Union[
        DimensionSource,
        MetricSource,
        OrganizationSource,
        TitleSource,
        TitleIdSource,
        DateSource,
    ],
    Field(discriminator='role'),
]


COUNTER_RECORD_FIELD_NAMES = [f.name for f in fields(CounterRecord)]
COUNTER_RECORD_FIELD_NAMES_SIMPLE = [
    f for f in COUNTER_RECORD_FIELD_NAMES if f not in ("value", "title_ids", "dimension_data")
]
COUNTER_RECORD_FIELD_NAMES_NESTED = ["title_ids", "dimension_data"]


logger = logging.getLogger(__name__)


# Makes sure that wrong definition doesn't create
# an infinite loop
MAX_DATA_CELLS = 1000


class DataHeaderBaseCondition(metaclass=ABCMeta):
    @abstractmethod
    def check(self, value: str, idx: int):
        pass


class ArithmeticsMixin:
    def __invert__(self):
        return DataHeaderNegCondition(self)

    def __or__(self, other):
        return DataHeaderOrCondition([self, other])

    def __and__(self, other):
        return DataHeaderAndCondition([self, other])


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderNegCondition(ArithmeticsMixin, DataHeaderBaseCondition, JsonEncorder):
    cond: 'DataHeaderCondition'

    kind: typing.Literal["neg"] = "neg"

    def check(self, *args, **kwargs):
        return not self.cond.check(*args, **kwargs)


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderAndCondition(ArithmeticsMixin, DataHeaderBaseCondition, JsonEncorder):
    conds: typing.List['DataHeaderCondition']

    kind: typing.Literal["and"] = "and"

    def check(self, *args, **kwargs):
        return all(e.check(*args, **kwargs) for e in self.conds)


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderOrCondition(ArithmeticsMixin, DataHeaderBaseCondition, JsonEncorder):
    conds: typing.List['DataHeaderCondition']

    kind: typing.Literal["or"] = "or"

    def check(self, *args, **kwargs):
        return any(e.check(*args, **kwargs) for e in self.conds)


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderRegexCondition(JsonEncorder):
    pattern: typing.Pattern

    kind: typing.Literal["regex"] = "regex"

    def check(self, value: typing.Any, idx: int) -> bool:
        if isinstance(value, str):
            return bool(self.pattern.match(value))

        return False


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderIndexCondition(JsonEncorder):
    max: typing.Optional[int]
    min: typing.Optional[int]

    kind: typing.Literal["index"] = "index"

    def check(self, value: typing.Any, idx: int) -> bool:
        max = self.max if self.max is not None else idx
        min = self.min if self.min is not None else 0
        return min <= idx <= max


DataHeaderCondition = Annotated[
    typing.Union[
        DataHeaderAndCondition,
        DataHeaderNegCondition,
        DataHeaderOrCondition,
        DataHeaderRegexCondition,
        DataHeaderIndexCondition,
    ],
    Field(discriminator='kind'),
]


DataHeaderNegCondition.__pydantic_model__.update_forward_refs()
DataHeaderAndCondition.__pydantic_model__.update_forward_refs()
DataHeaderOrCondition.__pydantic_model__.update_forward_refs()


class DataHeaderAction(Enum):
    PROCEED = "proceed"
    SKIP = "skip"
    STOP = "stop"

    def merge(self, dha: 'DataHeaderAction') -> 'DataHeaderAction':
        if self == DataHeaderAction.PROCEED:
            return dha
        elif self == DataHeaderAction.SKIP:
            if dha != DataHeaderAction.PROCEED:
                return dha

        return self


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderRule(JsonEncorder):
    condition: typing.Optional[DataHeaderCondition] = None
    on_condition_failed: DataHeaderAction = DataHeaderAction.STOP
    on_condition_passed: DataHeaderAction = DataHeaderAction.PROCEED
    on_error: DataHeaderAction = DataHeaderAction.STOP
    role_idx: typing.Optional[int] = None
    role_extract_params_override: typing.Optional[ExtractParams] = None
    role_source_offset: int = 0

    def process(
        self, sheet: SheetReader, idx: int, role: Role
    ) -> typing.Tuple[DataHeaderAction, typing.Optional[typing.Any]]:
        if role.cleanup_during_header_processing:
            role = deepcopy(role)

        if self.role_extract_params_override:
            role.extract_params = deepcopy(self.role_extract_params_override)

        try:
            value = role.extract(sheet, idx + self.role_source_offset)
            if self.condition is None or self.condition.check(value, idx):
                return self.on_condition_passed, value
            else:
                return self.on_condition_failed, None

        except TableException as e:
            if e.reason == "out-of-bounds":
                # terminate if no offset
                action = (
                    DataHeaderAction.STOP
                    if self.role_source_offset == 0
                    else DataHeaderAction.PROCEED
                )
            else:
                action = self.on_error

            logger.debug(
                "Header role %s processing rule %s failed (->%s): %s", role, self, action, e
            )
            return action, None


@pydantic_dataclass(config=PydanticConfig)
class DataHeaders(JsonEncorder):
    roles: typing.List[Role]

    data_cells: CoordRange  # first data after the header
    data_direction: Direction  # perpendicular to data_cells
    data_extract_params: ExtractParams = field(default_factory=lambda: ExtractParams())

    rules: typing.List[DataHeaderRule] = Field(default_factory=lambda: [DataHeaderRule()])

    def process_value(self, record: CounterRecord, role: Role, value: typing.Any):
        if isinstance(role, DimensionSource):
            record.dimension_data[role.name] = value
        elif isinstance(role, TitleIdSource):
            if value.strip():
                record.title_ids[role.name] = value
        elif isinstance(role, DateSource):
            record.start = start_month(value)
            record.end = end_month(value)
        else:
            setattr(record, role.role, value)

    def find_data_cells(self, sheet: SheetReader) -> typing.List['DataCells']:
        res: typing.List[DataCells] = []

        for idx, cell in itertools.takewhile(
            lambda x: x[0] < MAX_DATA_CELLS, enumerate(self.data_cells)
        ):

            record = CounterRecord(value=0)
            action = DataHeaderAction.PROCEED
            store = False
            for role_idx, role in enumerate(self.roles):

                value = None
                for rule_idx, rule in enumerate(self.rules):
                    if rule.role_idx and role_idx != rule.role_idx:
                        # Skip rules which doesn't match index
                        continue
                    cur_action, value = rule.process(sheet, idx, role)
                    action = action.merge(cur_action)

                    if action != DataHeaderAction.PROCEED or value is not None:
                        break

                if action != DataHeaderAction.PROCEED:
                    # Terminate processingof other roles
                    break

                if value:
                    self.process_value(record, role, value)
                    store = True

            if action == DataHeaderAction.SKIP:
                logger.debug("Header parsing skips: %s", cell)
                continue

            if action == DataHeaderAction.STOP:
                logger.debug("Header parsing stops: %s", cell)
                break

            if store:
                res.append(
                    DataCells(
                        record,
                        ValueSource(
                            source=CoordRange(cell, self.data_direction),
                            extract_params=self.data_extract_params,
                        ),
                    )
                )

        if not res:
            raise TableException(
                row=self.data_cells.coord.row,
                col=self.data_cells.coord.col,
                sheet=sheet.sheet_idx,
                reason="no-header-data-found",
            )

        return res


@dataclass
class DataCells:
    header_data: CounterRecord
    value_source: ValueSource

    def __iter__(self):
        return self.value_source.source.__iter__()

    def __next__(self):
        return self.value_source.source.__next__()

    def __getitem__(self, item: int) -> Coord:
        if isinstance(self.value_source.source, CoordRange):
            return self.value_source.source.__getitem__(item)
        else:
            raise TypeError()

    def __str__(self):
        records = {f"({k}={v})" for k, v in asdict(self.header_data).items() if v}
        return f"{'|'.join(records)} - {self.value_source}"

    def __repr__(self):
        return str(self)

    def merge_into_record(
        self,
        record: CounterRecord,
    ) -> CounterRecord:
        # Update the input record instead creating new one
        # to avoid unnecessary alocation

        # Update non-nested updatable fields
        for field_name in COUNTER_RECORD_FIELD_NAMES_SIMPLE:
            if new_value := getattr(self.header_data, field_name):
                setattr(record, field_name, new_value)

        # Update nested fields
        for nested_name in COUNTER_RECORD_FIELD_NAMES_NESTED:
            if header_nested_data := getattr(self.header_data, nested_name):
                if record_nested_data := getattr(record, nested_name):
                    record_nested_data.update(header_nested_data)
                else:
                    # Clone header data
                    setattr(record, nested_name, deepcopy(header_nested_data))

        return record


@pydantic_dataclass(config=PydanticConfig)
class DataFormatDefinition(JsonEncorder):
    name: str
    id: typing.Optional[int] = None
