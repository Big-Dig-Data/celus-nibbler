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
from pydantic.dataclasses import rebuild_dataclass
from typing_extensions import Annotated

from celus_nibbler.conditions import Condition
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
    VoidSource,
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
        VoidSource,
        ValueSource,
    ],
    Field(discriminator="role"),
]


COUNTER_RECORD_FIELD_NAMES = [f.name for f in fields(CounterRecord)]
COUNTER_RECORD_FIELD_NAMES_SIMPLE = [
    f for f in COUNTER_RECORD_FIELD_NAMES if f not in ("value", "title_ids", "dimension_data")
]
COUNTER_RECORD_FIELD_NAMES_NESTED = ["title_ids", "dimension_data"]


logger = logging.getLogger(__name__)


# Make sure that wrong definition doesn't create
# an infinite loop while processing header
MAX_DATA_CELLS = 1000

# Make sure that wrong definition doesn't create
# an infinite loop while looking for header
MAX_HEADER_OFFSET_LOOKUP_COUNT = 1_000_000


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
    cond: "DataHeaderCondition"

    kind: typing.Literal["neg"] = "neg"

    def check(self, *args, **kwargs):
        return not self.cond.check(*args, **kwargs)


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderAndCondition(ArithmeticsMixin, DataHeaderBaseCondition, JsonEncorder):
    conds: typing.List["DataHeaderCondition"]

    kind: typing.Literal["and"] = "and"

    def check(self, *args, **kwargs):
        return all(e.check(*args, **kwargs) for e in self.conds)


@pydantic_dataclass(config=PydanticConfig)
class DataHeaderOrCondition(ArithmeticsMixin, DataHeaderBaseCondition, JsonEncorder):
    conds: typing.List["DataHeaderCondition"]

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
    Field(discriminator="kind"),
]


rebuild_dataclass(DataHeaderNegCondition, force=True)
rebuild_dataclass(DataHeaderAndCondition, force=True)
rebuild_dataclass(DataHeaderOrCondition, force=True)


class DataHeaderAction(Enum):
    PROCEED = "proceed"
    BYPASS = "bypass"
    SKIP = "skip"
    STOP = "stop"

    def merge(self, dha: "DataHeaderAction") -> "DataHeaderAction":
        if self == DataHeaderAction.PROCEED:
            return dha
        elif self == DataHeaderAction.BYPASS:
            return dha if dha != DataHeaderAction.PROCEED else self
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
        self, sheet: SheetReader, idx: int, role: Role, row_offset: typing.Optional[int]
    ) -> typing.Tuple[DataHeaderAction, typing.Optional[typing.Any]]:
        if role.cleanup_during_header_processing:
            role = deepcopy(role)

        if self.role_extract_params_override:
            role.extract_params = deepcopy(self.role_extract_params_override)

        try:
            value = role.extract(sheet, idx + self.role_source_offset, row_offset=row_offset)
            if self.condition is None or self.condition.check(value, idx):
                return self.on_condition_passed, value
            else:
                return self.on_condition_failed, None

        except TableException as e:
            if e.action == TableException.Action.STOP:
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
    data_extract_params: ExtractParams = field(
        default_factory=lambda: ExtractParams(on_validation_error=TableException.Action.STOP)
    )
    data_allow_negative: bool = False

    rules: typing.List[DataHeaderRule] = Field(default_factory=lambda: [DataHeaderRule()])
    condition: typing.Optional[Condition] = None

    def process_value(
        self,
        record: CounterRecord,
        role: Role,
        value: typing.Any,
        get_metric_name: typing.Callable[[str], str],
        check_metric_name: typing.Callable[[str, str], None],
    ) -> DataHeaderAction:
        if isinstance(role, DimensionSource):
            record.dimension_data[role.name] = value
        elif isinstance(role, TitleIdSource):
            if value.strip():
                record.title_ids[role.name] = value
        elif isinstance(role, DateSource):
            record.start = start_month(value)
            record.end = end_month(value)
        elif isinstance(role, MetricSource):
            metric_name = get_metric_name(value)
            try:
                check_metric_name(metric_name, value)
            except TableException as e:
                if e.action == TableException.Action.SKIP:
                    return DataHeaderAction.SKIP
                elif e.action == TableException.Action.STOP:
                    return DataHeaderAction.STOP
                raise
            record.metric = metric_name
        elif isinstance(role, VoidSource):
            pass
        else:
            setattr(record, role.role, value)
        return DataHeaderAction.PROCEED

    def prepare_row_offset(
        self, sheet: SheetReader, initial_row_offset: typing.Optional[int]
    ) -> int:
        if not self.condition:
            # No condition specified -> use initial
            return initial_row_offset or 0

        # Iterate until condition matches or an exception is raised
        for offset in range(
            initial_row_offset, MAX_HEADER_OFFSET_LOOKUP_COUNT + initial_row_offset
        ):
            try:
                if self.condition.check(sheet, offset):
                    return offset

            except TableException as e:
                raise TableException(
                    row=None, col=None, sheet=e.sheet, reason="failed-to-detect-data-header"
                ) from e

    def find_data_cells(
        self,
        sheet: SheetReader,
        row_offset: typing.Optional[int],
        get_metric_name: typing.Callable[[str], str],
        check_metric_name: typing.Callable[[str, str], None],
    ) -> typing.Tuple[int, typing.List["DataCells"]]:
        res: typing.List[DataCells] = []

        # Derive row offset
        absolute_row_offset = self.prepare_row_offset(sheet, row_offset)

        # When role with ValueSource is present make it value_source
        # for all other roles
        root_value_source = None
        for role in self.roles:
            if isinstance(role, ValueSource):
                res.append(
                    DataCells(
                        header_data=CounterRecord(value=0),
                        value_source=role,
                    )
                )
                break

        for idx, cell in itertools.takewhile(
            lambda x: x[0] < MAX_DATA_CELLS, enumerate(self.data_cells)
        ):
            if absolute_row_offset:
                cell = cell.with_row_offset(absolute_row_offset)

            record = CounterRecord(value=0)
            action = DataHeaderAction.PROCEED
            store = False

            value_source = ValueSource(
                source=CoordRange(cell, self.data_direction),
                extract_params=self.data_extract_params,
                allow_negative=self.data_allow_negative,
            )

            # Process data from header
            for role_idx, role in enumerate(self.roles):
                if isinstance(role, ValueSource):
                    continue

                for rule_idx, rule in enumerate(self.rules):
                    if rule.role_idx and role_idx != rule.role_idx:
                        # Skip rules which doesn't match index
                        continue
                    cur_action, value = rule.process(sheet, idx, role, absolute_row_offset)
                    action = action.merge(cur_action)

                    if action != DataHeaderAction.PROCEED or value is not None:
                        break

                if action not in [DataHeaderAction.PROCEED, DataHeaderAction.BYPASS]:
                    # Terminate processing of other roles
                    break

                # merge with action which occured during processing
                if value and action == DataHeaderAction.PROCEED:
                    action = action.merge(
                        self.process_value(record, role, value, get_metric_name, check_metric_name)
                    )

                if action not in [DataHeaderAction.PROCEED, DataHeaderAction.BYPASS]:
                    # Terminate processing of other roles
                    break

                if value and action == DataHeaderAction.PROCEED:
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
                        root_value_source or value_source,
                    )
                )

        if not res and len(self.roles) > 0:
            raise TableException(
                row=self.data_cells.coord.row,
                col=self.data_cells.coord.col,
                sheet=sheet.sheet_idx,
                reason="no-header-data-found",
            )

        return absolute_row_offset - (row_offset or 0), res


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
