import logging
import typing
from abc import ABCMeta, abstractmethod
from dataclasses import asdict, dataclass, fields

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


logger = logging.getLogger(__name__)


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
    Field(discriminator='role'),
]


DataHeaderNegCondition.__pydantic_model__.update_forward_refs()
DataHeaderAndCondition.__pydantic_model__.update_forward_refs()
DataHeaderOrCondition.__pydantic_model__.update_forward_refs()


@pydantic_dataclass(config=PydanticConfig)
class DataHeaders(JsonEncorder):
    roles: typing.List[Role]

    data_cells: CoordRange  # first data after the header
    data_direction: Direction  # perpendicular to data_cells
    data_extract_params: ExtractParams = ExtractParams()

    skip_condition: typing.Optional[DataHeaderCondition] = None
    stop_condition: typing.Optional[DataHeaderCondition] = None

    def find_data_cells(self, sheet: SheetReader) -> typing.List['DataCells']:
        res = []

        try:
            for idx, cell in enumerate(self.data_cells):
                record = CounterRecord(value=0)  # dummy value - will be removed later
                skip = False
                stop = False
                for role in self.roles:
                    value = role.extract(sheet, idx)

                    #  skip or stop conditions
                    if cond := self.skip_condition:
                        if cond.check(value, idx):
                            skip = True
                            break
                    if cond := self.stop_condition:
                        if cond.check(value, idx):
                            stop = True
                            break

                    if isinstance(role, DimensionSource):
                        record.dimension_data[role.name] = value
                    elif isinstance(role, TitleIdSource):
                        record.title_ids[role.name] = value
                    elif isinstance(role, DateSource):
                        record.start = start_month(value)
                        record.end = end_month(value)
                    else:
                        setattr(record, role.role, value)

                if skip:
                    continue
                if stop:
                    break

                res.append(
                    DataCells(
                        record,
                        ValueSource(
                            source=CoordRange(cell, self.data_direction),
                            extract_params=self.data_extract_params,
                        ),
                    )
                )
        except TableException as e:
            # Stop processing when an exception occurs
            # (index out of bounds or unable to parse next field)
            logger.debug("Header parsing terminated: %s", e)

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
        for field_name in [
            f
            for f in COUNTER_RECORD_FIELD_NAMES
            if f not in ("value", "title_ids", "dimension_data")
        ]:
            if new_value := getattr(self.header_data, field_name):
                setattr(record, field_name, new_value)

        # Update nested fields
        for nested_name in ["dimension_data", "title_ids"]:
            if header_nested_data := getattr(self.header_data, nested_name):
                if record_nested_data := getattr(record, nested_name):
                    record_nested_data.update(header_nested_data)
                else:
                    # Clone header data
                    record[nested_name] = {k: v for k, v in header_nested_data.items()}

        return record


@pydantic_dataclass(config=PydanticConfig)
class DataFormatDefinition(JsonEncorder):
    name: str
    id: typing.Optional[int] = None
