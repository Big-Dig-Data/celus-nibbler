import logging
import typing
from dataclasses import asdict, dataclass

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


logger = logging.getLogger(__name__)


@pydantic_dataclass(config=PydanticConfig)
class DataHeaders(JsonEncorder):
    roles: typing.List[Role]
    data_cells: CoordRange  # first data after the header
    data_direction: Direction  # perpendicular to data_cells
    data_default: typing.Optional[int] = None

    def find_data_cells(self, sheet: SheetReader) -> typing.List['DataCells']:
        res = []

        try:
            for idx, cell in enumerate(self.data_cells):
                record = CounterRecord(value=0)  # dummy value - will be removed later
                for role in self.roles:
                    value = role.extract(sheet, idx)
                    if isinstance(role, DimensionSource):
                        record.dimension_data[role.name] = value
                    elif isinstance(role, TitleIdSource):
                        record.title_ids[role.name] = value
                    elif isinstance(role, DateSource):
                        record.start = start_month(value)
                        record.end = end_month(value)
                    else:
                        setattr(record, role.role, value)
                res.append(
                    DataCells(
                        record,
                        ValueSource(
                            source=CoordRange(cell, self.data_direction),
                            default=self.data_default,
                        ),
                    )
                )
        except TableException as e:
            # Stop processing when an exception occurs
            # (index out of bounds or unable to parse next field)
            logger.debug("Header parsing terminated: %s", e)
            pass

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
        # Prepare header data non-empty
        header_data = asdict(self.header_data)
        header_data.pop("value")  # value can't be set from header
        title_ids = {k: v for k, v in header_data.pop("title_ids") if v}
        dimension_data = {k: v for k, v in header_data.pop("dimension_data") if v}
        header_data = {k: v for k, v in header_data.items() if v}

        # update input data with non-empty header data
        record_dict = asdict(record)
        record_dict.update(header_data)

        # update nested
        if orig_dimension_data := record_dict.get('dimension_data'):
            orig_dimension_data.update(dimension_data)
        else:
            record_dict["dimension_data"] = dimension_data
        if orig_title_ids := record_dict.get('title_ids'):
            orig_title_ids.update(title_ids)
        else:
            record_dict["title_ids"] = title_ids

        return CounterRecord(**record_dict)


@pydantic_dataclass(config=PydanticConfig)
class DataFormatDefinition(JsonEncorder):
    name: str
    id: typing.Optional[int] = None
