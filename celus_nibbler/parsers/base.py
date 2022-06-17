import datetime
import itertools
import logging
import typing
from abc import ABCMeta, abstractmethod

from pydantic import BaseModel, ValidationError

import celus_nibbler
from celus_nibbler import validators
from celus_nibbler.conditions import BaseCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction, SheetAttr, Value
from celus_nibbler.errors import TableException
from celus_nibbler.reader import SheetReader
from celus_nibbler.record import CounterRecord
from celus_nibbler.utils import end_month, start_month

logger = logging.getLogger(__name__)


IDS = [
    ("DOI", validators.DOI),
    ("ISBN", validators.ISBN),
    ("Print_ISSN", validators.ISSN),
    ("Online_ISSN", validators.EISSN),
    ("Proprietary", validators.ProprietaryID),
]


class MonthDataCells:
    def __init__(self, month: datetime.date, range: CoordRange):
        self.range = range
        self.month = month

    def __iter__(self):
        return self.range.__iter__()

    def __next__(self):
        return self.range.__next__()

    def __getitem__(self, item: int) -> Coord:
        return self.range.__getitem__(item)

    def __str__(self):
        return f"{self.month.strftime('%Y-%m')} - {self.range}"

    def __repr__(self):
        return str(self)


class BaseArea(metaclass=ABCMeta):

    title_cells: typing.Optional[CoordRange] = None
    title_ids_cells: typing.Dict[str, CoordRange] = {}
    metric_cells: typing.Optional[CoordRange] = None
    dimensions_cells: typing.Dict[str, 'celus_nibbler.definitions.Source'] = {}

    def __init__(self, sheet: SheetReader, platform: str):
        self.sheet = sheet
        self.platform = platform
        self.setup()

    @property
    @abstractmethod
    def date_header_cells(self) -> CoordRange:
        pass

    def prepare_record(
        self,
        value: int,
        date: datetime.date,
        title: typing.Optional[str] = None,
        metric: typing.Optional[str] = None,
        title_ids: typing.Dict[str, str] = {},
        dimension_data: typing.Dict[str, str] = {},
    ) -> CounterRecord:
        start = start_month(date)
        end = end_month(date)
        # We need to fill at least the vaue
        return CounterRecord(
            value=value,
            start=start,
            end=end,
            title=title,
            metric=metric,
            title_ids=title_ids,
            dimension_data=dimension_data,
        )

    def parse_date(self, cell: Coord) -> datetime.date:
        content = cell.content(self.sheet)
        return validators.Date(value=content).value

    @abstractmethod
    def find_data_cells(self) -> typing.List[MonthDataCells]:
        pass

    def get_months(self) -> typing.List[datetime.date]:
        return [e.month for e in self.find_data_cells()]

    def setup(self):
        """To be overiden. It should serve to read fixed variables from the list"""
        pass


class BaseParser(metaclass=ABCMeta):
    metrics_to_skip: typing.List[str] = ["Total"]
    titles_to_skip: typing.List[str] = ["Total"]
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {"Platform": ["Total"]}
    dimensions_validators: typing.Dict[str, BaseModel] = {
        "Platform": validators.Platform,
    }
    heuristics: typing.Optional[BaseCondition] = None
    metric_aliases: typing.List[typing.Tuple[str, str]] = []
    dimension_aliases: typing.List[typing.Tuple[str, str]] = []

    @property
    @abstractmethod
    def format_name(self) -> str:
        pass

    @property
    @abstractmethod
    def areas(self) -> typing.List[typing.Type[BaseArea]]:
        """Areas defined"""
        pass

    def get_areas(self) -> typing.List[BaseArea]:
        """List of all data source areas withing the sheet"""
        return [area_class(self.sheet, self.platform) for area_class in self.areas]

    def __init__(self, sheet: SheetReader, platform: str):
        self.sheet = sheet
        self.platform = platform

    def heuristic_check(self) -> bool:
        if self.heuristics:
            return self.heuristics.check(self.sheet)
        return True

    @property
    @abstractmethod
    def platforms(self) -> typing.List[str]:
        """List of available platforms (used for validation)"""
        pass

    def _parse_content(
        self,
        seq: typing.Union[typing.Sequence[Coord], CoordRange, MonthDataCells, Value, SheetAttr],
        idx: int,
        validator: typing.Type[BaseModel],
    ) -> typing.Any:
        try:
            cell = seq[idx]
            content = cell.content(self.sheet)
            res = validator(value=content).value
        except ValidationError as e:
            if isinstance(seq, Value):
                raise TableException(
                    value=seq.value, sheet=self.sheet.sheet_idx, reason="wrong-value"
                )
            elif isinstance(seq, SheetAttr):
                raise TableException(
                    value=seq.sheet_attr, sheet=self.sheet.sheet_idx, reason="wrong-sheet-attr"
                )
            else:
                raise TableException(
                    content,
                    row=cell.row,
                    col=cell.col,
                    sheet=self.sheet.sheet_idx,
                    reason=e.model.__name__.lower() if content else "empty",
                ) from e
        except IndexError as e:
            raise TableException(
                row=cell.row,
                col=cell.col,
                sheet=self.sheet.sheet_idx,
                reason='out-of-bounds',
            ) from e

        return res

    def _parse(self) -> typing.Generator[CounterRecord, None, None]:
        for area in self.get_areas():
            yield from self.parse_area(area)

    def parse(self) -> typing.Generator[CounterRecord, None, None]:
        dimension_aliases = dict(self.dimension_aliases)
        metric_aliases = dict(self.metric_aliases)
        for record in self._parse():
            record.metric = (
                metric_aliases.get(record.metric, record.metric) if record.metric else record.metric
            )
            record.dimension_data = {
                dimension_aliases.get(key, key): value
                for key, value in record.dimension_data.items()
            }
            yield record

    def get_months(self) -> typing.List[typing.List[datetime.date]]:
        return [e.get_months() for e in self.get_areas()]

    def parse_area(self, area: BaseArea) -> typing.Generator[CounterRecord, None, None]:
        data_cells = area.find_data_cells()
        if not data_cells:
            date_cells = area.date_header_cells
            raise TableException(
                row=date_cells.coord.row,
                col=date_cells.coord.col,
                sheet=self.sheet.sheet_idx,
                reason="no-data-found",
            )

        try:
            for idx in itertools.count(0):
                # iterates through ranges
                if area.metric_cells:
                    metric = self._parse_content(area.metric_cells, idx, validators.Metric)
                    if metric in self.metrics_to_skip:
                        continue
                else:
                    metric = None

                if area.title_cells:
                    title = self._parse_content(area.title_cells, idx, validators.Title)
                    if title in self.titles_to_skip:
                        continue
                else:
                    title = None

                dimension_data = {}
                for k, rng in area.dimensions_cells.items():
                    dimension_data[k] = self._parse_content(
                        rng,
                        idx,
                        self.dimensions_validators.get(k, validators.Dimension),
                    )
                    if dimension_data[k] in self.dimensions_to_skip.get(k, []):
                        continue

                title_ids = {}
                for (key, validator) in IDS:
                    if rng := area.title_ids_cells.get(key):
                        value = self._parse_content(rng, idx, validator)
                        if value:
                            title_ids[key] = value
                        else:
                            title_ids[key] = ""

                for data in data_cells:
                    value = self._parse_content(data, idx, validators.Value)
                    res = area.prepare_record(
                        value=round(value),
                        date=data.month,
                        metric=metric,
                        title=title,
                        dimension_data=dimension_data,
                        title_ids=title_ids,
                    )
                    logger.debug("Parsed %s", res.serialize())
                    yield res

        except TableException as e:
            if e.reason in ["out-of-bounds", "empty"]:
                # end was reached
                pass
            else:
                raise


class VerticalArea(BaseArea):
    def find_data_cells(self) -> typing.List[MonthDataCells]:
        res = []
        for cell in self.date_header_cells:
            try:
                date = self.parse_date(cell)
                res.append(
                    MonthDataCells(
                        date.replace(day=1),
                        CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN),
                    )
                )

            except TableException as e:
                if e.reason == "out-of-bounds":
                    # We reached the end of row
                    break
                raise
            except ValidationError:
                # Found a content which can't be parse e.g. "Total"
                # We can exit here
                break

        logger.debug(f"Found data cells: {res}")
        return res
