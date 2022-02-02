import datetime
import itertools
import logging
import typing
from abc import ABCMeta, abstractmethod

from pydantic import BaseModel, ValidationError

from celus_nibbler import validators
from celus_nibbler.conditions import BaseCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.reader import SheetReader
from celus_nibbler.record import CounterRecord
from celus_nibbler.utils import end_month, start_month

logger = logging.getLogger(__name__)


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
    dimensions_cells: typing.Dict[str, CoordRange] = {}

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
            platform=self.platform,
        )

    def parse_date(self, cell: Coord) -> datetime.date:
        content = cell.content(self.sheet)
        return validators.Date(date=content).date

    @abstractmethod
    def find_data_cells(self) -> typing.List[MonthDataCells]:
        pass

    def setup(self):
        """To be overiden. It should serve to read fixed variables from the list"""
        pass


class BaseParser(metaclass=ABCMeta):
    metrics_to_skip: typing.List[str] = ["Total"]
    titles_to_skip: typing.List[str] = ["Total"]
    heuristics: typing.Optional[BaseCondition] = None

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
        seq: typing.Union[typing.Sequence[Coord], CoordRange, MonthDataCells],
        idx: int,
        validator: typing.Type[BaseModel],
        name: str,
    ) -> typing.Any:
        try:
            cell = seq[idx]
            content = cell.content(self.sheet)
            validated = validator(**{name: content})
        except ValidationError as e:
            raise TableException(
                content,
                row=cell.row,
                col=cell.col,
                sheet=self.sheet.sheet_idx,
                reason=name if content else "empty",
            ) from e
        except IndexError as e:
            raise TableException(
                row=cell.row,
                col=cell.col,
                sheet=self.sheet.sheet_idx,
                reason='out-of-bounds',
            ) from e

        return getattr(validated, name)

    def parse(self) -> typing.Generator[CounterRecord, None, None]:
        for area in self.get_areas():
            yield from self.parse_area(area)

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
                    metric = self._parse_content(
                        area.metric_cells, idx, validators.Metric, "metric"
                    )
                    if metric in self.metrics_to_skip:
                        continue
                else:
                    metric = None

                if area.title_cells:
                    title = self._parse_content(area.title_cells, idx, validators.Title, "title")
                    if title in self.titles_to_skip:
                        continue
                else:
                    title = None

                dimension_data = {}
                for k, rng in area.dimensions_cells.items():
                    dimension_data[k] = self._parse_content(
                        rng, idx, validators.Dimension, "dimension"
                    )

                title_ids = {}
                for key in ["DOI", "ISBN", "ISSN"]:
                    if rng := area.title_ids_cells.get(key):
                        value = self._parse_content(rng, idx, getattr(validators, key), key.lower())
                        if value:
                            title_ids[key] = value

                for data in data_cells:
                    value = self._parse_content(data, idx, validators.Value, "value")
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
                    MonthDataCells(date, CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN))
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
