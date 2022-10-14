import datetime
import itertools
import logging
import typing
from abc import ABCMeta, abstractmethod

from celus_nigiri import CounterRecord
from pydantic import BaseModel

from celus_nibbler import validators
from celus_nibbler.aggregator import BaseAggregator, NoAggregator
from celus_nibbler.conditions import BaseCondition
from celus_nibbler.coordinates import Coord, CoordRange
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
from celus_nibbler.utils import end_month, start_month

logger = logging.getLogger(__name__)


IDS = {
    "DOI",
    "ISBN",
    "Print_ISSN",
    "Online_ISSN",
    "Proprietary",
}


class DataCells:
    record_fields: typing.List[str]
    range: CoordRange
    values: typing.List[typing.Any]

    def __iter__(self):
        return self.range.__iter__()

    def __next__(self):
        return self.range.__next__()

    def __getitem__(self, item: int) -> Coord:
        return self.range.__getitem__(item)


class MetricDataCells(DataCells):
    record_fields = ["metric"]
    values: typing.List[str]

    def __init__(self, metric: str, range: CoordRange):
        self.range = range
        self.values = [metric]

    def __str__(self):
        return f"{self.values[0]} - {self.range}"

    def __repr__(self):
        return str(self)


class MonthDataCells(DataCells):
    record_fields = ["date"]
    values: typing.List[datetime.date]

    def __init__(self, month: datetime.date, range: CoordRange):
        self.range = range
        self.values = [month]

    def __str__(self):
        return f"{self.values[0].strftime('%Y-%m')} - {self.range}"

    def __repr__(self):
        return str(self)


class MonthMetricDataCells(DataCells):
    record_fields = ["date", "metric"]
    values: typing.List[typing.Union[datetime.date, str]]

    def __init__(self, month_and_metric: typing.Tuple[datetime.date, str], range: CoordRange):
        self.range = range
        self.values = [month_and_metric[0], month_and_metric[1]]

    def __str__(self):
        return f"{self.values[0].strftime('%Y-%m')}|{self.values[1]} - {self.range}"

    def __repr__(self):
        return str(self)


class BaseArea(metaclass=ABCMeta):
    organization_source: typing.Optional[OrganizationSource] = None
    date_source: typing.Optional[DateSource] = None
    title_source: typing.Optional[TitleSource] = None
    title_ids_sources: typing.Dict[str, TitleIdSource] = {}
    dimensions_sources: typing.Dict[str, DimensionSource] = {}
    metric_source: typing.Optional[MetricSource] = None
    aggregator: BaseAggregator = NoAggregator()

    def __init__(self, sheet: SheetReader, platform: str):
        self.sheet = sheet
        self.platform = platform
        self.setup()

    def setup(self):
        """To be overiden. It should serve to read fixed variables from the list"""
        pass

    def prepare_record(
        self,
        value: int,
        date: datetime.date,
        organization: typing.Optional[str] = None,
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
            organization=organization,
            title=title,
            metric=metric,
            title_ids=title_ids,
            dimension_data=dimension_data,
        )

    def parse_date(self, cell: Coord) -> datetime.date:
        content = cell.content(self.sheet)
        return validators.Date(value=content).value

    @abstractmethod
    def get_months(self) -> typing.List[datetime.date]:
        pass

    @abstractmethod
    def find_data_cells(self) -> typing.List[DataCells]:
        pass

    @property
    @abstractmethod
    def header_cells(self) -> CoordRange:
        pass


class BaseParser(metaclass=ABCMeta):
    metrics_to_skip: typing.List[str] = ["Total"]
    titles_to_skip: typing.List[str] = ["Total"]
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {"Platform": ["Total"]}
    dimensions_validators: typing.Dict[str, typing.Type[BaseModel]] = {
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
        return area.aggregator.aggregate(self._parse_area(area))

    def _parse_area(self, area: BaseArea) -> typing.Generator[CounterRecord, None, None]:
        data_cells = area.find_data_cells()
        if not data_cells:
            header_cells = area.header_cells
            raise TableException(
                row=header_cells.coord.row,
                col=header_cells.coord.col,
                sheet=self.sheet.sheet_idx,
                reason="no-data-found",
            )

        try:
            for idx in itertools.count(0):
                # iterates through ranges
                if area.metric_source:
                    metric = area.metric_source.extract(self.sheet, idx)
                    if metric in self.metrics_to_skip:
                        continue
                else:
                    metric = None

                if area.organization_source:
                    organization = area.organization_source.extract(self.sheet, idx)
                else:
                    organization = None

                if area.date_source:
                    date = area.date_source.extract(self.sheet, idx)
                else:
                    date = None

                if area.title_source:
                    title = area.title_source.extract(self.sheet, idx)
                    if title in self.titles_to_skip:
                        continue
                else:
                    title = None

                dimension_data = {}
                for k, dimension_source in area.dimensions_sources.items():
                    dimension_data[k] = dimension_source.extract(
                        self.sheet, idx, self.dimensions_validators.get(k)
                    )
                    if dimension_data[k] in self.dimensions_to_skip.get(k, []):
                        continue

                title_ids = {}
                for key in IDS:
                    if title_source := area.title_ids_sources.get(key):
                        value = title_source.extract(self.sheet, idx)

                        if value:
                            title_ids[key] = value
                        else:
                            title_ids[key] = ""

                for data_cell in data_cells:
                    value = ValueSource(data_cell.range).extract(self.sheet, idx)
                    kwargs = dict(
                        value=round(value),
                        organization=organization,
                        metric=metric,
                        title=title,
                        dimension_data=dimension_data,
                        title_ids=title_ids,
                        date=date,
                    )
                    for field, value in zip(data_cell.record_fields, data_cell.values):
                        kwargs[field] = value
                    res = area.prepare_record(**kwargs)
                    logger.debug("Parsed %s", res.as_csv())
                    yield res

        except TableException as e:
            if e.reason in ["out-of-bounds", "empty"]:
                # end was reached
                pass
            else:
                raise
