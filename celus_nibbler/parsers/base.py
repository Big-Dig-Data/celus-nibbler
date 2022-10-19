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
from celus_nibbler.coordinates import Coord
from celus_nibbler.data_headers import DataCells, DataFormatDefinition, DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.reader import SheetReader
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    OrganizationSource,
    TitleIdSource,
    TitleSource,
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
        record: CounterRecord,
    ) -> CounterRecord:
        return record

    def parse_date(self, cell: Coord) -> datetime.date:
        content = cell.content(self.sheet)
        return validators.Date(value=content).value

    @abstractmethod
    def get_months(self) -> typing.List[datetime.date]:
        pass

    @abstractmethod
    def find_data_cells(self) -> typing.List[DataCells]:
        pass


class BaseHeaderArea(BaseArea, metaclass=ABCMeta):
    @property
    @abstractmethod
    def data_headers(self) -> DataHeaders:
        pass

    def find_data_cells(self) -> typing.List[DataCells]:
        return self.data_headers.find_data_cells(self.sheet)


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
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def data_format(self) -> DataFormatDefinition:
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
                    value = data_cell.value_source.extract(self.sheet, idx)
                    record = CounterRecord(
                        value=round(value),
                        organization=organization,
                        metric=metric,
                        title=title,
                        dimension_data=dimension_data,
                        title_ids=title_ids,
                        start=start_month(date) if date else None,
                        end=end_month(date) if date else None,
                    )
                    record = data_cell.merge_into_record(record)
                    record = area.prepare_record(record)
                    logger.debug("Parsed %s", record.as_csv())
                    yield record

        except TableException as e:
            if e.reason in ["out-of-bounds", "empty"]:
                # end was reached
                pass
            else:
                raise
