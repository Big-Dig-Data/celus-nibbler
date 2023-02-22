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
from celus_nibbler.reader import CsvSheetReader, JsonCounter5SheetReader, SheetReader
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
    "URI",
}


class BaseArea(metaclass=ABCMeta):
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

    @abstractmethod
    def get_months(self) -> typing.List[datetime.date]:
        pass

    @property
    @abstractmethod
    def dimensions(self) -> typing.List[str]:
        pass


class BaseJsonArea(BaseArea):
    pass


class BaseTabularArea(BaseArea):
    organization_source: typing.Optional[OrganizationSource] = None
    date_source: typing.Optional[DateSource] = None
    title_source: typing.Optional[TitleSource] = None
    title_ids_sources: typing.Dict[str, TitleIdSource] = {}
    dimensions_sources: typing.Dict[str, DimensionSource] = {}
    metric_source: typing.Optional[MetricSource] = None

    def parse_date(self, cell: Coord) -> datetime.date:
        content = cell.content(self.sheet)
        return validators.Date(value=content).value

    @abstractmethod
    def find_data_cells(self) -> typing.List[DataCells]:
        pass


class BaseHeaderArea(BaseTabularArea):
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
    heuristics: typing.Optional[BaseCondition] = None
    metric_aliases: typing.List[typing.Tuple[str, str]] = []
    dimension_aliases: typing.List[typing.Tuple[str, str]] = []

    @classmethod
    @abstractmethod
    def sheet_reader_classes(cls) -> typing.List[typing.Type[SheetReader]]:
        pass

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

    def parse_area(self, area) -> typing.Generator[CounterRecord, None, None]:
        return area.aggregator.aggregate(self._parse_area(area))

    @abstractmethod
    def _parse_area(self, area: BaseArea) -> typing.Generator[CounterRecord, None, None]:
        pass

    def get_months(self) -> typing.List[typing.List[datetime.date]]:
        return [e.get_months() for e in self.get_areas()]


class BaseJsonParser(BaseParser):
    @classmethod
    def sheet_reader_classes(cls):
        return [JsonCounter5SheetReader]


class BaseTabularParser(BaseParser):
    dimensions_validators: typing.Dict[str, typing.Type[BaseModel]] = {
        "Platform": validators.Platform,
    }
    END_EXCEPTIONS = ["out-of-bounds", "empty"]

    @classmethod
    def sheet_reader_classes(cls):
        return [CsvSheetReader]

    def _parse_area(self, area: BaseTabularArea) -> typing.Generator[CounterRecord, None, None]:
        data_cells = area.find_data_cells()

        # Store title_ids and dimensions sources
        # so it can be reused in the for-cycle
        dimensions_sources = list(area.dimensions_sources.items())
        title_ids_sources = area.title_ids_sources
        metrics_to_skip = [e.lower() for e in self.metrics_to_skip]
        titles_to_skip = [e.lower() for e in self.titles_to_skip]
        dimensions_to_skip = {k: [e.lower() for e in v] for k, v in self.dimensions_to_skip.items()}

        try:
            for idx in itertools.count(0):
                # iterates through ranges
                if area.title_source:
                    title = area.title_source.extract(self.sheet, idx)
                    if title is not None and title.lower() in titles_to_skip:
                        continue
                else:
                    title = None

                if area.metric_source:
                    metric = area.metric_source.extract(self.sheet, idx)
                    if metric is not None and metric.lower() in metrics_to_skip:
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

                dimension_data = {}
                for k, dimension_source in dimensions_sources:
                    dimension_text = dimension_source.extract(
                        self.sheet, idx, self.dimensions_validators.get(k)
                    )
                    if (
                        dimension_text is not None
                        and dimension_text.lower() in dimensions_to_skip.get(k, [])
                    ):
                        continue

                    dimension_data[k] = dimension_text

                title_ids = {}
                for key in IDS:
                    if title_source := title_ids_sources.get(key):
                        value = title_source.extract(self.sheet, idx)
                        if value:
                            title_ids[key] = value

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
                    logger.debug("Parsed %s", record)
                    yield record

        except TableException as e:
            if e.reason in self.END_EXCEPTIONS:
                # end was reached
                pass
            else:
                raise
