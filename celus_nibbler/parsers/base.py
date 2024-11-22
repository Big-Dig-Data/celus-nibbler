import datetime
import itertools
import logging
import typing
from abc import ABCMeta, abstractmethod

from celus_nigiri import CounterRecord

from celus_nibbler import validators
from celus_nibbler.aggregator import BaseAggregator, NoAggregator
from celus_nibbler.conditions import BaseCondition
from celus_nibbler.data_headers import DataCells, DataFormatDefinition, DataHeaders
from celus_nibbler.errors import MissingDateInOutput, TableException
from celus_nibbler.reader import CsvSheetReader, JsonCounter5SheetReader, SheetReader
from celus_nibbler.sources import (
    AuthorsSource,
    DateSource,
    DimensionSource,
    ItemIdSource,
    ItemSource,
    MetricSource,
    OrganizationSource,
    PublicationDateSource,
    SpecialExtraction,
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

    def __init__(self, sheet: SheetReader, platform: str, **extras):
        self.sheet = sheet
        self.platform = platform
        self.extras = extras

    def prepare_record(
        self,
        record: CounterRecord,
    ) -> CounterRecord:
        return record

    def check_record(
        self,
        idx: int,
        record: CounterRecord,
    ):
        if record.start is None:
            raise MissingDateInOutput(idx, record)

    @abstractmethod
    def get_months(self) -> typing.List[datetime.date]:
        pass

    @property
    @abstractmethod
    def dimensions(self) -> typing.List[str]:
        pass

    @classmethod
    def make_areas(cls, sheet, platform: str, **extras) -> typing.List["BaseArea"]:
        return [cls(sheet, platform, **extras)]


class BaseJsonArea(BaseArea):
    pass


class BaseTabularArea(BaseArea):
    organization_source: typing.Optional[OrganizationSource] = None
    date_source: typing.Optional[DateSource] = None
    title_source: typing.Optional[TitleSource] = None
    title_ids_sources: typing.Dict[str, TitleIdSource] = {}
    item_source: typing.Optional[ItemSource] = None
    item_ids_sources: typing.Dict[str, ItemIdSource] = {}
    item_publication_date_source: typing.Optional[PublicationDateSource] = None
    item_authors_source: typing.Optional[AuthorsSource] = None
    dimensions_sources: typing.Dict[str, DimensionSource] = {}
    metric_source: typing.Optional[MetricSource] = None
    row_offset: int = 0
    max_areas_generated: typing.Optional[int] = 1
    min_valid_areas: int = 1

    def __init__(self, sheet: SheetReader, platform: str, initial_row_offset: int = 0, **extras):
        super().__init__(sheet, platform)
        # The offset of area is initially set to parser's offset
        self.row_offset = initial_row_offset

    @abstractmethod
    def find_data_cells(
        self,
        get_metric_name: typing.Callable[[str], str],
        check_metric_name: typing.Callable[[str, str], None],
    ) -> typing.List[DataCells]:
        pass

    @classmethod
    def make_areas(
        cls, sheet, platform: str, initial_row_offset: int = 0, **extras
    ) -> typing.List["BaseArea"]:
        logger.debug("Preparing areas for %s", cls)
        areas: typing.List["BaseTabularArea"] = []
        row_offset = initial_row_offset
        while not cls.max_areas_generated or len(areas) < cls.max_areas_generated:
            area = cls(sheet, platform, initial_row_offset=row_offset)
            try:
                # Try to detect the header
                area.find_data_cells(lambda x: x, lambda x, y: None)
            except TableException as e:
                # expect at least min_valid_areas => gracefully stop
                if e.reason == "no-header-data-found" and cls.min_valid_areas <= len(areas):
                    break
                # Otherwise raise and error
                raise

            logger.debug("Area with offset %d was prepared for %s", row_offset, cls)
            areas.append(area)

            # Recalculate offset
            row_offset = area.row_offset + 1

        return areas


class BaseHeaderArea(BaseTabularArea):
    @property
    @abstractmethod
    def data_headers(self) -> DataHeaders:
        pass

    def find_data_cells(
        self,
        get_metric_name: typing.Callable[[str], str],
        check_metric_name: typing.Callable[[str, str], None],
    ) -> typing.List[DataCells]:
        offset, data_cells = self.data_headers.detect_data_cells(
            self.sheet, self.row_offset, get_metric_name, check_metric_name
        )
        self.row_offset = offset  # Update detected offset
        return data_cells

    def _get_months_from_column(
        self,
        parser_row_offset: typing.Optional[int],
        area_row_offset: typing.Optional[int],
    ) -> typing.List[datetime.date]:
        """This will get all months from months column"""

        res = set()
        for idx in itertools.count(0):
            try:
                date = self.date_source.extract(
                    self.sheet,
                    idx,
                    parser_row_offset=parser_row_offset,
                    area_row_offset=area_row_offset,
                )
                res.add(date.replace(day=1))

            except TableException as e:
                if e.action == TableException.Action.SKIP:
                    continue
                elif e.action == TableException.Action.STOP:
                    break
                else:
                    raise

        return list(res)

    def _get_months_from_header(self) -> typing.List[datetime.date]:
        return [e.header_data.start for e in self.find_data_cells(lambda x: x, lambda x: None)]


class BaseParser(metaclass=ABCMeta):
    metrics_to_skip: typing.List[str] = ["Total"]
    available_metrics: typing.Optional[typing.List[str]] = None
    on_metric_check_failed: TableException.Action = TableException.Action.SKIP
    titles_to_skip: typing.List[str] = ["Total"]
    items_to_skip: typing.List[str] = ["Total"]
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {"Platform": ["Total"]}
    heuristics: typing.Optional[BaseCondition] = None
    metric_aliases: typing.Dict[str, str] = {}
    metric_value_extraction_overrides: typing.Dict[str, SpecialExtraction] = {}
    dimension_aliases: typing.Dict[str, str] = {}
    possible_row_offsets: typing.List[int] = [0]
    row_offset: int = 0

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
        return list(
            itertools.chain(
                *(area_class.make_areas(self.sheet, self.platform) for area_class in self.areas)
            )
        )

    def __init__(self, sheet: SheetReader, platform: str):
        self.sheet = sheet
        self.platform = platform

    def heuristic_check(self) -> bool:
        if self.heuristics:
            for row_offset in self.possible_row_offsets:
                # Default Coord.row_relative_to is set to "area"
                # and we don't want to override coords in heuristic definition
                # to Coord.row_relative_to="parser", so we use area_row_offset
                # not parser_row_offset
                if self.heuristics.check(self.sheet, 0, row_offset):
                    # Set detect offset to be used later
                    self.row_offset = row_offset
                    return True
            return False
        return True

    @property
    @classmethod
    @abstractmethod
    def platforms(self) -> typing.List[str]:
        """List of available platforms (used for validation)"""
        return []

    @classmethod
    def check_platform(cls, platform: str):
        if "*" in cls.platforms:
            # '*' will match any platform
            return True
        if platform in cls.platforms:
            return True
        return False

    def _parse(self) -> typing.Generator[typing.Tuple[int, CounterRecord], None, None]:
        for idx, area in enumerate(self.get_areas()):

            def gen():
                for e in self.parse_area(area):
                    yield idx, e

            yield from gen()

    def get_metric_name(self, name: str) -> str:
        return self.metric_aliases.get(name, name)

    def get_dimension_name(self, name):
        return self.dimension_aliases.get(name, name)

    def parse(self) -> typing.Generator[typing.Tuple[int, CounterRecord], None, None]:
        for idx, record in self._parse():
            yield idx, record

    def parse_area(self, area) -> typing.Generator[typing.Tuple[int, CounterRecord], None, None]:
        return area.aggregator.aggregate(self._parse_area(area))

    @abstractmethod
    def _parse_area(
        self, area: BaseArea
    ) -> typing.Generator[typing.Tuple[int, CounterRecord], None, None]:
        pass

    def get_months(self) -> typing.List[typing.List[datetime.date]]:
        return [e.get_months() for e in self.get_areas()]

    def get_extras(self) -> dict:
        return {}

    def analyze(self) -> typing.List[dict]:
        return []


class BaseJsonParser(BaseParser):
    @classmethod
    def sheet_reader_classes(cls):
        return [JsonCounter5SheetReader]

    def parse(self) -> typing.Generator[typing.Tuple[int, CounterRecord], None, None]:
        for idx, record in super().parse():
            # process aliases
            record.metric = self.get_metric_name(record.metric) if record.metric else record.metric
            record.dimension_data = {
                self.get_dimension_name(key): value for key, value in record.dimension_data.items()
            }
            yield idx, record


class BaseTabularParser(BaseParser):
    dimensions_validators: typing.Dict[str, typing.Type[validators.BaseValueModel]] = {
        "Platform": validators.Platform,
    }

    def get_areas(self) -> typing.List[BaseArea]:
        # We need to override this method to inject row_offset
        return list(
            itertools.chain(
                *(
                    area_class.make_areas(
                        self.sheet, self.platform, initial_row_offset=self.row_offset
                    )
                    for area_class in self.areas
                )
            )
        )

    @classmethod
    def sheet_reader_classes(cls):
        return [CsvSheetReader]

    def _metric_check(
        self,
        metric,
        orig_metric,
        metrics_to_skip: typing.List[str],
        available_metrics: typing.Optional[typing.List[str]],
        on_metric_check_failed: TableException.Action,
    ):
        def error():
            raise TableException(
                sheet=self.sheet.sheet_idx,
                value=orig_metric,
                reason="wrong-metric-found",
                action=on_metric_check_failed,
            )

        # ignore case during skip
        if metric.lower() in metrics_to_skip:
            error()

        # keep case when processing available metrics
        if available_metrics and metric not in available_metrics:
            error()

    def _parse_area(
        self, area: BaseTabularArea
    ) -> typing.Generator[typing.Tuple[int, CounterRecord], None, None]:
        count = 0
        metrics_to_skip = [e.lower() for e in self.metrics_to_skip]
        try:

            def check_metric_name(value: str, orig_value: str):
                self._metric_check(
                    value,
                    orig_value,
                    metrics_to_skip,
                    self.available_metrics,
                    self.on_metric_check_failed,
                )

            data_cells = area.find_data_cells(self.get_metric_name, check_metric_name)
        except TableException as e:
            if e.action == TableException.Action.FAIL:
                raise

        # Area offset should be absolute
        area_row_offset = area.row_offset
        parser_row_offset = self.row_offset

        # Store title_ids and dimensions sources
        # so it can be reused in the for-cycle
        dimensions_sources = list(area.dimensions_sources.items())
        title_ids_sources = area.title_ids_sources
        item_ids_sources = area.item_ids_sources
        item_authors_source = area.item_authors_source
        item_publication_date_source = area.item_publication_date_source
        metrics_to_skip = [e.lower() for e in self.metrics_to_skip]
        titles_to_skip = [e.lower() for e in self.titles_to_skip]
        items_to_skip = [e.lower() for e in self.items_to_skip]
        dimensions_to_skip = {k: [e.lower() for e in v] for k, v in self.dimensions_to_skip.items()}
        metric_value_extraction_overrides = self.metric_value_extraction_overrides

        for idx in itertools.count(0):
            try:
                # iterates through ranges
                if area.title_source:
                    title = area.title_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                    if title is not None and title.lower() in titles_to_skip:
                        continue
                else:
                    title = None

                if area.item_source:
                    item = area.item_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                    if item is not None and item.lower() in items_to_skip:
                        continue
                else:
                    item = None

                if area.metric_source:
                    orig_metric = area.metric_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                    metric = self.get_metric_name(orig_metric)
                    self._metric_check(
                        metric,
                        orig_metric,
                        metrics_to_skip,
                        self.available_metrics,
                        self.on_metric_check_failed,
                    )
                else:
                    metric = None

                if area.organization_source:
                    organization = area.organization_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                else:
                    organization = None

                if area.date_source:
                    date = area.date_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                else:
                    date = None

                dimension_data = {}
                skip = False
                for k, dimension_source in dimensions_sources:
                    dimension_text = dimension_source.extract(
                        self.sheet,
                        idx,
                        validator=self.dimensions_validators.get(k),
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                    if (
                        dimension_text is not None
                        and dimension_text.lower() in dimensions_to_skip.get(k, [])
                    ):
                        skip = True
                        break
                    dimension_data[self.get_dimension_name(k)] = dimension_text

                if skip:
                    continue

                title_ids = {}
                item_ids = {}
                for key in IDS:
                    if title_source := title_ids_sources.get(key):
                        value = title_source.extract(
                            self.sheet,
                            idx,
                            parser_row_offset=parser_row_offset,
                            area_row_offset=area_row_offset,
                        )
                        if value:
                            title_ids[title_source.last_key] = value

                    if item_source := item_ids_sources.get(key):
                        value = item_source.extract(
                            self.sheet,
                            idx,
                            parser_row_offset=parser_row_offset,
                            area_row_offset=area_row_offset,
                        )
                        if value:
                            item_ids[item_source.last_key] = value

                if item_publication_date_source:
                    item_publication_date = item_publication_date_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                else:
                    item_publication_date = None

                if item_authors_source:
                    item_authors = item_authors_source.extract(
                        self.sheet,
                        idx,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                else:
                    item_authors = None

            except TableException as e:
                if e.action == TableException.Action.SKIP:
                    continue
                if e.action == TableException.Action.STOP:
                    return
                else:
                    raise

            for data_cell in data_cells:
                try:
                    value_validator = metric_value_extraction_overrides.get(
                        data_cell.header_data.metric or metric or "",
                        SpecialExtraction.NO,
                    ).get_validator()

                    value = data_cell.value_source.extract(
                        self.sheet,
                        idx,
                        validator=value_validator,
                        parser_row_offset=parser_row_offset,
                        area_row_offset=area_row_offset,
                    )
                    record = CounterRecord(
                        value=round(value),
                        organization=organization,
                        metric=metric,
                        title=title,
                        item=item,
                        dimension_data=dimension_data,
                        title_ids=title_ids,
                        item_ids=item_ids,
                        item_publication_date=item_publication_date,
                        item_authors=item_authors,
                        start=start_month(date) if date else None,
                        end=end_month(date) if date else None,
                    )
                    record = data_cell.merge_into_record(record)
                    record = area.prepare_record(record)
                    logger.debug("Parsed %s", record)
                    area.check_record(count, record)
                    yield record
                    count += 1

                except TableException as e:
                    if e.action == TableException.Action.SKIP:
                        continue
                    if e.action == TableException.Action.STOP:
                        return
                    else:
                        raise
