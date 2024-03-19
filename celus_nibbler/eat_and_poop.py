import functools
import itertools
import logging
import pathlib
import typing
from collections import Counter, defaultdict
from datetime import date

from celus_nigiri import CounterRecord
from pydantic import Field
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from celus_nibbler.aggregator import CheckConflictingRecordsAggregator, CheckNonNegativeValues
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import (
    MultipleParsersFound,
    NibblerError,
    NoParserForFileTypeFound,
    NoParserForPlatformFound,
    NoParserFound,
    NoParserMatchesHeuristics,
    WrongFileFormatError,
)
from celus_nibbler.parsers import BaseParser, get_parsers
from celus_nibbler.reader import (
    CsvReader,
    JsonCounter5Reader,
    SheetReader,
    TableReader,
    XlsReader,
    XlsxReader,
)
from celus_nibbler.utils import JsonEncorder, PydanticConfig
from celus_nibbler.validators import Platform

logger = logging.getLogger(__name__)


@dataclass(config=PydanticConfig)
class StatUnit(JsonEncorder):
    count: int = 0
    sum: int = 0

    def inc(self, value: int):
        self.count += 1
        self.sum += value

    def __add__(self, other: "StatUnit") -> "StatUnit":
        return StatUnit(
            count=self.count + other.count,
            sum=self.sum + other.sum,
        )

    def __iadd__(self, other: "StatUnit"):
        self.sum += other.sum
        self.count += other.count
        return self


AnnotatedStatUnit = Annotated[StatUnit, Field(default_factory=lambda: StatUnit)]


@dataclass(config=PydanticConfig)
class PoopOrganizationStats(JsonEncorder):
    months: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    metrics: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    titles: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    title_ids: typing.Set[str] = Field(default_factory=lambda: set())
    items: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    item_ids: typing.Set[str] = Field(default_factory=lambda: set())
    dimensions: typing.DefaultDict[
        str,
        Annotated[
            typing.DefaultDict[str, AnnotatedStatUnit],
            Field(default_factory=lambda: defaultdict(StatUnit)),
        ],
    ] = Field(defaultdict(lambda: defaultdict(StatUnit)))
    total: StatUnit = Field(default_factory=lambda: StatUnit())

    def process_record(self, record: CounterRecord):
        self.months[record.start.strftime("%Y-%m")].inc(record.value)
        self.metrics[record.metric or ""].inc(record.value)
        self.titles[record.title or ""].inc(record.value)
        self.items[record.item or ""].inc(record.value)

        for title_id in record.title_ids.keys():
            self.title_ids.add(title_id)

        for item_id in record.item_ids.keys():
            self.item_ids.add(item_id)

        for dimension_name, dimension in record.dimension_data.items():
            self.dimensions[dimension_name][dimension].inc(record.value)

        self.total.inc(record.value)

    def __add__(self, other: "PoopOrganizationStats") -> "PoopOrganizationStats":
        result = PoopOrganizationStats()
        for month, data in list(self.months.items()) + list(other.months.items()):
            result.months[month] += data

        for metric, data in list(self.metrics.items()) + list(other.metrics.items()):
            result.metrics[metric] += data

        for title, data in list(self.titles.items()) + list(other.titles.items()):
            result.titles[title] += data

        for item, data in list(self.items.items()) + list(other.items.items()):
            result.items[item] += data

        for dimension_name, dimensions in list(self.dimensions.items()) + list(
            other.dimensions.items()
        ):
            for dimension, data in dimensions.items():
                result.dimensions[dimension_name][dimension] += data

        result.title_ids = self.title_ids | other.title_ids
        result.item_ids = self.item_ids | other.item_ids

        result.total = self.total + other.total
        return result


@dataclass(config=PydanticConfig)
class PoopStats(JsonEncorder):
    months: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    metrics: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    organizations: typing.DefaultDict[
        str,
        Annotated[PoopOrganizationStats, Field(default_factory=lambda: PoopOrganizationStats())],
    ] = Field(default_factory=lambda: defaultdict(PoopOrganizationStats))
    titles: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    title_ids: typing.Set[str] = Field(default_factory=lambda: set())
    items: typing.DefaultDict[str, AnnotatedStatUnit] = Field(
        default_factory=lambda: defaultdict(StatUnit)
    )
    item_ids: typing.Set[str] = Field(default_factory=lambda: set())
    dimensions: typing.DefaultDict[
        str,
        Annotated[
            typing.DefaultDict[str, AnnotatedStatUnit],
            Field(default_factory=lambda: defaultdict(StatUnit)),
        ],
    ] = Field(defaultdict(lambda: defaultdict(StatUnit)))
    total: StatUnit = Field(default_factory=lambda: StatUnit())

    def __add__(self, other: "PoopStats") -> "PoopStats":
        result = PoopStats()
        for month, data in list(self.months.items()) + list(other.months.items()):
            result.months[month] += data

        for metric, data in list(self.metrics.items()) + list(other.metrics.items()):
            result.metrics[metric] += data

        for organization, data in list(self.organizations.items()) + list(
            other.organizations.items()
        ):
            result.organizations[organization] += data

        for title, data in list(self.titles.items()) + list(other.titles.items()):
            result.titles[title] += data

        for item, data in list(self.items.items()) + list(other.items.items()):
            result.items[item] += data

        for dimension_name, dimensions in list(self.dimensions.items()) + list(
            other.dimensions.items()
        ):
            for dimension, data in dimensions.items():
                result.dimensions[dimension_name][dimension] += data

        result.title_ids = self.title_ids | other.title_ids
        result.item_ids = self.item_ids | other.item_ids

        result.total = self.total + other.total
        return result

    def process_record(self, record: CounterRecord):
        self.months[record.start.strftime("%Y-%m")].inc(record.value)
        self.metrics[record.metric or ""].inc(record.value)
        self.organizations[record.organization or ""].process_record(record)
        self.titles[record.title or ""].inc(record.value)
        self.items[record.item or ""].inc(record.value)

        for title_id in record.title_ids.keys():
            self.title_ids.add(title_id)

        for item_id in record.item_ids.keys():
            self.item_ids.add(item_id)

        for dimension_name, dimension in record.dimension_data.items():
            self.dimensions[dimension_name][dimension].inc(record.value)

        self.total.inc(record.value)


class Poop:
    """nibblonians dark matter"""

    def __init__(self, parser: BaseParser):
        self.parser = parser
        self.current_stats = PoopStats()
        self.area_counter: Counter[int] = Counter()
        self.extras = parser.get_extras()

    @property
    def sheet_idx(self):
        return self.parser.sheet.sheet_idx

    @property
    def data_format(self) -> DataFormatDefinition:
        return self.parser.data_format

    def records_basic(
        self,
        offset: int = 0,
        limit: typing.Optional[int] = None,
        same_check_size: int = 0,
    ) -> typing.Optional[typing.Generator[typing.Tuple[int, CounterRecord], None, None]]:
        if counter_records := self.parser.parse():
            if limit is None:
                return itertools.islice(counter_records, offset, None)
            return itertools.islice(counter_records, offset, offset + limit)

        return None

    def records_with_counter(
        self,
        offset: int = 0,
        limit: typing.Optional[int] = None,
        same_check_size: int = 0,
    ) -> typing.Optional[typing.Generator[CounterRecord, None, None]]:
        self.area_counter = Counter()
        if counter_records := self.records_basic(offset, limit, same_check_size):
            for idx, record in counter_records:
                self.area_counter[idx] += 1
                yield record
        return None

    def records(
        self,
        offset: int = 0,
        limit: typing.Optional[int] = None,
        same_check_size: int = 0,
    ) -> typing.Optional[typing.Generator[CounterRecord, None, None]]:
        if counter_records := self.records_with_counter(offset, limit, same_check_size):
            aggregator = CheckNonNegativeValues()
            if same_check_size:
                aggregator = aggregator | CheckConflictingRecordsAggregator(same_check_size)

            return aggregator.aggregate(counter_records)
        else:
            logger.warning("sheet %s has not been parsed", self.parser.sheet.sheet_idx + 1)
            return None

    def records_with_stats(
        self,
        offset: int = 0,
        limit: typing.Optional[int] = None,
        same_check_size: int = 0,
    ) -> typing.Optional[typing.Generator[CounterRecord, None, None]]:
        self.current_stats = PoopStats()
        if records := self.records(offset, limit, same_check_size):
            for record in records:
                self.current_stats.process_record(record)
                yield record

        return None

    @property
    def metrics(self):
        return self.get_stats().metrics.keys()

    @property
    def dimensions(self):
        return self.get_stats().dimensions.keys()

    @property
    def title_ids(self):
        return self.get_stats().title_ids

    @property
    def item_ids(self):
        return self.get_stats().item_ids

    @property
    def months(self):
        return self.get_stats().months.keys()

    @functools.lru_cache
    def get_stats(self) -> PoopStats:
        """Goes through all records and caluculates stats based on output records"""
        res = PoopStats()
        if records := self.records():
            for record in records:
                res.process_record(record)

        return res

    def get_months(self) -> typing.List[typing.List[date]]:
        """Get months of the sheet (divided into areas)"""
        return self.parser.get_months()

    def __del__(self):
        # Make sure that underlying file is closed
        self.parser.sheet.close()


def findparser(
    sheet: SheetReader,
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
    use_heuristics: bool = True,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> BaseParser:
    parser_classes = [
        (name, parser)
        for name, parser in get_parsers(parsers, dynamic_parsers)
        if not check_platform or platform in parser.platforms
    ]

    if len(parser_classes) < 1:
        logger.warning("there is no parser which expects your platform %s", platform)
        raise NoParserForPlatformFound(sheet.sheet_idx)

    parser_classes = [
        (name, parser)
        for name, parser in parser_classes
        if type(sheet) in parser.sheet_reader_classes()
    ]
    if len(parser_classes) < 1:
        logger.warning("no parser found for reader %s", type(sheet))
        raise NoParserForFileTypeFound(sheet.sheet_idx)

    parser_instances = [(name, parser(sheet, platform=platform)) for name, parser in parser_classes]
    if use_heuristics:
        parser_instances_filtered = [
            (name, parser) for (name, parser) in parser_instances if parser.heuristic_check()
        ]
    else:
        parser_instances_filtered = parser_instances

    if len(parser_instances_filtered) < 1:
        logger.warning("no parser found")
        raise NoParserMatchesHeuristics(
            sheet.sheet_idx,
            parsers_info={name: parser.analyze() for name, parser in parser_instances},
        )

    elif len(parser_instances_filtered) > 1:
        logger.warning("%s more than one parser found", len(parser_classes))
        raise MultipleParsersFound(sheet.sheet_idx, *(e[0] for e in parser_classes))

    name, parser = parser_instances_filtered[0]
    logger.info("Parser used: %s", name)
    return parser


def read_file(file_path: pathlib.Path) -> TableReader:
    if file_path.suffix.lower() in [".csv", ".tsv"]:
        return CsvReader(file_path)
    elif file_path.suffix.lower() == ".xlsx":
        return XlsxReader(file_path)
    elif XlsReader and file_path.suffix.lower() in [".xls", ".xlsb"]:
        return XlsReader(file_path)
    elif file_path.suffix.lower() == ".json":
        return JsonCounter5Reader(file_path)

    raise WrongFileFormatError(file_path, file_path.suffix)


def eat(
    file_path: typing.Union[pathlib.Path, str],
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
    use_heuristics: bool = True,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.List[typing.Union[Poop, NibblerError]]:
    platform = Platform(value=platform).value

    # make sure that file_path is Path instance
    file_path = pathlib.Path(file_path)

    logger.info('Eating file "%s"', file_path)

    reader = read_file(file_path)
    poops = []
    for sheet in reader:
        logger.info("Digesting sheet %d", sheet.sheet_idx)
        try:
            parser = findparser(
                sheet, platform, parsers, check_platform, use_heuristics, dynamic_parsers
            )
            poops.append(Poop(parser))
        except (NoParserFound, MultipleParsersFound) as e:
            logger.warning(
                "parser has not been chosen for sheet %s, the sheet wont be parsed",
                sheet.sheet_idx + 1,
            )
            poops.append(e)
            # Make sure that underlying file is closed
            sheet.close()

    return poops
