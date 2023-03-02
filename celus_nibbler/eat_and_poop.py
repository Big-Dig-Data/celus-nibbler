import functools
import itertools
import logging
import pathlib
import typing
from datetime import date

from celus_nigiri import CounterRecord

from celus_nibbler.aggregator import CheckConflictingRecordsAggregator
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
from celus_nibbler.reader import CsvReader, JsonCounter5Reader, SheetReader, TableReader, XlsxReader
from celus_nibbler.validators import Platform

logger = logging.getLogger(__name__)


class Poop:
    """nibblonians dark matter"""

    def __init__(self, parser: BaseParser):
        self.parser = parser

    @property
    def sheet_idx(self):
        return self.parser.sheet.sheet_idx

    @property
    def data_format(self) -> DataFormatDefinition:
        return self.parser.data_format

    def records(
        self,
        offset: int = 0,
        limit: typing.Optional[int] = None,
        same_check_size: int = 0,
    ) -> typing.Optional[typing.Generator[CounterRecord, None, None]]:
        if counter_records := self.parser.parse():
            if same_check_size:
                counter_records = CheckConflictingRecordsAggregator(same_check_size).aggregate(
                    counter_records
                )

            if limit is None:
                return itertools.islice(counter_records, offset, None)
            return itertools.islice(counter_records, offset, offset + limit)
        else:
            logger.warning('sheet %s has not been parsed', self.parser.sheet.sheet_idx + 1)
            return None

    @property
    def metrics(self):
        return self.get_metrics_dimensions_title_ids_months()[0]

    @property
    def dimensions(self):
        return self.get_metrics_dimensions_title_ids_months()[1]

    @property
    def title_ids(self):
        return self.get_metrics_dimensions_title_ids_months()[2]

    @property
    def months(self):
        return self.get_metrics_dimensions_title_ids_months()[3]

    @functools.lru_cache
    def get_metrics_dimensions_title_ids_months(
        self,
    ) -> typing.Tuple[typing.List[str], typing.List[str], typing.List[str], typing.List[str]]:
        seen_metrics = set()
        seen_dimensions: typing.Set[str] = set()
        seen_title_ids: typing.Set[str] = set()
        seen_months: typing.Set[str] = set()
        if records := self.records():
            for record in records:
                # add month
                seen_months.add(record.start)

                # add non-empty metrics
                if record.metric:
                    seen_metrics.add(record.metric)

                # add non-empty dimensions
                if record.dimension_data:
                    seen_dimensions.update(k for k, v in record.dimension_data.items() if v)
                # add non-empty titles
                if record.title_ids:
                    seen_title_ids.update(k for k, v in record.title_ids.items() if v)

            return (
                sorted(seen_metrics),
                sorted(seen_dimensions),
                sorted(seen_title_ids),
                sorted(seen_months),
            )
        return [], [], [], []

    def get_months(self) -> typing.List[typing.List[date]]:
        """Get months of the sheet (divided into areas)"""
        return self.parser.get_months()


def findparser(
    sheet: SheetReader,
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
    use_heuristics: bool = True,
    dynamic_parsers: typing.List[typing.Type[BaseParser]] = [],
) -> typing.Type[BaseParser]:
    parser_classes = [
        (name, parser)
        for name, parser in get_parsers(parsers, dynamic_parsers)
        if not check_platform or platform in parser.platforms
    ]

    if len(parser_classes) < 1:
        logger.warning('there is no parser which expects your platform %s', platform)
        raise NoParserForPlatformFound(sheet.sheet_idx)

    parser_classes = [
        (name, parser)
        for name, parser in parser_classes
        if type(sheet) in parser.sheet_reader_classes()
    ]
    if len(parser_classes) < 1:
        logger.warning('no parser found for reader %s', type(sheet))
        raise NoParserForFileTypeFound(sheet.sheet_idx)

    if use_heuristics:
        parser_classes = [
            (name, parser)
            for name, parser in parser_classes
            if parser(sheet, platform=platform).heuristic_check()
        ]

    if len(parser_classes) < 1:
        logger.warning('no parser found')
        raise NoParserMatchesHeuristics(sheet.sheet_idx)

    elif len(parser_classes) > 1:
        logger.warning('%s more than one parser found', len(parser_classes))
        raise MultipleParsersFound(sheet.sheet_idx, *(e[0] for e in parser_classes))

    logger.info(
        '%s parser, matching the heuristics in the file, has been found.',
        len(parser_classes),
    )
    name, parser = parser_classes[0]
    logger.info('Parser used: %s', name)
    return parser


def read_file(file_path: pathlib.Path) -> TableReader:
    if file_path.suffix.lower() in ['.csv', '.tsv']:
        return CsvReader(file_path)
    elif file_path.suffix.lower() == '.xlsx':
        return XlsxReader(file_path)
    elif file_path.suffix.lower() == '.json':
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
        logger.info('Digesting sheet %d', sheet.sheet_idx)
        try:
            parser = findparser(
                sheet, platform, parsers, check_platform, use_heuristics, dynamic_parsers
            )
            poops.append(Poop(parser(sheet, platform)))
        except (NoParserFound, MultipleParsersFound) as e:
            logger.warning(
                'parser has not been chosen for sheet %s, the sheet wont be parsed',
                sheet.sheet_idx + 1,
            )
            poops.append(e)

    return poops
