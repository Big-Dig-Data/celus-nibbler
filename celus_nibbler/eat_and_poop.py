import logging
import pathlib
import typing
from datetime import date

from celus_nibbler.errors import WrongFormatError
from celus_nibbler.parsers import BaseParser, filter_parsers
from celus_nibbler.reader import CsvReader, SheetReader, TableReader, XlsxReader
from celus_nibbler.record import CounterRecord
from celus_nibbler.validators import Platform

logger = logging.getLogger(__name__)


class Poop:
    """nibblonians dark matter"""

    def __init__(self, parser: BaseParser):
        self.parser = parser

    @property
    def sheet_idx(self):
        return self.parser.sheet.sheet_idx

    def records(self) -> typing.Optional[typing.Generator[CounterRecord, None, None]]:
        if counter_records := self.parser.parse():
            return counter_records
        else:
            logger.warning('sheet %s has not been parsed', self.parser.sheet.sheet_idx + 1)
            return None

    def metrics_and_dimensions(self) -> typing.Tuple[typing.List[str], typing.List[str]]:
        seen_metrics = set()
        seen_dimensions = set()
        if records := self.records():
            for record in records:
                # add non-empty metrics
                if record.metric:
                    seen_metrics.add(record.metric)
                # add non-empty dimensions
                if record.dimension_data:
                    seen_dimensions.update(k for k, v in record.dimension_data.items() if v)

            return list(e for e in seen_metrics), list(seen_dimensions)
        return ([], [])

    def get_months(self) -> typing.List[typing.List[date]]:
        """Get months of the sheet (divided into areas)"""
        return self.parser.get_months()


def findparser(
    sheet: SheetReader,
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
    use_heuristics: bool = True,
) -> typing.Optional[typing.Type[BaseParser]]:
    parsers = [
        parser
        for parser in filter_parsers(parsers)
        if not check_platform or platform in parser.platforms
    ]
    if len(parsers) < 1:
        logger.warning('there is no parser which expects your platform %s', platform)
    else:
        logger.info('there is %s parsers, which expects your platform %s', len(parsers), platform)

    if use_heuristics:
        parsers = [
            parser for parser in parsers if parser(sheet, platform=platform).heuristic_check()
        ]

    if len(parsers) < 1:
        logger.warning('no parser found')
        return None
    elif len(parsers) > 1:
        logger.warning('%s more than one parser found', len(parsers))
        return None

    logger.info(
        '%s parser, matching the heuristics in the file, has been found.',
        len(parsers),
    )
    parser = parsers[0]
    logger.info('Parser used: %s', parser)
    return parser


def read_file(file_path: pathlib.Path) -> TableReader:
    if file_path.suffix.lower() in ['.csv', '.tsv']:
        return CsvReader(file_path)
    elif file_path.suffix.lower() == '.xlsx':
        return XlsxReader(file_path)

    raise WrongFormatError(file_path, file_path.suffix)


def eat(
    file_path: typing.Union[pathlib.Path, str],
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
    use_heuristics: bool = True,
) -> typing.Optional[typing.List[typing.Optional[Poop]]]:
    platform = Platform(platform=platform).platform

    # make sure that file_path is Path instance
    file_path = pathlib.Path(file_path)

    logger.info('Eating file "%s"', file_path)

    reader = read_file(file_path)
    poops = []
    for sheet in reader:
        logger.info('Digesting sheet %d', sheet.sheet_idx)
        if parser := findparser(sheet, platform, parsers, check_platform, use_heuristics):
            poops.append(Poop(parser(sheet, platform)))
        else:
            logger.warning(
                'parser has not been chosen for sheet %s, the sheet wont be parsed',
                sheet.sheet_idx + 1,
            )
            poops.append(None)
    return poops
