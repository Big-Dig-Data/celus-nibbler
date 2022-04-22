import logging
import pathlib
import typing

from celus_nibbler.errors import WrongFormatError
from celus_nibbler.parsers import BaseParser, all_parsers
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


def findparser(
    sheet: SheetReader,
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
) -> typing.Optional[typing.Type[BaseParser]]:
    plat_OK = [
        parser
        for parser in all_parsers(parsers)
        if not check_platform or platform in parser.platforms
    ]
    if len(plat_OK) < 1:
        logger.warning('there is no parser which expects your platform %s', platform)
    else:
        logger.info('there is %s parsers, which expects your platform %s', len(plat_OK), platform)

    plat_heur_OK = [
        parser for parser in plat_OK if parser(sheet, platform=platform).heuristic_check()
    ]
    if len(plat_heur_OK) < 1:
        logger.warning('there is no parser which heuristics matching format of your uploaded file.')
        return None
    elif len(plat_heur_OK) > 1:
        logger.warning(
            '%s parsers, matching the heuristics in the file, has been found. Script needs to find exactly 1 parser, to work properly.',
            len(plat_heur_OK),
        )
        return None

    logger.info(
        '%s parser, matching the heuristics in the file, has been found.',
        len(plat_heur_OK),
    )
    parser = plat_heur_OK[0]
    logger.info('Parser used: %s', parser)
    return parser


def read_file(file_path: pathlib.Path) -> TableReader:
    if file_path.suffix.lower() in ['.csv', '.tsv']:
        return CsvReader(file_path)
    elif file_path.suffix.lower() == '.xlsx':
        return XlsxReader(file_path)

    raise WrongFormatError(file_path, file_path.suffix)


def eat(
    file_path: pathlib.Path,
    platform: str,
    parsers: typing.Optional[typing.List[str]] = None,
    check_platform: bool = True,
) -> typing.Optional[typing.List[Poop]]:
    platform = Platform(platform=platform).platform

    logger.info('Eating file "%s"', file_path)

    reader = read_file(file_path)
    poops = []
    for sheet in reader:
        logger.info('Digesting sheet %d', sheet.sheet_idx)
        if parser := findparser(sheet, platform, parsers, check_platform):
            poops.append(Poop(parser(sheet, platform)))
        else:
            logger.warning(
                'parser has not been chosen for sheet %s, the sheet wont be parsed',
                sheet.sheet_idx + 1,
            )
    return poops
