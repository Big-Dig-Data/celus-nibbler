import logging
import pathlib
import typing

from celus_nibbler.errors import WrongFormatError
from celus_nibbler.parsers import GeneralParser, all_parsers
from celus_nibbler.reader import CsvReader, SheetReader, TableReader, XlsxReader
from celus_nibbler.record import CounterRecord
from celus_nibbler.validators import Platform

logger = logging.getLogger(__name__)


def findparser(sheet: SheetReader, platform: str) -> typing.Optional[typing.Type[GeneralParser]]:
    plat_OK = [parser for parser in all_parsers() if platform in parser.platforms]
    if len(plat_OK) < 1:
        logger.warning('there is no parser which expects your platform %s', platform)
    else:
        logger.info('there is %s parsers, which expects your platform %s', len(plat_OK), platform)

    plat_heur_OK = [parser for parser in plat_OK if parser(sheet).heuristic_check()]
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
    if file_path.suffix.lower() == '.csv':
        with open(file_path) as file:
            return CsvReader(file)
    elif file_path.suffix.lower() == '.xlsx':
        return XlsxReader(file_path)

    raise WrongFormatError(file_path, file_path.suffix)


def findparser_and_parse(
    file_path: pathlib.Path, platform: str
) -> typing.Optional[typing.List[typing.List[CounterRecord]]]:

    platform = Platform(platform=platform).platform
    logger.info('\n\n----- file \'%s\'  is tested -----', file_path.name)
    sheets = read_file(file_path)
    sheets_of_counter_records = []
    for sheet in sheets:
        logger.info('\n-- sheet %s  is tested --', sheet.sheet_idx)
        counter_records = []
        if parser := findparser(sheet, platform):
            if counter_records := parser(sheet, sheet.sheet_idx, platform).parse():
                pass  # expected an indented block error if no block of code here
            else:
                logger.warning('sheet %s has not been parsed', sheet.sheet_idx + 1)
        else:
            logger.warning(
                'parser has not been chosen for sheet %s, the sheet wont be parsed',
                sheet.sheet_idx + 1,
            )
        sheets_of_counter_records.append(counter_records)
    return sheets_of_counter_records
