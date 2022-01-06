import logging
import pathlib
import typing

from celus_nibbler.errors import WrongFormatError
from celus_nibbler.parsers import GeneralParser, all_parsers
from celus_nibbler.reader import NaiveCSVReader, NaiveXlsxReader, TableReader
from celus_nibbler.record import CounterRecord
from celus_nibbler.templates import Sheet
from celus_nibbler.validators import Platform

logger = logging.getLogger(__name__)


def findparser(sheet: Sheet, platform: str) -> typing.Optional[GeneralParser]:
    plat_OK = [parser for parser in all_parsers() if platform in parser.platforms]

    if len(plat_OK) < 1:
        logger.warning('there is no parser which expects your platform %s', platform)
    else:
        logger.info(
            'there is %s parsers, which expects your platform %s. These parsers are: %s',
            len(plat_OK),
            platform,
            [parser.__name__ for parser in plat_OK],
        )
    plat_sheetname_OK = [parser for parser in plat_OK if parser(sheet).sheet_name_check()]

    plat_sheetname_heur_OK = [
        parser for parser in plat_sheetname_OK if parser(sheet).heuristic_check()
    ]
    if len(plat_sheetname_heur_OK) < 1:
        logger.warning('there is no parser which heuristics matching format of your uploaded file.')
    else:
        logger.info(
            'there is %s parsers, which heuristics matching format of your uploaded file. These parsers are: %s',
            len(plat_sheetname_heur_OK),
            [parser.__name__ for parser in plat_sheetname_heur_OK],
        )

    plat_sheetname_heur_metrtitle_OK = [
        parser for parser in plat_sheetname_heur_OK if parser(sheet).metric_title_check()
    ]
    if len(plat_sheetname_heur_metrtitle_OK) < 1:
        logger.warning('the metric_title, which parser expect to find in the file, was not found')
        return None
    elif len(plat_sheetname_heur_metrtitle_OK) > 1:
        logger.warning(
            '%s parsers, matching the metric_title in the file, has been found. Script needs to find exactly 1 parser, to work properly. These parsers are: %s',
            len(plat_sheetname_heur_metrtitle_OK),
            [parser.__name__ for parser in plat_sheetname_heur_metrtitle_OK],
        )
        return None
    elif len(plat_sheetname_heur_metrtitle_OK) == 1:
        logger.info(
            '%s parser, matching the metric_title in the file, has been found. This parser is: %s',
            len(plat_sheetname_heur_metrtitle_OK),
            [parser.__name__ for parser in plat_sheetname_heur_metrtitle_OK],
        )
        parser = plat_sheetname_heur_metrtitle_OK[0]
        logger.info('Parser used: %s', parser.__name__)
        return parser
    return None


def read_file(file_path: pathlib.Path) -> TableReader:
    if file_path.suffix.lower() == '.csv':
        with open(file_path) as file:
            sheets = NaiveCSVReader(file)
    elif file_path.suffix.lower() == '.xlsx':
        sheets = NaiveXlsxReader(file_path)
    else:
        raise WrongFormatError(file_path, file_path.suffix)
    return sheets


def findparser_and_parse(
    file_path: pathlib.Path, platform: str
) -> typing.Optional[typing.List[typing.List[CounterRecord]]]:

    platform = Platform(platform=platform).platform
    logger.info('\n\n----- file \'%s\'  is tested -----', file_path.name)
    sheets = read_file(file_path)
    sheets_of_counter_records = []
    for sheet_idx, sheet in enumerate(sheets):
        logger.info('\n-- sheet %s  with name: %s is tested --', sheet.idx, sheet.name)
        counter_records = []
        if parser := findparser(sheet, platform):
            if counter_records := parser(sheet, sheet_idx, platform).parse():
                pass  # expected an indented block error if no block of code here
            else:
                logger.warning('sheet %s has not been parsed', sheet_idx)
        else:
            logger.warning(
                'parser has not been chosen for sheet %s, the sheet wont be parsed', sheet_idx
            )
        sheets_of_counter_records.append(counter_records)
    return sheets_of_counter_records
