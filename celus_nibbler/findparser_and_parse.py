import csv
import logging
import pathlib
import typing

from celus_nibbler.parsers import GeneralParser, all_parsers
from celus_nibbler.validators import Platform

logger = logging.getLogger(__name__)


def findparser(table: list, platform: str) -> typing.Optional[typing.Type[GeneralParser]]:
    plat_OK = [parser for parser in all_parsers if platform in parser.platforms]
    if len(plat_OK) < 1:
        logger.warning('there is no parser which expects your platform %s', platform)
    else:
        logger.info('there is %s parsers, which expects your platform %s', len(plat_OK), platform)

    plat_heur_OK = [parser for parser in plat_OK if parser.heuristic_check(parser(table))]
    if len(plat_heur_OK) < 1:
        logger.warning('there is no parser which heuristics matching your format of the table.')
    else:
        logger.info(
            'there is %s parsers, which heuristics matching your format of the table.',
            len(plat_heur_OK),
        )

    plat_heur_metrtitle_OK = [
        parser for parser in plat_heur_OK if parser.metric_title_check(parser(table))
    ]
    if len(plat_heur_metrtitle_OK) < 1:
        logger.warning('the metric_title, which parser expect to find in the file, was not found')
        return None
    elif len(plat_heur_metrtitle_OK) > 1:
        logger.warning(
            '%s parsers, matching the metric_title in the file, has been found. Script needs to find exactly 1 parser, to work properly.',
            len(plat_heur_metrtitle_OK),
        )
        return None
    elif len(plat_heur_metrtitle_OK) == 1:
        logger.info(
            '%s parser, matching the metric_title in the file, has been found.',
            len(plat_heur_metrtitle_OK),
        )
        parser = plat_heur_metrtitle_OK[0]
        return parser

    return None


def findparser_and_parse(
    file: pathlib.Path, platform: str
) -> typing.Optional[typing.Tuple[GeneralParser, list, list]]:
    platform = Platform(platform=platform).platform
    with open(file) as f:
        logger.info('----- file \'%s\'  is tested -----', file.name)
        reader = csv.reader(f)
        table = list(reader)
        logger.info('findparser() function called')
        parser = findparser(table, platform)
        if not parser:
            logger.warning('parser has not been chosen, the file wont be parsed')
            return None
        else:
            logger.info('findparser() function finished')
            new_metrics = parser.find_new_metrics(parser(table))
            # TODO logger for new metrics
            logger.info('parse() function called')
            counter_report = parser.parse(parser(table, platform))
            logger.info('parse() function finished')
            output = (parser, new_metrics, counter_report)
            logger.info('Parser used: %s', output[0])
            logger.info('New metrics found: %s', output[1])
            return output
