import csv
import logging
import pathlib
from dataclasses import asdict

import pytest

from celus_nibbler import findparser, findparser_and_parse
from tests.testing_data import data, testing_dirs

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("parser, platform, filename", testing_dirs())
def test_findparser(parser: str, platform: str, filename: str) -> bool:
    """
    goes through each filename in data/csv/ and checks whether findparser() assigns correct parser which corresponds with the name of the filenames directory (for ex.: Parser_1_3_1)
    """
    logger.info('= = = = = test_findparser() has been called = = = = = ')
    file_path = pathlib.Path(
        pathlib.Path(__file__).parent,
        pathlib.Path(
            'data',
            'csv',
            parser,
            platform,
            filename,
        ),
    )
    with open(file_path) as f:
        logger.info('----- file \'%s\'  is tested -----', file_path.name)
        reader = csv.reader(f)
        report = list(reader)
        logger.info('findparser() function called')
        found_parser = findparser(report, platform)
        logger.info('findparser() function finished')
        assert found_parser.__name__ == parser


@pytest.mark.parametrize("filename, parser, platform, records", data)
def test_findparser_and_parse(filename: str, parser: str, platform: str, records: list) -> bool:
    """
    checks whether findparser_and_parse() finds correct parser and parse data correctly according to examples in testing_data.py
    """
    # following block of code is executed for each item in `data` thanks to @pytest.mark.parametrize()
    logger.info('= = = = = test_findparser_and_parse() has been called = = = = =')
    output = findparser_and_parse(
        pathlib.Path(
            pathlib.Path(__file__).parent,
            pathlib.Path('data', 'csv', parser, platform, filename),
        ),
        platform,
    )
    output_parser = output[0]
    assert parser == output_parser.__name__
    for record in records:
        assert asdict(output[2][record[0]]) == record[1]
