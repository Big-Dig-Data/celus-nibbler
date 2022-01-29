import csv
import logging
import pathlib
import typing

import pytest

from celus_nibbler import findparser, findparser_and_parse, get_supported_platforms
from celus_nibbler.reader import CsvReader, XlsxReader

logger = logging.getLogger(__name__)


def detect_test_files(
    ext: str,
) -> typing.List[typing.Tuple[typing.Tuple[str, ...], str, pathlib.Path]]:
    """Find files with given extension for testing
    returns list of ((<parser_name>, <parser_name>, ), <platform_name>, <file_path>)
    """
    file_path = pathlib.Path(__file__).parent / 'data/parser'

    test_files = []
    for e in file_path.glob(f"**/*.{ext}"):
        list_of_parsers = e.stem.split('-')[:-1]
        test_files.append((tuple(list_of_parsers), e.parent.name, e))
    return test_files


def format_test_id(val):
    if isinstance(val, pathlib.Path):
        return val.name
    return val


@pytest.mark.parametrize(
    "parsers, platform, file_path", detect_test_files(ext='csv'), ids=format_test_id
)
def test_reader_and_findparser_csv(
    parsers: typing.Tuple[str], platform: str, file_path: pathlib.Path
):
    """
    goes through each filename in data/csv/ and checks whether findparser() assigns correct parser which corresponds with the name of the filenames directory (for ex.: Parser_1_3_1)
    """
    with file_path.open() as file:
        logger.info('----- file \'%s\'  is tested -----', file_path)
        sheets = CsvReader(file)
        for sheet in sheets:
            logger.info('\n-- sheet %s  is tested --', sheet.sheet_idx)
            found_parser = findparser(sheet, platform)
            if parsers[sheet.sheet_idx] == '0':
                assert (
                    found_parser is None
                ), f"parser {found_parser.__name__} found for sheet {sheet.sheet_idx + 1} where are no data to be parsed"
            else:
                assert found_parser is not None, "No parser found"
                assert (
                    found_parser.__name__ == 'Parser_' + parsers[sheet.sheet_idx]
                ), f"Parser mismatch: parser found ({found_parser.__name__}) but should find parser (Parser_{parsers[sheet.sheet_idx]})"

        # found_parser = findparser(sheet, platform)
        # assert found_parser is not None, "No parser found"
        # assert found_parser.__name__ == 'Parser_' + parsers[0], "Parser mismatch"


@pytest.mark.parametrize(
    "parsers, platform, file_path", detect_test_files(ext='xlsx'), ids=format_test_id
)
def test_reader_and_findparser_xlsx(
    parsers: typing.Tuple[str], platform: str, file_path: pathlib.Path
):
    """
    goes through each filename in data/xlsx/ and checks whether findparser() assigns correct parser which corresponds with the name of the filenames directory (for ex.: Parser_1_3_1)
    """
    logger.info('----- file \'%s\'  is tested -----', file_path)
    sheets = XlsxReader(file_path)
    for sheet in sheets:
        logger.info('\n-- sheet %s  is tested --', sheet.sheet_idx)
        found_parser = findparser(sheet, platform)
        if parsers[sheet.sheet_idx] == '0':
            assert (
                found_parser is None
            ), f"parser {found_parser.__name__} found for sheet {sheet.sheet_idx + 1} where are no data to be parsed"
        else:
            assert found_parser is not None, "No parser found"
            assert (
                found_parser.__name__ == 'Parser_' + parsers[sheet.sheet_idx]
            ), f"Parser mismatch: parser found ({found_parser.__name__}) but should find parser (Parser_{parsers[sheet.sheet_idx]})"


@pytest.mark.parametrize(
    "parsers,platform,file_path", detect_test_files(ext='csv'), ids=format_test_id
)
def test_findparser_and_parse_csv(
    parsers: typing.Tuple[str], platform: str, file_path: pathlib.Path
):
    """
    checks whether findparser_and_parse() finds correct parser and parse data correctly
    """

    with open(f"{file_path}.out") as results_file:
        logger.info('----- file \'%s\'  is tested -----', file_path)
        reader = csv.reader(results_file)
        sheets_of_counter_records = findparser_and_parse(file_path, platform)
        assert sheets_of_counter_records is not None
        for sheet in sheets_of_counter_records:
            for record in sheet:
                assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"


@pytest.mark.parametrize(
    "parsers,platform,file_path", detect_test_files(ext='xlsx'), ids=format_test_id
)
def test_findparser_and_parse_xlsx(
    parsers: typing.Tuple[str], platform: str, file_path: pathlib.Path
):
    """
    checks whether findparser_and_parse() finds correct parser and parse data correctly
    """

    with open(f"{file_path}.out") as results_file:
        logger.info('----- file \'%s\'  is tested -----', file_path)
        reader = csv.reader(results_file)
        sheets_of_counter_records = findparser_and_parse(file_path, platform)
        assert sheets_of_counter_records is not None
        for sheet in sheets_of_counter_records:
            for record in sheet:
                assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"


def test_get_supported_platforms():
    """ Test whether all supported platforms are properly returned """
    assert get_supported_platforms() == [
        'ACS',
        'Bisnode',
        'Brepolis',
        'CHBeck',
        'ClassiquesGarnierNumerique',
        'InCites',
        'Knovel',
        'Micromedex',
        'Naxos',
        'Ovid',
        'SUS_FLVC_Ulrichs',
        'SciFinder',
        'SciFinder_n',
        'SciVal',
        'SpringerLink',
        'Uptodate',
    ], "supported platforms doesn't match"
