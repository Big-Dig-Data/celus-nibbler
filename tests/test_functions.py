import csv
import logging
import pathlib
import typing

import pytest

from celus_nibbler import eat, findparser, get_supported_platforms
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
def test_eat_csv(parsers: typing.Tuple[str], platform: str, file_path: pathlib.Path):
    """
    goes through each filename in data/csv/ and checks whether findparser() assigns correct parser which corresponds with the name of the filenames directory (for ex.: Parser_1_3_1)
    """
    logger.info('----- file \'%s\'  is tested -----', file_path)
    sheets = CsvReader(file_path)
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
    "parsers, platform, file_path", detect_test_files(ext='xlsx'), ids=format_test_id
)
def test_eat_xlsx(parsers: typing.Tuple[str], platform: str, file_path: pathlib.Path):
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
    checks whether eat() finds correct parser and parse data correctly
    """

    with open(f"{file_path}.out") as results_file:
        logger.info('----- file \'%s\'  is tested -----', file_path)
        reader = csv.reader(results_file)
        poops = eat(file_path, platform)
        assert poops, "not None or empty"
        for poop in poops:
            for record in poop.records():
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
    checks whether eat() finds correct parser and parse data correctly
    """

    with open(f"{file_path}.out") as results_file:
        logger.info('----- file \'%s\'  is tested -----', file_path)
        reader = csv.reader(results_file)
        poops = eat(file_path, platform)
        assert poops, "not None or empty"
        for poop in poops:
            for record in poop.records():
                assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"


def test_get_supported_platforms():
    """Test whether all supported platforms are properly returned"""
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


def test_extra_poop_info():
    file_path = (
        pathlib.Path(__file__).parent / 'data/parser/xlsx/ClassiquesGarnierNumerique/1_2-1_2-a.xlsx'
    )

    poops = eat(file_path, "ClassiquesGarnierNumerique")
    assert poops and len(poops) == 2
    assert [sorted(e) for e in poops[0].metrics_and_dimensions()] == [
        ["Consult", "Search"],
        ["Authentization"],
    ]
    assert [sorted(e) for e in poops[1].metrics_and_dimensions()] == [
        ["Consult", "Search"],
        ["Authentization"],
    ]
