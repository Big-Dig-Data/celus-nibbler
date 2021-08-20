import csv
import logging
import pathlib
import typing

import pytest

from celus_nibbler import findparser, findparser_and_parse

logger = logging.getLogger(__name__)


def detect_test_files(ext: str = "csv") -> typing.List[typing.Tuple[str, str, pathlib.Path]]:
    """Find files with given extension for testing
    returns list of (<parser_name>, <platform_name>, <file_path>)
    """
    data_path = pathlib.Path(__file__).parent / 'data'
    return [(e.parent.parent.name, e.parent.name, e) for e in data_path.glob(f"**/*.{ext}")]


def format_test_id(val):
    if isinstance(val, pathlib.Path):
        return val.name
    return val


@pytest.mark.parametrize("parser, platform, path", detect_test_files(), ids=format_test_id)
def test_findparser_csv(parser: str, platform: str, path: pathlib.Path):
    """
    goes through each filename in data/csv/ and checks whether findparser() assigns correct parser which corresponds with the name of the filenames directory (for ex.: Parser_1_3_1)
    """
    with path.open() as f:
        logger.info('----- file \'%s\'  is tested -----', path)
        reader = csv.reader(f)
        report = list(reader)
        found_parser = findparser(report, platform)
        assert found_parser is not None, "No parser found"
        assert found_parser.__name__ == parser, "Parser mismatch"


@pytest.mark.parametrize("parser,platform,path", detect_test_files(), ids=format_test_id)
def test_findparser_and_parse_csv(parser: str, platform: str, path: pathlib.Path):
    """
    checks whether findparser_and_parse() finds correct parser and parse data correctly
    """

    with open(f"{path}.out") as results_file:
        logger.info('----- file \'%s\'  is tested -----', path)
        reader = csv.reader(results_file)
        output = findparser_and_parse(path, platform)
        assert output is not None
        output_parser, _, records = output
        assert parser == output_parser.__name__
        for record in records:
            assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"
