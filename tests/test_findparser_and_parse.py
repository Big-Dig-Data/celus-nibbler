import csv
import pathlib
from dataclasses import asdict

import pytest

from celus_nibbler import findparser, findparser_and_parse

from .testing_data import data


@pytest.fixture
def testing_dir():
    """
    creating a list such as:
    data = [
        [parser,
            [platform,
                [table,
                table,
                table],
            ],
            [platform,
                [table,
                table,
                table]
            ]
        ],
        [parser,
            [platform,
                [table,
                table,
                table],
            ],
            [platform,
                [table,
                table,
                table]
            ]
        ],
        ]
    """

    csv_dir = pathlib.Path(pathlib.Path(__file__).parent, pathlib.Path('data', 'csv'))
    data = []
    for parser_dir in csv_dir.glob('*'):
        if parser_dir.name != '.DS_Store':
            # to deal with .DS_Store files I have installed dtd, but it doesnt seem to work.
            parser_and_platforms_and_tables = [
                parser_dir.name,
            ]
            for platform_dir in parser_dir.glob('*'):
                if platform_dir.name != '.DS_Store':
                    platform_and_tables = [
                        platform_dir.name,
                    ]
                    tables = []
                    for table in platform_dir.glob('*'):
                        if table.name != '.DS_Store':
                            tables.append(table.name)
                        else:
                            continue
                    platform_and_tables.append(tables)
                    parser_and_platforms_and_tables.append(platform_and_tables)
                else:
                    continue
            data.append(parser_and_platforms_and_tables)
        else:
            continue
    return data


def test_findparser(testing_dir):
    """
    goes through each table in data/csv/ and checks whether findparser() assigns correct parser which corresponds with the name of the tables directory (for ex.: Parser_1_3_1)
    """
    for parser_and_platforms_and_tables in testing_dir:
        for platform_and_tables in parser_and_platforms_and_tables[1:]:
            for table in platform_and_tables[1]:
                file_path = pathlib.Path(
                    pathlib.Path(__file__).parent,
                    pathlib.Path(
                        'data',
                        'csv',
                        parser_and_platforms_and_tables[0],
                        platform_and_tables[0],
                        table,
                    ),
                )
                with open(file_path) as f:
                    reader = csv.reader(f)
                    report = list(reader)
                    parser = findparser(report, platform_and_tables[0])
                    assert parser.__name__ == parser_and_platforms_and_tables[0]


@pytest.fixture
def testing_examples():
    return data


def test_findparser_and_parse(testing_examples):
    """
    checks whether findparser_and_parse() finds correct parser and parse data correctly according to examples in testing_data.py
    """
    for example in testing_examples:
        file_path = pathlib.Path(
            pathlib.Path(__file__).parent,
            pathlib.Path('data', 'csv', example['parser'], example['platform'], example['file']),
        )
        # TODO make sure that you wont miss any table, there might be a situation when the table was placed in a wrong directory - at that situation the table wont get tested at all
        output = findparser_and_parse(file_path, example['platform'])
        output_parser = output[0]

        assert output_parser.__name__ == example['parser']

        for record in example['records']:
            assert asdict(output[2][record['position']]) == record['counter-record']
