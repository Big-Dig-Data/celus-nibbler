import pathlib
import typing
from datetime import date


def testing_dirs() -> typing.List[tuple]:
    """
    creating a list such as:
    data = [
        (parser, platform, filename),
        (parser, platform, filename),
        ...,
        ]
    """

    csv_dir = pathlib.Path(pathlib.Path(__file__).parent, pathlib.Path('data', 'csv'))
    data = []
    for parser_dir in csv_dir.glob('*'):
        if parser_dir.name != '.DS_Store':
            # to deal with .DS_Store files I have installed dtd, but it doesnt seem to work.
            for platform_dir in parser_dir.glob('*'):
                if platform_dir.name != '.DS_Store':
                    for filename in platform_dir.glob('*'):
                        if filename.name != '.DS_Store':
                            item = (parser_dir.name.capitalize(), platform_dir.name, filename.name)
                            data.append(item)
                        else:
                            continue
                else:
                    continue
        else:
            continue
    return data


data = [
    # (filename, parser, platform, [(position, counter-record), (position, counter-record)])
    (
        'AMU_NML_2020.xlsx - Sheet1.csv',
        'Parser_1_3_1',
        'Naxos',
        [
            (
                0,
                {
                    "start": date(2020, 1, 1),
                    "end": date(2020, 1, 31),
                    "value": 752,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Tracks",
                    "platform": "Naxos",
                },
            ),
            (
                1,
                {
                    "start": date(2020, 2, 1),
                    "end": date(2020, 2, 29),
                    "value": 531,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Tracks",
                    "platform": "Naxos",
                },
            ),
        ],
    ),
    (
        'KKPCE_nml_2020.xlsx - Sheet1.csv',
        'Parser_1_3_1',
        'Naxos',
        [
            (
                0,
                {
                    "start": date(2020, 1, 1),
                    "end": date(2020, 1, 31),
                    "value": 7,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Tracks",
                    "platform": "Naxos",
                },
            ),
            (
                9,
                {
                    "start": date(2020, 10, 1),
                    "end": date(2020, 10, 31),
                    "value": 3,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Tracks",
                    "platform": "Naxos",
                },
            ),
        ],
    ),
    (
        'Naxos_JVKCB-00073504_2018_01-12.csv',
        'Parser_1_3_2',
        'Naxos',
        [
            (
                0,
                {
                    "start": date(2018, 1, 1),
                    "end": date(2018, 1, 31),
                    "value": 2665,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Tracks",
                    "platform": "Naxos",
                },
            ),
            (
                9,
                {
                    "start": date(2018, 10, 1),
                    "end": date(2018, 10, 31),
                    "value": 2329,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Tracks",
                    "platform": "Naxos",
                },
            ),
        ],
    ),
    (
        'AGROTEST-25328859_Agrotest fyto, s.r.o. - Sheet1(1_5_1).csv',
        'Parser_1_5_1',
        'InCites',
        [
            (
                0,
                {
                    "start": date(2019, 1, 1),
                    "end": date(2019, 1, 31),
                    "value": 3,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Queries",
                    "platform": "InCites",
                },
            ),
            (
                12,
                {
                    "start": date(2019, 1, 1),
                    "end": date(2019, 1, 31),
                    "value": 1,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Views",
                    "platform": "InCites",
                },
            ),
        ],
    ),
    (
        'Micromedex_FNMOTOL-00064203_2019 - Sheet1-2(1_5_2).csv',
        'Parser_1_5_2',
        'Micromedex',
        [
            (
                0,
                {
                    "start": date(2019, 1, 1),
                    "end": date(2019, 1, 31),
                    "value": 0,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Document_count",
                    "platform": "Micromedex",
                },
            ),
            (
                12,
                {
                    "start": date(2019, 1, 1),
                    "end": date(2019, 1, 31),
                    "value": 0,
                    "dimension_data": None,
                    "title_ids": None,
                    "title": None,
                    "metric": "Document_count",
                    "platform": "Micromedex",
                },
            ),
        ],
    ),
]
