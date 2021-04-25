from enum import Enum, auto

from .generalparser import HorizontalDatesParser


class MonthsDirection(Enum):
    VERTICAL = auto()
    HORIZONTAL = auto()


class FirstMonth:
    JANUARY = 'january'


class Parser_1_3_1(HorizontalDatesParser):

    platforms = [
        'Naxos',
        'CHBeck',
        'Knovel',
        'Uptodate',
        'SciFinder',
        'SciVal',
    ]

    metric_list = [
        'Sessions',
        'Documents',
        'Tracks',
        'Resources',
        'Views',
        'Access_from_IP',
        'FP',
        'login',
    ]

    table_map = {
        'heuristics': [
            {'row': 0, 'col': 0, 'content': 'Metric'},
        ],
        'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
        'months': {
            'direction': MonthsDirection.HORIZONTAL,
            'start_at': {
                'row': 0,
                'col': 1,
                'month': FirstMonth.JANUARY,  # could be any other month
            },
        },
    }


class Parser_1_3_2(HorizontalDatesParser):
    platforms = [
        'Bisnode',
        'CHBeck',
        'ACS',
        'Micromedex',
        'SpringerLink',
        'Naxos',
    ]

    metric_list = [
        'back+front',
        'Document Count',
        'BR2',
        'Exports',
        'Documents',
        'Tracks',
        'Exports',
    ]

    table_map = {
        'heuristics': [
            {'row': 1, 'col': 0, 'content': ''},
        ],
        'metric_title': {'row': 1, 'col': 0, 'content': ''},
        'months': {
            'direction': MonthsDirection.HORIZONTAL,
            'start_at': {
                'row': 1,
                'col': 1,
                'month': FirstMonth.JANUARY,  # could be any other month
            },
        },
    }


class Parser_1_5_1(HorizontalDatesParser):

    platforms = [
        'InCites',
    ]

    metric_list = [
        'Result Clicks',
        'Platform Page Views',
        'Platform Sessions',
        'Queries',
        'Queries ESI',
        'Result Clicks ESI',
        'Sessions ESI',
        'Views',
        'Visits',
        'Sessions',
    ]

    table_map = {
        'heuristics': [
            {'row': 0, 'col': 0, 'content': 'Title'},
            {'row': 0, 'col': 1, 'content': 'Metric'},
        ],
        'metric_title': {'row': 0, 'col': 1, 'content': 'Metric'},
        'months': {
            'direction': MonthsDirection.HORIZONTAL,
            'start_at': {
                'row': 0,
                'col': 2,
                'month': FirstMonth.JANUARY,  # could be any other month
            },
        },
    }


class Parser_1_5_2(HorizontalDatesParser):

    platforms = [
        'Micromedex',
        'Naxos',
    ]

    metric_list = [
        'Sessions',
        'Document_count',
        'Naxos Music Library',
        'Naxos Music Library Jazz',
        'Naxos Music Library World',
        'Naxos Spoken Word Library',
        'NVL',
    ]

    table_map = {
        'heuristics': [
            {'row': 0, 'col': 0, 'content': 'Metric'},
            {'row': 0, 'col': 1, 'content': 'Title'},
        ],
        'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
        'months': {
            'direction': MonthsDirection.HORIZONTAL,
            'start_at': {
                'row': 0,
                'col': 2,
                'month': FirstMonth.JANUARY,  # could be any other month
            },
        },
    }
