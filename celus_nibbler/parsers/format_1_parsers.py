from .generalparser import HorizontalDatesParser, MonthsDirection, Occurrence

# # old Parser_1_3_1
# class Parser_1_3_1(HorizontalDatesParser):

#     platforms = [
#         'Naxos',
#         'CHBeck',
#         'Knovel',
#         'Uptodate',
#         'SciFinder',
#         'SciVal',
#     ]

#     metric_list = [
#         'Sessions',
#         'Documents',
#         'Tracks',
#         'Resources',
#         'Views',
#         'Access_from_IP',
#         'FP',
#         'login',
#     ]

#     table_map = {
#         'heuristics': [
#             {'row': 0, 'col': 0, 'content': 'Metric'},
#         ],
#         'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
#         'months': {
#             'direction': MonthsDirection.HORIZONTAL,
#             'start_at': {
#                 'row': 0,
#                 'col': 1,
#             },
#         },
#     }


# new Parser_1_3_1
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
        'table_direction': MonthsDirection.HORIZONTAL,
        'values': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 1},
        },
        'metric': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 0},
        },
        'months': {
            'occurrence': Occurrence.ONE_FOR_WHOLE_COL,
            'first_value_position': {'row': 0, 'col': 1},
        },
        'title': None,
        'title_ids': None,
        'dimension_data': None,
    }


# # old Parser_1_3_2
# class Parser_1_3_2(HorizontalDatesParser):
#     platforms = [
#         'Bisnode',
#         'CHBeck',
#         'ACS',
#         'Micromedex',
#         'SpringerLink',
#         'Naxos',
#     ]

#     metric_list = [
#         'back+front',
#         'Document Count',
#         'BR2',
#         'Exports',
#         'Documents',
#         'Tracks',
#         'Exports',
#     ]

#     table_map = {
#         'heuristics': [
#             {'row': 1, 'col': 0, 'content': ''},
#         ],
#         'metric_title': {'row': 1, 'col': 0, 'content': ''},
#         'months': {
#             'direction': MonthsDirection.HORIZONTAL,
#             'start_at': {
#                 'row': 1,
#                 'col': 1,
#             },
#         },
#     }


# new Parser_1_3_2
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
        'table_direction': MonthsDirection.HORIZONTAL,
        'values': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 2, 'col': 1},
        },
        'metric': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 2, 'col': 0},
        },
        'months': {
            'occurrence': Occurrence.ONE_FOR_WHOLE_COL,
            'first_value_position': {'row': 1, 'col': 1},
        },
        'title': None,
        'title_ids': None,
        'dimension_data': None,
        # TOASK is this correct? are there really no dimension data?
    }


# # old Parser_1_5_1
# class Parser_1_5_1(HorizontalDatesParser):

#     platforms = [
#         'InCites',
#     ]

#     metric_list = [
#         'Result Clicks',
#         'Platform Page Views',
#         'Platform Sessions',
#         'Queries',
#         'Queries ESI',
#         'Result Clicks ESI',
#         'Sessions ESI',
#         'Views',
#         'Visits',
#         'Sessions',
#     ]

#     table_map = {
#         'heuristics': [
#             {'row': 0, 'col': 0, 'content': 'Title'},
#             {'row': 0, 'col': 1, 'content': 'Metric'},
#         ],
#         'metric_title': {'row': 0, 'col': 1, 'content': 'Metric'},
#         'months': {
#             'direction': MonthsDirection.HORIZONTAL,
#             'start_at': {
#                 'row': 0,
#                 'col': 2,
#             },
#         },
#     }


# new Parser_1_5_1
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
        'table_direction': MonthsDirection.HORIZONTAL,
        'values': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 2},
        },
        'metric': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 1},
        },
        'months': {
            'occurrence': Occurrence.ONE_FOR_WHOLE_COL,
            'first_value_position': {'row': 0, 'col': 2},
        },
        'title': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 0},
        },
        'title_ids': None,
        'dimension_data': None,
    }


# # old Parser_1_5_2
# class Parser_1_5_2(HorizontalDatesParser):

#     platforms = [
#         'Micromedex',
#         'Naxos',
#     ]

#     metric_list = [
#         'Sessions',
#         'Document_count',
#         'Naxos Music Library',
#         'Naxos Music Library Jazz',
#         'Naxos Music Library World',
#         'Naxos Spoken Word Library',
#         'NVL',
#     ]

#     table_map = {
#         'heuristics': [
#             {'row': 0, 'col': 0, 'content': 'Metric'},
#             {'row': 0, 'col': 1, 'content': 'Title'},
#         ],
#         'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
#         'months': {
#             'direction': MonthsDirection.HORIZONTAL,
#             'start_at': {
#                 'row': 0,
#                 'col': 2,
#             },
#         },
#         'title_title': None,
#         'title_ids_title': None,
#         'dimension_data_title': None,
#     }


# new Parser_1_5_2
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
        'table_direction': MonthsDirection.HORIZONTAL,
        'values': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 2},
        },
        'metric': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 0},
        },
        'months': {
            'occurrence': Occurrence.ONE_FOR_WHOLE_COL,
            'first_value_position': {'row': 0, 'col': 2},
        },
        'title': {
            'occurrence': Occurrence.FOR_EACH_VALUE,
            'first_value_position': {'row': 1, 'col': 1},
        },
        'title_ids': None,
        'dimension_data': None,
    }
