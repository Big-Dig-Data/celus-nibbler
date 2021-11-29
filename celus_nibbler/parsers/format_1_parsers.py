from celus_nibbler import validators
from celus_nibbler.descriptors import Content, Coord, RelatedTo, Text

from .horizontal_dates_parser import HorizontalDatesParser


class Parser_1_3_1(HorizontalDatesParser):

    platforms = [
        'Naxos',
        'CHBeck',
        'Knovel',
        'Uptodate',
        'SciFinder',
        'SciVal',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(0, 0, Content(Text.IS, 'Metric')),
    ]
    metric_title = Coord(0, 0, Content(Text.IS, 'Metric'))
    values = Coord(1, 1, relation=RelatedTo.FIELD)
    metric = Coord(1, 0, relation=RelatedTo.ROW)
    months = Coord(0, 1, relation=RelatedTo.COL)
    separate_year = None
    title = None
    title_ids = None
    dimension_data = None


class Parser_1_3_2(HorizontalDatesParser):
    platforms = [
        'Bisnode',
        'CHBeck',
        'ACS',
        'Micromedex',
        'SpringerLink',
        'Naxos',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(1, 0, Content(Text.IS, '')),
    ]
    metric_title = Coord(1, 0, Content(Text.IS, ''))
    values = Coord(2, 1, relation=RelatedTo.FIELD)
    metric = Coord(2, 0, relation=RelatedTo.ROW)
    months = Coord(1, 1, relation=RelatedTo.COL)
    separate_year = None
    title = None
    title_ids = None
    dimension_data = None
    # TOASK is this correct? are there in this table really no dimension data?


class Parser_1_5_1(HorizontalDatesParser):

    platforms = [
        'InCites',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(0, 1, Content(Text.IS, 'Metric')),
    ]
    metric_title = Coord(0, 1, Content(Text.IS, 'Metric'))
    values = Coord(1, 2, relation=RelatedTo.FIELD)
    metric = Coord(1, 1, relation=RelatedTo.ROW)
    months = Coord(0, 2, relation=RelatedTo.COL)
    separate_year = None
    title = Coord(1, 0, relation=RelatedTo.ROW)
    title_ids = None
    dimension_data = None


class Parser_1_5_2(HorizontalDatesParser):

    platforms = [
        'Micromedex',
        'Naxos',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(0, 0, Content(Text.IS, 'Metric')),
        Coord(0, 1, Content(Text.IS, 'Title')),
    ]
    metric_title = Coord(0, 0, Content(Text.IS, 'Metric'))
    values = Coord(1, 2, relation=RelatedTo.FIELD)
    metric = Coord(1, 0, relation=RelatedTo.ROW)
    months = Coord(0, 2, relation=RelatedTo.COL)
    separate_year = None
    title = Coord(1, 1, relation=RelatedTo.ROW)
    title_ids = None
    dimension_data = None


class Parser_1_2(HorizontalDatesParser):

    platforms = [
        'ClassiquesGarnierNumerique',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(0, 0, Content(Text.IS, 'Title')),
        Coord(0, 1, Content(Text.IS, 'Metric')),
        Coord(0, 2, Content(Text.IS, 'Authentization')),
    ]
    metric_title = Coord(0, 1, Content(Text.IS, 'Metric'))
    values = Coord(1, 3, relation=RelatedTo.FIELD)
    metric = Coord(1, 1, relation=RelatedTo.ROW)
    months = Coord(0, 3, relation=RelatedTo.COL)
    separate_year = None
    title = Coord(1, 0, relation=RelatedTo.ROW)
    title_ids = None
    dimension_data = None


class Parser_1_3_3(HorizontalDatesParser):

    platforms = [
        'Naxos',
        'Brepolis',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(1, 0, Content(Text.IS, 'Name:')),
        Coord(2, 0, Content(Text.IS, 'ID Number:')),
        Coord(3, 0, Content(Text.IS, 'Type of license:')),
    ]
    metric_title = Coord(7, 1, Content(Text.IS, None))
    values = Coord(8, 2, relation=RelatedTo.FIELD)
    metric = Coord(8, 1, relation=RelatedTo.ROW)
    months = Coord(7, 2, relation=RelatedTo.COL)
    separate_year = Coord(6, 2, relation=RelatedTo.TABLE)
    title = Coord(0, 1, relation=RelatedTo.TABLE)
    title_ids = None
    dimension_data = None
    date_validation = validators.DateInString


class Parser_1_1_1(HorizontalDatesParser):

    platforms = [
        'SUS_FLVC_Ulrichs',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(0, 0, Content(Text.STARTSWITH, 'Total Searches from')),
        Coord(4, 0, Content(Text.STARTSWITH, 'Full record views from')),
    ]
    metric_title = Coord(5, 0, Content(Text.IS, 'Usage Type'))
    values = Coord(6, 1, relation=RelatedTo.FIELD)
    metric = Coord(6, 0, relation=RelatedTo.ROW)
    months = Coord(5, 1, relation=RelatedTo.COL)
    separate_year = None
    title = None
    title_ids = None
    dimension_data = None


# class Parser_1_1_2(HorizontalDatesParser):

#     platforms = [
#         'SciFinder',
#     ]

#     sheet_name = [Content(Text.ISANY#
#         Coord(0, 0, type=Text.STARTSWITH, Content(Text.IS,'SciFinder-)web Activity Usage Summary'),
#         Coord(4, 0, type=Text.STARTSWITH, Content(Text.IS,'Full )record views from'),
#     ]
#     metric_title = Coord(5, 0, Content(Text.IS,'Usage )Type')
#     values = Coord(6, 1, relation=RelatedTo.FIELD)
#     metric = Coord(6, 0, relation=RelatedTo.ROW)
#     months = Coord(5, 1, relation=RelatedTo.COL)
#     separate_year = None
#     title = None
#     title_ids = None
#     dimension_data = None


class Parser_1_1_4(HorizontalDatesParser):

    platforms = [
        'SciFinder',
    ]

    sheet_name = Content(Text.ISANY)
    heuristics = [
        Coord(0, 0, Content(Text.IS, 'Name')),
        Coord(0, 1, Content(Text.IS, 'Type')),
    ]
    metric_title = Coord(0, 1, Content(Text.IS, 'Type'))
    values = Coord(1, 2, relation=RelatedTo.FIELD)
    metric = Coord(1, 1, relation=RelatedTo.ROW)
    months = Coord(0, 2, relation=RelatedTo.COL)
    separate_year = None
    title = None
    title_ids = None
    dimension_data = None
