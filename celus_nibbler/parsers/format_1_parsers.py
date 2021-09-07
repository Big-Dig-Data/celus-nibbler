from celus_nibbler import validators

from .generalparser import Coord, HorizontalDatesParser, RelatedTo


class Parser_1_3_1(HorizontalDatesParser):

    platforms = [
        'Naxos',
        'CHBeck',
        'Knovel',
        'Uptodate',
        'SciFinder',
        'SciVal',
    ]

    heuristics = [
        Coord(0, 0, content='Metric'),
    ]
    metric_title = Coord(0, 0, content='Metric')
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

    heuristics = [
        Coord(1, 0, content=''),
    ]
    metric_title = Coord(1, 0, content='')
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

    heuristics = [
        Coord(0, 1, content='Metric'),
    ]
    metric_title = Coord(0, 1, content='Metric')
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

    heuristics = [
        Coord(0, 0, content='Metric'),
        Coord(0, 1, content='Title'),
    ]
    metric_title = Coord(0, 0, content='Metric')
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

    heuristics = [
        Coord(0, 0, content='Title'),
        Coord(0, 1, content='Metric'),
        Coord(0, 2, content='Authentization'),
    ]
    metric_title = Coord(0, 1, content='Metric')
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

    heuristics = [
        Coord(1, 0, content='Name:'),
        Coord(2, 0, content='ID Number:'),
        Coord(3, 0, content='Type of license:'),
    ]
    metric_title = Coord(7, 1, content=None)
    values = Coord(8, 2, relation=RelatedTo.FIELD)
    metric = Coord(8, 1, relation=RelatedTo.ROW)
    months = Coord(7, 2, relation=RelatedTo.COL)
    separate_year = Coord(6, 2, relation=RelatedTo.TABLE)
    title = Coord(0, 1, relation=RelatedTo.TABLE)
    title_ids = None
    dimension_data = None
    date_validation = validators.DateInString
