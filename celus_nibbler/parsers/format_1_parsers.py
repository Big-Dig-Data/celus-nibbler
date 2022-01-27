import re

from celus_nibbler import coordinates, validators
from celus_nibbler.conditions import RegexCondition

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

    heuristics = RegexCondition(re.compile("^Metric$"), coordinates.Coord(0, 0))

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

    heuristics = RegexCondition(re.compile("^$"), coordinates.Coord(1, 0))
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

    heuristics = RegexCondition(re.compile("^Metric$"), coordinates.Coord(0, 1))
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

    heuristics = RegexCondition(re.compile("^Metric$"), coordinates.Coord(0, 0)) & RegexCondition(
        re.compile("^Title$"), coordinates.Coord(0, 1)
    )
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

    heuristics = (
        RegexCondition(re.compile("^Title$"), coordinates.Coord(0, 0))
        & RegexCondition(re.compile("^Metric$"), coordinates.Coord(0, 1))
        & RegexCondition(re.compile("^Authentization$"), coordinates.Coord(0, 2))
    )
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

    heuristics = (
        RegexCondition(re.compile("^Name:$"), coordinates.Coord(1, 0))
        & RegexCondition(re.compile("^ID Number:$"), coordinates.Coord(2, 0))
        & RegexCondition(re.compile("^Type of license:$"), coordinates.Coord(3, 0))
    )
    metric_title = Coord(7, 1, content="")
    values = Coord(8, 2, relation=RelatedTo.FIELD)
    metric = Coord(8, 1, relation=RelatedTo.ROW)
    months = Coord(7, 2, relation=RelatedTo.COL)
    separate_year = Coord(6, 2, relation=RelatedTo.TABLE)
    title = Coord(0, 1, relation=RelatedTo.TABLE)
    title_ids = None
    dimension_data = None
    date_validation = validators.DateInString
