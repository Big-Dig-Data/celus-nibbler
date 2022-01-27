import datetime
import re

from celus_nibbler import validators
from celus_nibbler.conditions import RegexCondition, StemmerCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.record import CounterRecord

from .base import VerticalParser


class Parser_1_2(VerticalParser):
    platforms = [
        'ClassiquesGarnierNumerique',
    ]

    heuristics = (
        RegexCondition(re.compile("^Title$"), Coord(0, 0))
        & RegexCondition(re.compile("^Metric$"), Coord(0, 1))
        & RegexCondition(re.compile("^Authentization$"), Coord(0, 2))
    )
    date_header_cells = CoordRange(Coord(0, 3), Direction.RIGHT)
    metric_cells = CoordRange(Coord(1, 1), Direction.DOWN)
    title_cells = CoordRange(Coord(1, 0), Direction.DOWN)
    dimensions_cells = {"Authentization": CoordRange(Coord(1, 2), Direction.DOWN)}


class Parser_1_3_1(VerticalParser):

    platforms = [
        'Naxos',
        'CHBeck',
        'Knovel',
        'Uptodate',
        'SciFinder',
        'SciVal',
    ]

    heuristics = RegexCondition(re.compile("^Metric$"), Coord(0, 0))
    date_header_cells = CoordRange(Coord(0, 1), Direction.RIGHT)
    metric_cells = CoordRange(Coord(1, 0), Direction.DOWN)


class Parser_1_3_2(VerticalParser):

    platforms = [
        'Bisnode',
        'CHBeck',
        'ACS',
        'Micromedex',
        'SpringerLink',
        'Naxos',
    ]
    heuristics = RegexCondition(re.compile("^$"), Coord(1, 0))
    date_header_cells = CoordRange(Coord(1, 1), Direction.RIGHT)
    metric_cells = CoordRange(Coord(2, 0), Direction.DOWN)


class Parser_1_3_3(VerticalParser):
    platforms = [
        'Naxos',
        'Brepolis',
    ]

    heuristics = (
        RegexCondition(re.compile("^Name:$"), Coord(1, 0))
        & RegexCondition(re.compile("^ID Number:$"), Coord(2, 0))
        & RegexCondition(re.compile("^Type of license:$"), Coord(3, 0))
        & StemmerCondition("", Coord(7, 1))
    )
    date_header_cells = CoordRange(Coord(7, 2), Direction.RIGHT)
    metric_cells = CoordRange(Coord(8, 1), Direction.DOWN)

    def prepare_record(self, *args, **kwargs) -> CounterRecord:
        res = super().prepare_record(*args, **kwargs)
        content = Coord(0, 1).content(self.sheet)
        title = validators.Title(title=content).title
        res.title = title
        return res

    def parse_date(self, cell: Coord) -> datetime.date:
        content = cell.content(self.sheet)
        month_cell_date = validators.Date(date=content).date
        content = Coord(6, 2).content(self.sheet)
        year_cell_date = validators.DateInString(date=content).date
        return datetime.date(year_cell_date.year, month_cell_date.month, 1)


class Parser_1_5_1(VerticalParser):

    platforms = [
        'InCites',
    ]

    heuristics = RegexCondition(re.compile("^Metric$"), Coord(0, 1))
    date_header_cells = CoordRange(Coord(0, 2), Direction.RIGHT)
    title_cells = CoordRange(Coord(1, 0), Direction.DOWN)
    metric_cells = CoordRange(Coord(1, 1), Direction.DOWN)


class Parser_1_5_2(VerticalParser):
    platforms = [
        'Micromedex',
        'Naxos',
    ]

    heuristics = RegexCondition(re.compile("^Metric$"), Coord(0, 0)) & RegexCondition(
        re.compile("^Title$"), Coord(0, 1)
    )
    date_header_cells = CoordRange(Coord(0, 2), Direction.RIGHT)
    metric_cells = CoordRange(Coord(1, 0), Direction.DOWN)
    title_cells = CoordRange(Coord(1, 1), Direction.DOWN)
