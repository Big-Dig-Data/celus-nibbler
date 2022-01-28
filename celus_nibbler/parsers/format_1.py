import datetime
import re
import typing

from celus_nibbler import validators
from celus_nibbler.conditions import RegexCondition, StemmerCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.record import CounterRecord

from .base import BaseParser, VerticalArea


class Parser_1_1_1(BaseParser):
    platforms = [
        "SUS_FLVC_Ulrichs",
    ]

    heuristics = (
        RegexCondition(re.compile("^Total Searches from"), Coord(0, 0))
        & RegexCondition(re.compile("^Full record views from"), Coord(4, 0))
        & RegexCondition(re.compile("^Usage Type$"), Coord(5, 0))
    )

    class Area2(VerticalArea):
        date_header_cells = CoordRange(Coord(5, 1), Direction.RIGHT)
        metric_cells: typing.Optional[CoordRange] = CoordRange(Coord(5, 0), Direction.DOWN)

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            # Update platform name
            res = super().prepare_record(*args, **kwargs)
            dimension_data: typing.Dict[str, str] = res.dimension_data or {}
            if self.sheet.name:
                dimension_data["Library"] = self.sheet.name
            res.dimension_data = dimension_data
            return res

    class Area1(Area2):
        metric_cells = None

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Total Searches"
            return res

    areas = [Area1, Area2]


class Parser_1_2(BaseParser):
    platforms = [
        'ClassiquesGarnierNumerique',
    ]

    heuristics = (
        RegexCondition(re.compile("^Title$"), Coord(0, 0))
        & RegexCondition(re.compile("^Metric$"), Coord(0, 1))
        & RegexCondition(re.compile("^Authentization$"), Coord(0, 2))
    )

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(0, 3), Direction.RIGHT)
        metric_cells = CoordRange(Coord(1, 1), Direction.DOWN)
        title_cells = CoordRange(Coord(1, 0), Direction.DOWN)
        dimensions_cells = {"Authentization": CoordRange(Coord(1, 2), Direction.DOWN)}

    areas = [Area]


class Parser_1_3_1(BaseParser):

    platforms = [
        'Naxos',
        'CHBeck',
        'Knovel',
        'Uptodate',
        'SciFinder',
        'SciVal',
    ]

    heuristics = RegexCondition(re.compile("^Metric$"), Coord(0, 0))

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(0, 1), Direction.RIGHT)
        metric_cells = CoordRange(Coord(1, 0), Direction.DOWN)

    areas = [Area]


class Parser_1_3_2(BaseParser):

    platforms = [
        'Bisnode',
        'CHBeck',
        'ACS',
        'Micromedex',
        'SpringerLink',
        'Naxos',
    ]
    heuristics = RegexCondition(re.compile("^$"), Coord(1, 0))

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(1, 1), Direction.RIGHT)
        metric_cells = CoordRange(Coord(2, 0), Direction.DOWN)

    areas = [Area]


class Parser_1_3_3(BaseParser):
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

    class Area(VerticalArea):
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

    areas = [Area]


class Parser_1_1_4(BaseParser):
    platforms = [
        'SciFinder',
        'SciFinder_n',
    ]

    # fmt: off
    heuristics = (
        RegexCondition(re.compile("^Name$"), Coord(0, 0))
        & RegexCondition(re.compile("^Type$"), Coord(0, 1))
    )
    # fmt: on

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(0, 2), Direction.RIGHT)
        dimensions_cells = {"Name": CoordRange(Coord(1, 0), Direction.DOWN)}

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            # Update platform name
            res = super().prepare_record(*args, **kwargs)
            if self.sheet.name == "SFoW":
                res.platform = "SciFinder"
            elif self.sheet.name == "SFn":
                res.platform = "SciFinder_n"
            return res

    areas = [Area]


class Parser_1_5_1(BaseParser):

    platforms = [
        'InCites',
    ]

    heuristics = RegexCondition(re.compile("^Metric$"), Coord(0, 1))

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(0, 2), Direction.RIGHT)
        title_cells = CoordRange(Coord(1, 0), Direction.DOWN)
        metric_cells = CoordRange(Coord(1, 1), Direction.DOWN)

    areas = [Area]


class Parser_1_5_2(BaseParser):
    platforms = [
        'Micromedex',
        'Naxos',
    ]

    # fmt: off
    heuristics = (
        RegexCondition(re.compile("^Metric$"), Coord(0, 0))
        & RegexCondition(re.compile("^Title$"), Coord(0, 1))
    )
    # fmt: on

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(0, 2), Direction.RIGHT)
        metric_cells = CoordRange(Coord(1, 0), Direction.DOWN)
        title_cells = CoordRange(Coord(1, 1), Direction.DOWN)

    areas = [Area]
