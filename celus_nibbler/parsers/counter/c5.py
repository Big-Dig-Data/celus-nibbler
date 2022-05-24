import re
import typing

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseParser

from . import CounterHeaderArea


class Counter5HeaderArea(CounterHeaderArea):
    HEADER_DATE_START = 3


class DR(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "All Databases"]

    platforms = [
        "AAAS",
        "APA",
        "ABC-Clio",
        "Gale",
        "Emerald",
        "EbscoHost",
        "MathSciNet",
        "Ovid",
        "ProQuest",
        "Scopus",
        "RCS",
        "WileyOnlineLibrary",
        "WebOfKnowledge",
    ]
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Database Master Report$"), Coord(0, 1))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^DR$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        @property
        def metric_cells(self):
            for cell in self.header_row:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip().lower() == "Metric_Type".lower():

                        return CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        raise TableException(
                            value="Metric_Type",
                            row=cell.row,
                            sheet=self.sheet.sheet_idx,
                            reason="missing-metric-in-header",
                        )

    areas = [Area]
