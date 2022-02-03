import re
import typing

from ..conditions import RegexCondition
from ..coordinates import Coord, CoordRange, Direction
from ..record import CounterRecord
from .base import BaseParser, VerticalArea


class BR1(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all titles"]

    platforms = [
        "ACM",
        "Ovid",
        "Psychiatry Online",
        "ProQuest",
    ]
    heuristics = RegexCondition(re.compile(r"^Book Report 1 \(R4\)"), Coord(0, 0))

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(7, 8), Direction.RIGHT)
        title_cells = CoordRange(Coord(8, 0), Direction.DOWN)
        dimensions_cells = {"Publisher": CoordRange(Coord(8, 1), Direction.DOWN)}
        title_ids_cells = {
            "DOI": CoordRange(Coord(8, 3), Direction.DOWN),
            "ISBN": CoordRange(Coord(8, 5), Direction.DOWN),
            "ISSN": CoordRange(Coord(8, 6), Direction.DOWN),
        }

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Book Title Requests"
            return res

    areas = [Area]


class BR2(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all titles"]

    platforms = [
        "Access Medicine",
        "ProQuestEbookCentral",
        "APH",
        "WileyOnlineLibrary",
        "ProQuest",
        "SpringerLink",
        "Psychiatry Online",
        "AMS",
    ]
    heuristics = RegexCondition(re.compile(r"^Book Report 2 \(R4\)"), Coord(0, 0))

    class Area(VerticalArea):
        date_header_cells = CoordRange(Coord(7, 8), Direction.RIGHT)
        title_cells = CoordRange(Coord(8, 0), Direction.DOWN)
        dimensions_cells = {"Publisher": CoordRange(Coord(8, 1), Direction.DOWN)}
        title_ids_cells = {
            "DOI": CoordRange(Coord(8, 3), Direction.DOWN),
            "ISBN": CoordRange(Coord(8, 5), Direction.DOWN),
            "ISSN": CoordRange(Coord(8, 6), Direction.DOWN),
        }

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Book Section Requests"
            return res

    areas = [Area]
