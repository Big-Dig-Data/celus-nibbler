import itertools
import re
import typing

from pydantic import ValidationError

from ..conditions import RegexCondition
from ..coordinates import Coord, CoordRange, Direction
from ..errors import TableException
from ..record import CounterRecord
from .base import BaseParser, VerticalArea


class CounterHeaderArea(VerticalArea):
    MAX_HEADER_ROW = 50
    DOI_NAMES = {
        "Book DOI",
        "DOI",
        "Journal DOI",
    }
    ISBN_NAMES = {"ISBN"}
    ISSN_NAMES = {"ISSN", "Print ISSN", "Online ISSN"}
    DIMENSION_NAMES_MAP = [
        ("Publisher", {"Publisher"}),
    ]

    @property
    def header_row(self) -> CoordRange:
        """Find the line where counter header is"""
        # Right now it picks a first row with more than one column were the last column
        # is a date

        # Try to use cached
        if hasattr(self, '_header_row'):
            return self._header_row

        for idx in range(self.MAX_HEADER_ROW):
            crange = CoordRange(Coord(idx, 1), Direction.RIGHT)

            last = None
            for cell in crange:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip():
                        last = cell
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        break  # last cell reached
                    raise

            if last:
                try:
                    # Throws exception when it is not a date
                    self.parse_date(last)
                    self._header_row = CoordRange(Coord(idx, 0), Direction.RIGHT)
                    return self._header_row
                except ValidationError:
                    continue  # doesn't match the header

        raise TableException(
            sheet=self.sheet.sheet_idx,
            reason="no-counter-header-found",
        )

    @property
    def date_header_cells(self):
        # First date which is parsed in the header
        for cell in itertools.islice(self.header_row, 1, None):
            try:
                # Throws exception when it is not a date
                self.parse_date(cell)
                return CoordRange(cell, Direction.RIGHT)
            except ValidationError:
                continue  # doesn't match the header

    @property
    def title_cells(self):
        return CoordRange(Coord(self.header_row[0].row + 1, 0), Direction.DOWN)

    @property
    def title_ids_cells(self):
        ids_cells = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached

            if content.strip() in self.DOI_NAMES:
                ids_cells["DOI"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
            elif content.strip() in self.ISBN_NAMES:
                ids_cells["ISBN"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
            elif content.strip() in self.ISSN_NAMES:
                ids_cells["ISSN"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)

        return ids_cells

    @property
    def dimensions_cells(self):
        dim_cells = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached
            for dimension, names in self.DIMENSION_NAMES_MAP:
                if content.strip() in names:
                    dim_cells[dimension] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)

        return dim_cells


class BR1(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all titles"]

    platforms = [
        "ACM",
        "Ovid",
        "Psychiatry Online",
        "ProQuest",
    ]
    heuristics = RegexCondition(re.compile(r"^Book Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
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

    class Area(CounterHeaderArea):
        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Book Section Requests"
            return res

    areas = [Area]


class BR3(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all titles"]

    platforms = [
        "AMS",
        "Ovid",
        "ProQuest",
        "Psychiatry Online",
        "ScienceDirect",
        "SpringerLink",
        "WileyOnlineLibrary",
    ]
    heuristics = RegexCondition(re.compile(r"^Book Report 3 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        @property
        def metric_cells(self):
            for cell in self.header_row:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip().lower() == "Access Denied Category".lower():

                        return CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        raise TableException(
                            value="Access Denied Category",
                            row=cell.row,
                            sheet=self.sheet.sheet_idx,
                            reason="missing-metric-in-header",
                        )

    areas = [Area]


class DB1(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all databases"]
    platforms = [
        "Encyclopaedia Britannica",
        "Journal of Clinical Psychiatry",
        "Ovid",
        "ProQuest",
        "Tandfonline",
        "WebOfKnowledge",
    ]
    heuristics = RegexCondition(re.compile(r"^Database Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        @property
        def metric_cells(self):
            for cell in self.header_row:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip().lower() == "User Activity".lower():

                        return CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        raise TableException(
                            value="User Activity",
                            row=cell.row,
                            sheet=self.sheet.sheet_idx,
                            reason="missing-metric-in-header",
                        )

    areas = [Area]


class DB2(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all databases"]
    platforms = [
        "ProQuest",
        "Tandfonline",
    ]
    heuristics = RegexCondition(re.compile(r"^Database Report 2 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        @property
        def metric_cells(self):
            for cell in self.header_row:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip().lower() == "Access denied category".lower():

                        return CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        raise TableException(
                            value="Access denied category",
                            row=cell.row,
                            sheet=self.sheet.sheet_idx,
                            reason="missing-metric-in-header",
                        )

    areas = [Area]


class PR1(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all platforms"]
    platforms = [
        "ProQuest",
        "Tandfonline",
    ]
    heuristics = RegexCondition(re.compile(r"^Platform Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        @property
        def title_cells(self):
            return None

        @property
        def platform_cells(self) -> CoordRange:
            return CoordRange(Coord(self.header_row[0].row + 1, 0), Direction.DOWN)

        @property
        def metric_cells(self):
            for cell in self.header_row:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip().lower() == "User Activity".lower():

                        return CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        raise TableException(
                            value="User Activity",
                            row=cell.row,
                            sheet=self.sheet.sheet_idx,
                            reason="missing-metric-in-header",
                        )

    areas = [Area]


class JR1(BaseParser):
    titles_to_skip: typing.List[str] = ["Total", "Total for all journals"]
    platforms = [
        "AACR",
        "AACN",
        "AAP",
        "AASM",
        "ACM",
        "AJTMH",
        "AHA",
        "Allen",
        "AMA",
        "AMS",
        "Annual Reviews",
        "ARRS",
        "Berghahn Journals",
        "Bone and Joint Journal",
        "CS",
        "CSIRO",
        "DeGruyter",
        "Edinburgh University Press",
        "Emerald",
        "Gale",
        "Health Affairs",
        "IET",
        "IOS Press",
        "JCO - Journal of Clinical Oncology",
        "JOSPT",
        "JNS - Journal of Neurosurgery",
        "Liebert Online",
        "Journal of Clinical Psychiatry",
        "MAG",
        "Nature_com",
        "OUP",
        "Ovid",
        "ProQuest",
        "Psychiatry Online",
        "Radiology & Radiographics",
        "Sage",
        "ScienceDirect",
        "SpringerLink",
        "Tandfonline",
        "Thieme",
        "Topics in Spinal Cord Injury Rehabilitation",
        "WileyOnlineLibrary",
        "World Scientific",
    ]
    heuristics = RegexCondition(re.compile(r"^Journal Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "FT Article Requests"
            return res

    areas = [Area]
