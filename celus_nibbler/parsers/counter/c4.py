import re
import typing

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseParser
from celus_nibbler.record import CounterRecord

from . import CounterHeaderArea


class BR1(BaseParser):
    format_name = "BR1"

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
    format_name = "BR2"

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
    format_name = "BR3"

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
    format_name = "DB1"

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
    format_name = "DB2"

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
    format_name = "PR1"

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
    format_name = "JR1"

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


class JR1a(BaseParser):
    format_name = "JR1a"

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals"]
    platforms = [
        "Liebert Online",
        "Ovid",
        "Sage",
    ]
    heuristics = RegexCondition(re.compile(r"^Journal Report 1a \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Archive Article Requests"
            return res

    areas = [Area]


class JR1GOA(BaseParser):
    format_name = "JR1GOA"

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals"]
    platforms = [
        "Journal of Clinical Psychiatry",
        "Liebert Online",
        "OUP",
        "ProQuest",
        "Psychiatry Online",
        "Sage",
        "ScienceDirect",
        "SpringerLink",
        "Tandfonline",
        "World Scientific",
    ]
    heuristics = RegexCondition(re.compile(r"^Journal Report 1 GOA \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Gold Open Access Article Requests"
            return res

    areas = [Area]


class JR2(BaseParser):
    format_name = "JR2"

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals"]
    heuristics = RegexCondition(re.compile(r"^Journal Report 2 \(R4\)"), Coord(0, 0))
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


class MR1(BaseParser):
    format_name = "MR1"

    titles_to_skip: typing.List[str] = ["Total", "Total for all collections"]
    platforms = [
        "ProQuest",
    ]
    heuristics = RegexCondition(re.compile(r"^Multimedia Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        DIMENSION_NAMES_MAP = [
            ("Content Provider", {"Content Provider"}),
            ("Platform", {"Platform", "platform"}),
        ]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Multimedia Full Content Unit Requests"
            return res

    areas = [Area]
