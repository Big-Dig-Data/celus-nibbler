import re
import typing

from celus_nigiri import CounterRecord

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseTabularParser
from celus_nibbler.validators import BaseValueModel

from . import CounterHeaderArea


class BaseCounter4Parser(BaseTabularParser):
    Area: typing.Type[CounterHeaderArea]

    # TODO perhaps implement some kind of YOP validator
    dimensions_validators: typing.Dict[str, typing.Type[BaseValueModel]] = {}

    HEADER_DATA_OFFSETS = (
        ("Created", 1),
        ("Reporting_Period", 3),
        ("Institution_Name", 6),
    )

    REPORT_TYPE_NAME: str

    @property
    def name(self):
        return f"counter4.{self.data_format.name}"

    def get_extras(self) -> dict:
        area = self.Area(self.sheet, self.platform)

        # get header row
        try:
            col = area.header_row.coord.col
            row = area.header_row.coord.row
        except TableException:
            return {}

        res = {}
        # use offsets to extract data
        for key, offset in self.HEADER_DATA_OFFSETS:
            coord = Coord(row=row - offset, col=col)

            if coord.row < 0:
                # out of bounds
                continue
            try:
                res[key] = coord.content(self.sheet)
            except (TableException, IndexError):
                continue

        return res

    def analyze(self) -> typing.List[dict]:
        area = self.Area(self.sheet, self.platform)

        # get header row
        try:
            col = area.header_row.coord.col
            row = area.header_row.coord.row
        except TableException:
            return [{"code": "header-row-not-found"}]

        rtn_coord = Coord(col=col, row=row - 7)
        if rtn_coord.row < 0:
            return [{"code": "report-name-not-in-header"}]

        res = []
        rtn = rtn_coord.content(self.sheet)
        if self.REPORT_TYPE_NAME != rtn:
            res.append(
                {
                    "code": "wrong-report-type",
                    "found": rtn,
                    "expected": self.REPORT_TYPE_NAME,
                }
            )

        return res


class BR1(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Book Report 1 (R4)"

    data_format = DataFormatDefinition(name="BR1")

    titles_to_skip: typing.List[str] = ["Total", "Total for all titles", ""]

    platforms = [
        "ACM",
        "Ovid",
        "Psychiatry Online",
        "ProQuest",
    ]
    heuristics = RegexCondition(re.compile(r"^Book Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        TITLE_COLUMN_NAMES = ["Book"]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Book Title Requests"
            return res

    areas = [Area]


class BR2(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Book Report 2 (R4)"

    data_format = DataFormatDefinition(name="BR2")

    titles_to_skip: typing.List[str] = ["Total", "Total for all titles", ""]

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
        TITLE_COLUMN_NAMES = ["Book"]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Book Section Requests"
            return res

    areas = [Area]


class BR3(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Book Report 3 (R4)"

    data_format = DataFormatDefinition(name="BR3")

    titles_to_skip: typing.List[str] = ["Total", "Total for all titles", ""]

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
        TITLE_COLUMN_NAMES = ["Book"]
        METRIC_COLUMN_NAMES = ["Access Denied Category"]

    areas = [Area]


class DB1(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Database Report 1 (R4)"

    data_format = DataFormatDefinition(name="DB1")

    titles_to_skip: typing.List[str] = ["Total", "Total for all databases", ""]
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
        TITLE_COLUMN_NAMES = ["Database"]
        METRIC_COLUMN_NAMES = ["User Activity"]

    areas = [Area]


class DB2(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Database Report 2 (R4)"

    data_format = DataFormatDefinition(name="DB2")

    titles_to_skip: typing.List[str] = ["Total", "Total for all databases", ""]
    platforms = [
        "ProQuest",
        "Tandfonline",
    ]
    heuristics = RegexCondition(re.compile(r"^Database Report 2 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        TITLE_COLUMN_NAMES = ["Database"]
        METRIC_COLUMN_NAMES = ["Access denied category"]

    areas = [Area]


class PR1(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Platform Report 1 (R4)"

    data_format = DataFormatDefinition(name="PR1")

    platforms = [
        "ProQuest",
        "Tandfonline",
    ]
    heuristics = RegexCondition(re.compile(r"^Platform Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        METRIC_COLUMN_NAMES = ["User Activity"]

        @property
        def title_source(self):
            return None

    areas = [Area]


class JR1(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Journal Report 1 (R4)"

    data_format = DataFormatDefinition(name="JR1")

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals", ""]
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
        TITLE_COLUMN_NAMES = ["Journal"]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "FT Article Requests"
            return res

    areas = [Area]


class JR1a(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Journal Report 1a (R4)"

    data_format = DataFormatDefinition(name="JR1a")

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals", ""]
    platforms = [
        "Liebert Online",
        "Ovid",
        "Sage",
    ]
    heuristics = RegexCondition(re.compile(r"^Journal Report 1a \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        TITLE_COLUMN_NAMES = ["Journal"]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Archive Article Requests"
            return res

    areas = [Area]


class JR1GOA(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Journal Report 1 GOA (R4)"

    data_format = DataFormatDefinition(name="JR1GOA")

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals", ""]
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
        TITLE_COLUMN_NAMES = ["Journal"]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Gold Open Access Article Requests"
            return res

    areas = [Area]


class JR2(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Journal Report 2 (R4)"

    data_format = DataFormatDefinition(name="JR2")

    titles_to_skip: typing.List[str] = ["Total", "Total for all journals", ""]
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
        TITLE_COLUMN_NAMES = ["Journal"]
        METRIC_COLUMN_NAMES = ["Access Denied Category"]

    areas = [Area]


class MR1(BaseCounter4Parser):
    REPORT_TYPE_NAME = "Multimedia Report 1 (R4)"

    data_format = DataFormatDefinition(name="MR1")

    titles_to_skip: typing.List[str] = ["Total", "Total for all collections", ""]
    platforms = [
        "ProQuest",
    ]
    heuristics = RegexCondition(re.compile(r"^Multimedia Report 1 \(R4\)"), Coord(0, 0))

    class Area(CounterHeaderArea):
        DIMENSION_NAMES_MAP = [
            ("Content Provider", {"Content Provider"}),
            ("Platform", {"Platform"}),
        ]
        TITLE_COLUMN_NAMES = ["Collection"]

        def prepare_record(self, *args, **kwargs) -> CounterRecord:
            res = super().prepare_record(*args, **kwargs)
            res.metric = "Multimedia Full Content Unit Requests"
            return res

    areas = [Area]
