import re
import typing

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseTabularParser
from celus_nibbler.sources import DimensionSource
from celus_nibbler.validators import BaseValueModel

from . import CounterHeaderArea


class Counter5HeaderArea(CounterHeaderArea):
    HEADER_DATE_COL_START = 3
    METRIC_COLUMN_NAMES = ["Metric_Type", "Metric Type"]
    TITLE_COLUMN_NAMES = ["Title"]
    ORGANIZATION_COLUMN_NAMES = ["Institution_Name", "Institution Name"]


class Counter5ParserAnalyzeMixin:
    def analyze(self) -> typing.List[dict]:
        header = self.get_extras()
        if not header:
            return [
                {
                    "code": "no-header-data-extracted",
                }
            ]
        if "Report_ID" not in header:
            return [
                {
                    "code": "report-id-not-in-header",
                }
            ]
        if header["Report_ID"] != self.data_format.name:
            return [
                {
                    "code": "wrong-report-type",
                    "found": header["Report_ID"],
                    "expected": self.data_format.name,
                }
            ]


class BaseCounter5Parser(Counter5ParserAnalyzeMixin, BaseTabularParser):
    data_format: DataFormatDefinition
    Area: typing.Type[CounterHeaderArea]

    # TODO perhaps implement some kind of YOP validator
    dimensions_validators: typing.Dict[str, typing.Type[BaseValueModel]] = {}

    @property
    def name(self):
        return f"counter5.{self.data_format.name}"

    def get_extras(self) -> dict:
        area = self.Area(self.sheet, self.platform)

        # get header row
        try:
            col = area.header_row.coord.col
            row = area.header_row.coord.row
        except TableException:
            return {}

        if row == 0:
            return {}

        res = {}
        for coord in CoordRange(Coord(col=col, row=row - 1), direction=Direction.UP):
            try:
                key = coord.content(self.sheet).strip()
                if not key:
                    continue
                right_coord = Coord(col=coord.col + 1, row=coord.row)
                value = right_coord.content(self.sheet).strip()
                res[key] = value
            except TableException:
                # Ignore out of bounds
                continue

        return res


class DR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="DR")

    titles_to_skip: typing.List[str] = ["Total", "All Databases", ""]

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
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^DR$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        DIMENSION_NAMES_MAP = [
            ("Access_Method", {"Access_Method", "Access Method"}),
            ("Data_Type", {"Data Type", "Data_Type"}),
            ("Platform", {"Platform"}),
            ("Publisher", {"Publisher"}),
        ]
        TITLE_COLUMN_NAMES = ["Title", "Database"]

    areas = [Area]


class PR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="PR")

    platforms = [
        "AAAS",
        "ACS",
        "Access Medicine",
        "Annual Reviews",
        "APA",
        "APS",
        "ABC-Clio",
        "ASME",
        "ASCE",
        "Blood",
        "CUP",
        "Gale",
        "Emerald",
        "EbscoHost",
        "ICE",
        "IOP",
        "IEEEXplore",
        "JSTOR",
        "MIT",
        "MathSciNet",
        "OSA",
        "Ovid",
        "OUP",
        "Oxford_Music",
        "ProQuest",
        "ProQuestEbookCentral",
        "Sage",
        "ScienceDirect",
        "Scitation",
        "Scopus",
        "RCS",
        "T&F ebooks",
        "WileyOnlineLibrary",
        "WebOfKnowledge",
    ]
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^PR$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        title_source = None
        DIMENSION_NAMES_MAP = [
            ("Access_Method", {"Access_Method", "Access Method"}),
            ("Data_Type", {"Data Type", "Data_Type"}),
            ("Platform", {"Platform"}),
        ]

        @property
        def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
            sources = super().dimensions_sources
            # Set platform as first col after header
            sources["Platform"] = DimensionSource(
                "Platform",
                CoordRange(Coord(self.header_row[0].row + 1, 0), Direction.DOWN),
            )
            return sources

    areas = [Area]


class TR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="TR")

    titles_to_skip: typing.List[str] = ["Total", "All Titles", ""]

    platforms = [
        "AAAS",
        "AACN",
        "AACR",
        "AAP",
        "Access Medicine",
        "ACM",
        "ACS",
        "AHA",
        "AIM",
        "Allen",
        "Annual Reviews",
        "AMS",
        "APA",
        "APS",
        "ARRS",
        "ASCE",
        "ASME",
        "AUA - American Urological Association",
        "BioOne",
        "BIR",
        "Blood",
        "BMJ",
        "Bone and Joint Journal",
        "CUP",
        "CSIRO",
        "Emerald",
        "EbscoHost",
        "Future Science",
        "Gale",
        "GSW",
        "Health Affairs",
        "ICE",
        "IEEEXplore",
        "IOP",
        "JCO - Journal of Clinical Oncology",
        "JNS - Journal of Neurosurgery",
        "JOSPT",
        "JSTOR",
        "Liebert Online",
        "MAG",
        "MIT",
        "Nature_com",
        "NEJM",
        "NRC Res. Press",
        "Ovid",
        "OUP",
        "OSA",
        "Physiology.org",
        "ProQuest",
        "ProQuestEbookCentral",
        "RSC",
        "Sage",
        "ScienceDirect",
        "Scitation",
        "SpringerLink",
        "WileyOnlineLibrary",
        "Tandfonline",
        "T&F ebooks",
        "Topics in Spinal Cord Injury Rehabilitation",
        "Thieme",
    ]

    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & (
            RegexCondition(re.compile(r"^TR$"), Coord(1, 1))
            | RegexCondition(re.compile(r"^TR_B1$"), Coord(1, 1))
        )
    )

    class Area(Counter5HeaderArea):
        DIMENSION_NAMES_MAP = [
            ("Access_Type", {"Access Type", "Access_Type"}),
            ("Access_Method", {"Access_Method", "Access Method"}),
            ("Data_Type", {"Data Type", "Data_Type"}),
            ("Section_Type", {"Section Type", "Section_Type"}),
            ("YOP", {"YOP", "Year of Publication", "Year_of_Publication"}),
            ("Publisher", {"Publisher"}),
            ("Platform", {"Platform"}),
        ]

    areas = [Area]


class IR_M1(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="IR_M1")

    titles_to_skip: typing.List[str] = ["Total", "All", ""]

    platforms = [
        "Access Medicine",
        "Adam Matthew Digital",
        "AlexanderStreet",
        "Artstor",
        "Bloomsbury",
        "Drama Online",
        "Films on Demand",
        "HistoryMakers",
        "Infobase",
        "JSTOR",
        "ProQuest",
    ]
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^IR_M1$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        title_source = None
        title_ids_sources = {}
        DIMENSION_NAMES_MAP = [
            ("Platform", {"Platform"}),
            ("Publisher", {"Publisher"}),
        ]
        ITEM_COLUMN_NAMES = ["Title", "Item"]

        ITEM_DOI_NAMES = Counter5HeaderArea.TITLE_DOI_NAMES
        ITEM_ISBN_NAMES = Counter5HeaderArea.TITLE_ISBN_NAMES
        ITEM_ISSN_NAMES = Counter5HeaderArea.TITLE_ISSN_NAMES
        ITEM_EISSN_NAMES = Counter5HeaderArea.TITLE_EISSN_NAMES
        ITEM_URI_NAMES = Counter5HeaderArea.TITLE_URI_NAMES
        ITEM_PROPRIETARY_NAMES = Counter5HeaderArea.TITLE_PROPRIETARY_NAMES

    areas = [Area]


class IR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="IR")

    titles_to_skip: typing.List[str] = []
    items_to_skip: typing.List[str] = ["Total", "All"]

    platforms = [
        "IEEEXplore",
        "JSTOR",
        "OUP",
        "Ovid",
        "Sage",
    ]
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^IR$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        DIMENSION_NAMES_MAP = [
            ("Article_Version", {"Article Version", "Article_Version"}),
            ("Access_Type", {"Access Type", "Access_Type"}),
            ("Access_Method", {"Access_Method", "Access Method"}),
            ("Data_Type", {"Data Type", "Data_Type"}),
            ("Parent_Data_Type", {"Parent Data Type", "Parent_Data_Type"}),
            ("YOP", {"YOP", "Year of Publication", "Year_of_Publication"}),
            ("Platform", {"Platform"}),
            ("Publisher", {"Publisher"}),
        ]
        TITLE_COLUMN_NAMES = ["Parent_Title"]
        ITEM_COLUMN_NAMES = ["Title", "Item"]

        ITEM_DOI_NAMES = Counter5HeaderArea.TITLE_DOI_NAMES
        ITEM_ISBN_NAMES = Counter5HeaderArea.TITLE_ISBN_NAMES
        ITEM_ISSN_NAMES = Counter5HeaderArea.TITLE_ISSN_NAMES
        ITEM_EISSN_NAMES = Counter5HeaderArea.TITLE_EISSN_NAMES
        ITEM_URI_NAMES = Counter5HeaderArea.TITLE_URI_NAMES
        ITEM_PROPRIETARY_NAMES = Counter5HeaderArea.TITLE_PROPRIETARY_NAMES
        ITEM_AUTHORS_NAMES = {"Authors"}
        ITEM_PUBLICATION_DATE_NAMES = {"Publication_Date"}

        TITLE_DOI_NAMES = {"Parent_DOI"}
        TITLE_ISBN_NAMES = {"Parent_ISBN"}
        TITLE_ISSN_NAMES = {"Parent_Print_ISSN"}
        TITLE_EISSN_NAMES = {"Parent_Online_ISSN"}
        TITLE_URI_NAMES = {"Parent_URI"}
        TITLE_PROPRIETARY_NAMES = {"Parent_Proprietary_ID"}

    areas = [Area]
