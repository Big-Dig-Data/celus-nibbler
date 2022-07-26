import re
import typing

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.parsers.base import BaseTabularParser
from celus_nibbler.sources import DimensionSource

from . import CounterHeaderArea


class Counter5HeaderArea(CounterHeaderArea):
    HEADER_DATE_START = 3
    METRIC_COLUMN_NAMES = ["Metric_Type"]


class BaseCounter5Parser(BaseTabularParser):
    Area: typing.Type[CounterHeaderArea]

    @property
    def name(self):
        return f"counter5.{self.data_format.name}"


class DR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="DR")

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
        DIMENSION_NAMES_MAP = [
            ("Access_Method", {"Access_Method", "Access Method"}),
            ("Data_Type", {"Data Type", "Data_Type"}),
            ("Platform", {"Platform", "platform"}),
            ("Publisher", {"Publisher"}),
        ]

    areas = [Area]


class PR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="PR")

    titles_to_skip: typing.List[str] = ["Total", "All Platforms"]

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
        & RegexCondition(re.compile(r"^Platform Master Report$"), Coord(0, 1))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^PR$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        title_source = None
        DIMENSION_NAMES_MAP = [
            ("Access_Method", {"Access_Method", "Access Method"}),
            ("Data_Type", {"Data Type", "Data_Type"}),
            ("Platform", {"Platform", "platform"}),
        ]

        @property
        def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
            return {
                "Platform": DimensionSource(
                    "Platform", CoordRange(Coord(self.header_row[0].row + 1, 0), Direction.DOWN)
                ),
            }

    areas = [Area]


class TR(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="TR")

    titles_to_skip: typing.List[str] = ["Total", "All Titles"]

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
        & (
            RegexCondition(re.compile(r"^Title Master Report$"), Coord(0, 1))
            | RegexCondition(re.compile(r"^Book Requests \(Excluding OA_Gold\)$"), Coord(0, 1))
        )
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
            ("YOP", {"YOP", "Year of Publication", "year of publication"}),
            ("Publisher", {"Publisher"}),
            ("Platform", {"Platform", "platform"}),
        ]

    areas = [Area]


class IR_M1(BaseCounter5Parser):
    data_format = DataFormatDefinition(name="IR_M1")

    titles_to_skip: typing.List[str] = ["Total", "All"]

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
        & RegexCondition(re.compile(r"^Multimedia Item Requests$"), Coord(0, 1))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^IR_M1$"), Coord(1, 1))
    )

    class Area(Counter5HeaderArea):
        DIMENSION_NAMES_MAP = [
            ("Platform", {"Platform", "platform"}),
            ("Publisher", {"Publisher"}),
        ]

    areas = [Area]
