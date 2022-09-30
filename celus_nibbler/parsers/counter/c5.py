import re
import typing

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.definitions import Source
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseParser

from . import CounterHeaderArea


class Counter5HeaderArea(CounterHeaderArea):
    HEADER_DATE_START = 3

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


class DR(BaseParser):
    format_name = "DR"

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
        pass

    areas = [Area]


class PR(BaseParser):
    format_name = "PR"

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
        @property
        def title_cells(self):
            return None

        @property
        def dimensions_cells(self) -> typing.Dict[str, Source]:
            return {
                "Platform": CoordRange(Coord(self.header_row[0].row + 1, 0), Direction.DOWN),
            }

    areas = [Area]


class TR(BaseParser):
    format_name = "TR"

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
        pass

    areas = [Area]
