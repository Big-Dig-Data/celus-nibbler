import re

from celus_nibbler.conditions import RegexCondition
from celus_nibbler.coordinates import Coord
from celus_nibbler.data_headers import DataFormatDefinition

from . import c5


class DR(c5.DR):
    data_format = DataFormatDefinition(name="DR51")
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^DR$"), Coord(1, 1))
        & RegexCondition(re.compile(r"^Release$"), Coord(2, 0))
        & RegexCondition(re.compile(r"^5.1$"), Coord(2, 1))
    )


class PR(c5.PR):
    data_format = DataFormatDefinition(name="PR51")
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^PR$"), Coord(1, 1))
        & RegexCondition(re.compile(r"^Release$"), Coord(2, 0))
        & RegexCondition(re.compile(r"^5.1$"), Coord(2, 1))
    )


class TR(c5.TR):
    data_format = DataFormatDefinition(name="TR51")
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^TR$"), Coord(1, 1))
        & RegexCondition(re.compile(r"^Release$"), Coord(2, 0))
        & RegexCondition(re.compile(r"^5.1$"), Coord(2, 1))
    )


class IR(c5.IR):
    data_format = DataFormatDefinition(name="IR51")
    heuristics = (
        RegexCondition(re.compile(r"^Report_Name$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Report_ID$"), Coord(1, 0))
        & RegexCondition(re.compile(r"^IR$"), Coord(1, 1))
        & RegexCondition(re.compile(r"^Release$"), Coord(2, 0))
        & RegexCondition(re.compile(r"^5.1$"), Coord(2, 1))
    )
