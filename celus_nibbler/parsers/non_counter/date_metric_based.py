import datetime
import logging
import re
import typing
from abc import ABCMeta

from celus_nibbler.aggregator import SameAggregator
from celus_nibbler.conditions import RegexCondition, SheetIdxCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseHeaderArea
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    ExtractParams,
    MetricSource,
    OrganizationSource,
)

from .base import BaseNonCounterParser

logger = logging.getLogger(__name__)


class BaseDateMetricArea(BaseHeaderArea, metaclass=ABCMeta):
    aggregator = SameAggregator()

    def get_months(self, row_offset: typing.Optional[int]) -> typing.List[datetime.date]:
        return self._get_months_from_header(row_offset)


class DateMetricBasedParser(BaseNonCounterParser):
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None


class MyDateMetricArea(BaseDateMetricArea):
    dimensions_sources = {
        "Extra": DimensionSource("Extra", CoordRange(Coord(1, 0), Direction.DOWN)),
    }
    organization_source = OrganizationSource(
        CoordRange(Coord(1, 1), Direction.DOWN),
        ExtractParams(
            regex=re.compile(r"^MYCONS - (.*)$"),
            on_validation_error=TableException.Action.STOP,
        ),
    )

    data_headers = DataHeaders(
        roles=[
            DateSource(
                CoordRange(Coord(0, 2), Direction.RIGHT),
                ExtractParams(regex=re.compile(r"in (\d+\/\d+)$")),
            ),
            MetricSource(
                CoordRange(Coord(0, 2), Direction.RIGHT),
                ExtractParams(regex=re.compile(r"^([^ ]+.+[^ ]+) in")),
            ),
        ],
        data_cells=CoordRange(Coord(1, 2), Direction.RIGHT),
        data_direction=Direction.DOWN,
    )

    @property
    def dimensions(self) -> typing.List[str]:
        return list(self.dimensions_sources.keys())


class MyDateMetricBasedParser(DateMetricBasedParser):
    data_format = DataFormatDefinition(name="MY")
    platforms = ["My Platform"]
    heuristics = (
        RegexCondition(re.compile(r"^Extra$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Organization$"), Coord(0, 1))
        & SheetIdxCondition(max=0)
    )

    areas = [MyDateMetricArea]
