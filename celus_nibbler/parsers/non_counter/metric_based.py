import datetime
import re
import typing
from abc import ABCMeta

from pydantic import ValidationError

from celus_nibbler.aggregator import SameAggregator
from celus_nibbler.conditions import RegexCondition, SheetNameRegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseHeaderArea
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    OrganizationSource,
    ExtractParams,
)

from .base import BaseNonCounterParser


class BaseMetricArea(BaseHeaderArea, metaclass=ABCMeta):
    aggregator = SameAggregator()

    def get_months(self, row_offset: typing.Optional[int]) -> typing.List[datetime.date]:
        res = set()
        try:
            for cell in self.date_source.source:

                if row_offset:
                    cell = cell.with_row_offset(row_offset)

                date = self.parse_date(cell)
                res.add(date.replace(day=1))

        except TableException as e:
            if e.action == TableException.Action.STOP:
                pass
            else:
                raise
        except ValidationError:
            # failed to parse date => assume that input ended
            pass

        return list(res)


class MetricBasedParser(BaseNonCounterParser):
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None


class MyMetricArea(BaseMetricArea):
    date_source = DateSource(
        CoordRange(Coord(15, 1), Direction.DOWN),
        extract_params=ExtractParams(on_validation_error=TableException.Action.STOP),
    )
    dimensions_sources = {
        "Dimension1": DimensionSource("Dimension1", CoordRange(Coord(15, 2), Direction.DOWN)),
    }
    organization_source = OrganizationSource(
        CoordRange(Coord(15, 3), Direction.DOWN),
        extract_params=ExtractParams(on_validation_error=TableException.Action.STOP),
    )

    data_headers = DataHeaders(
        roles=[
            MetricSource(
                CoordRange(Coord(14, 7), Direction.RIGHT),
            )
        ],
        data_cells=CoordRange(Coord(15, 7), Direction.RIGHT),
        data_direction=Direction.DOWN,
    )

    @property
    def dimensions(self) -> typing.List[str]:
        return list(self.dimensions_sources.keys())


class MyMetricBasedParser(MetricBasedParser):
    data_format = DataFormatDefinition(name="MY")
    platforms = ["My Platform"]
    heuristics = (
        RegexCondition(re.compile(r"^My Online Summary Usage Report$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Dimension1$"), Coord(14, 2))
        & RegexCondition(re.compile(r"^Organization$"), Coord(14, 3))
        & ~SheetNameRegexCondition(re.compile(r"^ips_"))
    )

    areas = [MyMetricArea]
