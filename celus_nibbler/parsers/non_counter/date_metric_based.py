import datetime
import logging
import re
import typing
from abc import ABCMeta

from pydantic import ValidationError

from celus_nibbler import validators
from celus_nibbler.conditions import RegexCondition, SheetIdxCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseArea, BaseParser, MonthMetricDataCells
from celus_nibbler.sources import DimensionSource, OrganizationSource

logger = logging.getLogger(__name__)


class BaseDateMetricArea(BaseArea, metaclass=ABCMeta):
    data_header_metric_offset: Coord = Coord(0, 0)
    data_header_metric_regex: typing.Optional[typing.Pattern] = None
    data_header_month_offset: Coord = Coord(0, 0)
    data_header_month_regex: typing.Optional[typing.Pattern] = None
    data_header_data_skip: int = 1

    def _parse_with_regex(
        self, cell: Coord, regex: typing.Optional[typing.Pattern] = None
    ) -> typing.Optional[str]:
        content = cell.content(self.sheet)
        if regex:
            if found := regex.search(content):
                return found.group(1)
            else:
                return None
        return content

    def get_months(self) -> typing.List[datetime.date]:
        return [e.values[0] for e in self.find_data_cells()]

    def parse_metric(self, cell: Coord, regex: typing.Optional[typing.Pattern] = None) -> str:
        content = self._parse_with_regex(cell, regex)
        return validators.Metric(value=content).value

    def parse_date(
        self, cell: Coord, regex: typing.Optional[typing.Pattern] = None
    ) -> datetime.date:
        content = self._parse_with_regex(cell, regex)
        return validators.Date(value=content).value

    def find_data_cells_in_direction(
        self, direction: Direction
    ) -> typing.List[MonthMetricDataCells]:
        res = []
        try:
            for cell in self.header_cells:
                # parse metric
                metric_coord = cell + self.data_header_metric_offset
                metric = self.parse_metric(metric_coord, self.data_header_metric_regex)

                # parse date
                date_coord = cell + self.data_header_month_offset
                date = self.parse_date(date_coord, self.data_header_month_regex)
                date = date.replace(day=1)

                res.append(
                    MonthMetricDataCells(
                        (date, metric),
                        CoordRange(cell, direction).skip(self.data_header_data_skip),
                    )
                )

        except TableException as e:
            if e.reason in ["out-of-bounds"]:
                pass  # last cell reached
            else:
                raise
        except ValidationError:
            # failed to parse date or metric => assume that input ended
            pass

        logger.debug(f"Found data cells: {res}")
        return res


class VerticalDateMetricArea(BaseDateMetricArea):
    def find_data_cells(self) -> typing.List[MonthMetricDataCells]:
        return self.find_data_cells_in_direction(Direction.DOWN)


class DateMetricBasedParser(BaseParser):
    format_name = "non_counter.date_metric_based"
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None


class MyDateMetricArea(VerticalDateMetricArea):
    data_header_month_regex = re.compile(r"in (\d+\/\d+)$")
    data_header_metric_regex = re.compile(r"^([^ ]+.+[^ ]+) in")

    dimensions_sources = {
        "Extra": DimensionSource("Extra", CoordRange(Coord(1, 0), Direction.DOWN)),
    }
    organization_source = OrganizationSource(
        CoordRange(Coord(1, 1), Direction.DOWN),
        re.compile(r"^MYCONS - (.*)$"),
    )
    header_cells = CoordRange(Coord(0, 2), Direction.RIGHT)


class MyDateMetricBasedParser(DateMetricBasedParser):
    platforms = ["My Platform"]
    heuristics = (
        RegexCondition(re.compile(r"^Extra$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Organization$"), Coord(0, 1))
        & SheetIdxCondition(max=0)
    )

    areas = [MyDateMetricArea]
