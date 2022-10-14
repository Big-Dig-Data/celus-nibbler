import datetime
import re
import typing
from abc import ABCMeta

from pydantic import ValidationError

from celus_nibbler import validators
from celus_nibbler.aggregator import SameAggregator
from celus_nibbler.conditions import RegexCondition, SheetNameRegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseArea, BaseParser, MetricDataCells
from celus_nibbler.sources import DateSource, DimensionSource, OrganizationSource


class BaseMetricArea(BaseArea, metaclass=ABCMeta):
    aggregator = SameAggregator()

    def get_months(self) -> typing.List[datetime.date]:
        res = set()
        try:
            for cell in self.date_source.source:
                date = self.parse_date(cell)
                res.add(date.replace(day=1))

        except TableException as e:
            if e.reason in ["out-of-bounds"]:
                pass  # last cell reached
            else:
                raise
        except ValidationError:
            # failed to parse date => assume that input ended
            pass

        return list(res)

    def parse_metric(self, cell: Coord) -> str:
        content = cell.content(self.sheet)
        return validators.Metric(value=content).value


class VerticalMetricArea(BaseMetricArea):
    def find_data_cells(self) -> typing.List[MetricDataCells]:
        res = []
        for cell in self.header_cells:
            try:
                metric = self.parse_metric(cell)
                res.append(MetricDataCells(metric, CoordRange(cell, Direction.DOWN).skip(1)))
            except TableException as e:
                if e.reason == "out-of-bounds":
                    # We reached the end of row
                    break
                raise
            except ValidationError:
                # Found a content which can't be parsed
                # We can exit here
                break

        return res


class MetricBasedParser(BaseParser):
    format_name = "non_counter.metric_based"
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None


class MyMetricArea(VerticalMetricArea):
    date_source = DateSource(CoordRange(Coord(15, 1), Direction.DOWN))
    dimensions_sources = {
        "Dimension1": DimensionSource("Dimension1", CoordRange(Coord(15, 2), Direction.DOWN)),
    }
    organization_source = OrganizationSource(CoordRange(Coord(15, 3), Direction.DOWN))

    header_cells = CoordRange(Coord(14, 7), Direction.RIGHT)


class MyMetricBasedParser(MetricBasedParser):
    platforms = ["My Platform"]
    heuristics = (
        RegexCondition(re.compile(r"^My Online Summary Usage Report$"), Coord(0, 0))
        & RegexCondition(re.compile(r"^Dimension1$"), Coord(14, 2))
        & RegexCondition(re.compile(r"^Organization$"), Coord(14, 3))
        & ~SheetNameRegexCondition(re.compile(r"^ips_"))
    )

    areas = [MyMetricArea]
