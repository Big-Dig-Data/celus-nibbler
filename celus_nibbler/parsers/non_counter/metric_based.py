import datetime
import typing
from abc import ABCMeta

from celus_nibbler.aggregator import SameAggregator
from celus_nibbler.parsers.base import BaseHeaderArea

from .base import BaseNonCounterParser


class BaseMetricArea(BaseHeaderArea, metaclass=ABCMeta):
    aggregator = SameAggregator()

    def get_months(self) -> typing.List[datetime.date]:
        # populate Area.row_offset
        self.find_data_cells(lambda x: x, lambda x: None)
        return self._get_months_from_column(0, self.row_offset)


class MetricBasedParser(BaseNonCounterParser):
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None
