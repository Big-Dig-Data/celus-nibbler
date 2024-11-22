import datetime
import logging
import typing
from abc import ABCMeta

from celus_nibbler.aggregator import SameAggregator
from celus_nibbler.parsers.base import BaseHeaderArea

from .base import BaseNonCounterParser

logger = logging.getLogger(__name__)


class BaseDateMetricArea(BaseHeaderArea, metaclass=ABCMeta):
    aggregator = SameAggregator()

    def get_months(self) -> typing.List[datetime.date]:
        return self._get_months_from_header()


class DateMetricBasedParser(BaseNonCounterParser):
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None
