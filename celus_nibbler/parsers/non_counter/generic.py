import datetime
import typing
from abc import ABCMeta

from celus_nibbler.parsers.base import BaseHeaderArea

from .base import BaseNonCounterParser


class BaseGenericArea(BaseHeaderArea, metaclass=ABCMeta):
    def get_months(self, row_offset: typing.Optional[int]) -> typing.List[datetime.date]:
        if self.date_source is None:
            # Extract months from header_data
            return self._get_months_from_column(row_offset)
        else:
            return self._get_months_from_header(row_offset)


class BaseGenericParser(BaseNonCounterParser):
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None
