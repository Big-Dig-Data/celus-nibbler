import datetime
import typing
from abc import ABCMeta

from celus_nibbler.parsers.base import BaseHeaderArea

from .base import BaseNonCounterParser


class BaseGenericArea(BaseHeaderArea, metaclass=ABCMeta):
    def get_months(self) -> typing.List[datetime.date]:
        if self.date_source:
            self.find_data_cells(lambda x: x, lambda x, y: None)
            return self._get_months_from_column(0, self.row_offset)
        else:
            # Extract months from header_data
            # populate Area.row_offset
            return self._get_months_from_header()


class BaseGenericParser(BaseNonCounterParser):
    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None
