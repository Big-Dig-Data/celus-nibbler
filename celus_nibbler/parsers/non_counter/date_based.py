import datetime
import logging
import typing

from celus_nibbler.parsers.base import BaseHeaderArea

logger = logging.getLogger(__name__)


class BaseDateArea(BaseHeaderArea):
    def get_months(self) -> typing.List[datetime.date]:
        return self._get_months_from_header()
