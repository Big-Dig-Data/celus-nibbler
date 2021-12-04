import datetime
import logging
import typing
from abc import ABCMeta, abstractmethod

from celus_nibbler import validators
from celus_nibbler.record import CounterRecord
from celus_nibbler.templates import Sheet
from celus_nibbler.utils import content_check

# from celus_nibbler.warnings import ValueNotUsedWarning

logger = logging.getLogger(__name__)


class GeneralParser(metaclass=ABCMeta):
    date_validation = validators.Date

    def __init__(self, sheet: Sheet, sheet_idx: typing.Optional[int] = None, platform: str = None):
        self.header = None
        self.sheet = sheet
        self.sheet_idx = sheet_idx
        self.platform = platform

    def sheet_name_check(self) -> bool:
        """
        check if the the name of the sheet is expected
        """
        return content_check(self.sheet.name, self.sheet_name)

    def heuristic_check(self) -> bool:
        """
        check if there is an expected content in the expected location of the sheet
        """
        for heuristic in self.heuristics:
            content_check_outcome = content_check(
                self.sheet.values[heuristic.start_row][heuristic.start_col], heuristic.contains
            )
            if content_check_outcome is not True:
                return False
        return True

    def metric_title_check(self) -> bool:
        """
        check if column with metrics has expected title
        """
        row = self.metric_title.start_row
        col = self.metric_title.start_col
        return content_check(self.sheet.values[row][col], self.metric_title.contains)

    @abstractmethod
    def parse_dates(self) -> typing.List[datetime.date]:
        pass

    @abstractmethod
    def parse(self) -> typing.List[CounterRecord]:
        pass

    @property
    @abstractmethod
    def platforms(self) -> typing.List[str]:
        pass
