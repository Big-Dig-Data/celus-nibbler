import datetime
import logging
import typing
from abc import ABCMeta, abstractmethod

from jellyfish import porter_stem
from unidecode import unidecode

from celus_nibbler import validators
from celus_nibbler.reader import TableReader
from celus_nibbler.record import CounterRecord

# from celus_nibbler.warnings import ValueNotUsedWarning

logger = logging.getLogger(__name__)


class GeneralParser(metaclass=ABCMeta):
    date_validation = validators.Date

    def __init__(
        self, table: TableReader, sheet_idx: typing.Optional[int] = None, platform: str = None
    ):
        self.header = None
        self.table = table
        self.sheet_idx = sheet_idx
        self.platform = platform

    def heuristic_check(self) -> bool:
        """
        check if there is an expected content in the expected location of the table
        """
        for heuristic in self.heuristics:
            if self.table[heuristic.start_row][heuristic.start_col] != heuristic.content:
                return False
        return True

    def metric_title_check(self) -> bool:
        """
        check if column with metrics has expected title
        """
        row = self.metric_title.start_row
        col = self.metric_title.start_col
        expected_content = self.metric_title.content
        given_content = self.table[row][col]
        if isinstance(expected_content, str):
            expected_content = porter_stem(unidecode(expected_content.strip()).lower())
        if isinstance(given_content, str):
            given_content = porter_stem(unidecode(given_content.strip()).lower())
        return given_content == expected_content

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
