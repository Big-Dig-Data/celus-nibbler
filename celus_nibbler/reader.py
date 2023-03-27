import csv
import io
import itertools
import logging
import pathlib
import tempfile
from abc import ABCMeta, abstractmethod
from collections import deque
from functools import lru_cache
from io import BytesIO, StringIO
from typing import IO, Any, Dict, List, Optional, Sequence, Union

import openpyxl
from celus_nigiri.counter5 import Counter5ReportBase
from celus_nigiri.csv_detect import DEFAULT_ENCODING, detect_csv_dialect, detect_file_encoding

logger = logging.getLogger(__name__)


class SheetReader(metaclass=ABCMeta):
    @property
    @abstractmethod
    def sheet_idx(self) -> int:
        pass

    @property
    @abstractmethod
    def name(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def extra(self) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __next__(self):
        pass

    @abstractmethod
    def close(self):
        pass


class CsvSheetReader(SheetReader):
    """
    Class representing a single table
    """

    WINDOW_SIZE = 1000  # number of lines to be cached
    sheet_idx = 0
    name = None
    extra = None

    def __init__(
        self,
        sheet_idx: int,
        name: Optional[str],
        file: IO[str],
        encoding: str = DEFAULT_ENCODING,
        window_size: int = WINDOW_SIZE,
        delimiters: Optional[str] = None,
        dialect: Optional[str] = None,
    ):
        self.name = name
        self.sheet_idx = sheet_idx
        self.file = file

        self.dialect = dialect or detect_csv_dialect(file)
        self.csv_reader = csv.reader(file, self.dialect)
        # Load basic window
        self.window_start = 0
        self.window_size = window_size
        self.window: Optional[deque[List[str]]] = None
        self.update_window(0)

    def update_window(self, window_start: int):
        if self.window is not None and self.window_start <= window_start:
            # moving forward
            if self.window_start <= window_start - self.window_size:
                # not overlapping
                start = window_start - self.window_start - self.window_size
                stop = window_start - self.window_start
                self.window = deque(
                    itertools.islice(
                        self.csv_reader,
                        start,
                        stop,
                    ),
                    self.window_size,
                )
                self.window_start = window_start
            else:
                # overlapping
                while self.window_start < window_start:
                    self.inc_window()
            return

        # moving backward or at start
        self.file.seek(0)
        self.csv_reader = csv.reader(self.file, self.dialect)
        self.window_start = window_start
        self.window = deque(
            itertools.islice(
                self.csv_reader, self.window_start, self.window_start + self.window_size
            ),
            self.window_size,
        )

    def inc_window(self):
        try:
            row = next(self.csv_reader)
            self.window_start += 1
            self.window.append(row)
        except StopIteration:
            if len(self.window):
                self.window_start += 1
                self.window.popleft()

    @lru_cache(WINDOW_SIZE * 2)  # cache lines to avoid rewinding while reading the header
    def __getitem__(self, item) -> Sequence[str]:
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")

        # in current window
        if self.window_start <= item < (self.window_start + self.window_size):
            if self.window_start + len(self.window) < item:
                raise IndexError(f"{item} is out of range")
            return self.window[item - self.window_start]

        # Set window
        self.update_window(item)
        if len(self.window) < 1:
            raise IndexError(f"{item} is out of range")
        return self.window[0]

    def __next__(self):
        if len(self.window) > 0:
            row = self.window[0]
            self.inc_window()
            return row
        else:
            raise StopIteration

    def __len__(self):
        res = 0
        while self.window:
            res += len(self.window)
            self.update_window(self.window_start + self.window_size)
        return res

    def close(self):
        self.file.close()


class JsonCounter5SheetReader(SheetReader):

    WINDOW_SIZE = 1000  # number of lines to be cached
    sheet_idx = 0
    name = None
    extra = None

    def reset(self):
        report = Counter5ReportBase()

        self.file.seek(0)

        header, items = report.fd_to_dicts(self.file)

        self.items = items
        self.extra = header

    def __init__(
        self,
        file: IO[bytes],
        window_size: int = WINDOW_SIZE,
    ):
        self.file = file

        self.window_start = 0
        self.window_size = window_size
        self.update_window(0)

    def update_window(self, window_start: int):
        self.reset()
        self.window_start = window_start
        self.window = deque(
            itertools.islice(self.items, self.window_start, self.window_start + self.window_size),
            self.window_size,
        )

    def inc_window(self):
        try:
            next_dict = next(self.items)
            self.window_start += 1
            self.window.append(next_dict)
        except StopIteration:
            if len(self.window):
                self.window_start += 1
                self.window.popleft()

    @lru_cache(WINDOW_SIZE * 2)  # cache lines to avoid rewinding while reading the header
    def __getitem__(self, item) -> Sequence[dict]:
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")

        # in current window
        if self.window_start <= item < (self.window_start + self.window_size):
            if self.window_start + len(self.window) < item:
                raise IndexError(f"{item} is out of range")
            return self.window[item - self.window_start]

        # Set window
        self.update_window(item)
        if len(self.window) < 1:
            raise IndexError(f"{item} is out of range")
        return self.window[0]

    def __next__(self):
        if len(self.window) > 0:
            item_dict = self.window[0]
            self.inc_window()
            return item_dict
        else:
            raise StopIteration

    def __len__(self):
        res = 0
        while self.window:
            res += len(self.window)
            self.update_window(self.window_start + self.window_size)
        return res

    def close(self):
        self.file.close()


class TableReader(metaclass=ABCMeta):
    """
    Abstract reader for tabular data - defines the API to be used by parsers when reading input data
    """

    @abstractmethod
    def __getitem__(self, item) -> SheetReader:
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError()


class CsvReader(TableReader):
    """
    Reads CSV file in stream mode
    """

    def __init__(self, source: Union[bytes, str, pathlib.Path]):
        # detect encoding
        file: IO[str]
        delimiters = None
        if isinstance(source, bytes):
            encoding = detect_file_encoding(io.BytesIO(source))
            logger.debug("Encoding '%s' was found for csv data", encoding)
            file = StringIO(source.decode(encoding or "utf8"))
        elif isinstance(source, (str, pathlib.Path)):
            source = pathlib.Path(source)  # make user that source is Path
            with open(source, "rb") as f:
                encoding = detect_file_encoding(f)
            logger.debug("Encoding '%s' was found for csv file", encoding)

            # Determine delimiters based on the source name
            if source.suffix == ".tsv":
                delimiters = "\t"

            file = open(source, "r", encoding=encoding)
        else:
            raise NotImplementedError()

        self.sheets = [CsvSheetReader(0, None, file, encoding, delimiters=delimiters)]

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()


class XlsxReader(TableReader):
    """
    Reads XLSX file in stream mode (TODO verify this)
    """

    def __init__(self, source: Union[str, pathlib.Path]):
        with open(source, "rb") as file:
            workbook = openpyxl.load_workbook(
                file, read_only=True, data_only=True, keep_links=False
            )
            self.sheets = []

            # Store each sheet as temporary CSV file
            for idx, sheet in enumerate(workbook.worksheets):

                # write data to csv
                f = tempfile.TemporaryFile("w+")
                # unix dialect escapes all by default
                dialect = csv.get_dialect("unix")
                writer = csv.writer(f, dialect=dialect)
                row_length = 0
                for row in sheet.rows:

                    # Make sure that length of the row is extending
                    current_length = len(row)
                    row_length = max(row_length, current_length)
                    extra_cells = [''] * (row_length - current_length)

                    writer.writerow([cell.value for cell in row] + extra_cells)
                f.seek(0)
                self.sheets.append(CsvSheetReader(idx, workbook.sheetnames[idx], f, dialect="unix"))

            workbook.close()

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()


class JsonCounter5Reader(TableReader):
    """Reads JSON file (in Counter 5 format) in stream mode"""

    def reset(self):
        report = Counter5ReportBase()

        with open(self.source, "rb") as file:
            header, items = report.fd_to_dicts(file)

        self.items = items
        self.header = header

    def __init__(self, source: Union[str, pathlib.Path, bytes]):

        file: IO[bytes]
        if isinstance(source, bytes):
            file = BytesIO(source)
        elif isinstance(source, (str, pathlib.Path)):
            source = pathlib.Path(source)  # make user that source is Path
            file = open(source, "rb")
        else:
            raise ValueError('source')

        self.sheets = [JsonCounter5SheetReader(file)]

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()
