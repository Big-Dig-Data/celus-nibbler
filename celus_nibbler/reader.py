import csv
import itertools
import pathlib
import tempfile
from abc import ABCMeta, abstractmethod
from collections import deque
from io import StringIO
from typing import IO, Optional, Sequence, Union

import openpyxl


class SheetReader:
    """
    Class representing a single table
    """

    WINDOW_SIZE = 1000  # number of lines to be cached

    def __init__(
        self, sheet_idx: int, name: Optional[str], file: IO[str], window_size: int = WINDOW_SIZE
    ):
        self.name = name
        self.sheet_idx = sheet_idx
        self.file = file
        self.csv_reader = csv.reader(file)
        # Load basic window
        self.window_start = 0
        self.window_size = window_size
        self.update_window(0)

    def update_window(self, window_start: int):
        self.file.seek(0)
        self.csv_reader = csv.reader(self.file)
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


class TableReader(metaclass=ABCMeta):
    """
    Abstract reader for tabular data - defines the API to be used by parsers when reading input data
    """

    def __init__(self, source: Union[bytes, str, IO, pathlib.Path]):
        self.needs_close = False
        if hasattr(source, 'read'):
            self.file = source
        elif isinstance(source, bytes):
            self.file = StringIO(source.decode('utf-8'))
        elif isinstance(source, (str, pathlib.Path)):
            self.file = open(source, 'rb')
            self.needs_close = True
        else:
            raise NotImplementedError()

    def close(self):
        if self.needs_close:
            self.file.close()
            self.needs_close = False

    # abstract methods to implement

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

    def __init__(self, source: Union[bytes, str, IO[str], pathlib.Path]):
        super().__init__(source)
        self.file: IO[str]
        self.sheets = [SheetReader(0, None, self.file)]

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()


class XlsxReader(TableReader):
    """
    Reads XLSX file in stream mode (TODO verify this)
    """

    def __init__(self, source: Union[bytes, str, IO, pathlib.Path]):
        super().__init__(source)
        workbook = openpyxl.load_workbook(
            self.file, read_only=True, data_only=True, keep_links=False
        )
        self.sheets = []

        # Store each sheet as temporary CSV file
        for idx, sheet in enumerate(workbook.worksheets):
            f = tempfile.TemporaryFile("w+")
            writer = csv.writer(f)
            for row in sheet.rows:
                writer.writerow([cell.value for cell in row])
            f.seek(0)
            self.sheets.append(SheetReader(idx, workbook.sheetnames[idx], f))

        self.close()

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()
