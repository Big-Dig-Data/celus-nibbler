import csv
import io
import itertools
import logging
import pathlib
import tempfile
from abc import ABCMeta, abstractmethod
from collections import deque
from functools import lru_cache
from io import BufferedIOBase, BytesIO, RawIOBase, StringIO, TextIOBase, TextIOWrapper
from typing import IO, Any, Dict, List, Optional, Sequence, Union

import openpyxl
from celus_nigiri.counter5 import Counter5ReportBase
from celus_nigiri.csv_detect import detect_csv_dialect, detect_file_encoding

from .errors import XlsError

logger = logging.getLogger(__name__)


class _TextIOWrapperNoClose(TextIOWrapper):
    """Wrapper around TextIOWrapper which can't be closed

    Usually TextIOWrapper closes the opened file when it is discarded in `__del__` method.
    This behaviour is not desired, because the underlying file can't be reused afterwards

    This class simply supresses such behaviour
    """

    def __del__(self):
        # Override __del__ method of parent and do nothing
        pass


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

    def dict_reader(self) -> "DictReader":
        return DictReader(SheetReaderWithLineNum(self))


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
        window_size: int = WINDOW_SIZE,
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

    def dict_reader(self):
        raise NotImplementedError()


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

    def __init__(self, source: Union[IO, bytes, str, pathlib.Path]):
        file: IO[str]
        if isinstance(source, bytes):
            encoding = detect_file_encoding(io.BytesIO(source))
            logger.debug("Encoding '%s' was found for csv data", encoding)
            file = StringIO(source.decode(encoding or "utf8"))
        elif isinstance(source, (str, pathlib.Path)):
            source = pathlib.Path(source)  # make user that source is Path
            with source.open("rb") as f:
                encoding = detect_file_encoding(f)
            logger.debug("Encoding '%s' was found for csv file", encoding)
            file = open(source, "r", encoding=encoding)
        elif isinstance(source, (RawIOBase, BufferedIOBase)):
            encoding = detect_file_encoding(source)
            logger.debug("Encoding '%s' was found for csv data", encoding)
            file = _TextIOWrapperNoClose(source, encoding=encoding)
        elif isinstance(source, TextIOBase):
            # Opened as a text file => don't try to detect encoding
            file = source
        else:
            raise NotImplementedError()

        self.sheets = [CsvSheetReader(0, None, file)]

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()


class XlsxReader(TableReader):
    """
    Reads XLSX file in stream mode (TODO verify this)
    """

    def __init__(self, source: Union[str, pathlib.Path, RawIOBase, BufferedIOBase]):
        workbook = openpyxl.load_workbook(source, read_only=True, data_only=True, keep_links=False)
        self.sheets = []

        # Store each sheet as temporary CSV file
        for idx, sheet in enumerate(workbook.worksheets):
            # For some reason in it necessary to reset dimension for some files
            # which display that only a single cell is present in the data
            if sheet.calculate_dimension(force=True) == "A1:A1":
                sheet.reset_dimensions()

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
                extra_cells = [""] * (row_length - current_length)

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
            raise ValueError("source")

        self.sheets = [JsonCounter5SheetReader(file)]

    def __getitem__(self, item) -> SheetReader:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()


try:
    import xlrd
except ImportError:
    XlsReader = None
else:

    class XlsReader(TableReader):  # noqa
        """
        Reads XLS file it probably loads entire file into memory
        """

        def __init__(self, source: Union[str, pathlib.Path, RawIOBase, BufferedIOBase]):
            try:
                if isinstance(source, (RawIOBase, BufferedIOBase)):
                    workbook = xlrd.open_workbook(file_contents=source.read())
                else:
                    workbook = xlrd.open_workbook(filename=source)

                self.sheets = []

                # Store each sheet as temporary CSV file
                for idx in range(workbook.nsheets):
                    sheet = workbook.sheet_by_index(idx)

                    # write data to csv
                    f = tempfile.TemporaryFile("w+")
                    # unix dialect escapes all by default
                    dialect = csv.get_dialect("unix")
                    writer = csv.writer(f, dialect=dialect)
                    row_length = sheet.ncols
                    for rx in range(sheet.nrows):
                        row = sheet.row(rx)

                        # Make sure that length of the row is extending
                        current_length = len(row)
                        extra_cells = [""] * (row_length - current_length)

                        writer.writerow([self._cell_to_str(cell) for cell in row] + extra_cells)
                    f.seek(0)

                    self.sheets.append(CsvSheetReader(idx, sheet.name, f, dialect="unix"))
                    workbook.unload_sheet(idx)

                workbook.release_resources()
            except xlrd.compdoc.CompDocError as e:
                raise XlsError(e) from e

        def _cell_to_str(self, cell: xlrd.sheet.Cell) -> str:
            if cell.ctype in [xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK, xlrd.XL_CELL_ERROR]:
                return ""

            elif cell.ctype == xlrd.XL_CELL_TEXT:
                return cell.value
            elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
                return str(bool(cell.value))
            elif cell.ctype == xlrd.XL_CELL_NUMBER:
                # value is float
                return str(cell.value)
            elif cell.ctype == xlrd.XL_CELL_DATE:
                # value is float
                return xlrd.xldate.xldate_as_datetime(cell.value, 0).isoformat(sep=" ")

            raise NotImplementedError()

        def __getitem__(self, item) -> SheetReader:
            return self.sheets[item]

        def __iter__(self):
            return self.sheets.__iter__()


class SheetReaderWithLineNum:
    def __init__(self, sheet_reader: SheetReader):
        self.sheet_reader = sheet_reader
        self.line_num = 0

    def __next__(self):
        next_element = next(self.sheet_reader)
        self.line_num += 1
        return next_element


class DictReader(csv.DictReader):
    """Try to wrap csv.DictReader arond SheetReader"""

    def __init__(self, sheet_reader: "SheetReaderWithLineNum"):
        self.reader = sheet_reader
        self._fieldnames = None
        self.restkey = None
        self.restval = None
        self.dialect = "excel"
        self.line_num = 0
