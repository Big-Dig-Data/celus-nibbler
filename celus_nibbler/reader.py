import csv
from io import StringIO
from typing import Iterable, Sequence

import openpyxl


class TableReader(metaclass=abc.ABCMeta):
    """
    Abstract reader for tabular data - defines the API to be used by parsers when reading input data
    """

    def __init__(self, source: typing.Union[bytes, str, typing.IO]):
        self.needs_close = False
        if hasattr(filename_or_stream, 'read'):
            self.stream = filename_or_stream
        elif isinstance(filename_or_stream, bytes):
            self.stream = StringIO(filename_or_stream.decode('utf-8'))
        else:
            self.stream = open(filename_or_stream, 'rb')
            self.needs_close = True

    def close(self):
        if self.needs_close:
            self.stream.close()
            self.needs_close = False

    # abstract methods to implement

    def __getitem__(self, item) -> Sequence:
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError()


class NaiveCSVReader(TableReader):
    """
    Reader for CSV which simply loads all the data into memory as a list of lists and then exposes
    this data using the `TableReader` API.

    Useful as transitional implementation, should be replaced in the future.
    """

    def __init__(self, filename_or_stream):
        super().__init__(filename_or_stream)
        self.data = []
        reader = csv.reader(self.stream)
        self.data = list(reader)
        self.close()

    def __getitem__(self, item) -> Iterable:
        return self.data[item]

    def __iter__(self):
        return self.data.__iter__()


class NaiveXlsxReader(TableReader):
    """
    Reader for XLSX which simply loads all the data into memory as a list of lists and then exposes
    this data using the `TableReader` API.

    Useful as transitional implementation, should be replaced in the future.
    """

    def __init__(self, filename_or_stream):
        super().__init__(filename_or_stream)
        workbook = openpyxl.load_workbook(
            self.stream, read_only=True, data_only=True, keep_links=False
        )
        sheet = workbook.active
        self.data = []
        for row in sheet.rows:
            self.data.append([cell.value for cell in row])
        self.close()

    def __getitem__(self, item) -> Iterable:
        return self.data[item]

    def __iter__(self):
        return self.data.__iter__()
