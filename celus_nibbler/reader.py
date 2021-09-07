import csv
from abc import ABCMeta, abstractmethod
from io import StringIO
from typing import IO, Sequence, Union

import openpyxl


class TableReader(metaclass=ABCMeta):
    """
    Abstract reader for tabular data - defines the API to be used by parsers when reading input data
    """

    def __init__(self, source: Union[bytes, str, IO]):
        self.needs_close = False
        if hasattr(source, 'read'):
            self.stream = source
        elif isinstance(source, bytes):
            self.stream = StringIO(source.decode('utf-8'))
        else:
            self.stream = open(source, 'rb')
            self.needs_close = True

    def close(self):
        if self.needs_close:
            self.stream.close()
            self.needs_close = False

    # abstract methods to implement

    @abstractmethod
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

    def __init__(self, source: Union[bytes, str, IO]):
        super().__init__(source)
        reader = csv.reader(self.stream)
        self.sheets = [list(reader)]
        self.close()

    def __getitem__(self, item) -> Sequence:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()


class NaiveXlsxReader(TableReader):
    """
    Reader for XLSX which simply loads all the data into memory as a list of lists and then exposes
    this data using the `TableReader` API.

    Useful as transitional implementation, should be replaced in the future.
    """

    def __init__(self, source: Union[bytes, str, IO]):
        super().__init__(source)
        workbook = openpyxl.load_workbook(
            self.stream, read_only=True, data_only=True, keep_links=False
        )
        self.sheets = []
        for sheet in workbook.worksheets:
            values = []
            for row in sheet.rows:
                values.append([cell.value for cell in row])
            self.sheets.append(values)
        self.close()

    def __getitem__(self, item) -> Sequence:
        return self.sheets[item]

    def __iter__(self):
        return self.sheets.__iter__()
