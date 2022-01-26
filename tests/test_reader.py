from io import StringIO
from pathlib import Path

import pytest

from celus_nibbler.reader import CsvReader, SheetReader, XlsxReader


class TestSheetReader:
    @pytest.fixture
    def sheet_csv(self):
        return StringIO("Name,Value\nFirst,1\nSecond,2\nThird,3\nFourth,4\n")

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_iteration(self, window_size, sheet_csv):
        reader = SheetReader(0, None, sheet_csv, window_size)
        assert list(reader) == [
            ["Name", "Value"],
            ["First", "1"],
            ["Second", "2"],
            ["Third", "3"],
            ["Fourth", "4"],
        ]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_next(self, window_size, sheet_csv):
        reader = SheetReader(0, None, sheet_csv, window_size)
        assert next(reader) == ["Name", "Value"]
        assert next(reader) == ["First", "1"]
        assert next(reader) == ["Second", "2"]
        assert next(reader) == ["Third", "3"]
        assert next(reader) == ["Fourth", "4"]
        with pytest.raises(StopIteration):
            next(reader)

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_getitem(self, window_size, sheet_csv):
        reader = SheetReader(0, None, sheet_csv, window_size)
        assert reader[0] == ["Name", "Value"]
        assert reader[4] == ["Fourth", "4"]
        assert reader[1] == ["First", "1"]
        assert reader[3] == ["Third", "3"]
        assert reader[2] == ["Second", "2"]
        with pytest.raises(IndexError):
            reader[5]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_len(self, window_size, sheet_csv):
        reader = SheetReader(0, None, sheet_csv, window_size)
        assert len(reader) == 5


class TestCsvReader:

    data_csv = b'a,b,c\n1,3,4\nhi,there,"how are you?"\n'
    data_list = [[['a', 'b', 'c'], ['1', '3', '4'], ['hi', 'there', 'how are you?']]]
    onlysheet_idx = 0  # CsvReader.__getitem__() can receive only argument index 0

    def test_indexing(self):
        sheets = CsvReader(self.data_csv)
        for row_idx, row in enumerate(sheets[self.onlysheet_idx]):
            assert row == self.data_list[self.onlysheet_idx][row_idx]

        with pytest.raises(IndexError):
            assert sheets[self.onlysheet_idx][3]

    def test_iteration(self):
        sheets = CsvReader(self.data_csv)
        for i, row in enumerate(sheets[self.onlysheet_idx]):
            assert row == self.data_list[self.onlysheet_idx][i]


class TestXlsxReader:

    file_path = Path(__file__).parent / 'data/reader/test-simple.xlsx'
    data_list = [[['a', 'b', 'c'], ['1', '3', '4'], ['hi', 'there', 'how are you?']]]

    def test_indexing(self):
        sheets = XlsxReader(self.file_path)
        for sheet_idx, sheet in enumerate(sheets):
            for row_idx in range(len(sheet)):
                assert sheets[sheet_idx][row_idx] == self.data_list[sheet_idx][row_idx]
        with pytest.raises(IndexError):
            assert sheets[0][3]

    def test_iteration(self):
        sheets = XlsxReader(self.file_path)
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]
