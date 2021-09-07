from pathlib import Path

import pytest

from celus_nibbler.reader import NaiveCSVReader, NaiveXlsxReader


class TestNaiveCSVReader:

    data_csv = b'a,b,c\n1,3,4\nhi,there,"how are you?"\n'
    data_list = [[['a', 'b', 'c'], ['1', '3', '4'], ['hi', 'there', 'how are you?']]]
    onlysheet_idx = 0  # NaiveCSVReader.__getitem__() can receive only argument index 0

    def test_indexing(self):
        sheets = NaiveCSVReader(self.data_csv)
        for row_idx, row in enumerate(sheets[self.onlysheet_idx]):
            assert row == self.data_list[self.onlysheet_idx][row_idx]

        with pytest.raises(IndexError):
            assert sheets[self.onlysheet_idx][3]
        assert sheets[self.onlysheet_idx][-1] == self.data_list[self.onlysheet_idx][2]

    def test_slicing(self):
        sheets = NaiveCSVReader(self.data_csv)
        assert sheets[self.onlysheet_idx][0:2] == self.data_list[self.onlysheet_idx][0:2]
        assert sheets[self.onlysheet_idx][1:] == self.data_list[self.onlysheet_idx][1:]

    def test_iteration(self):
        sheets = NaiveCSVReader(self.data_csv)
        for i, row in enumerate(sheets[self.onlysheet_idx]):
            assert row == self.data_list[self.onlysheet_idx][i]


class TestNaiveXlsxReader:

    file_path = Path(__file__).parent / 'data/reader/test-simple.xlsx'
    data_list = [[['a', 'b', 'c'], [1, 3, 4], ['hi', 'there', 'how are you?']]]

    def test_indexing(self):
        sheets = NaiveXlsxReader(self.file_path)
        for sheet_idx, sheet in enumerate(sheets):
            for row_idx in range(len(sheet)):
                assert sheets[sheet_idx][row_idx] == self.data_list[sheet_idx][row_idx]
        with pytest.raises(IndexError):
            assert sheets[0][3]
        assert sheets[0][-1] == self.data_list[0][2]

    def test_slicing(self):
        sheets = NaiveXlsxReader(self.file_path)
        assert sheets[0][0:2] == self.data_list[0][0:2]
        assert sheets[0][1:] == self.data_list[0][1:]

    def test_iteration(self):
        sheets = NaiveXlsxReader(self.file_path)
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]
