from pathlib import Path

import pytest

from celus_nibbler.reader import NaiveCSVReader, NaiveXlsxReader


class TestNaiveCSVReader:

    data_csv = b'a,b,c\n1,3,4\nhi,there,"how are you?"\n'
    data_list = [['a', 'b', 'c'], ['1', '3', '4'], ['hi', 'there', 'how are you?']]

    def test_indexing(self):
        reader = NaiveCSVReader(self.data_csv)
        assert reader[1] == self.data_list[1]
        assert reader[2] == self.data_list[2]
        with pytest.raises(IndexError):
            assert reader[3]
        assert reader[-1] == self.data_list[2]

    def test_slicing(self):
        reader = NaiveCSVReader(self.data_csv)
        assert reader[0:2] == self.data_list[0:2]
        assert reader[1:] == self.data_list[1:]

    def test_iteration(self):
        reader = NaiveCSVReader(self.data_csv)
        for i, row in enumerate(reader):
            assert row == self.data_list[i]


class TestNaiveXlsxReader:

    test_file = Path(__file__).parent / 'data/reader/test-simple.xlsx'
    data_list = [['a', 'b', 'c'], [1, 3, 4], ['hi', 'there', 'how are you?']]

    def test_indexing(self):
        reader = NaiveXlsxReader(self.test_file)
        assert reader[1] == self.data_list[1]
        assert reader[2] == self.data_list[2]
        with pytest.raises(IndexError):
            assert reader[3]
        assert reader[-1] == self.data_list[2]

    def test_slicing(self):
        reader = NaiveXlsxReader(self.test_file)
        assert reader[0:2] == self.data_list[0:2]
        assert reader[1:] == self.data_list[1:]

    def test_iteration(self):
        reader = NaiveXlsxReader(self.test_file)
        for i, row in enumerate(reader):
            assert row == self.data_list[i]
