from pathlib import Path

import pytest

from celus_nibbler.reader import (
    CsvReader,
    CsvSheetReader,
    JsonCounter5Reader,
    JsonCounter5SheetReader,
    XlsxReader,
)


class TestJsonCounter5SheetReader:
    @pytest.mark.parametrize("window_size", [2, 100])
    def test_iteration(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size)
        assert list(reader) == [
            ["Name", "Values"],
            ["First", "1"],
            ["Second", "2"],
            ["Third", "3"],
            ["Fourth", "4"],
        ]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_next(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size)
        assert next(reader) == ["Name", "Values"]
        assert next(reader) == ["First", "1"]
        assert next(reader) == ["Second", "2"]
        assert next(reader) == ["Third", "3"]
        assert next(reader) == ["Fourth", "4"]
        with pytest.raises(StopIteration):
            next(reader)

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_getitem(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size)
        assert reader[0] == ["Name", "Values"]
        assert reader[4] == ["Fourth", "4"]
        assert reader[1] == ["First", "1"]
        assert reader[3] == ["Third", "3"]
        assert reader[2] == ["Second", "2"]
        with pytest.raises(IndexError):
            reader[5]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_len(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size)
        assert len(reader) == 5


class TestJsonSheetReader:
    @pytest.mark.parametrize("window_size", [2, 100])
    def test_iteration(self, window_size, sheet_json):
        reader = JsonCounter5SheetReader(sheet_json, window_size)
        assert reader.extra == {
            "Created": "2020-01-01T00:00:00Z",
            "Created_By": "My services",
            "Customer_ID": "88888888",
            "Report_ID": "DR",
            "Release": "5",
            "Report_Name": "Database Master Report",
            "Institution_Name": "My institution",
            "Institution_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:88888888"}],
            "Report_Filters": [
                {"Name": "Begin_Date", "Value": "2020-01-01"},
                {"Name": "End_Date", "Value": "2020-01-31"},
            ],
            "Report_Attributes": [
                {"Name": "Attributes_To_Show", "Value": "Data_Type|Access_Method"}
            ],
        }
        assert list(reader) == [
            {
                "Platform": "MyPlatform",
                "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:000"}],
                "Database": "Database1",
                "Publisher": "MyPublisher",
                "Data_Type": "Database",
                "Access_Method": "Regular",
                "Performance": [
                    {
                        "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                        "Instance": [{"Metric_Type": "Searches_Regular", "Count": 5}],
                    }
                ],
            },
            {
                "Platform": "MyPlatform",
                "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:111"}],
                "Database": "Database2",
                "Publisher": "MyPublisher",
                "Data_Type": "Book",
                "Access_Method": "Regular",
                "Performance": [
                    {
                        "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                        "Instance": [
                            {"Metric_Type": "Total_Item_Investigations", "Count": 25},
                            {"Metric_Type": "Total_Item_Requests", "Count": 13},
                            {"Metric_Type": "Unique_Item_Investigations", "Count": 14},
                            {"Metric_Type": "Unique_Item_Requests", "Count": 11},
                            {"Metric_Type": "Unique_Title_Investigations", "Count": 13},
                            {"Metric_Type": "Unique_Title_Requests", "Count": 11},
                        ],
                    }
                ],
            },
            {
                "Platform": "MyPlatform",
                "Title": "Title1",
                "Item_ID": [
                    {"Type": "Print_ISSN", "Value": "9999-9999"},
                    {"Type": "Proprietary", "Value": "IIIIIIIII:222"},
                ],
                "Database": "Database3",
                "Publisher": "MyPublisher",
                "Data_Type": "Other",
                "Section_Type": "Article",
                "YOP": "2018",
                "Access_Type": "Controlled",
                "Access_Method": "Regular",
                "Performance": [
                    {
                        "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                        "Instance": [{"Metric_Type": "Unique_Item_Investigations", "Count": 9}],
                    }
                ],
            },
        ]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_next(self, window_size, sheet_json):
        reader = JsonCounter5SheetReader(sheet_json, window_size)
        assert next(reader) == {
            "Platform": "MyPlatform",
            "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:000"}],
            "Database": "Database1",
            "Publisher": "MyPublisher",
            "Data_Type": "Database",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [{"Metric_Type": "Searches_Regular", "Count": 5}],
                }
            ],
        }
        assert next(reader) == {
            "Platform": "MyPlatform",
            "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:111"}],
            "Database": "Database2",
            "Publisher": "MyPublisher",
            "Data_Type": "Book",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [
                        {"Metric_Type": "Total_Item_Investigations", "Count": 25},
                        {"Metric_Type": "Total_Item_Requests", "Count": 13},
                        {"Metric_Type": "Unique_Item_Investigations", "Count": 14},
                        {"Metric_Type": "Unique_Item_Requests", "Count": 11},
                        {"Metric_Type": "Unique_Title_Investigations", "Count": 13},
                        {"Metric_Type": "Unique_Title_Requests", "Count": 11},
                    ],
                }
            ],
        }
        assert next(reader) == {
            "Platform": "MyPlatform",
            "Title": "Title1",
            "Item_ID": [
                {"Type": "Print_ISSN", "Value": "9999-9999"},
                {"Type": "Proprietary", "Value": "IIIIIIIII:222"},
            ],
            "Database": "Database3",
            "Publisher": "MyPublisher",
            "Data_Type": "Other",
            "Section_Type": "Article",
            "YOP": "2018",
            "Access_Type": "Controlled",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [{"Metric_Type": "Unique_Item_Investigations", "Count": 9}],
                }
            ],
        }
        with pytest.raises(StopIteration):
            next(reader)

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_getitem(self, window_size, sheet_json):
        reader = JsonCounter5SheetReader(sheet_json, window_size)
        assert reader[0] == {
            "Platform": "MyPlatform",
            "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:000"}],
            "Database": "Database1",
            "Publisher": "MyPublisher",
            "Data_Type": "Database",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [{"Metric_Type": "Searches_Regular", "Count": 5}],
                }
            ],
        }
        assert reader[2] == {
            "Platform": "MyPlatform",
            "Title": "Title1",
            "Item_ID": [
                {"Type": "Print_ISSN", "Value": "9999-9999"},
                {"Type": "Proprietary", "Value": "IIIIIIIII:222"},
            ],
            "Database": "Database3",
            "Publisher": "MyPublisher",
            "Data_Type": "Other",
            "Section_Type": "Article",
            "YOP": "2018",
            "Access_Type": "Controlled",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [{"Metric_Type": "Unique_Item_Investigations", "Count": 9}],
                }
            ],
        }
        assert reader[1] == {
            "Platform": "MyPlatform",
            "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:111"}],
            "Database": "Database2",
            "Publisher": "MyPublisher",
            "Data_Type": "Book",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [
                        {"Metric_Type": "Total_Item_Investigations", "Count": 25},
                        {"Metric_Type": "Total_Item_Requests", "Count": 13},
                        {"Metric_Type": "Unique_Item_Investigations", "Count": 14},
                        {"Metric_Type": "Unique_Item_Requests", "Count": 11},
                        {"Metric_Type": "Unique_Title_Investigations", "Count": 13},
                        {"Metric_Type": "Unique_Title_Requests", "Count": 11},
                    ],
                }
            ],
        }
        with pytest.raises(IndexError):
            reader[3]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_len(self, window_size, sheet_json):
        reader = JsonCounter5SheetReader(sheet_json, window_size)
        assert len(reader) == 3


class TestCsvReader:

    data_csv = b'a,b,c\n1,3,4\nhi,there,"how are you?"\n'
    data_list = [[['a', 'b', 'c'], ['1', '3', '4'], ['hi', 'there', 'how are you?']]]

    def test_indexing(self):
        sheets = CsvReader(self.data_csv)
        for idx in range(3):
            assert sheets[0][idx] == self.data_list[0][idx]

        with pytest.raises(IndexError):
            assert sheets[0][3]

    def test_iteration(self):
        sheets = CsvReader(self.data_csv)
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]


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


class TestJsonCounter5Reader:
    data_list = [
        {
            "Platform": "MyPlatform",
            "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:000"}],
            "Database": "Database1",
            "Publisher": "MyPublisher",
            "Data_Type": "Database",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [{"Metric_Type": "Searches_Regular", "Count": 5}],
                }
            ],
        },
        {
            "Platform": "MyPlatform",
            "Item_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:111"}],
            "Database": "Database2",
            "Publisher": "MyPublisher",
            "Data_Type": "Book",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [
                        {"Metric_Type": "Total_Item_Investigations", "Count": 25},
                        {"Metric_Type": "Total_Item_Requests", "Count": 13},
                        {"Metric_Type": "Unique_Item_Investigations", "Count": 14},
                        {"Metric_Type": "Unique_Item_Requests", "Count": 11},
                        {"Metric_Type": "Unique_Title_Investigations", "Count": 13},
                        {"Metric_Type": "Unique_Title_Requests", "Count": 11},
                    ],
                }
            ],
        },
        {
            "Platform": "MyPlatform",
            "Title": "Title1",
            "Item_ID": [
                {"Type": "Print_ISSN", "Value": "9999-9999"},
                {"Type": "Proprietary", "Value": "IIIIIIIII:222"},
            ],
            "Database": "Database3",
            "Publisher": "MyPublisher",
            "Data_Type": "Other",
            "Section_Type": "Article",
            "YOP": "2018",
            "Access_Type": "Controlled",
            "Access_Method": "Regular",
            "Performance": [
                {
                    "Period": {"Begin_Date": "2020-01-01", "End_Date": "2020-01-31"},
                    "Instance": [{"Metric_Type": "Unique_Item_Investigations", "Count": 9}],
                }
            ],
        },
    ]

    def test_indexing(self, sheet_json):
        sheets = JsonCounter5Reader(sheet_json.read())
        for idx in range(3):
            assert sheets[0][idx] == self.data_list[idx]

        with pytest.raises(IndexError):
            assert sheets[0][3]

    def test_iteration(self, sheet_json):
        sheets = JsonCounter5Reader(sheet_json.read())
        for idx, item in enumerate(sheets[0]):
            assert item == self.data_list[idx]


class TestEncoding:
    utf8_path = Path(__file__).parent / 'data/reader/utf8.csv'
    cp1250_path = Path(__file__).parent / 'data/reader/cp1250.csv'
    output = [
        [
            ["Žluťoučký kůń", "1"],
            ["Pěl ďábelské", "2"],
            ["Ódy", "3"],
            ["Řeřicha", "4"],
        ]
    ]

    @pytest.mark.parametrize(
        "encoding",
        [
            "utf8",
            pytest.param(
                "cp1250", marks=pytest.mark.xfail(reason="cp1250 is not supported by chardet")
            ),
        ],
    )
    def test_encoding(self, encoding):
        path = Path(__file__).parent / f'data/reader/{encoding}.csv'
        sheets = CsvReader(path)
        for i, row in enumerate(sheets[0]):
            assert row == self.output[0][i]


class TestTsv:
    sample_path = Path(__file__).parent / 'data/reader/sample.tsv'

    def test_tsv(self):
        self.sample_path
        (sheet,) = CsvReader(self.sample_path)
        assert next(sheet) == ["Tabs", "mixed,with,", "commas"]
        assert next(sheet) == ["1", "2", "3"]
        assert next(sheet) == ["4", "5", "6"]
        assert next(sheet) == ["7", "8", "9"]
        with pytest.raises(StopIteration):
            next(sheet)
