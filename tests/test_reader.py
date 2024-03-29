from io import BytesIO, StringIO
from pathlib import Path

import pytest

from celus_nibbler.errors import XlsError
from celus_nibbler.reader import (
    CsvReader,
    CsvSheetReader,
    JsonCounter5Reader,
    JsonCounter5SheetReader,
    XlsReader,
    XlsxReader,
)


class TestCsvSheetReader:
    @pytest.mark.parametrize("window_size", [2, 100])
    def test_iteration(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size=window_size)
        assert list(reader) == [
            ["Name", "Values"],
            ["First", "1"],
            ["Second", "2"],
            ["Third", "3"],
            ["Fourth", "4"],
        ]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_next(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size=window_size)
        assert next(reader) == ["Name", "Values"]
        assert next(reader) == ["First", "1"]
        assert next(reader) == ["Second", "2"]
        assert next(reader) == ["Third", "3"]
        assert next(reader) == ["Fourth", "4"]
        with pytest.raises(StopIteration):
            next(reader)

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_getitem(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size=window_size)
        assert reader[0] == ["Name", "Values"]
        assert reader[4] == ["Fourth", "4"]
        assert reader[1] == ["First", "1"]
        assert reader[3] == ["Third", "3"]
        assert reader[2] == ["Second", "2"]

        assert reader[0] == ["Name", "Values"]
        assert reader[2] == ["Second", "2"]
        assert reader[4] == ["Fourth", "4"]
        assert reader[1] == ["First", "1"]
        assert reader[3] == ["Third", "3"]

        assert reader[0] == ["Name", "Values"]
        assert reader[1] == ["First", "1"]
        assert reader[2] == ["Second", "2"]
        assert reader[3] == ["Third", "3"]
        assert reader[4] == ["Fourth", "4"]

        with pytest.raises(IndexError):
            reader[5]

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_len(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size=window_size)
        assert len(reader) == 5

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_dict_reader(self, window_size, sheet_csv):
        reader = CsvSheetReader(0, None, sheet_csv, window_size=window_size)
        dict_reader = reader.dict_reader()
        assert dict_reader.fieldnames == ["Name", "Values"]
        assert [e for e in dict_reader] == [
            {"Name": "First", "Values": "1"},
            {"Name": "Second", "Values": "2"},
            {"Name": "Third", "Values": "3"},
            {"Name": "Fourth", "Values": "4"},
        ]


class TestJsonSheetReader:
    @pytest.mark.parametrize("window_size", [2, 100])
    def test_iteration(self, window_size, sheet_json):
        reader = JsonCounter5SheetReader(sheet_json, window_size=window_size)
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
        reader = JsonCounter5SheetReader(sheet_json, window_size=window_size)
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
        reader = JsonCounter5SheetReader(sheet_json, window_size=window_size)
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
        reader = JsonCounter5SheetReader(sheet_json, window_size=window_size)
        assert len(reader) == 3

    @pytest.mark.parametrize("window_size", [2, 100])
    def test_dict_reader(self, window_size, sheet_json):
        reader = JsonCounter5SheetReader(sheet_json, window_size=window_size)
        with pytest.raises(NotImplementedError):
            reader.dict_reader()


def no_io_wrapper(io):
    return io


class TestCsvReader:
    data_csv = b'a,b,c\n1,3,4\nhi,there,"how are you?"\n'
    text_csv = data_csv.decode()
    data_list = [[["a", "b", "c"], ["1", "3", "4"], ["hi", "there", "how are you?"]]]

    @pytest.mark.parametrize(
        "io_wrapper,data",
        [
            (no_io_wrapper, data_csv),
            (BytesIO, data_csv),
            (StringIO, text_csv),
        ],
    )
    def test_indexing(self, io_wrapper, data):
        source = io_wrapper(data)
        sheets = CsvReader(source)
        for idx in range(3):
            assert sheets[0][idx] == self.data_list[0][idx]

        with pytest.raises(IndexError):
            assert sheets[0][3]

        del sheets

        # Retry to see whether the code handles opened files correctly
        # - it should not close them
        sheets = CsvReader(source)
        for idx in range(3):
            assert sheets[0][idx] == self.data_list[0][idx]

        with pytest.raises(IndexError):
            assert sheets[0][3]

    @pytest.mark.parametrize(
        "io_wrapper,data",
        [
            (no_io_wrapper, data_csv),
            (BytesIO, data_csv),
            (StringIO, text_csv),
        ],
    )
    def test_iteration(self, io_wrapper, data):
        source = io_wrapper(data)
        sheets = CsvReader(source)
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]

        del sheets

        # Retry to see whether the code handles opened files correctly
        # - it should not close them
        sheets = CsvReader(source)
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]

    @pytest.mark.parametrize(
        "io_wrapper,data",
        [
            (no_io_wrapper, data_csv),
            (BytesIO, data_csv),
            (StringIO, text_csv),
        ],
    )
    def test_dict_reader(self, io_wrapper, data):
        source = io_wrapper(data)
        sheets = CsvReader(source)
        reader = sheets[0].dict_reader()
        assert [e for e in reader] == [
            {"a": "1", "b": "3", "c": "4"},
            {"a": "hi", "b": "there", "c": "how are you?"},
        ]

        del sheets
        del reader

        # Retry to see whether the code handles opened files correctly
        # - it should not close them
        sheets = CsvReader(source)
        reader = sheets[0].dict_reader()
        assert [e for e in reader] == [
            {"a": "1", "b": "3", "c": "4"},
            {"a": "hi", "b": "there", "c": "how are you?"},
        ]


def open_file_binary(path):
    return open(path, "rb")


class TestXlsxReader:
    file_path = Path(__file__).parent / "data/reader/test-simple.xlsx"
    data_list = [
        [
            ["a", "b", "c"],
            ["1", "3", "4"],
            ["hi", "there", "how are you?"],
            ["", "", ""],
            ["", "", ""],
            ["another", "", ""],
            ["Extra", "line", "present"],
        ],
    ]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_indexing(self, io_wrapper):
        sheets = XlsxReader(io_wrapper(self.file_path))
        for sheet_idx, sheet in enumerate(sheets):
            for row_idx in range(len(sheet)):
                assert sheets[sheet_idx][row_idx] == self.data_list[sheet_idx][row_idx]
        with pytest.raises(IndexError):
            assert sheets[0][row_idx + 1]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_iteration(self, io_wrapper):
        sheets = XlsxReader(io_wrapper(self.file_path))
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_dict_reader(self, io_wrapper):
        sheets = XlsxReader(io_wrapper(self.file_path))
        reader = sheets[0].dict_reader()
        assert [e for e in reader] == [
            {"a": "1", "b": "3", "c": "4"},
            {"a": "hi", "b": "there", "c": "how are you?"},
            {"a": "", "b": "", "c": ""},
            {"a": "", "b": "", "c": ""},
            {"a": "another", "b": "", "c": ""},
            {"a": "Extra", "b": "line", "c": "present"},
        ]


class TestXlsReader:
    file_path = Path(__file__).parent / "data/reader/test-simple.xls"
    data_list = [
        [
            ["", "2023-09-01 00:00:00", "2023-10-01 00:00:00", "2023-11-01 00:00:00"],
            ["A", "1.0", "", "3.145"],
            ["B", "1.2", "3.0", "3.999"],
            ["C", "3.0", "4.0", "8.99999"],
            ["", "True", "False", "True"],
        ],
    ]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_indexing(self, io_wrapper):
        sheets = XlsReader(io_wrapper(self.file_path))
        for sheet_idx, sheet in enumerate(sheets):
            for row_idx in range(len(sheet)):
                assert sheets[sheet_idx][row_idx] == self.data_list[sheet_idx][row_idx]
        with pytest.raises(IndexError):
            assert sheets[0][row_idx + 1]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_iteration(self, io_wrapper):
        sheets = XlsReader(io_wrapper(self.file_path))
        for i, row in enumerate(sheets[0]):
            assert row == self.data_list[0][i]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_dict_reader(self, io_wrapper):
        sheets = XlsReader(io_wrapper(self.file_path))
        reader = sheets[0].dict_reader()
        assert [e for e in reader] == [
            {
                "": "A",
                "2023-09-01 00:00:00": "1.0",
                "2023-10-01 00:00:00": "",
                "2023-11-01 00:00:00": "3.145",
            },
            {
                "": "B",
                "2023-09-01 00:00:00": "1.2",
                "2023-10-01 00:00:00": "3.0",
                "2023-11-01 00:00:00": "3.999",
            },
            {
                "": "C",
                "2023-09-01 00:00:00": "3.0",
                "2023-10-01 00:00:00": "4.0",
                "2023-11-01 00:00:00": "8.99999",
            },
            {
                "": "",
                "2023-09-01 00:00:00": "True",
                "2023-10-01 00:00:00": "False",
                "2023-11-01 00:00:00": "True",
            },
        ]

    @pytest.mark.parametrize("io_wrapper", [no_io_wrapper, open_file_binary])
    def test_xls_error(self, io_wrapper, monkeypatch):
        from celus_nibbler.reader import xlrd

        def raise_error(*args, **kwargs):
            raise xlrd.compdoc.CompDocError("inner XLS error")

        monkeypatch.setattr(xlrd, "open_workbook", raise_error)

        with pytest.raises(XlsError):
            XlsReader(io_wrapper(self.file_path))


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

    def test_dict_reader(self, sheet_json):
        sheets = JsonCounter5Reader(sheet_json.read())
        with pytest.raises(NotImplementedError):
            sheets[0].dict_reader()


class TestEncoding:
    utf8_path = Path(__file__).parent / "data/reader/utf8.csv"
    cp1250_path = Path(__file__).parent / "data/reader/cp1250.csv"
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
        path = Path(__file__).parent / f"data/reader/{encoding}.csv"
        sheets = CsvReader(path)
        for i, row in enumerate(sheets[0]):
            assert row == self.output[0][i]


class TestTsv:
    sample_path = Path(__file__).parent / "data/reader/sample.tsv"

    def test_tsv(self):
        self.sample_path
        (sheet,) = CsvReader(self.sample_path)
        assert next(sheet) == ["Tabs", "mixed,with,", "commas"]
        assert next(sheet) == ["1", "2", "3"]
        assert next(sheet) == ["4", "5", "6"]
        assert next(sheet) == ["7", "8", "9"]
        with pytest.raises(StopIteration):
            next(sheet)
