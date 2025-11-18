import csv
import pathlib
from datetime import date

import pytest

from celus_nibbler import eat
from celus_nibbler.errors import NoParserMatchesHeuristics


@pytest.mark.parametrize(
    "file,parser,success,extras",
    (
        (
            "51/DR_sample_r51.tsv",
            "static.counter51.DR.Tabular",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Exceptions": "",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Sample Institution",
                "Metric_Types": "",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": "Attributes_To_Show=Access_Method",
                "Report_Filters": "",
                "Report_ID": "DR",
                "Report_Name": "Database Report",
                "Reporting_Period": "Begin_Date=2022-01-01; End_Date=2022-12-31",
            },
        ),
        (
            "51/PR_sample_r51.tsv",
            "static.counter51.PR.Tabular",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Exceptions": "",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Sample Institution",
                "Metric_Types": "",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": "Attributes_To_Show=Access_Method",
                "Report_Filters": "",
                "Report_ID": "PR",
                "Report_Name": "Platform Report",
                "Reporting_Period": "Begin_Date=2022-01-01; End_Date=2022-12-31",
            },
        ),
        (
            "51/TR_sample_r51.tsv",
            "static.counter51.TR.Tabular",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Exceptions": "",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Sample Institution",
                "Metric_Types": "",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": "Attributes_To_Show=YOP|Access_Type|Access_Method",
                "Report_Filters": "",
                "Report_ID": "TR",
                "Report_Name": "Title Report",
                "Reporting_Period": "Begin_Date=2022-01-01; End_Date=2022-12-31",
            },
        ),
        (
            "51/IR_sample_r51.tsv",
            "static.counter51.IR.Tabular",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Exceptions": "",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Sample Institution",
                "Metric_Types": "",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": "Attributes_To_Show=Authors|Publication_Date|"
                "Article_Version|YOP|Access_Type|Access_Method; Include_Parent_Details=True",
                "Report_Filters": "",
                "Report_ID": "IR",
                "Report_Name": "Item Report",
                "Reporting_Period": "Begin_Date=2022-01-01; End_Date=2022-12-31",
            },
        ),
        (
            "51/DR_sample_r51.json",
            "static.counter51.DR.Json",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Institution_ID": {"ISNI": ["1234123412341234"]},
                "Institution_Name": "Sample Institution",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": {"Attributes_To_Show": ["Access_Method"]},
                "Report_Filters": {"Begin_Date": "2022-01-01", "End_Date": "2022-12-31"},
                "Report_ID": "DR",
                "Report_Name": "Database Report",
            },
        ),
        (
            "51/PR_sample_r51.json",
            "static.counter51.PR.Json",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Institution_ID": {"ISNI": ["1234123412341234"]},
                "Institution_Name": "Sample Institution",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": {"Attributes_To_Show": ["Access_Method"]},
                "Report_Filters": {"Begin_Date": "2022-01-01", "End_Date": "2022-12-31"},
                "Report_ID": "PR",
                "Report_Name": "Platform Report",
            },
        ),
        (
            "51/TR_sample_r51.json",
            "static.counter51.TR.Json",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Institution_ID": {"ISNI": ["1234123412341234"]},
                "Institution_Name": "Sample Institution",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": {
                    "Attributes_To_Show": ["YOP", "Access_Type", "Access_Method"]
                },
                "Report_Filters": {"Begin_Date": "2022-01-01", "End_Date": "2022-12-31"},
                "Report_ID": "TR",
                "Report_Name": "Title Report",
            },
        ),
        (
            "51/IR_sample_r51.json",
            "static.counter51.IR.Json",
            True,
            {
                "Created": "2023-02-15T09:11:12Z",
                "Created_By": "Sample Publisher",
                "Institution_ID": {"ISNI": ["1234123412341234"]},
                "Institution_Name": "Sample Institution",
                "Release": "5.1",
                "Registry_Record": "https://registry.projectcounter.org/platform/99999999-9999-9999-9999-999999999999",
                "Report_Attributes": {
                    "Attributes_To_Show": [
                        "Authors",
                        "Publication_Date",
                        "Article_Version",
                        "YOP",
                        "Access_Type",
                        "Access_Method",
                    ],
                    "Include_Parent_Details": "True",
                },
                "Report_Filters": {"Begin_Date": "2022-01-01", "End_Date": "2022-12-31"},
                "Report_ID": "IR",
                "Report_Name": "Item Report",
            },
        ),
    ),
)
def test_success(file, parser, success, extras):
    source_path = pathlib.Path(__file__).parent / "data/counter" / file
    output_path = pathlib.Path(__file__).parent / "data/counter" / f"{file}.out"

    poop = eat(source_path, "P1", check_platform=False, parsers=[parser], use_heuristics=True)[0]
    if not success:
        assert isinstance(poop, NoParserMatchesHeuristics)
        return

    assert poop.extras == extras
    assert poop.get_months() == [
        [
            date(2022, 1, 1),
            date(2022, 2, 1),
            date(2022, 3, 1),
            date(2022, 4, 1),
            date(2022, 5, 1),
            date(2022, 6, 1),
            date(2022, 7, 1),
            date(2022, 8, 1),
            date(2022, 9, 1),
            date(2022, 10, 1),
            date(2022, 11, 1),
            date(2022, 12, 1),
        ]
    ]
    assert poop.parser.name == parser

    with output_path.open() as f:
        reader = csv.reader(f)

        for record in poop.records():
            assert next(reader) == list(record.as_csv()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"


@pytest.mark.parametrize(
    "file,parser,months",
    (
        (
            "51/DR_empty_r51.tsv",
            "static.counter51.DR.Tabular",
            [date(2022, i, 1) for i in range(1, 13)],
        ),
        (
            "51/PR_empty_r51.tsv",
            "static.counter51.PR.Tabular",
            [date(2022, i, 1) for i in range(1, 13)],
        ),
        (
            "51/TR_empty_r51.tsv",
            "static.counter51.TR.Tabular",
            [date(2022, i, 1) for i in range(1, 13)],
        ),
        (
            "51/IR_empty_r51.tsv",
            "static.counter51.IR.Tabular",
            [date(2022, i, 1) for i in range(1, 13)],
        ),
        ("51/DR_empty_r51.json", "static.counter51.DR.Json", [date(2022, 1, 1)]),
        ("51/PR_empty_r51.json", "static.counter51.PR.Json", [date(2022, 1, 1)]),
        ("51/TR_empty_r51.json", "static.counter51.TR.Json", [date(2022, 1, 1)]),
        ("51/IR_empty_r51.json", "static.counter51.IR.Json", [date(2022, 1, 1)]),
    ),
)
def test_empty(file, parser, months):
    source_path = pathlib.Path(__file__).parent / "data/counter" / file
    poop = eat(source_path, "Platform1", parsers=[parser])[0]
    assert poop.get_months() == [months]
    assert len([e for e in poop.records()]) == 0
    assert poop.parser.name == parser
