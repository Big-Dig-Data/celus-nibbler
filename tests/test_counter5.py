import csv
import pathlib

import pytest

from celus_nibbler import eat
from celus_nibbler.errors import NoParserMatchesHeuristics, TableException


@pytest.mark.parametrize(
    "platform,file,parser,heuristics,success,extras",
    (
        (
            "WebOfKnowledge",
            "5/DR-a.tsv",
            "static.counter5.DR.Tabular",
            True,
            True,
            {
                "Created": "2020-01-01T00:00:00Z",
                "Created_By": "Web of Science",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "Searches_Federated; Searches_Regular; "
                "Total_Item_Investigations; Unique_Item_Investigations; "
                "Limit_Exceeded; No_License",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "",
                "Report_ID": "DR",
                "Report_Name": "Database Master Report",
                "Reporting_Period": "Begin_Date=2019-10-01; End_Date=2022-02-28",
            },
        ),
        ("WebOfKnowledge", "5/DR-b.tsv", "static.counter5.DR.Tabular", False, True, {}),
        ("WebOfKnowledge", "5/DR-b.tsv", "static.counter5.DR.Tabular", True, False, {}),
        (
            "WebOfKnowledge",
            "5/DR-c.json",
            "static.counter5.DR.Json",
            True,
            True,
            {
                "Created": "2020-01-01T00:00:00Z",
                "Created_By": "My services",
                "Customer_ID": "88888888",
                "Institution_ID": [{"Type": "Proprietary", "Value": "IIIIIIIII:88888888"}],
                "Institution_Name": "My institution",
                "Release": "5",
                "Report_Attributes": [
                    {"Name": "Attributes_To_Show", "Value": "Data_Type|Access_Method"}
                ],
                "Report_Filters": [
                    {"Name": "Begin_Date", "Value": "2020-01-01"},
                    {"Name": "End_Date", "Value": "2020-01-31"},
                ],
                "Report_ID": "DR",
                "Report_Name": "Database Master Report",
            },
        ),
        ("WebOfKnowledge", "5/DR-d.json", "static.counter5.DR.Json", False, True, {}),
        ("WebOfKnowledge", "5/DR-d.json", "static.counter5.DR.Json", True, False, {}),
        (
            "WebOfKnowledge",
            "5/DR-e.csv",
            "static.counter5.DR.Tabular",
            True,
            True,
            {
                "Created": "2020-01-01T00:00:00Z",
                "Created_By": "Web of Science",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "Searches_Federated; Searches_Regular; "
                "Total_Item_Investigations; Unique_Item_Investigations; "
                "Limit_Exceeded; No_License",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "",
                "Report_ID": "DR",
                "Report_Name": "Database Master Report xxx",
                "Reporting_Period": "Begin_Date=2019-10-01; End_Date=2022-02-28",
            },
        ),
        (
            "WebOfKnowledge",
            "5/DR-sample.tsv",
            "static.counter5.DR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:35:16Z",
                "Created_By": "Publisher Platform Alpha",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Access_Method",
                "Report_ID": "DR",
                "Report_Name": "Database Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "WebOfKnowledge",
            "5/DR-f.tsv",
            "static.counter5.DR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:35:16Z",
                "Created_By": "Publisher Platform Alpha",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Access_Method",
                "Report_ID": "DR",
                "Report_Name": "Database Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "MIT",
            "5/PR-a.tsv",
            "static.counter5.PR.Tabular",
            True,
            True,
            {
                "Created": "2020-00-00T00:00:00Z",
                "Created_By": "Silverchair",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "",
                "Report_ID": "PR",
                "Report_Name": "Platform Master Report",
                "Reporting_Period": "Begin_Date=2021-12-01; End_Date=2022-04-30",
            },
        ),
        ("MIT", "5/PR-b.tsv", "static.counter5.PR.Tabular", False, True, {}),
        ("MIT", "5/PR-b.tsv", "static.counter5.PR.Tabular", True, False, {}),
        (
            "MIT",
            "5/PR-c.json",
            "static.counter5.PR.Json",
            True,
            True,
            {
                "Created": "2019-09-25T15:26:44Z",
                "Created_By": "MyProvider",
                "Customer_ID": "1234567",
                "Institution_ID": [{"Type": "Proprietary", "Value": "MyProvider:1234567"}],
                "Institution_Name": "My university",
                "Release": "5",
                "Report_Attributes": [
                    {"Name": "Attributes_To_Show", "Value": "Data_Type|Access_Method"}
                ],
                "Report_Filters": [
                    {"Name": "Begin_Date", "Value": "2019-04-01"},
                    {"Name": "End_Date", "Value": "2019-04-30"},
                    {"Name": "Site", "Value": "1234568"},
                    {
                        "Name": "Metric_Type",
                        "Value": "Searches_Platform|Total_Item_Investigations|"
                        "Unique_Item_Investigations|Unique_Title_Investigations|"
                        "Total_Item_Requests|Unique_Item_Requests|Unique_Title_Requests",
                    },
                    {
                        "Name": "Data_Type",
                        "Value": "Platform|Database|Book|Journal|Multimedia|"
                        "Newspaper_or_Newsletter|Report|Thesis_or_Dissertation|Other",
                    },
                    {"Name": "Access_Method", "Value": "Regular"},
                ],
                "Report_ID": "PR",
                "Report_Name": "Platform Master Report",
            },
        ),
        ("MIT", "5/PR-d.json", "static.counter5.PR.Json", False, True, {}),
        ("MIT", "5/PR-d.json", "static.counter5.PR.Json", True, False, {}),
        (
            "Ovid",
            "5/PR-e.csv",
            "static.counter5.PR.Tabular",
            True,
            True,
            {
                "Created": "2022-01-01T00:00:00Z",
                "Created_By": "Ovid",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Access_Method",
                "Report_Filters": "Data_Type=Book|Database|Journal|Platform; "
                "Access_Method=Regular",
                "Report_ID": "PR",
                "Report_Name": "Platform Master Report xxx",
                "Reporting_Period": "Begin_Date=2020-01-01; End_Date=2020-12-31",
            },
        ),
        (
            "Ovid",
            "5/PR-sample.tsv",
            "static.counter5.PR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:39:38Z",
                "Created_By": "Sample Publisher",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Access_Method",
                "Report_ID": "PR",
                "Report_Name": "Platform Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "Ovid",
            "5/PR-f.tsv",
            "static.counter5.PR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:39:38Z",
                "Created_By": "Sample Publisher",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Access_Method",
                "Report_ID": "PR",
                "Report_Name": "Platform Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "Ovid",
            "5/PR-g.tsv",
            "static.counter5.PR.Tabular",
            True,
            True,
            {
                "Created": "2020-00-00T00:00:00Z",
                "Created_By": "Silverchair",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "",
                "Report_ID": "PR",
                "Report_Name": "Platform Master Report",
                "Reporting_Period": "Begin_Date=2021-12-01; End_Date=2022-04-30",
            },
        ),
        (
            "MIT",
            "5/TR-a.tsv",
            "static.counter5.TR.Tabular",
            True,
            True,
            {
                "Created": "2020-01-01T00:00:00Z",
                "Created_By": "Silverchair",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "",
                "Report_ID": "TR",
                "Report_Name": "Title Master Report",
                "Reporting_Period": "Begin_Date=2021-12-01; End_Date=2022-04-30",
            },
        ),
        ("MIT", "5/TR-b.tsv", "static.counter5.TR.Tabular", False, True, {}),
        ("MIT", "5/TR-b.tsv", "static.counter5.TR.Tabular", True, False, {}),
        (
            "MIT",
            "5/TR-c.json",
            "static.counter5.TR.Json",
            True,
            True,
            {
                "Created": "2020-07-01T02:04:04Z",
                "Created_By": "Someone",
                "Customer_ID": "9999999",
                "Exceptions": [
                    {
                        "Code": 3032,
                        "Data": "First date processed is 2018-07-01",
                        "Message": "Usage No Longer Available for Requested Dates",
                        "Severity": "Warning",
                    }
                ],
                "Institution_ID": [{"Type": "Proprietary", "Value": "XXX:9999999"}],
                "Institution_Name": "My Institution",
                "Release": "5",
                "Report_Attributes": [
                    {
                        "Name": "Attributes_To_Show",
                        "Value": "YOP|Access_Method|Access_Type|Data_Type|Section_Type",
                    }
                ],
                "Report_Filters": [
                    {"Name": "Begin_Date", "Value": "2017-01-01"},
                    {"Name": "End_Date", "Value": "2018-12-31"},
                ],
                "Report_ID": "TR",
                "Report_Name": "Title Master Report",
            },
        ),
        ("MIT", "5/TR-d.json", "static.counter5.TR.Json", False, True, {}),
        ("MIT", "5/TR-d.json", "static.counter5.TR.Json", True, False, {}),
        (
            "MIT",
            "5/TR-e.tsv",
            "static.counter5.TR.Tabular",
            True,
            True,
            {
                "Created": "2020-01-01T00:00:00Z",
                "Created_By": "Silverchair",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "Celus University",
                "Metric_Types": "",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "",
                "Report_ID": "TR",
                "Report_Name": "Title Master Report xxx",
                "Reporting_Period": "Begin_Date=2021-12-01; End_Date=2022-04-30",
            },
        ),
        (
            "MIT",
            "5/TR-sample.tsv",
            "static.counter5.TR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:39:56Z",
                "Created_By": "Publisher Platform Delta",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Section_Type|"
                "YOP|Access_Type|Access_Method",
                "Report_ID": "TR",
                "Report_Name": "Title Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "MIT",
            "5/TR-f.tsv",
            "static.counter5.TR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:39:56Z",
                "Created_By": "Publisher Platform Delta",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Section_Type|"
                "YOP|Access_Type|Access_Method",
                "Report_ID": "TR",
                "Report_Name": "Title Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "MIT",
            "5/TR-with-organization.tsv",
            "static.counter5.TR.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:39:56Z",
                "Created_By": "Publisher Platform Delta",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Release": "5",
                "Report_Attributes": "Attributes_To_Show=Data_Type|Section_Type|"
                "YOP|Access_Type|Access_Method",
                "Report_ID": "TR",
                "Report_Name": "Title Master Report",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "T&F ebooks",
            "5/TR_B1-a.xlsx",
            "static.counter5.TR.Tabular",
            True,
            True,
            {
                "Created": "2020-01-01T00:00:00Z",
                "Created_By": "My Provider",
                "Exceptions": "",
                "Institution_ID": "my_provider:999999",
                "Institution_Name": "My University",
                "Metric_Types": "Total_Item_Requests; Unique_Title_Requests",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "Access_Method=Regular; Access_Type=Controlled; "
                "Data_Type=Book",
                "Report_ID": "TR_B1",
                "Report_Name": "Book Requests (Excluding OA_Gold)",
                "Reporting_Period": "Begin_Date=2022-07-01; End_Date=2022-07-31",
            },
        ),
        (
            "JSTOR",
            "5/IR_M1-a.csv",
            "static.counter5.IR_M1.Tabular",
            True,
            True,
            {
                "Created": "2022-10-01",
                "Created_By": "My Platform",
                "Exceptions": "",
                "Institution_ID": "",
                "Institution_Name": "My Institution",
                "Metric_Types": "Total_Item_Requests",
                "Release": "5",
                "Report_Attributes": "",
                "Report_Filters": "Data_Type=Multimedia; Access_Method=Regular",
                "Report_ID": "IR_M1",
                "Report_Name": "Multimedia Item Requests xxx",
                "Reporting_Period": "2021-07-01 to 2022-06-30",
            },
        ),
        ("JSTOR", "5/IR_M1-b.csv", "static.counter5.IR_M1.Tabular", False, True, {}),
        ("JSTOR", "5/IR_M1-b.csv", "static.counter5.IR_M1.Tabular", True, False, {}),
        (
            "JSTOR",
            "5/IR_M1-c.json",
            "static.counter5.IR_M1.Json",
            True,
            True,
            {
                "Created": "2022-11-01T00:00:00Z",
                "Created_By": "MyProvider",
                "Customer_ID": "11111",
                "Institution_ID": [{"Type": "Proprietary", "Value": "MyProvider:11111"}],
                "Institution_Name": "MyUniversity",
                "Release": "5",
                "Report_Filters": [
                    {"Name": "Platform", "Value": "MyPlatform"},
                    {"Name": "Begin_Date", "Value": "2022-10-01"},
                    {"Name": "End_Date", "Value": "2022-10-31"},
                    {"Name": "Metric_Type", "Value": "Total_Item_Requests"},
                    {"Name": "Data_Type", "Value": "Multimedia"},
                    {"Name": "Access_Method", "Value": "Regular"},
                ],
                "Report_ID": "IR_M1",
                "Report_Name": "Multimedia Item Requests",
            },
        ),
        ("JSTOR", "5/IR_M1-d.json", "static.counter5.IR_M1.Json", False, True, {}),
        ("JSTOR", "5/IR_M1-d.json", "static.counter5.IR_M1.Json", True, False, {}),
        (
            "JSTOR",
            "5/IR_M1-sample.tsv",
            "static.counter5.IR_M1.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:38:17Z",
                "Created_By": "Sample Institutional Repository",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Metric_Types": "Total_Item_Requests",
                "Release": "5",
                "Report_Filters": "Data_Type=Multimedia; Access_Method=Regular",
                "Report_ID": "IR_M1",
                "Report_Name": "Multimedia Item Requests",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
        (
            "JSTOR",
            "5/IR_M1-f.tsv",
            "static.counter5.IR_M1.Tabular",
            True,
            True,
            {
                "Created": "2019-04-25T11:38:17Z",
                "Created_By": "Sample Institutional Repository",
                "Institution_ID": "ISNI:1234123412341234",
                "Institution_Name": "Client Demo Site",
                "Metric_Types": "Total_Item_Requests",
                "Release": "5",
                "Report_Filters": "Data_Type=Multimedia; Access_Method=Regular",
                "Report_ID": "IR_M1",
                "Report_Name": "Multimedia Item Requests",
                "Reporting_Period": "Begin_Date=2016-01-01; End_Date=2016-03-31",
            },
        ),
    ),
)
def test_success(platform, file, parser, heuristics, success, extras):
    source_path = pathlib.Path(__file__).parent / "data/counter" / file
    output_path = pathlib.Path(__file__).parent / "data/counter" / f"{file}.out"

    poop = eat(source_path, platform, parsers=[parser], use_heuristics=heuristics)[0]
    if not success:
        assert isinstance(poop, NoParserMatchesHeuristics)
        return

    assert poop.extras == extras

    with output_path.open() as f:
        reader = csv.reader(f)

        for record in poop.records():
            assert next(reader) == list(record.as_csv()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"


@pytest.mark.parametrize(
    "file,exception",
    [
        (
            "5/errors/TR-wrong_months1.tsv",
            TableException(reason="no-counter-header-found", sheet=0),
        ),
        (
            "5/errors/TR-wrong_months2.tsv",
            TableException(reason="no-counter-header-found", sheet=0),
        ),
        (
            "5/errors/TR-exclude_monthly_details.csv",
            TableException(reason="counter-header-without-monthly-details", sheet=0, row=13),
        ),
    ],
)
def test_error(file, exception):
    source_path = pathlib.Path(__file__).parent / "data/counter" / file
    poop = eat(source_path, "Platform1", check_platform=False)[0]
    with pytest.raises(type(exception)) as exc:
        list(poop.records())
    assert exc.value == exception
