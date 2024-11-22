import csv
import pathlib

import pytest

from celus_nibbler import eat
from celus_nibbler.errors import NoParserMatchesHeuristics, TableException


@pytest.mark.parametrize(
    "file,parser,heuristics,success,extras",
    (
        (
            "4/BR1-a.tsv",
            "static.counter4.BR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "Celus University",
                "Created": "2020-01-01",
                "Reporting_Period": "2018-01-01 to 2018-12-31",
            },
        ),
        ("4/BR1-b.tsv", "static.counter4.BR1.Tabular", False, True, {}),
        ("4/BR1-b.tsv", "static.counter4.BR1.Tabular", True, False, {}),
        (
            "4/BR1-empty1.tsv",
            "static.counter4.BR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "Celus University",
                "Created": "2020-01-01",
                "Reporting_Period": "2018-01-01 to 2018-12-31",
            },
        ),
        (
            "4/BR1-empty2.tsv",
            "static.counter4.BR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "Celus University",
                "Created": "2020-01-01",
                "Reporting_Period": "2018-01-01 to 2018-12-31",
            },
        ),
        (
            "4/BR1-empty_values.csv",
            "static.counter4.BR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "Celus University",
                "Created": "2020-01-01",
                "Reporting_Period": "2018-01-01 to 2018-12-31",
            },
        ),
        (
            "4/BR2-a.tsv",
            "static.counter4.BR2.Tabular",
            True,
            True,
            {
                "Institution_Name": "Celus University",
                "Created": "2020-01-01",
                "Reporting_Period": "2017-01-01 to 2017-12-31",
            },
        ),
        ("4/BR2-b.tsv", "static.counter4.BR2.Tabular", False, True, {}),
        ("4/BR2-b.tsv", "static.counter4.BR2.Tabular", True, False, {}),
        (
            "4/BR3-a.tsv",
            "static.counter4.BR3.Tabular",
            True,
            True,
            {
                "Institution_Name": "Celus University",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        ("4/BR3-b.tsv", "static.counter4.BR3.Tabular", False, True, {}),
        ("4/BR3-b.tsv", "static.counter4.BR3.Tabular", True, False, {}),
        (
            "4/DB1-a.tsv",
            "static.counter4.DB1.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        ("4/DB1-b.tsv", "static.counter4.DB1.Tabular", False, True, {}),
        ("4/DB1-b.tsv", "static.counter4.DB1.Tabular", True, False, {}),
        (
            "4/DB2-a.tsv",
            "static.counter4.DB2.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2012-07-01",
                "Reporting_Period": "2012-01-01 to 2012-06-30",
            },
        ),
        ("4/DB2-b.tsv", "static.counter4.DB2.Tabular", False, True, {}),
        ("4/DB2-b.tsv", "static.counter4.DB2.Tabular", True, False, {}),
        (
            "4/PR1-a.tsv",
            "static.counter4.PR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "My library",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        ("4/PR1-b.tsv", "static.counter4.PR1.Tabular", False, True, {}),
        ("4/PR1-b.tsv", "static.counter4.PR1.Tabular", True, False, {}),
        (
            "4/PR1-c.tsv",
            "static.counter4.PR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "My library",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        (
            "4/JR1-a.tsv",
            "static.counter4.JR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        ("4/JR1-b.tsv", "static.counter4.JR1.Tabular", False, True, {}),
        ("4/JR1-b.tsv", "static.counter4.JR1.Tabular", True, False, {}),
        (
            "4/JR1-d.csv",
            "static.counter4.JR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "MyOrg",
                "Created": "1/1/2023",
                "Reporting_Period": "2022-01-01 to 2022-12-31",
            },
        ),
        (
            "4/JR1a-a.tsv",
            "static.counter4.JR1a.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2021-01-01",
                "Reporting_Period": "2020-01-01 to 2020-09-30",
            },
        ),
        ("4/JR1a-b.tsv", "static.counter4.JR1a.Tabular", False, True, {}),
        ("4/JR1a-b.tsv", "static.counter4.JR1a.Tabular", True, False, {}),
        (
            "4/JR1GOA-a.tsv",
            "static.counter4.JR1GOA.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        ("4/JR1GOA-b.tsv", "static.counter4.JR1GOA.Tabular", False, True, {}),
        ("4/JR1GOA-b.tsv", "static.counter4.JR1GOA.Tabular", True, False, {}),
        (
            "4/JR2-a.tsv",
            "static.counter4.JR2.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2022-04-01",
                "Reporting_Period": "2022-03-01 to 2022-03-31",
            },
        ),
        ("4/JR2-b.tsv", "static.counter4.JR2.Tabular", False, True, {}),
        ("4/JR2-b.tsv", "static.counter4.JR2.Tabular", True, False, {}),
        (
            "4/MR1-a.tsv",
            "static.counter4.MR1.Tabular",
            True,
            True,
            {
                "Institution_Name": "My Library",
                "Created": "2020-11-10",
                "Reporting_Period": "2018-01-01 to 2018-12-31",
            },
        ),
        ("4/MR1-b.tsv", "static.counter4.MR1.Tabular", False, True, {}),
        ("4/MR1-b.tsv", "static.counter4.MR1.Tabular", True, False, {}),
    ),
)
def test_success(file, parser, heuristics, success, extras):
    source_path = pathlib.Path(__file__).parent / "data/counter" / file
    output_path = pathlib.Path(__file__).parent / "data/counter" / f"{file}.out"

    poop = eat(source_path, "Platform1", parsers=[parser], use_heuristics=heuristics)[0]

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
            "4/errors/BR1-wrong_months1.tsv",
            TableException(reason="no-counter-header-found", sheet=0),
        ),
        (
            "4/errors/BR1-wrong_months2.tsv",
            TableException(reason="no-counter-header-found", sheet=0),
        ),
    ],
)
def test_error(file, exception):
    source_path = pathlib.Path(__file__).parent / "data/counter" / file
    poop = eat(source_path, "Platform1", check_platform=False)[0]
    with pytest.raises(type(exception)) as exc:
        list(poop.records())
    assert exc.value == exception
