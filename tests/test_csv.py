import functools
from dataclasses import asdict
from datetime import date

from celus_nibbler import CsvDefaultReport


def test_default(base_path):
    report = CsvDefaultReport(
        str(base_path / 'data/csv/ex-title-metric-publisher-success.csv'),
        platform_name="My platform",
    )
    gen = report.output()

    # check first two records
    output = [e for e in gen]
    assert len(output) == 36

    assert asdict(output[0]) == {
        "start": date(2018, 1, 1),
        "end": date(2018, 1, 31),
        "value": 1206,
        "dimension_data": {"Publisher": "Pub 1", "Success": "Success"},
        "title_ids": {},
        "title": "AAA",
        "metric": "Exports",
        "platform_name": "My platform",
    }, "First record"

    assert asdict(output[-1]) == {
        "start": date(2018, 6, 1),
        "end": date(2018, 6, 30),
        "value": 15,
        "dimension_data": {"Publisher": "Pub 1", "Success": "Denied"},
        "title_ids": {},
        "title": "CCC",
        "metric": "Exports",
        "platform_name": "My platform",
    }, "Last record"

    total = functools.reduce(lambda acc, rec: rec.value + acc, output, 0)
    assert total == 6393, "Total"
