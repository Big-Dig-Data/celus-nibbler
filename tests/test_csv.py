import functools
from dataclasses import asdict
from datetime import date

import pytest

from celus_nibbler import CsvDefaultReport


@pytest.mark.skip(
    reason="due to currently using different approach to testing, this test is not in use"
)
def test_default(base_path):
    report = CsvDefaultReport(
        str(base_path / 'data/csv/ex-title-metric-publisher-success.csv'),
        platform="My platform",
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
        "platform": "My platform",
    }, "First record"

    assert asdict(output[-1]) == {
        "start": date(2018, 6, 1),
        "end": date(2018, 6, 30),
        "value": 15,
        "dimension_data": {"Publisher": "Pub 1", "Success": "Denied"},
        "title_ids": {},
        "title": "CCC",
        "metric": "Exports",
        "platform": "My platform",
    }, "Last record"

    total = functools.reduce(
        lambda acc, rec: rec.value + acc, output, 0
    )  # functools.reduce(function, iterable[, initializer])
    # Apply function of two arguments cumulatively to the items of iterable,
    # from left to right, so as to reduce the iterable to a single value.
    # For example, reduce(lambda x, y: x+y, [1, 2, 3, 4, 5]) calculates ((((1+2)+3)+4)+5).
    assert total == 6393, "Total"
