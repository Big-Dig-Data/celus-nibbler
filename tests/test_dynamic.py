import csv
import json
import pathlib

import pytest

from celus_nibbler import eat
from celus_nibbler.definitions import Definition
from celus_nibbler.errors import TableException
from celus_nibbler.parsers import available_parsers
from celus_nibbler.parsers.dynamic import gen_parser


@pytest.mark.parametrize(
    "platform,name,ext,parser",
    (
        ("Platform1", "simple", "csv", "nibbler.dynamic.simple"),
        ("Platform1", "sheet_attr", "xlsx", "nibbler.dynamic.sheet_attr"),
        ("Platform1", "coord", "csv", "nibbler.dynamic.coord"),
        ("Platform1", "value", "csv", "nibbler.dynamic.value"),
        ("Platform1", "aliases", "csv", "nibbler.dynamic.aliases"),
        ("Platform1", "organization", "csv", "nibbler.dynamic.organization"),
    ),
)
def test_dynamic(platform, name, ext, parser):
    definition_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{name}.json"
    input_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{name}.{ext}"
    output_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{name}.{ext}.out"

    with definition_path.open() as f:
        definition = json.load(f)
    dynamic_parsers = [gen_parser(Definition.parse(definition))]

    # Parser should be present among available parsers
    assert parser in available_parsers(dynamic_parsers)

    with output_path.open() as f:
        reader = csv.reader(f)
        poop = eat(input_path, platform, parsers=[parser], dynamic_parsers=dynamic_parsers)[0]

        for record in poop.records():
            assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"


@pytest.mark.parametrize(
    "platform,name,ext,parser,exception",
    (
        (
            "Platform1",
            "date_cells_out_of_range",
            "csv",
            "nibbler.dynamic.date_cells_out_of_range",
            TableException(sheet=0, col=5, row=0, reason="no-data-found", value=None),
        ),
        (
            "Platform1",
            "wrong_fixed_value",
            "csv",
            "nibbler.dynamic.wrong_fixed_value",
            TableException(sheet=0, reason="wrong-value", value="not_issn"),
        ),
        (
            "Platform1",
            "metric_number",
            "csv",
            "nibbler.dynamic.metric_number",
            TableException(sheet=0, reason="metric", value="2", row=2, col=1),
        ),
    ),
)
def test_dynamic_errors(platform, name, ext, parser, exception):
    definition_path = pathlib.Path(__file__).parent / 'data/dynamic/errors' / f"{name}.json"
    input_path = pathlib.Path(__file__).parent / 'data/dynamic/errors' / f"{name}.{ext}"

    with definition_path.open() as f:
        definition = json.load(f)
    dynamic_parsers = [gen_parser(Definition.parse(definition))]

    # Parser should be present among available parsers
    assert parser in available_parsers(dynamic_parsers)
    poop = eat(input_path, platform, parsers=[parser], dynamic_parsers=dynamic_parsers)[0]
    with pytest.raises(TableException) as exc:
        list(poop.records())
    assert exc.value == exception
