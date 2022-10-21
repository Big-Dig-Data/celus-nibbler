import csv
import json
import pathlib

import pytest

from celus_nibbler import Poop, eat
from celus_nibbler.definitions import Definition
from celus_nibbler.errors import TableException
from celus_nibbler.parsers import available_parsers
from celus_nibbler.parsers.dynamic import gen_parser


@pytest.mark.parametrize(
    "platform,filename,ext,parser,aggregated",
    (
        ("Platform1", "simple", "csv", "dynamic.non_counter.simple_format.simple", False),
        (
            "Platform1",
            "sheet_attr",
            "xlsx",
            "dynamic.non_counter.simple_format.sheet_attr",
            False,
        ),
        ("Platform1", "coord", "csv", "dynamic.non_counter.simple_format.coord", False),
        ("Platform1", "value", "csv", "dynamic.non_counter.simple_format.value", False),
        ("Platform1", "aliases", "csv", "dynamic.non_counter.simple_format.aliases", False),
        (
            "Platform1",
            "organization",
            "csv",
            "dynamic.non_counter.organization_format.organization",
            False,
        ),
        (
            "Platform1",
            "no_title",
            "csv",
            "dynamic.non_counter.simple_format.no_title",
            False,
        ),
        (
            "Platform1",
            "zero_value",
            "csv",
            "dynamic.non_counter.simple_format.zero_value",
            False,
        ),
        (
            "Platform1",
            "last_as_default",
            "csv",
            "dynamic.non_counter.simple_format.last_as_default",
            False,
        ),
        ("Platform1", "blank", "csv", "dynamic.non_counter.simple_format.blank", False),
        ("MIT", "counter5.TR", "xlsx", "dynamic.counter5.TR.MY_TR_CUSTOM", False),
        ("JSTOR", "counter5.IR_M1", "csv", "dynamic.counter5.IR_M1.MY_IR_M1_CUSTOM", False),
        (
            "PlatformWithCustomDR",
            "counter5.DR_MY_CUSTOM",
            "xlsx",
            "dynamic.counter5.MY_DR.DR_MY_CUSTOM",
            False,
        ),
        (
            "My Platform",
            "non_counter/my-metric-based",
            "xlsx",
            "dynamic.non_counter.simple_format.my-metric-based",
            True,
        ),
        (
            "My Platform",
            "non_counter/my-date-metric-based",
            "xlsx",
            "dynamic.non_counter.simple_format.my-date-metric-based",
            False,
        ),
    ),
)
def test_dynamic(platform, filename, ext, parser, aggregated):
    definition_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{filename}.json"
    input_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{filename}.{ext}"
    output_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{filename}.{ext}.out"

    with definition_path.open() as f:
        definition = json.load(f)
    dynamic_parsers = [gen_parser(Definition.parse(definition))]

    # Parser should be present among available parsers
    assert parser in available_parsers(dynamic_parsers)

    with output_path.open() as f:
        reader = csv.reader(f)
        poops = eat(input_path, platform, parsers=[parser], dynamic_parsers=dynamic_parsers)
        for poop in [poop for poop in poops if isinstance(poop, Poop)]:
            # Aggregated records might be out of order, we need to sort it first
            records = (
                sorted(poop.records(), key=lambda x: x.as_csv()) if aggregated else poop.records()
            )

            for record in records:
                assert next(reader) == list(record.as_csv()), "Compare lines"

            with pytest.raises(StopIteration):
                assert next(reader) is None, "No more date present in the file"


@pytest.mark.parametrize(
    "platform,name,ext,parser,exception",
    (
        (
            "Platform1",
            "date_cells_out_of_range",
            "csv",
            "dynamic.non_counter.simple_format.date_cells_out_of_range",
            TableException(sheet=0, col=5, row=1, reason="no-header-data-found", value=None),
        ),
        (
            "Platform1",
            "wrong_fixed_value",
            "csv",
            "dynamic.non_counter.simple_format.wrong_fixed_value",
            TableException(sheet=0, reason="wrong-value", value="not_issn"),
        ),
        (
            "Platform1",
            "metric_number",
            "csv",
            "dynamic.non_counter.simple_format.metric_number",
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
