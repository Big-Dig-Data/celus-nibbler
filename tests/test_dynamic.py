import csv
import json
import pathlib
from datetime import date

import pytest
from celus_nigiri import CounterRecord

from celus_nibbler import Poop, eat
from celus_nibbler.definitions import Definition
from celus_nibbler.errors import NegativeValueInOutput, SameRecordsInOutput, TableException
from celus_nibbler.parsers import available_parsers
from celus_nibbler.parsers.dynamic import gen_parser


@pytest.mark.parametrize(
    "platform,filename,ext,parser,ignore_order",
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
            True,
        ),
        (
            "Platform1",
            "non_counter/celus_format-2d-3x2x3-endate",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-2d-3x2x3-isodate",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-2d-3x2x3-org-isodate",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-simple-3x3-endate",
            "csv",
            "dynamic.non_counter.celus_format2.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-simple-3x3-isodate",
            "csv",
            "dynamic.non_counter.celus_format2.tabular",
            False,
        ),
        (
            "Platform Complex",
            "non_counter/celus_format-complex",
            "csv",
            "dynamic.non_counter.Complex.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-no-metric-column",
            "csv",
            "dynamic.non_counter.celus_format2.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-2d-3x2x3-%b-%y",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "Platform1",
            "non_counter/celus_format-2d-3x2x3-%y-%b",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        ("Platform1", "skip_column1", "csv", "dynamic.non_counter.skip_format.skip_column1", False),
        ("Platform1", "skip_column2", "csv", "dynamic.non_counter.skip_format.skip_column2", False),
        ("Platform1", "stop_column1", "csv", "dynamic.non_counter.stop_format.stop_column1", False),
        ("Platform1", "stop_column2", "csv", "dynamic.non_counter.stop_format.stop_column2", False),
        (
            "Platform1",
            "complex_header_parsing",
            "csv",
            "dynamic.non_counter.complex_header_parsing.complex_header_parsing",
            True,
        ),
        (
            "PlatformX",
            "organization_sheet_name",
            "xlsx",
            "dynamic.non_counter.org_sheet_name.org_sheet_name",
            False,
        ),
        (
            "Platform1",
            "dr1_c3",
            "csv",
            "dynamic.non_counter.dr1_c3.dr1_c3",
            False,
        ),
        (
            "Platform1",
            "value_commas",
            "csv",
            "dynamic.non_counter.simple_format.value_commas",
            False,
        ),
        ("Platform1", "simple-dates-US", "csv", "dynamic.non_counter.simple_format.simple", False),
        ("Platform1", "simple-dates-EU", "csv", "dynamic.non_counter.simple_format.simple", False),
        (
            "Platform1",
            "two_line_dates1",
            "csv",
            "dynamic.non_counter.simple_format.two_line_dates1",
            False,
        ),
        (
            "Platform1",
            "two_line_dates2",
            "csv",
            "dynamic.non_counter.simple_format.two_line_dates2",
            False,
        ),
        (
            "Platform2",
            "row_offset",
            "csv",
            "dynamic.non_counter.simple_format.row_offset",
            False,
        ),
        (
            "Platform2",
            "prefix_and_suffix_extraction",
            "csv",
            "dynamic.non_counter.simple_format.prefix_and_suffix_extraction",
            False,
        ),
        (
            "Platform1",
            "data_header-condition",
            "csv",
            "dynamic.non_counter.simple_format.data_header-condition",
            False,
        ),
        (
            "Platform1",
            "ids-fallback",
            "csv",
            "dynamic.non_counter.simple_format.ids-fallback",
            False,
        ),
        (
            "Platform1",
            "negative-values",
            "csv",
            "dynamic.non_counter.simple_format.negative-values",
            True,
        ),
        (
            "Platform1",
            "generic",
            "csv",
            "dynamic.non_counter.simple_format.generic",
            True,
        ),
    ),
)
def test_dynamic(platform, filename, ext, parser, ignore_order):
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
        if ignore_order:
            reader = iter(sorted(reader))
        poops = eat(input_path, platform, parsers=[parser], dynamic_parsers=dynamic_parsers)
        for poop in [poop for poop in poops if isinstance(poop, Poop)]:
            # Aggregated records might be out of order, we need to sort it first
            records = (
                sorted(poop.records(), key=lambda x: x.as_csv()) if ignore_order else poop.records()
            )

            for idx, record in enumerate(records):
                in_file = next(reader)
                assert in_file == list(record.as_csv()), f"Compare {idx + 1}."

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
            TableException(sheet=0, reason="wrong-value", value=" 1 "),
        ),
        (
            "Platform1",
            "metric_number",
            "csv",
            "dynamic.non_counter.simple_format.metric_number",
            TableException(sheet=0, reason="metric", value="2", row=2, col=1),
        ),
        (
            "Platform1",
            "unknown_column_in_celus_format",
            "csv",
            "dynamic.non_counter.unknown_column.unknown_column_in_celus_format",
            TableException(sheet=0, reason="unknown-column", value="Extra", row=0, col=2),
        ),
        (
            "Platform1",
            "same_records",
            "csv",
            "dynamic.non_counter.simple_format.same_records",
            SameRecordsInOutput(
                CounterRecord(
                    start=date(2022, 1, 1),
                    end=date(2022, 1, 31),
                    organization=None,
                    metric="M1",
                    title="T1",
                    dimension_data={"Dim1": "D11"},
                    value=3,
                )
            ),
        ),
        (
            "Platform1",
            "negative-values-in-output",
            "csv",
            "dynamic.non_counter.simple_format.negative-values-in-output",
            NegativeValueInOutput(
                CounterRecord(
                    start=date(2020, 1, 1),
                    end=date(2020, 1, 31),
                    organization=None,
                    metric="Denied",
                    title="T1",
                    title_ids={"Print_ISSN": "1111-1111"},
                    dimension_data={"Platform": "P1", "YOP": "2015"},
                    value=-1,
                )
            ),
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
    with pytest.raises(type(exception)) as exc:
        list(poop.records(same_check_size=100))
    assert exc.value == exception
