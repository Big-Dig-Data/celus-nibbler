import csv
import json
import pathlib
from datetime import date

import pytest
from celus_nigiri import CounterRecord

from celus_nibbler import Poop, eat
from celus_nibbler.definitions import Definition
from celus_nibbler.errors import (
    MissingDateInOutput,
    NegativeValueInOutput,
    SameRecordsInOutput,
    TableException,
)
from celus_nibbler.parsers import available_parsers
from celus_nibbler.parsers.dynamic import gen_parser


@pytest.mark.parametrize(
    "filename,ext,parser,ignore_order",
    (
        ("simple", "csv", "dynamic.non_counter.simple_format.simple", False),
        (
            "sheet_attr",
            "xlsx",
            "dynamic.non_counter.simple_format.sheet_attr",
            False,
        ),
        (
            "sheet_attr",
            "xls",
            "dynamic.non_counter.simple_format.sheet_attr",
            False,
        ),
        ("coord", "csv", "dynamic.non_counter.simple_format.coord", False),
        ("value", "csv", "dynamic.non_counter.simple_format.value", False),
        ("aliases", "csv", "dynamic.non_counter.simple_format.aliases", False),
        ("aliases2", "csv", "dynamic.non_counter.simple_format.aliases2", True),
        (
            "organization",
            "csv",
            "dynamic.non_counter.organization_format.organization",
            False,
        ),
        (
            "no_title",
            "csv",
            "dynamic.non_counter.simple_format.no_title",
            False,
        ),
        (
            "zero_value",
            "csv",
            "dynamic.non_counter.simple_format.zero_value",
            False,
        ),
        (
            "last_as_default",
            "csv",
            "dynamic.non_counter.simple_format.last_as_default",
            False,
        ),
        ("blank", "csv", "dynamic.non_counter.simple_format.blank", False),
        ("counter5.TR", "xlsx", "dynamic.counter5.TR.MY_TR_CUSTOM", False),
        ("counter5.IR_M1", "csv", "dynamic.counter5.IR_M1.MY_IR_M1_CUSTOM", False),
        (
            "counter5.DR_MY_CUSTOM",
            "xlsx",
            "dynamic.counter5.MY_DR.DR_MY_CUSTOM",
            False,
        ),
        (
            "counter51.PR_MY_CUSTOM",
            "tsv",
            "dynamic.counter51.MY_PR.PR_MY_CUSTOM",
            False,
        ),
        (
            "non_counter/my-metric-based",
            "xlsx",
            "dynamic.non_counter.simple_format.my-metric-based",
            True,
        ),
        (
            "non_counter/my-date-metric-based",
            "xlsx",
            "dynamic.non_counter.simple_format.my-date-metric-based",
            True,
        ),
        (
            "non_counter/celus_format-2d-3x2x3-endate",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "non_counter/celus_format-2d-3x2x3-isodate",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "non_counter/celus_format-2d-3x2x3-org-isodate",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "non_counter/celus_format-simple-3x3-endate",
            "csv",
            "dynamic.non_counter.celus_format2.tabular",
            False,
        ),
        (
            "non_counter/celus_format-simple-3x3-isodate",
            "csv",
            "dynamic.non_counter.celus_format2.tabular",
            False,
        ),
        (
            "non_counter/celus_format-complex",
            "csv",
            "dynamic.non_counter.Complex.tabular",
            False,
        ),
        (
            "non_counter/celus_format-no-metric-column",
            "csv",
            "dynamic.non_counter.celus_format2.tabular",
            False,
        ),
        (
            "non_counter/celus_format-2d-3x2x3-%b-%y",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        (
            "non_counter/celus_format-2d-3x2x3-%y-%b",
            "csv",
            "dynamic.non_counter.celus_format1.tabular",
            False,
        ),
        ("skip_column1", "csv", "dynamic.non_counter.skip_format.skip_column1", False),
        ("skip_column2", "csv", "dynamic.non_counter.skip_format.skip_column2", False),
        ("stop_column1", "csv", "dynamic.non_counter.stop_format.stop_column1", False),
        ("stop_column2", "csv", "dynamic.non_counter.stop_format.stop_column2", False),
        (
            "complex_header_parsing",
            "csv",
            "dynamic.non_counter.complex_header_parsing.complex_header_parsing",
            True,
        ),
        (
            "complex_header_parsing2",
            "csv",
            "dynamic.non_counter.complex_header_parsing2.complex_header_parsing2",
            False,
        ),
        (
            "organization_sheet_name",
            "xlsx",
            "dynamic.non_counter.org_sheet_name.org_sheet_name",
            False,
        ),
        (
            "dr1_c3",
            "csv",
            "dynamic.non_counter.dr1_c3.dr1_c3",
            False,
        ),
        (
            "value_commas",
            "csv",
            "dynamic.non_counter.simple_format.value_commas",
            False,
        ),
        (
            "value_minutes_to_seconds",
            "csv",
            "dynamic.non_counter.minutes_format.value_minutes_to_seconds",
            False,
        ),
        (
            "value_minutes_to_seconds2",
            "csv",
            "dynamic.non_counter.minutes_format.value_minutes_to_seconds2",
            False,
        ),
        (
            "value_minutes_to_seconds3",
            "csv",
            "dynamic.non_counter.minutes_format.value_minutes_to_seconds3",
            True,
        ),
        (
            "value_hours_to_seconds",
            "csv",
            "dynamic.non_counter.hours_format.value_hours_to_seconds",
            False,
        ),
        (
            "value_duration_to_seconds",
            "csv",
            "dynamic.non_counter.duration_format.value_duration_to_seconds",
            False,
        ),
        (
            "value_duration_to_seconds2",
            "csv",
            "dynamic.non_counter.duration_format.value_duration_to_seconds2",
            False,
        ),
        (
            "value_duration_to_seconds3",
            "csv",
            "dynamic.non_counter.duration_format.value_duration_to_seconds3",
            False,
        ),
        ("simple-dates-US", "csv", "dynamic.non_counter.simple_format.simple", False),
        ("simple-dates-EU", "csv", "dynamic.non_counter.simple_format.simple", False),
        (
            "two_line_dates1",
            "csv",
            "dynamic.non_counter.simple_format.two_line_dates1",
            False,
        ),
        (
            "two_line_dates2",
            "csv",
            "dynamic.non_counter.simple_format.two_line_dates2",
            False,
        ),
        (
            "row_offset",
            "csv",
            "dynamic.non_counter.simple_format.row_offset",
            False,
        ),
        (
            "prefix_and_suffix_extraction",
            "csv",
            "dynamic.non_counter.simple_format.prefix_and_suffix_extraction",
            False,
        ),
        (
            "data_header-condition",
            "csv",
            "dynamic.non_counter.simple_format.data_header-condition",
            False,
        ),
        (
            "ids-fallback",
            "csv",
            "dynamic.non_counter.simple_format.ids-fallback",
            False,
        ),
        (
            "ids-fallback-isbn10",
            "csv",
            "dynamic.non_counter.simple_format.ids-fallback-isbn10",
            False,
        ),
        (
            "ids-fallback-isbn13",
            "csv",
            "dynamic.non_counter.simple_format.ids-fallback-isbn13",
            False,
        ),
        (
            "negative-values",
            "csv",
            "dynamic.non_counter.simple_format.negative-values",
            True,
        ),
        (
            "generic",
            "csv",
            "dynamic.non_counter.simple_format.generic",
            False,
        ),
        (
            "generic-no_values",
            "csv",
            "dynamic.non_counter.simple_format.generic-no_values",
            True,
        ),
        (
            "available-metrics",
            "csv",
            "dynamic.non_counter.simple_format.available-metrics",
            False,
        ),
        (
            "available-metrics2",
            "csv",
            "dynamic.non_counter.simple_format.available-metrics2",
            True,
        ),
        (
            "available-metrics-stop",
            "csv",
            "dynamic.non_counter.simple_format.available-metrics-stop",
            False,
        ),
        (
            "available-metrics2-stop",
            "csv",
            "dynamic.non_counter.simple_format.available-metrics2-stop",
            True,
        ),
        (
            "metrics-to-skip1",
            "csv",
            "dynamic.non_counter.simple_format.metrics-to-skip1",
            False,
        ),
        (
            "metrics-to-skip2",
            "csv",
            "dynamic.non_counter.simple_format.metrics-to-skip2",
            True,
        ),
        (
            "metrics-max_idx",
            "csv",
            "dynamic.non_counter.simple_format.metrics-max_idx",
            False,
        ),
        (
            "ignore-second-row",
            "csv",
            "dynamic.non_counter.ignore_second_row.ignore_second_row",
            False,
        ),
        (
            "dimension-skip",
            "csv",
            "dynamic.non_counter.simple_format.dimension_skip",
            False,
        ),
        (
            "dimensions-validators",
            "csv",
            "dynamic.non_counter.simple_format.dimensions_validators",
            False,
        ),
        (
            "metrics-fallback",
            "csv",
            "dynamic.non_counter.simple_format.metrics-fallback",
            False,
        ),
        (
            "dynamic_areas",
            "csv",
            "dynamic.non_counter.simple_format.dynamic_areas",
            False,
        ),
        (
            "dynamic_areas_value_offset",
            "csv",
            "dynamic.non_counter.simple_format.dynamic_areas_value_offset",
            False,
        ),
        (
            "dynamic_areas_coord_offsets",
            "csv",
            "dynamic.non_counter.simple_format.dynamic_areas_coord_offsets",
            False,
        ),
        (
            "dynamic_areas_fixed_area_offset",
            "csv",
            "dynamic.non_counter.simple_format.dynamic_areas_fixed_area_offset",
            False,
        ),
        (
            "datetime_dates",
            "csv",
            "dynamic.non_counter.simple_format.datetime_dates",
            False,
        ),
        (
            "year-column-month-row",
            "csv",
            "dynamic.non_counter.simple_format.year-column-month-row",
            False,
        ),
    ),
)
def test_dynamic(filename, ext, parser, ignore_order):
    definition_path = pathlib.Path(__file__).parent / "data/dynamic" / f"{filename}.json"
    input_path = pathlib.Path(__file__).parent / "data/dynamic" / f"{filename}.{ext}"
    output_path = pathlib.Path(__file__).parent / "data/dynamic" / f"{filename}.{ext}.out"

    with definition_path.open() as f:
        definition = json.load(f)
    dynamic_parsers = [gen_parser(Definition.parse(definition))]

    # Parser should be present among available parsers
    assert parser in available_parsers(dynamic_parsers)

    with output_path.open() as f:
        reader = csv.reader(f)
        if ignore_order:
            reader = iter(sorted(reader))

        poops = eat(
            input_path,
            "Platform1",
            check_platform=False,
            parsers=[parser],
            dynamic_parsers=dynamic_parsers,
        )

        idx = 0
        for poop in [poop for poop in poops if isinstance(poop, Poop)]:
            # Aggregated records might be out of order, we need to sort it first
            records = (
                sorted(poop.records(), key=lambda x: x.as_csv()) if ignore_order else poop.records()
            )
            records = list(records)

            # Just call get_months to see whether the function doesn't crash
            poop.get_months()

            assert poop.parser.name == parser

            for idx, record in enumerate(records, 1):
                in_file = next(reader)
                assert in_file == list(record.as_csv()), f"Compare {idx}."

        with pytest.raises(StopIteration):
            assert next(reader) is None, f"No more date present in the file (read {idx})."


@pytest.mark.parametrize(
    "name,ext,parser,exception",
    (
        (
            "date_cells_out_of_range",
            "csv",
            "dynamic.non_counter.simple_format.date_cells_out_of_range",
            TableException(sheet=0, col=5, row=1, reason="no-header-data-found", value=None),
        ),
        (
            "wrong_fixed_value",
            "csv",
            "dynamic.non_counter.simple_format.wrong_fixed_value",
            TableException(sheet=0, reason="wrong-value", value=" 1 "),
        ),
        (
            "metric_number",
            "csv",
            "dynamic.non_counter.simple_format.metric_number",
            TableException(sheet=0, reason="cant-be-digit", value="2", row=2, col=1),
        ),
        (
            "unknown_column_in_celus_format",
            "csv",
            "dynamic.non_counter.unknown_column.unknown_column_in_celus_format",
            TableException(sheet=0, reason="unknown-column", value="Extra", row=0, col=2),
        ),
        (
            "same_records",
            "csv",
            "dynamic.non_counter.simple_format.same_records",
            SameRecordsInOutput(
                0,
                2,
                CounterRecord(
                    start=date(2022, 1, 1),
                    end=date(2022, 1, 31),
                    organization=None,
                    metric="M1",
                    title="T1",
                    dimension_data={"Dim1": "D11"},
                    value=3,
                ),
            ),
        ),
        (
            "negative-values-in-output",
            "csv",
            "dynamic.non_counter.simple_format.negative-values-in-output",
            NegativeValueInOutput(
                0,
                CounterRecord(
                    start=date(2020, 1, 1),
                    end=date(2020, 1, 31),
                    organization=None,
                    metric="Denied",
                    title="T1",
                    title_ids={"Print_ISSN": "1111-1111"},
                    dimension_data={"Platform": "P1", "YOP": "2015"},
                    value=-1,
                ),
            ),
        ),
        (
            "available-metrics-fail",
            "csv",
            "dynamic.non_counter.simple_format.available-metrics-fail",
            TableException(sheet=0, reason="wrong-metric-found", value="Denied"),
        ),
        (
            "available-metrics2-fail",
            "csv",
            "dynamic.non_counter.simple_format.available-metrics2-fail",
            TableException(sheet=0, reason="wrong-metric-found", value="Metric3"),
        ),
        (
            "dynamic_areas-min_valid",
            "csv",
            "dynamic.non_counter.simple_format.dynamic_areas-min_valid",
            TableException(sheet=0, reason="no-header-data-found"),
        ),
        (
            "missing-dates",
            "csv",
            "dynamic.non_counter.simple_format.missing-dates",
            MissingDateInOutput(
                idx=0,
                record=CounterRecord(
                    start=None,
                    end=None,
                    metric="M1",
                    value=1,
                ),
            ),
        ),
        (
            "not-aligned-date-US",
            "csv",
            "dynamic.non_counter.simple_format.not-aligned-date-US",
            TableException(sheet=0, reason="date-not-aligned", row=2, col=0, value="1/2/2020"),
        ),
        (
            "not-aligned-date-EU",
            "csv",
            "dynamic.non_counter.simple_format.not-aligned-date-EU",
            TableException(sheet=0, reason="date-not-aligned", row=2, col=0, value="2/1/2020"),
        ),
        (
            "absolute_coords_in_exception",
            "csv",
            "dynamic.non_counter.simple_format.absolute_coords_in_exception",
            TableException(sheet=0, reason="value", value="X", row=5, col=2),
        ),
        (
            "absolute_coords_in_exception2",
            "csv",
            "dynamic.non_counter.simple_format.absolute_coords_in_exception2",
            TableException(sheet=0, reason="no-header-data-found", value=None, row=4, col=1),
        ),
    ),
)
def test_dynamic_errors(name, ext, parser, exception):
    definition_path = pathlib.Path(__file__).parent / "data/dynamic/errors" / f"{name}.json"
    input_path = pathlib.Path(__file__).parent / "data/dynamic/errors" / f"{name}.{ext}"

    with definition_path.open() as f:
        definition = json.load(f)
    dynamic_parsers = [gen_parser(Definition.parse(definition))]

    # Parser should be present among available parsers
    assert parser in available_parsers(dynamic_parsers)
    poop = eat(
        input_path,
        "Platform1",
        check_platform=False,
        parsers=[parser],
        dynamic_parsers=dynamic_parsers,
    )[0]
    with pytest.raises(type(exception)) as exc:
        list(poop.records(same_check_size=100))
    assert exc.value == exception
