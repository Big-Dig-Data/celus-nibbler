import csv
import json
import pathlib

import pytest

from celus_nibbler import eat
from celus_nibbler.definitions import Definition
from celus_nibbler.parsers import available_parsers, load_parser_definitions


@pytest.mark.parametrize(
    "platform,name,parser", (("Platform1", "simple", "nibbler.dynamic.simple"),)
)
def test_dynamic(platform, name, parser):
    definition_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{name}.json"
    input_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{name}.csv"
    output_path = pathlib.Path(__file__).parent / 'data/dynamic' / f"{name}.csv.out"

    with definition_path.open() as f:
        definition = json.load(f)
    load_parser_definitions([Definition.parse(definition)])

    # Parser should be present among available parsers
    assert parser in available_parsers()

    with output_path.open() as f:
        reader = csv.reader(f)
        poop = eat(input_path, platform, parsers=[parser])[0]

        for record in poop.records():
            assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"
