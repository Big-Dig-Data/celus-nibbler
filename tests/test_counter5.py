import csv
import pathlib

import pytest

from celus_nibbler import eat


@pytest.mark.parametrize(
    "platform,file,parser,heuristics",
    (
        ("WebOfKnowledge", "5/DR-a.tsv", "nibbler.counter5.DR", True),
        ("WebOfKnowledge", "5/DR-b.tsv", "nibbler.counter5.DR", False),
        ("MIT", "5/PR-a.tsv", "nibbler.counter5.PR", True),
        ("MIT", "5/PR-b.tsv", "nibbler.counter5.PR", False),
        ("MIT", "5/TR-a.tsv", "nibbler.counter5.TR", True),
        ("MIT", "5/TR-b.tsv", "nibbler.counter5.TR", False),
    ),
)
def test_tsv(platform, file, parser, heuristics):
    source_path = pathlib.Path(__file__).parent / 'data/counter' / file
    output_path = pathlib.Path(__file__).parent / 'data/counter' / f"{file}.out"
    with output_path.open() as f:
        reader = csv.reader(f)
        poop = eat(source_path, platform, parsers=[parser], use_heuristics=heuristics)[0]

        for record in poop.records():
            assert next(reader) == list(record.as_csv()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"
