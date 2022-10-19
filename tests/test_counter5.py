import csv
import pathlib

import pytest

from celus_nibbler import eat


@pytest.mark.parametrize(
    "platform,file,parser,heuristics",
    (
        ("WebOfKnowledge", "5/DR-a.tsv", "static.counter5.DR.Tabular", True),
        ("WebOfKnowledge", "5/DR-b.tsv", "static.counter5.DR.Tabular", False),
        ("MIT", "5/PR-a.tsv", "static.counter5.PR.Tabular", True),
        ("MIT", "5/PR-b.tsv", "static.counter5.PR.Tabular", False),
        ("MIT", "5/TR-a.tsv", "static.counter5.TR.Tabular", True),
        ("MIT", "5/TR-b.tsv", "static.counter5.TR.Tabular", False),
        ("T&F ebooks", "5/TR_B1-a.xlsx", "static.counter5.TR.Tabular", True),
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
