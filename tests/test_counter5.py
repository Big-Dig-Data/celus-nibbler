import csv
import pathlib

import pytest

from celus_nibbler import eat


@pytest.mark.parametrize(
    "platform,file,parser,heuristics",
    (
        ("WebOfKnowledge", "5/DR-a.tsv", "static.counter5.DR.Tabular", True),
        ("WebOfKnowledge", "5/DR-b.tsv", "static.counter5.DR.Tabular", False),
        ("WebOfKnowledge", "5/DR-c.json", "static.counter5.DR.Json", True),
        ("WebOfKnowledge", "5/DR-d.json", "static.counter5.DR.Json", False),
        ("WebOfKnowledge", "5/DR-e.csv", "static.counter5.DR.Tabular", True),
        ("MIT", "5/PR-a.tsv", "static.counter5.PR.Tabular", True),
        ("MIT", "5/PR-b.tsv", "static.counter5.PR.Tabular", False),
        ("MIT", "5/PR-c.json", "static.counter5.PR.Json", True),
        ("MIT", "5/PR-d.json", "static.counter5.PR.Json", False),
        ("Ovid", "5/PR-e.csv", "static.counter5.PR.Tabular", False),
        ("MIT", "5/TR-a.tsv", "static.counter5.TR.Tabular", True),
        ("MIT", "5/TR-b.tsv", "static.counter5.TR.Tabular", False),
        ("MIT", "5/TR-c.json", "static.counter5.TR.Json", True),
        ("MIT", "5/TR-d.json", "static.counter5.TR.Json", False),
        ("MIT", "5/TR-e.tsv", "static.counter5.TR.Tabular", True),
        ("T&F ebooks", "5/TR_B1-a.xlsx", "static.counter5.TR.Tabular", True),
        ("JSTOR", "5/IR_M1-a.csv", "static.counter5.IR_M1.Tabular", True),
        ("JSTOR", "5/IR_M1-b.csv", "static.counter5.IR_M1.Tabular", False),
        ("JSTOR", "5/IR_M1-c.json", "static.counter5.IR_M1.Json", True),
        ("JSTOR", "5/IR_M1-d.json", "static.counter5.IR_M1.Json", False),
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
