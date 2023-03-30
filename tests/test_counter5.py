import csv
import pathlib

import pytest

from celus_nibbler import eat
from celus_nibbler.errors import NoParserMatchesHeuristics


@pytest.mark.parametrize(
    "platform,file,parser,heuristics,success",
    (
        ("WebOfKnowledge", "5/DR-a.tsv", "static.counter5.DR.Tabular", True, True),
        ("WebOfKnowledge", "5/DR-b.tsv", "static.counter5.DR.Tabular", False, True),
        ("WebOfKnowledge", "5/DR-b.tsv", "static.counter5.DR.Tabular", True, False),
        ("WebOfKnowledge", "5/DR-c.json", "static.counter5.DR.Json", True, True),
        ("WebOfKnowledge", "5/DR-d.json", "static.counter5.DR.Json", False, True),
        ("WebOfKnowledge", "5/DR-d.json", "static.counter5.DR.Json", True, False),
        ("WebOfKnowledge", "5/DR-e.csv", "static.counter5.DR.Tabular", True, True),
        ("WebOfKnowledge", "5/DR-sample.tsv", "static.counter5.DR.Tabular", True, True),
        ("WebOfKnowledge", "5/DR-f.tsv", "static.counter5.DR.Tabular", True, True),
        ("MIT", "5/PR-a.tsv", "static.counter5.PR.Tabular", True, True),
        ("MIT", "5/PR-b.tsv", "static.counter5.PR.Tabular", False, True),
        ("MIT", "5/PR-b.tsv", "static.counter5.PR.Tabular", True, False),
        ("MIT", "5/PR-c.json", "static.counter5.PR.Json", True, True),
        ("MIT", "5/PR-d.json", "static.counter5.PR.Json", False, True),
        ("MIT", "5/PR-d.json", "static.counter5.PR.Json", True, False),
        ("Ovid", "5/PR-e.csv", "static.counter5.PR.Tabular", True, True),
        ("Ovid", "5/PR-sample.tsv", "static.counter5.PR.Tabular", True, True),
        ("Ovid", "5/PR-f.tsv", "static.counter5.PR.Tabular", True, True),
        ("MIT", "5/TR-a.tsv", "static.counter5.TR.Tabular", True, True),
        ("MIT", "5/TR-b.tsv", "static.counter5.TR.Tabular", False, True),
        ("MIT", "5/TR-b.tsv", "static.counter5.TR.Tabular", True, False),
        ("MIT", "5/TR-c.json", "static.counter5.TR.Json", True, True),
        ("MIT", "5/TR-d.json", "static.counter5.TR.Json", False, True),
        ("MIT", "5/TR-d.json", "static.counter5.TR.Json", True, False),
        ("MIT", "5/TR-e.tsv", "static.counter5.TR.Tabular", True, True),
        ("MIT", "5/TR-sample.tsv", "static.counter5.TR.Tabular", True, True),
        ("MIT", "5/TR-f.tsv", "static.counter5.TR.Tabular", True, True),
        ("T&F ebooks", "5/TR_B1-a.xlsx", "static.counter5.TR.Tabular", True, True),
        ("JSTOR", "5/IR_M1-a.csv", "static.counter5.IR_M1.Tabular", True, True),
        ("JSTOR", "5/IR_M1-b.csv", "static.counter5.IR_M1.Tabular", False, True),
        ("JSTOR", "5/IR_M1-b.csv", "static.counter5.IR_M1.Tabular", True, False),
        ("JSTOR", "5/IR_M1-c.json", "static.counter5.IR_M1.Json", True, True),
        ("JSTOR", "5/IR_M1-d.json", "static.counter5.IR_M1.Json", False, True),
        ("JSTOR", "5/IR_M1-d.json", "static.counter5.IR_M1.Json", True, False),
        ("JSTOR", "5/IR_M1-sample.tsv", "static.counter5.IR_M1.Tabular", True, True),
        ("JSTOR", "5/IR_M1-f.tsv", "static.counter5.IR_M1.Tabular", True, True),
    ),
)
def test_tsv(platform, file, parser, heuristics, success):
    source_path = pathlib.Path(__file__).parent / 'data/counter' / file
    output_path = pathlib.Path(__file__).parent / 'data/counter' / f"{file}.out"
    with output_path.open() as f:
        reader = csv.reader(f)
        poop = eat(source_path, platform, parsers=[parser], use_heuristics=heuristics)[0]

        if not success:
            assert isinstance(poop, NoParserMatchesHeuristics)
            return

        for record in poop.records():
            assert next(reader) == list(record.as_csv()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"
