import csv
import pathlib

import pytest

from celus_nibbler import eat


@pytest.mark.parametrize(
    "platform,file,parser,heuristics",
    (
        ("Ovid", "4/BR1-a.tsv", "static.counter4.BR1.Tabular", True),
        ("Ovid", "4/BR1-b.tsv", "static.counter4.BR1.Tabular", False),
        ("Ovid", "4/BR1-empty1.tsv", "static.counter4.BR1.Tabular", True),
        ("Ovid", "4/BR1-empty2.tsv", "static.counter4.BR1.Tabular", True),
        ("WileyOnlineLibrary", "4/BR2-a.tsv", "static.counter4.BR2.Tabular", True),
        ("WileyOnlineLibrary", "4/BR2-b.tsv", "static.counter4.BR2.Tabular", False),
        ("Psychiatry Online", "4/BR3-a.tsv", "static.counter4.BR3.Tabular", True),
        ("Psychiatry Online", "4/BR3-b.tsv", "static.counter4.BR3.Tabular", False),
        ("Tandfonline", "4/DB1-a.tsv", "static.counter4.DB1.Tabular", True),
        ("Tandfonline", "4/DB1-b.tsv", "static.counter4.DB1.Tabular", False),
        ("Tandfonline", "4/DB2-a.tsv", "static.counter4.DB2.Tabular", True),
        ("Tandfonline", "4/DB2-b.tsv", "static.counter4.DB2.Tabular", False),
        ("ProQuest", "4/PR1-a.tsv", "static.counter4.PR1.Tabular", True),
        ("ProQuest", "4/PR1-b.tsv", "static.counter4.PR1.Tabular", False),
        ("Thieme", "4/JR1-a.tsv", "static.counter4.JR1.Tabular", True),
        ("Thieme", "4/JR1-b.tsv", "static.counter4.JR1.Tabular", False),
        ("Sage", "4/JR1a-a.tsv", "static.counter4.JR1a.Tabular", True),
        ("Sage", "4/JR1a-b.tsv", "static.counter4.JR1a.Tabular", False),
        ("OUP", "4/JR1GOA-a.tsv", "static.counter4.JR1GOA.Tabular", True),
        ("OUP", "4/JR1GOA-b.tsv", "static.counter4.JR1GOA.Tabular", False),
        ("ProQuest", "4/JR2-a.tsv", "static.counter4.JR2.Tabular", True),
        ("ProQuest", "4/JR2-b.tsv", "static.counter4.JR2.Tabular", False),
        ("ProQuest", "4/MR1-a.tsv", "static.counter4.MR1.Tabular", True),
        ("ProQuest", "4/MR1-b.tsv", "static.counter4.MR1.Tabular", False),
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
