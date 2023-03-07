import csv
import pathlib

import pytest

from celus_nibbler import eat
from celus_nibbler.errors import NoParserMatchesHeuristics


@pytest.mark.parametrize(
    "platform,file,parser,heuristics,success",
    (
        ("Ovid", "4/BR1-a.tsv", "static.counter4.BR1.Tabular", True, True),
        ("Ovid", "4/BR1-b.tsv", "static.counter4.BR1.Tabular", False, True),
        ("Ovid", "4/BR1-b.tsv", "static.counter4.BR1.Tabular", True, False),
        ("Ovid", "4/BR1-empty1.tsv", "static.counter4.BR1.Tabular", True, True),
        ("Ovid", "4/BR1-empty2.tsv", "static.counter4.BR1.Tabular", True, True),
        ("Ovid", "4/BR1-empty_values.csv", "static.counter4.BR1.Tabular", True, True),
        ("WileyOnlineLibrary", "4/BR2-a.tsv", "static.counter4.BR2.Tabular", True, True),
        ("WileyOnlineLibrary", "4/BR2-b.tsv", "static.counter4.BR2.Tabular", False, True),
        ("WileyOnlineLibrary", "4/BR2-b.tsv", "static.counter4.BR2.Tabular", True, False),
        ("Psychiatry Online", "4/BR3-a.tsv", "static.counter4.BR3.Tabular", True, True),
        ("Psychiatry Online", "4/BR3-b.tsv", "static.counter4.BR3.Tabular", False, True),
        ("Psychiatry Online", "4/BR3-b.tsv", "static.counter4.BR3.Tabular", True, False),
        ("Tandfonline", "4/DB1-a.tsv", "static.counter4.DB1.Tabular", True, True),
        ("Tandfonline", "4/DB1-b.tsv", "static.counter4.DB1.Tabular", False, True),
        ("Tandfonline", "4/DB1-b.tsv", "static.counter4.DB1.Tabular", True, False),
        ("Tandfonline", "4/DB2-a.tsv", "static.counter4.DB2.Tabular", True, True),
        ("Tandfonline", "4/DB2-b.tsv", "static.counter4.DB2.Tabular", False, True),
        ("Tandfonline", "4/DB2-b.tsv", "static.counter4.DB2.Tabular", True, False),
        ("ProQuest", "4/PR1-a.tsv", "static.counter4.PR1.Tabular", True, True),
        ("ProQuest", "4/PR1-b.tsv", "static.counter4.PR1.Tabular", False, True),
        ("ProQuest", "4/PR1-b.tsv", "static.counter4.PR1.Tabular", True, False),
        ("Thieme", "4/JR1-a.tsv", "static.counter4.JR1.Tabular", True, True),
        ("Thieme", "4/JR1-b.tsv", "static.counter4.JR1.Tabular", False, True),
        ("Thieme", "4/JR1-b.tsv", "static.counter4.JR1.Tabular", True, False),
        ("Sage", "4/JR1a-a.tsv", "static.counter4.JR1a.Tabular", True, True),
        ("Sage", "4/JR1a-b.tsv", "static.counter4.JR1a.Tabular", False, True),
        ("Sage", "4/JR1a-b.tsv", "static.counter4.JR1a.Tabular", True, False),
        ("OUP", "4/JR1GOA-a.tsv", "static.counter4.JR1GOA.Tabular", True, True),
        ("OUP", "4/JR1GOA-b.tsv", "static.counter4.JR1GOA.Tabular", False, True),
        ("OUP", "4/JR1GOA-b.tsv", "static.counter4.JR1GOA.Tabular", True, False),
        ("ProQuest", "4/JR2-a.tsv", "static.counter4.JR2.Tabular", True, True),
        ("ProQuest", "4/JR2-b.tsv", "static.counter4.JR2.Tabular", False, True),
        ("ProQuest", "4/JR2-b.tsv", "static.counter4.JR2.Tabular", True, False),
        ("ProQuest", "4/MR1-a.tsv", "static.counter4.MR1.Tabular", True, True),
        ("ProQuest", "4/MR1-b.tsv", "static.counter4.MR1.Tabular", False, True),
        ("ProQuest", "4/MR1-b.tsv", "static.counter4.MR1.Tabular", True, False),
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
