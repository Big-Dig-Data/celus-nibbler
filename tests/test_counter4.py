import csv
import pathlib

import pytest

from celus_nibbler import eat


@pytest.mark.parametrize(
    "platform,file,parser,heuristics",
    (
        ("Ovid", "4/BR1-a.tsv", "nibbler.counter4.BR1", True),
        ("Ovid", "4/BR1-b.tsv", "nibbler.counter4.BR1", False),
        ("Ovid", "4/BR1-empty1.tsv", "nibbler.counter4.BR1", True),
        ("Ovid", "4/BR1-empty2.tsv", "nibbler.counter4.BR1", True),
        ("WileyOnlineLibrary", "4/BR2-a.tsv", "nibbler.counter4.BR2", True),
        ("WileyOnlineLibrary", "4/BR2-b.tsv", "nibbler.counter4.BR2", False),
        ("Psychiatry Online", "4/BR3-a.tsv", "nibbler.counter4.BR3", True),
        ("Psychiatry Online", "4/BR3-b.tsv", "nibbler.counter4.BR3", False),
        ("Tandfonline", "4/DB1-a.tsv", "nibbler.counter4.DB1", True),
        ("Tandfonline", "4/DB1-b.tsv", "nibbler.counter4.DB1", False),
        ("Tandfonline", "4/DB2-a.tsv", "nibbler.counter4.DB2", True),
        ("Tandfonline", "4/DB2-b.tsv", "nibbler.counter4.DB2", False),
    ),
)
def test_tsv(platform, file, parser, heuristics):
    source_path = pathlib.Path(__file__).parent / 'data/counter' / file
    output_path = pathlib.Path(__file__).parent / 'data/counter' / f"{file}.out"
    with output_path.open() as f:
        reader = csv.reader(f)
        poop = eat(source_path, platform, parsers=[parser], use_heuristics=heuristics)[0]

        for record in poop.records():
            assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"
