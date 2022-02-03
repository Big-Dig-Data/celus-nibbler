import csv
import pathlib

import pytest

from celus_nibbler import eat


@pytest.mark.parametrize(
    "platform,file",
    (
        ("Ovid", "4/BR1-a.tsv"),
        ("WileyOnlineLibrary", "4/BR2-a.tsv"),
    ),
)
def test_tsv(platform, file):
    source_path = pathlib.Path(__file__).parent / 'data/counter' / file
    output_path = pathlib.Path(__file__).parent / 'data/counter' / f"{file}.out"
    with output_path.open() as f:
        reader = csv.reader(f)
        poop = eat(source_path, platform)[0]

        for record in poop.records():
            assert next(reader) == list(record.serialize()), "Compare lines"

        with pytest.raises(StopIteration):
            assert next(reader) is None, "No more date present in the file"
