import csv
import pathlib

import pytest

from celus_nibbler import Poop, eat


@pytest.mark.parametrize(
    "platform,file,parser,poops_presence,heuristics",
    [
        (
            "My Platform",
            "my-metric-based.xlsx",
            "nibbler.non_counter.MyMetricBased",
            [True, False, False, False],
            True,
        )
    ],
)
def test_non_counter(platform, file, parser, poops_presence, heuristics):
    source_path = pathlib.Path(__file__).parent / 'data/non_counter' / file
    output_path = pathlib.Path(__file__).parent / 'data/non_counter' / f"{file}.out"

    with output_path.open() as f:
        reader = csv.reader(f)
        poops = eat(source_path, platform, parsers=[parser], use_heuristics=heuristics)
        assert [isinstance(p, Poop) for p in poops] == poops_presence

        for poop in [poop for poop in poops if isinstance(poop, Poop)]:
            for record in poop.records():
                assert next(reader) == list(record.as_csv()), "Compare lines"

            with pytest.raises(StopIteration):
                assert next(reader) is None, "No more date present in the sheet"
