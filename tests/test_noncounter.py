import csv
import pathlib

import pytest

from celus_nibbler import Poop, eat


@pytest.mark.parametrize(
    "platform,file,parser,poops_presence,heuristics,aggregated",
    [
        (
            "My Platform",
            "my-metric-based.xlsx",
            "nibbler.non_counter.MyMetricBased",
            [True, False, False, False],
            True,
            True,
        )
    ],
)
def test_non_counter(platform, file, parser, poops_presence, heuristics, aggregated):
    source_path = pathlib.Path(__file__).parent / 'data/non_counter' / file
    output_path = pathlib.Path(__file__).parent / 'data/non_counter' / f"{file}.out"

    with output_path.open() as f:
        reader = csv.reader(f)
        poops = eat(source_path, platform, parsers=[parser], use_heuristics=heuristics)
        assert [isinstance(p, Poop) for p in poops] == poops_presence

        for poop in [poop for poop in poops if isinstance(poop, Poop)]:

            # Aggregated records might be out of order, we need to sort it first
            records = (
                sorted(poop.records(), key=lambda x: x.as_csv()) if aggregated else poop.records()
            )
            for record in records:
                assert next(reader) == list(record.as_csv()), "Compare lines"

            with pytest.raises(StopIteration):
                assert next(reader) is None, "No more date present in the sheet"
