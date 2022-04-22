import pathlib
from datetime import date

from celus_nibbler import eat


def test_extra_poop_info():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    poops = eat(file_path, "Ovid")
    assert poops and len(poops) == 1
    assert [sorted(e) for e in poops[0].metrics_and_dimensions()] == [
        ["Book Title Requests"],
        ["Publisher"],
    ]
    assert poops[0].sheet_idx == 0
    assert poops[0].get_months() == [[date(2018, i, 1) for i in range(1, 13)]]

    for file in ['BR1-empty1.tsv', 'BR1-empty2.tsv']:
        file_path = pathlib.Path(__file__).parent / 'data/counter/4/' / file

        poops = eat(file_path, "Ovid")
        assert poops and len(poops) == 1
        assert [sorted(e) for e in poops[0].metrics_and_dimensions()] == [[], []]
        assert poops[0].sheet_idx == 0
        assert poops[0].get_months() == [[date(2018, i, 1) for i in range(1, 13)]]
