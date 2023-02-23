import pathlib
from datetime import date

from celus_nibbler import eat


def test_extra_poop_info():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    poops = eat(file_path, "Ovid", parsers=["static.counter4.BR1.Tabular"])
    assert poops and len(poops) == 1
    assert [sorted(e) for e in poops[0].get_metrics_dimensions_title_ids_months()] == [
        ["Book Title Requests"],
        ["Platform", "Publisher"],
        ["ISBN"],
        [
            date(2018, 1, 1),
            date(2018, 2, 1),
            date(2018, 3, 1),
            date(2018, 4, 1),
            date(2018, 5, 1),
            date(2018, 6, 1),
            date(2018, 7, 1),
            date(2018, 8, 1),
            date(2018, 9, 1),
            date(2018, 10, 1),
            date(2018, 11, 1),
            date(2018, 12, 1),
        ],
    ]
    assert poops[0].sheet_idx == 0
    assert poops[0].get_months() == [[date(2018, i, 1) for i in range(1, 13)]]

    for file in ['BR1-empty1.tsv', 'BR1-empty2.tsv']:
        file_path = pathlib.Path(__file__).parent / 'data/counter/4/' / file

        poops = eat(file_path, "Ovid", parsers=["static.counter4.BR1.Tabular"])
        assert poops and len(poops) == 1
        assert [sorted(e) for e in poops[0].get_metrics_dimensions_title_ids_months()] == [
            [],
            [],
            [],
            [],
        ]
        assert poops[0].sheet_idx == 0
        assert poops[0].get_months() == [[date(2018, i, 1) for i in range(1, 13)]]


def test_poop_offset_limit():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'
    poops = eat(file_path, "Ovid", parsers=["static.counter4.BR1.Tabular"])
    assert poops and len(poops) == 1

    poop = poops[0]

    assert len([e for e in poop.records()]) == 24
    assert len([e for e in poop.records(offset=12)]) == 12
    assert len([e for e in poop.records(limit=10)]) == 10
    assert len([e for e in poop.records(offset=18, limit=10)]) == 6
