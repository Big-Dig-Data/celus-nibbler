import pathlib

from celus_nibbler import eat


def test_extra_poop_info():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    poops = eat(file_path, "Ovid")
    assert poops and len(poops) == 1
    assert [sorted(e) for e in poops[0].metrics_and_dimensions()] == [
        ["Book Title Requests"],
        ["Publisher"],
    ]
