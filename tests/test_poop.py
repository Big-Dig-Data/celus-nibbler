import pathlib
from datetime import date

from celus_nibbler import PoopOrganizationStats, PoopStats, eat
from celus_nibbler.eat_and_poop import StatUnit


def test_extra_poop_info():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    poops = eat(file_path, "Ovid", parsers=["static.counter4.BR1.Tabular"])
    assert poops and len(poops) == 1
    assert poops[0].get_stats() == PoopStats(
        months={
            "2018-01": StatUnit(count=2, sum=3),
            "2018-02": StatUnit(count=2, sum=2),
            "2018-03": StatUnit(count=2, sum=3),
            "2018-04": StatUnit(count=2, sum=1),
            "2018-05": StatUnit(count=2, sum=10),
            "2018-06": StatUnit(count=2, sum=0),
            "2018-07": StatUnit(count=2, sum=6),
            "2018-08": StatUnit(count=2, sum=0),
            "2018-09": StatUnit(count=2, sum=0),
            "2018-10": StatUnit(count=2, sum=0),
            "2018-11": StatUnit(count=2, sum=0),
            "2018-12": StatUnit(count=2, sum=13),
        },
        metrics={'Book Title Requests': StatUnit(count=24, sum=38)},
        organizations={
            '': PoopOrganizationStats(
                months={
                    "2018-01": StatUnit(count=2, sum=3),
                    "2018-02": StatUnit(count=2, sum=2),
                    "2018-03": StatUnit(count=2, sum=3),
                    "2018-04": StatUnit(count=2, sum=1),
                    "2018-05": StatUnit(count=2, sum=10),
                    "2018-06": StatUnit(count=2, sum=0),
                    "2018-07": StatUnit(count=2, sum=6),
                    "2018-08": StatUnit(count=2, sum=0),
                    "2018-09": StatUnit(count=2, sum=0),
                    "2018-10": StatUnit(count=2, sum=0),
                    "2018-11": StatUnit(count=2, sum=0),
                    "2018-12": StatUnit(count=2, sum=13),
                },
                metrics={'Book Title Requests': StatUnit(count=24, sum=38)},
                titles={"Title1": StatUnit(count=12, sum=32), "Title2": StatUnit(count=12, sum=6)},
                title_ids={"ISBN"},
                dimensions={
                    "Platform": {
                        "Ovid": StatUnit(count=24, sum=38),
                    },
                    "Publisher": {
                        "Publisher1": StatUnit(count=12, sum=32),
                        "Publisher2": StatUnit(count=12, sum=6),
                    },
                },
                total=StatUnit(count=24, sum=38),
            )
        },
        titles={"Title1": StatUnit(count=12, sum=32), "Title2": StatUnit(count=12, sum=6)},
        title_ids={"ISBN"},
        dimensions={
            "Platform": {
                "Ovid": StatUnit(count=24, sum=38),
            },
            "Publisher": {
                "Publisher1": StatUnit(count=12, sum=32),
                "Publisher2": StatUnit(count=12, sum=6),
            },
        },
        total=StatUnit(count=24, sum=38),
    )
    assert poops[0].sheet_idx == 0
    assert poops[0].get_months() == [[date(2018, i, 1) for i in range(1, 13)]]

    for file in ['BR1-empty1.tsv', 'BR1-empty2.tsv']:
        file_path = pathlib.Path(__file__).parent / 'data/counter/4/' / file

        poops = eat(file_path, "Ovid", parsers=["static.counter4.BR1.Tabular"])
        assert poops and len(poops) == 1
        assert poops[0].get_stats() == PoopStats()
        assert poops[0].sheet_idx == 0
        assert poops[0].get_months() == [[date(2018, i, 1) for i in range(1, 13)]]


def test_poop_offset_limit():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'
    poops = eat(file_path, "Ovid", parsers=["static.counter4.BR1.Tabular"])
    assert poops and len(poops) == 1

    poop = poops[0]

    # Without storing stats
    assert len([e for e in poop.records()]) == 24
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 24
    assert len([e for e in poop.records(offset=12)]) == 12
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 12
    assert len([e for e in poop.records(limit=10)]) == 10
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 10
    assert len([e for e in poop.records(offset=18, limit=10)]) == 6
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 6

    # With stats stored and updated

    assert len([e for e in poop.records_with_stats()]) == 24
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 24
    assert poop.current_stats == PoopStats(
        months={
            "2018-01": StatUnit(count=2, sum=3),
            "2018-02": StatUnit(count=2, sum=2),
            "2018-03": StatUnit(count=2, sum=3),
            "2018-04": StatUnit(count=2, sum=1),
            "2018-05": StatUnit(count=2, sum=10),
            "2018-06": StatUnit(count=2, sum=0),
            "2018-07": StatUnit(count=2, sum=6),
            "2018-08": StatUnit(count=2, sum=0),
            "2018-09": StatUnit(count=2, sum=0),
            "2018-10": StatUnit(count=2, sum=0),
            "2018-11": StatUnit(count=2, sum=0),
            "2018-12": StatUnit(count=2, sum=13),
        },
        metrics={'Book Title Requests': StatUnit(count=24, sum=38)},
        organizations={
            '': PoopOrganizationStats(
                months={
                    "2018-01": StatUnit(count=2, sum=3),
                    "2018-02": StatUnit(count=2, sum=2),
                    "2018-03": StatUnit(count=2, sum=3),
                    "2018-04": StatUnit(count=2, sum=1),
                    "2018-05": StatUnit(count=2, sum=10),
                    "2018-06": StatUnit(count=2, sum=0),
                    "2018-07": StatUnit(count=2, sum=6),
                    "2018-08": StatUnit(count=2, sum=0),
                    "2018-09": StatUnit(count=2, sum=0),
                    "2018-10": StatUnit(count=2, sum=0),
                    "2018-11": StatUnit(count=2, sum=0),
                    "2018-12": StatUnit(count=2, sum=13),
                },
                metrics={'Book Title Requests': StatUnit(count=24, sum=38)},
                titles={"Title1": StatUnit(count=12, sum=32), "Title2": StatUnit(count=12, sum=6)},
                title_ids={"ISBN"},
                dimensions={
                    "Platform": {
                        "Ovid": StatUnit(count=24, sum=38),
                    },
                    "Publisher": {
                        "Publisher1": StatUnit(count=12, sum=32),
                        "Publisher2": StatUnit(count=12, sum=6),
                    },
                },
                total=StatUnit(count=24, sum=38),
            )
        },
        titles={"Title1": StatUnit(count=12, sum=32), "Title2": StatUnit(count=12, sum=6)},
        title_ids={"ISBN"},
        dimensions={
            "Platform": {
                "Ovid": StatUnit(count=24, sum=38),
            },
            "Publisher": {
                "Publisher1": StatUnit(count=12, sum=32),
                "Publisher2": StatUnit(count=12, sum=6),
            },
        },
        total=StatUnit(count=24, sum=38),
    )
    assert len([e for e in poop.records_with_stats(offset=12)]) == 12
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 12
    assert poop.current_stats == PoopStats(
        months={
            "2018-01": StatUnit(count=1, sum=0),
            "2018-02": StatUnit(count=1, sum=0),
            "2018-03": StatUnit(count=1, sum=0),
            "2018-04": StatUnit(count=1, sum=0),
            "2018-05": StatUnit(count=1, sum=0),
            "2018-06": StatUnit(count=1, sum=0),
            "2018-07": StatUnit(count=1, sum=0),
            "2018-08": StatUnit(count=1, sum=0),
            "2018-09": StatUnit(count=1, sum=0),
            "2018-10": StatUnit(count=1, sum=0),
            "2018-11": StatUnit(count=1, sum=0),
            "2018-12": StatUnit(count=1, sum=6),
        },
        metrics={'Book Title Requests': StatUnit(count=12, sum=6)},
        organizations={
            '': PoopOrganizationStats(
                months={
                    "2018-01": StatUnit(count=1, sum=0),
                    "2018-02": StatUnit(count=1, sum=0),
                    "2018-03": StatUnit(count=1, sum=0),
                    "2018-04": StatUnit(count=1, sum=0),
                    "2018-05": StatUnit(count=1, sum=0),
                    "2018-06": StatUnit(count=1, sum=0),
                    "2018-07": StatUnit(count=1, sum=0),
                    "2018-08": StatUnit(count=1, sum=0),
                    "2018-09": StatUnit(count=1, sum=0),
                    "2018-10": StatUnit(count=1, sum=0),
                    "2018-11": StatUnit(count=1, sum=0),
                    "2018-12": StatUnit(count=1, sum=6),
                },
                metrics={'Book Title Requests': StatUnit(count=12, sum=6)},
                titles={"Title2": StatUnit(count=12, sum=6)},
                title_ids={"ISBN"},
                dimensions={
                    "Platform": {
                        "Ovid": StatUnit(count=12, sum=6),
                    },
                    "Publisher": {
                        "Publisher2": StatUnit(count=12, sum=6),
                    },
                },
                total=StatUnit(count=12, sum=6),
            )
        },
        titles={"Title2": StatUnit(count=12, sum=6)},
        title_ids={"ISBN"},
        dimensions={
            "Platform": {
                "Ovid": StatUnit(count=12, sum=6),
            },
            "Publisher": {
                "Publisher2": StatUnit(count=12, sum=6),
            },
        },
        total=StatUnit(count=12, sum=6),
    )
    assert len([e for e in poop.records_with_stats(limit=10)]) == 10
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 10
    assert poop.current_stats == PoopStats(
        months={
            "2018-01": StatUnit(count=1, sum=3),
            "2018-02": StatUnit(count=1, sum=2),
            "2018-03": StatUnit(count=1, sum=3),
            "2018-04": StatUnit(count=1, sum=1),
            "2018-05": StatUnit(count=1, sum=10),
            "2018-06": StatUnit(count=1, sum=0),
            "2018-07": StatUnit(count=1, sum=6),
            "2018-08": StatUnit(count=1, sum=0),
            "2018-09": StatUnit(count=1, sum=0),
            "2018-10": StatUnit(count=1, sum=0),
        },
        metrics={'Book Title Requests': StatUnit(count=10, sum=25)},
        organizations={
            '': PoopOrganizationStats(
                months={
                    "2018-01": StatUnit(count=1, sum=3),
                    "2018-02": StatUnit(count=1, sum=2),
                    "2018-03": StatUnit(count=1, sum=3),
                    "2018-04": StatUnit(count=1, sum=1),
                    "2018-05": StatUnit(count=1, sum=10),
                    "2018-06": StatUnit(count=1, sum=0),
                    "2018-07": StatUnit(count=1, sum=6),
                    "2018-08": StatUnit(count=1, sum=0),
                    "2018-09": StatUnit(count=1, sum=0),
                    "2018-10": StatUnit(count=1, sum=0),
                },
                metrics={'Book Title Requests': StatUnit(count=10, sum=25)},
                titles={"Title1": StatUnit(count=10, sum=25)},
                title_ids={"ISBN"},
                dimensions={
                    "Platform": {
                        "Ovid": StatUnit(count=10, sum=25),
                    },
                    "Publisher": {
                        "Publisher1": StatUnit(count=10, sum=25),
                    },
                },
                total=StatUnit(count=10, sum=25),
            )
        },
        titles={"Title1": StatUnit(count=10, sum=25)},
        title_ids={"ISBN"},
        dimensions={
            "Platform": {
                "Ovid": StatUnit(count=10, sum=25),
            },
            "Publisher": {
                "Publisher1": StatUnit(count=10, sum=25),
            },
        },
        total=StatUnit(count=10, sum=25),
    )
    assert len([e for e in poop.records_with_stats(offset=18, limit=10)]) == 6
    assert len(poop.area_counter) == 1, "only single area present"
    assert poop.area_counter[0] == 6
    assert poop.current_stats == PoopStats(
        months={
            "2018-07": StatUnit(count=1, sum=0),
            "2018-08": StatUnit(count=1, sum=0),
            "2018-09": StatUnit(count=1, sum=0),
            "2018-10": StatUnit(count=1, sum=0),
            "2018-11": StatUnit(count=1, sum=0),
            "2018-12": StatUnit(count=1, sum=6),
        },
        metrics={'Book Title Requests': StatUnit(count=6, sum=6)},
        organizations={
            '': PoopOrganizationStats(
                months={
                    "2018-07": StatUnit(count=1, sum=0),
                    "2018-08": StatUnit(count=1, sum=0),
                    "2018-09": StatUnit(count=1, sum=0),
                    "2018-10": StatUnit(count=1, sum=0),
                    "2018-11": StatUnit(count=1, sum=0),
                    "2018-12": StatUnit(count=1, sum=6),
                },
                metrics={'Book Title Requests': StatUnit(count=6, sum=6)},
                titles={"Title2": StatUnit(count=6, sum=6)},
                title_ids={"ISBN"},
                dimensions={
                    "Platform": {
                        "Ovid": StatUnit(count=6, sum=6),
                    },
                    "Publisher": {
                        "Publisher2": StatUnit(count=6, sum=6),
                    },
                },
                total=StatUnit(count=6, sum=6),
            )
        },
        titles={"Title2": StatUnit(count=6, sum=6)},
        title_ids={"ISBN"},
        dimensions={
            "Platform": {
                "Ovid": StatUnit(count=6, sum=6),
            },
            "Publisher": {
                "Publisher2": StatUnit(count=6, sum=6),
            },
        },
        total=StatUnit(count=6, sum=6),
    )


def test_stats():
    assert PoopStats() + PoopStats() == PoopStats(), "empty stats"

    stat1 = PoopStats()
    stat1.months["2020-01"] = StatUnit(sum=5, count=2)
    stat1.months["2020-02"] = StatUnit(sum=5, count=2)
    stat1.metrics["M1"] = StatUnit(sum=5, count=2)
    stat1.metrics["M2"] = StatUnit(sum=5, count=2)
    stat1.organizations["O1"] = PoopOrganizationStats(
        months={
            "2020-01": StatUnit(count=1, sum=1),
            "2020-02": StatUnit(count=1, sum=1),
        },
        metrics={'M1': StatUnit(count=1, sum=1), 'M2': StatUnit(count=1, sum=1)},
        titles={"T1": StatUnit(count=1, sum=1), "T2": StatUnit(count=1, sum=1)},
        title_ids={"ISBN", "URI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=1, sum=1), "bbb": StatUnit(count=1, sum=1)},
            "D2": {"aaa": StatUnit(count=2, sum=2)},
        },
        total=StatUnit(count=2, sum=2),
    )
    stat1.organizations["O2"] = PoopOrganizationStats(
        months={
            "2020-01": StatUnit(count=2, sum=2),
            "2020-02": StatUnit(count=2, sum=2),
        },
        metrics={'M1': StatUnit(count=2, sum=2), 'M2': StatUnit(count=2, sum=2)},
        titles={"T1": StatUnit(count=2, sum=2), "T2": StatUnit(count=2, sum=2)},
        title_ids={"ISBN", "URI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=2, sum=2), "bbb": StatUnit(count=2, sum=2)},
            "D2": {"aaa": StatUnit(count=4, sum=4)},
        },
        total=StatUnit(count=4, sum=4),
    )
    stat1.titles["T1"] = StatUnit(sum=5, count=2)
    stat1.titles["T2"] = StatUnit(sum=5, count=2)
    stat1.title_ids = {"ISBN", "URI"}
    stat1.total = StatUnit(sum=5, count=2)
    stat1.dimensions["D1"]["aaa"] = StatUnit(sum=5, count=2)
    stat1.dimensions["D1"]["bbb"] = StatUnit(sum=5, count=2)
    stat1.dimensions["D2"]["aaa"] = StatUnit(sum=5, count=2)

    stat2 = PoopStats()
    stat2.months["2020-02"] = StatUnit(sum=3, count=1)
    stat2.months["2020-03"] = StatUnit(sum=3, count=1)
    stat2.metrics["M2"] = StatUnit(sum=3, count=1)
    stat2.metrics["M3"] = StatUnit(sum=3, count=1)
    stat2.organizations["O2"] = PoopOrganizationStats(
        months={
            "2020-02": StatUnit(count=2, sum=2),
            "2020-03": StatUnit(count=2, sum=2),
        },
        metrics={'M2': StatUnit(count=2, sum=2), 'M3': StatUnit(count=2, sum=2)},
        titles={"T2": StatUnit(count=2, sum=2), "T3": StatUnit(count=2, sum=2)},
        title_ids={"ISBN", "DOI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=4, sum=4)},
            "D2": {"aaa": StatUnit(count=2, sum=2), "bbb": StatUnit(count=2, sum=2)},
        },
        total=StatUnit(count=4, sum=4),
    )
    stat2.organizations["O3"] = PoopOrganizationStats(
        months={
            "2020-02": StatUnit(count=3, sum=3),
            "2020-03": StatUnit(count=3, sum=3),
        },
        metrics={'M2': StatUnit(count=3, sum=3), 'M3': StatUnit(count=3, sum=3)},
        titles={"T2": StatUnit(count=3, sum=3), "T3": StatUnit(count=3, sum=3)},
        title_ids={"ISBN", "DOI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=6, sum=6)},
            "D2": {"aaa": StatUnit(count=3, sum=3), "bbb": StatUnit(count=3, sum=3)},
        },
        total=StatUnit(count=6, sum=6),
    )
    stat2.titles["T2"] = StatUnit(sum=3, count=1)
    stat2.titles["T3"] = StatUnit(sum=3, count=1)
    stat2.title_ids = {"ISBN", "DOI"}
    stat2.total = StatUnit(sum=3, count=1)
    stat2.dimensions["D1"]["bbb"] = StatUnit(sum=3, count=1)
    stat2.dimensions["D2"]["aaa"] = StatUnit(sum=3, count=1)
    stat2.dimensions["D2"]["bbb"] = StatUnit(sum=3, count=1)

    stat3 = PoopStats()
    stat3.months["2020-01"] = StatUnit(sum=5, count=2)
    stat3.months["2020-02"] = StatUnit(sum=8, count=3)
    stat3.months["2020-03"] = StatUnit(sum=3, count=1)
    stat3.metrics["M1"] = StatUnit(sum=5, count=2)
    stat3.metrics["M2"] = StatUnit(sum=8, count=3)
    stat3.metrics["M3"] = StatUnit(sum=3, count=1)
    stat3.organizations["O1"] = PoopOrganizationStats(
        months={
            "2020-01": StatUnit(count=1, sum=1),
            "2020-02": StatUnit(count=1, sum=1),
        },
        metrics={'M1': StatUnit(count=1, sum=1), 'M2': StatUnit(count=1, sum=1)},
        titles={"T1": StatUnit(count=1, sum=1), "T2": StatUnit(count=1, sum=1)},
        title_ids={"ISBN", "URI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=1, sum=1), "bbb": StatUnit(count=1, sum=1)},
            "D2": {"aaa": StatUnit(count=2, sum=2)},
        },
        total=StatUnit(count=2, sum=2),
    )
    stat3.organizations["O2"] = PoopOrganizationStats(
        months={
            "2020-01": StatUnit(count=2, sum=2),
            "2020-02": StatUnit(count=4, sum=4),
            "2020-03": StatUnit(count=2, sum=2),
        },
        metrics={
            'M1': StatUnit(count=2, sum=2),
            'M2': StatUnit(count=4, sum=4),
            'M3': StatUnit(count=2, sum=2),
        },
        titles={
            "T1": StatUnit(count=2, sum=2),
            "T2": StatUnit(count=4, sum=4),
            "T3": StatUnit(count=2, sum=2),
        },
        title_ids={"ISBN", "DOI", "URI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=6, sum=6), "bbb": StatUnit(count=2, sum=2)},
            "D2": {"aaa": StatUnit(count=6, sum=6), "bbb": StatUnit(count=2, sum=2)},
        },
        total=StatUnit(count=8, sum=8),
    )
    stat3.organizations["O3"] = PoopOrganizationStats(
        months={
            "2020-02": StatUnit(count=3, sum=3),
            "2020-03": StatUnit(count=3, sum=3),
        },
        metrics={'M2': StatUnit(count=3, sum=3), 'M3': StatUnit(count=3, sum=3)},
        titles={"T2": StatUnit(count=3, sum=3), "T3": StatUnit(count=3, sum=3)},
        title_ids={"ISBN", "DOI"},
        dimensions={
            "D1": {"aaa": StatUnit(count=6, sum=6)},
            "D2": {"aaa": StatUnit(count=3, sum=3), "bbb": StatUnit(count=3, sum=3)},
        },
        total=StatUnit(count=6, sum=6),
    )
    stat3.titles["T1"] = StatUnit(sum=5, count=2)
    stat3.titles["T2"] = StatUnit(sum=8, count=3)
    stat3.titles["T3"] = StatUnit(sum=3, count=1)
    stat3.title_ids = {"ISBN", "URI", "DOI"}
    stat3.dimensions["D1"]["aaa"] = StatUnit(sum=5, count=2)
    stat3.dimensions["D1"]["bbb"] = StatUnit(sum=8, count=3)
    stat3.dimensions["D2"]["aaa"] = StatUnit(sum=8, count=3)
    stat3.dimensions["D2"]["bbb"] = StatUnit(sum=3, count=1)
    stat3.total = StatUnit(sum=8, count=3)
    assert (stat1 + stat2).model_dump() == stat3.model_dump()
