import json
import pathlib

from celus_nibbler import Poop, eat
from celus_nibbler.definitions import Definition
from celus_nibbler.errors import (
    MultipleParsersFound,
    NoParserForPlatformFound,
    NoParserMatchesHeuristics,
)
from celus_nibbler.parsers.dynamic import gen_parser


def test_eat():
    file_path = pathlib.Path(__file__).parent / "data/dynamic/simple.csv"
    definition_path = pathlib.Path(__file__).parent / "data/dynamic/simple.json"
    with definition_path.open() as f:
        definition = json.load(f)
    dynamic_parsers = [gen_parser(Definition.parse(definition))]

    # Default call
    poops = eat(file_path, "Platform1", dynamic_parsers=dynamic_parsers)
    assert all(isinstance(e, Poop) for e in poops)
    assert all(e.data_format.name == "simple_format" for e in poops)

    # Wrong platform
    poops = eat(
        file_path,
        "Unknown",
        dynamic_parsers=dynamic_parsers,
        parsers=[r"(?!static)"],  # Skip static (counter parsers)
    )
    assert all(isinstance(e, NoParserForPlatformFound) for e in poops)
    assert poops[0].dict() == {
        "name": "NoParserForPlatformFound",
        "sheet_idx": 0,
    }

    # Ignore platform
    poops = eat(file_path, "Unknown", check_platform=False, dynamic_parsers=dynamic_parsers)
    assert all(isinstance(e, Poop) for e in poops)

    # Non matching parsers
    poops = eat(
        file_path,
        "Unknown",
        check_platform=False,
        parsers=["non-existing"],
        dynamic_parsers=dynamic_parsers,
    )
    assert all(isinstance(e, NoParserForPlatformFound) for e in poops)
    assert poops[0].dict() == {
        "name": "NoParserForPlatformFound",
        "sheet_idx": 0,
    }

    # Parser with mismatched heuristics
    poops = eat(
        pathlib.Path(__file__).parent / "data/counter/4/BR1-a.tsv",
        platform="Ovid",
        check_platform=False,
        parsers=[
            "static.counter4.JR1.Tabular",
            "static.counter5.DR.Tabular",
        ],
        dynamic_parsers=dynamic_parsers,
    )
    assert all(isinstance(e, NoParserMatchesHeuristics) for e in poops)
    assert poops[0].dict() == {
        "name": "NoParserMatchesHeuristics",
        "sheet_idx": 0,
        "parsers_info": {
            "static.counter4.JR1.Tabular": [
                {
                    "code": "wrong-report-type",
                    "expected": "Journal Report 1 (R4)",
                    "found": "Book Report 1 (R4)",
                }
            ],
            "static.counter5.DR.Tabular": [
                {
                    "code": "report-id-not-in-header",
                }
            ],
        },
    }

    # Parser with mismatched heuristics
    poops = eat(
        pathlib.Path(__file__).parent / "data/counter/5/DR-a.tsv",
        platform="Ovid",
        check_platform=False,
        parsers=[
            "static.counter4.JR2.Tabular",
            "static.counter5.TR.Tabular",
        ],
    )
    assert all(isinstance(e, NoParserMatchesHeuristics) for e in poops)
    assert poops[0].dict() == {
        "name": "NoParserMatchesHeuristics",
        "sheet_idx": 0,
        "parsers_info": {
            "static.counter4.JR2.Tabular": [
                {
                    "code": "wrong-report-type",
                    "expected": "Journal Report 2 (R4)",
                    "found": "Institution_Name",
                }
            ],
            "static.counter5.TR.Tabular": [
                {
                    "code": "wrong-report-type",
                    "expected": "TR",
                    "found": "DR",
                }
            ],
        },
    }

    # parser exact match
    poops = eat(
        pathlib.Path(__file__).parent / "data/counter/4/BR1-a.tsv",
        "Unknown",
        parsers=["non-existing", "static.counter4.BR1.Tabular"],
    )
    assert all(isinstance(e, Poop) for e in poops)

    # parser startswith match
    poops = eat(
        pathlib.Path(__file__).parent / "data/counter/4/BR1-a.tsv",
        "Ovid",
        parsers=["non-existing", "static.counter4"],
    )
    assert all(isinstance(e, Poop) for e in poops)

    # parser regex match
    poops = eat(
        pathlib.Path(__file__).parent / "data/counter/4/BR1-a.tsv",
        "Ovid",
        parsers=["non-existing", r".*\.BR1"],
    )
    assert all(isinstance(e, Poop) for e in poops)

    # multiple parsers found
    poops = eat(
        pathlib.Path(__file__).parent / "data/counter/4/BR1-a.tsv",
        "Ovid",
        parsers=["non-existing", "static.counter4.BR"],
        use_heuristics=False,
    )
    assert all(isinstance(e, MultipleParsersFound) for e in poops)
    assert set(poops[0].parsers) == {
        "static.counter4.BR1.Tabular",
        "static.counter4.BR2.Tabular",
        "static.counter4.BR3.Tabular",
    }
    assert poops[0].dict() == {
        "name": "MultipleParsersFound",
        "sheet_idx": 0,
        "parsers": [
            "static.counter4.BR1.Tabular",
            "static.counter4.BR2.Tabular",
            "static.counter4.BR3.Tabular",
        ],
    }


def test_parsers_info_of_c5_json():
    file_path = pathlib.Path(__file__).parent / "data/counter/5/TR_J1-sample.json"

    poops = eat(
        file_path,
        "Ovid",
        parsers=["static.counter5.TR.Json"],
        check_platform=False,
        use_heuristics=True,
    )
    assert len(poops) == 1
    poop = poops[0]
    assert isinstance(poop, NoParserMatchesHeuristics)
    assert poop.dict() == {
        "name": "NoParserMatchesHeuristics",
        "sheet_idx": 0,
        "parsers_info": {
            "static.counter5.TR.Json": [
                {
                    "code": "wrong-report-type",
                    "expected": "TR",
                    "found": "TR_J1",
                }
            ],
        },
    }
