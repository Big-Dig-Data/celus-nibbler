import pathlib

from celus_nibbler import Poop, eat
from celus_nibbler.errors import MultipleParsersFound, NoParserFound


def test_eat():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    # Default call
    poops = eat(file_path, "Ovid")
    assert all(isinstance(e, Poop) for e in poops)

    # Wrong platform
    poops = eat(file_path, "Unknown")
    assert all(isinstance(e, NoParserFound) for e in poops)

    # Ignore platform
    poops = eat(file_path, "Unknown", check_platform=False)
    assert all(isinstance(e, Poop) for e in poops)

    # Non matching parsers
    poops = eat(file_path, "Unknown", check_platform=False, parsers=["non-existing"])
    assert all(isinstance(e, NoParserFound) for e in poops)

    # parser exact match
    poops = eat(
        file_path, "Unknown", check_platform=False, parsers=["non-existing", "nibbler.counter4.BR1"]
    )
    assert all(isinstance(e, Poop) for e in poops)

    # parser startswith match
    poops = eat(file_path, "Ovid", parsers=["non-existing", "nibbler.counter4"])
    assert all(isinstance(e, Poop) for e in poops)

    # parser regex match
    poops = eat(file_path, "Ovid", parsers=["non-existing", r".*\.BR1"])
    assert all(isinstance(e, Poop) for e in poops)

    # multiple parsers found
    poops = eat(
        file_path,
        "Ovid",
        parsers=["non-existing", "nibbler.counter4.BR"],
        check_platform=False,
        use_heuristics=False,
    )
    assert all(isinstance(e, MultipleParsersFound) for e in poops)
    assert set(poops[0].parsers) == {
        "nibbler.counter4.BR1",
        "nibbler.counter4.BR2",
        "nibbler.counter4.BR3",
    }
