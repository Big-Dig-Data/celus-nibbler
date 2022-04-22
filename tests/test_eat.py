import pathlib

from celus_nibbler import eat


def test_eat():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    # Default call
    poops = eat(file_path, "Ovid")
    assert len(poops) == 1

    # Wrong platform
    poops = eat(file_path, "Unknown")
    assert len(poops) == 0

    # Ignore platform
    poops = eat(file_path, "Unknown", check_platform=False)
    assert len(poops) == 1

    # Non matching parsers
    poops = eat(file_path, "Unknown", check_platform=False, parsers=["non-existing"])
    assert len(poops) == 0

    # parser exact match
    poops = eat(
        file_path, "Unknown", check_platform=False, parsers=["non-existing", "nibbler.counter4.BR1"]
    )
    assert len(poops) == 1

    # parser startswith match
    poops = eat(file_path, "Ovid", parsers=["non-existing", "nibbler.counter4"])
    assert len(poops) == 1
