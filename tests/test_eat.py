import pathlib

from celus_nibbler import eat


def test_eat():
    file_path = pathlib.Path(__file__).parent / 'data/counter/4/BR1-a.tsv'

    # Default call
    poops = eat(file_path, "Ovid")
    assert all(poops)

    # Wrong platform
    poops = eat(file_path, "Unknown")
    assert not any(poops)

    # Ignore platform
    poops = eat(file_path, "Unknown", check_platform=False)
    assert all(poops)

    # Non matching parsers
    poops = eat(file_path, "Unknown", check_platform=False, parsers=["non-existing"])
    assert not any(poops)

    # parser exact match
    poops = eat(
        file_path, "Unknown", check_platform=False, parsers=["non-existing", "nibbler.counter4.BR1"]
    )
    assert all(poops)

    # parser startswith match
    poops = eat(file_path, "Ovid", parsers=["non-existing", "nibbler.counter4"])
    assert all(poops)
