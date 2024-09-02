import json

import pytest

from celus_nibbler.conditions import IsDateCondition, RegexCondition, StemmerCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction, RelativeTo


def test_regex(csv_sheet_reader):
    assert RegexCondition("^Name$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader) is True
    assert RegexCondition("^3$", Coord(3, 1, RelativeTo.START)).check(csv_sheet_reader) is True
    assert (
        RegexCondition("no match", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader) is False
    )

    assert (
        RegexCondition("insufficient cols", Coord(0, 3, RelativeTo.START)).check(csv_sheet_reader)
        is False
    )
    assert (
        RegexCondition("insufficient rows", Coord(5, 0, RelativeTo.START)).check(csv_sheet_reader)
        is False
    )
    assert (
        RegexCondition("insufficient rows and cols", Coord(5, 3, RelativeTo.START)).check(
            csv_sheet_reader
        )
        is False
    )

    # with ranges
    assert (
        RegexCondition("^Name$", CoordRange(Coord(0, 0, RelativeTo.START), Direction.LEFT)).check(
            csv_sheet_reader
        )
        is True
    )
    assert (
        RegexCondition("^3$", CoordRange(Coord(3, 0, RelativeTo.START), Direction.RIGHT)).check(
            csv_sheet_reader
        )
        is True
    )
    assert (
        RegexCondition("no match", CoordRange(Coord(0, 2, RelativeTo.START), Direction.UP)).check(
            csv_sheet_reader
        )
        is False
    )

    assert (
        RegexCondition(
            "insufficient cols", CoordRange(Coord(0, 2, RelativeTo.START), Direction.DOWN)
        ).check(csv_sheet_reader)
        is False
    )
    assert (
        RegexCondition(
            "insufficient rows",
            CoordRange(Coord(0, 0, RelativeTo.START), Direction.DOWN, max_count=6),
        ).check(csv_sheet_reader)
        is False
    )
    assert (
        RegexCondition(
            "insufficient rows and cols",
            CoordRange(Coord(0, 3, RelativeTo.START), Direction.DOWN, max_count=8),
        ).check(csv_sheet_reader)
        is False
    )


def test_and(csv_sheet_reader):
    all_pass = (
        RegexCondition("^Name$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^Values$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^First$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert all_pass is True

    one_fail = (
        RegexCondition("^Name$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^doesn't match$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^First$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert one_fail is False

    one_pass = (
        RegexCondition("^doesn't match$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^doesn't match$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^First$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert one_pass is False

    all_fail = (
        RegexCondition("^doesn't match$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^doesn't match$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        & RegexCondition("^doesn't match$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert all_fail is False


def test_or(csv_sheet_reader):
    all_pass = (
        RegexCondition("^Name$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^Values$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^First$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert all_pass is True

    one_fail = (
        RegexCondition("^Name$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^doesn't match$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^First$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert one_fail is True

    one_pass = (
        RegexCondition("^doesn't match$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^doesn't match$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^First$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert one_pass is True

    all_fail = (
        RegexCondition("^doesn't match$", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^doesn't match$", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader)
        | RegexCondition("^doesn't match$", Coord(1, 0, RelativeTo.START)).check(csv_sheet_reader)
    )
    assert all_fail is False


def test_neg(csv_sheet_reader):
    assert (~RegexCondition("^Name$", Coord(0, 0, RelativeTo.START))).check(
        csv_sheet_reader
    ) is False
    assert (~RegexCondition("^3$", Coord(3, 1, RelativeTo.START))).check(csv_sheet_reader) is False
    assert (~RegexCondition("no match", Coord(0, 1, RelativeTo.START))).check(
        csv_sheet_reader
    ) is True


def test_stemmer(csv_sheet_reader):
    assert (
        StemmerCondition("Names", Coord(0, 0, RelativeTo.START)).check(csv_sheet_reader) is True
    ), "Names should match Name"
    assert (
        StemmerCondition("Value", Coord(0, 1, RelativeTo.START)).check(csv_sheet_reader) is True
    ), "Value should match Values"

    # with range
    assert (
        StemmerCondition("Names", CoordRange(Coord(1, 0, RelativeTo.START), Direction.UP)).check(
            csv_sheet_reader
        )
        is True
    ), "Names should match Name"
    assert (
        StemmerCondition("Value", CoordRange(Coord(0, 1, RelativeTo.START), Direction.DOWN)).check(
            csv_sheet_reader
        )
        is True
    ), "Value should match Values"


@pytest.mark.parametrize(
    "coord,date_format,data,result",
    (
        # Coord iso format
        (Coord(0, 0, RelativeTo.START), None, "2021", True),
        (Coord(0, 0, RelativeTo.START), None, "2021-01", True),
        (Coord(0, 0, RelativeTo.START), None, "2021-01", True),
        (Coord(0, 0, RelativeTo.START), None, "2021-01-01", True),
        (Coord(0, 0, RelativeTo.START), None, "2021-12-31", True),
        (Coord(0, 0, RelativeTo.START), None, "2021-11-31", False),
        # Coord US/EU
        (Coord(0, 0, RelativeTo.START), None, "01/2021", True),
        (Coord(0, 0, RelativeTo.START), None, "01/01/2021", True),
        (Coord(0, 0, RelativeTo.START), None, "12/31/2021", True),
        (Coord(0, 0, RelativeTo.START), None, "31/12/2021", True),
        (Coord(0, 0, RelativeTo.START), None, "11/31/2021", False),
        (Coord(0, 0, RelativeTo.START), None, "31/11/2021", False),
        # CoordRange
        (CoordRange(Coord(0, 1, RelativeTo.START), Direction.LEFT), None, "2021,Total", True),
        (CoordRange(Coord(0, 1, RelativeTo.START), Direction.RIGHT), None, "2021,Total", False),
        (
            CoordRange(Coord(0, 1, RelativeTo.START), Direction.LEFT),
            None,
            "11/31/2021,Total",
            False,
        ),
        # OutOfBounds
        (CoordRange(Coord(1, 1, RelativeTo.START), Direction.LEFT), None, "2021,Total", False),
        (Coord(0, 1, RelativeTo.START), None, "2021", False),
        # With date format set
        (Coord(0, 0, RelativeTo.START), "%Y", "2021", True),
        (Coord(0, 0, RelativeTo.START), "%Y-%m", "2021", False),
        (Coord(0, 0, RelativeTo.START), "%b-%Y", "Jan-2021", True),
        (Coord(0, 0, RelativeTo.START), "%b-%Y", "Led-2021", False),
        (Coord(0, 0, RelativeTo.START), "%Y-%m", "2021-01", True),
        (Coord(0, 0, RelativeTo.START), "%Y-%m", "2021-1", True),
    ),
)
def test_is_date(coord, date_format, data, result, csv_sheet_generator):
    reader = csv_sheet_generator(data)
    assert IsDateCondition(coord, date_format).check(reader) is result


def test_serialization():
    regex = RegexCondition("1234", Coord(0, 1, RelativeTo.START))
    regex_dict = json.loads(regex.json())
    assert regex_dict == {
        "coord": {"col": 1, "row": 0, "row_relative_to": "start"},
        "kind": "regex",
        "pattern": "1234",
    }
    assert regex == RegexCondition.parse(regex_dict)

    stemmer = StemmerCondition("1234", Coord(1, 2))
    stemmer_dict = json.loads(stemmer.json())
    assert stemmer_dict == {
        "coord": {"col": 2, "row": 1, "row_relative_to": "area"},
        "kind": "stemmer",
        "content": "1234",
    }
    assert stemmer == StemmerCondition.parse(stemmer_dict)

    regex2 = RegexCondition("5678", Coord(3, 2))
    regex2_dict = json.loads(regex2.json())

    complex = (~regex2 | stemmer) & regex

    complex_dict = json.loads(complex.json())
    assert complex_dict == {
        "kind": "and",
        "conds": [
            {
                "kind": "or",
                "conds": [
                    {
                        "kind": "neg",
                        "cond": regex2_dict,
                    },
                    stemmer_dict,
                ],
            },
            regex_dict,
        ],
    }
