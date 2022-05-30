import json

from celus_nibbler.conditions import RegexCondition, StemmerCondition
from celus_nibbler.coordinates import Coord


def test_regex(sheet):
    assert RegexCondition("^Name$", Coord(0, 0)).check(sheet) is True
    assert RegexCondition("^3$", Coord(3, 1)).check(sheet) is True
    assert RegexCondition("no match", Coord(0, 1)).check(sheet) is False

    assert RegexCondition("insufficient cols", Coord(0, 3)).check(sheet) is False
    assert RegexCondition("insufficient rows", Coord(5, 0)).check(sheet) is False
    assert RegexCondition("insufficient rows and cols", Coord(5, 3)).check(sheet) is False


def test_and(sheet):
    all_pass = (
        RegexCondition("^Name$", Coord(0, 0)).check(sheet)
        & RegexCondition("^Values$", Coord(0, 1)).check(sheet)
        & RegexCondition("^First$", Coord(1, 0)).check(sheet)
    )
    assert all_pass is True

    one_fail = (
        RegexCondition("^Name$", Coord(0, 0)).check(sheet)
        & RegexCondition("^doesn't match$", Coord(0, 1)).check(sheet)
        & RegexCondition("^First$", Coord(1, 0)).check(sheet)
    )
    assert one_fail is False

    one_pass = (
        RegexCondition("^doesn't match$", Coord(0, 0)).check(sheet)
        & RegexCondition("^doesn't match$", Coord(0, 1)).check(sheet)
        & RegexCondition("^First$", Coord(1, 0)).check(sheet)
    )
    assert one_pass is False

    all_fail = (
        RegexCondition("^doesn't match$", Coord(0, 0)).check(sheet)
        & RegexCondition("^doesn't match$", Coord(0, 1)).check(sheet)
        & RegexCondition("^doesn't match$", Coord(1, 0)).check(sheet)
    )
    assert all_fail is False


def test_or(sheet):
    all_pass = (
        RegexCondition("^Name$", Coord(0, 0)).check(sheet)
        | RegexCondition("^Values$", Coord(0, 1)).check(sheet)
        | RegexCondition("^First$", Coord(1, 0)).check(sheet)
    )
    assert all_pass is True

    one_fail = (
        RegexCondition("^Name$", Coord(0, 0)).check(sheet)
        | RegexCondition("^doesn't match$", Coord(0, 1)).check(sheet)
        | RegexCondition("^First$", Coord(1, 0)).check(sheet)
    )
    assert one_fail is True

    one_pass = (
        RegexCondition("^doesn't match$", Coord(0, 0)).check(sheet)
        | RegexCondition("^doesn't match$", Coord(0, 1)).check(sheet)
        | RegexCondition("^First$", Coord(1, 0)).check(sheet)
    )
    assert one_pass is True

    all_fail = (
        RegexCondition("^doesn't match$", Coord(0, 0)).check(sheet)
        | RegexCondition("^doesn't match$", Coord(0, 1)).check(sheet)
        | RegexCondition("^doesn't match$", Coord(1, 0)).check(sheet)
    )
    assert all_fail is False


def test_neg(sheet):
    assert (~RegexCondition("^Name$", Coord(0, 0))).check(sheet) is False
    assert (~RegexCondition("^3$", Coord(3, 1))).check(sheet) is False
    assert (~RegexCondition("no match", Coord(0, 1))).check(sheet) is True


def test_stemmer(sheet):
    assert StemmerCondition("Names", Coord(0, 0)).check(sheet) is True, "Names should match Name"
    assert StemmerCondition("Value", Coord(0, 1)).check(sheet) is True, "Value should match Values"


def test_serialization():
    regex = RegexCondition("1234", Coord(0, 1))
    regex_dict = json.loads(regex.json())
    assert regex_dict == {'coord': {'col': 1, 'row': 0}, 'kind': 'regex', 'pattern': '1234'}
    assert regex == RegexCondition.parse(regex_dict)

    stemmer = StemmerCondition("1234", Coord(1, 2))
    stemmer_dict = json.loads(stemmer.json())
    assert stemmer_dict == {'coord': {'col': 2, 'row': 1}, 'kind': 'stemmer', 'content': '1234'}
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
