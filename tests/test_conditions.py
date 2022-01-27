import re

import pytest

from celus_nibbler.conditions import RegexCondition, StemmerCondition
from celus_nibbler.coordinates import Coord
from celus_nibbler.errors import TableException


def test_regex(sheet):
    assert RegexCondition(re.compile("^Name$"), Coord(0, 0)).check(sheet) is True
    assert RegexCondition(re.compile("^3$"), Coord(3, 1)).check(sheet) is True
    assert RegexCondition(re.compile("no match"), Coord(0, 1)).check(sheet) is False

    with pytest.raises(TableException):
        RegexCondition(re.compile("insufficient cols"), Coord(0, 3)).check(sheet)
    with pytest.raises(TableException):
        RegexCondition(re.compile("insufficient rows"), Coord(5, 0)).check(sheet)
    with pytest.raises(TableException):
        RegexCondition(re.compile("insufficient rows and cols"), Coord(5, 3)).check(sheet)


def test_and(sheet):
    all_pass = (
        RegexCondition(re.compile("^Name$"), Coord(0, 0)).check(sheet)
        & RegexCondition(re.compile("^Values$"), Coord(0, 1)).check(sheet)
        & RegexCondition(re.compile("^First$"), Coord(1, 0)).check(sheet)
    )
    assert all_pass is True

    one_fail = (
        RegexCondition(re.compile("^Name$"), Coord(0, 0)).check(sheet)
        & RegexCondition(re.compile("^doesn't match$"), Coord(0, 1)).check(sheet)
        & RegexCondition(re.compile("^First$"), Coord(1, 0)).check(sheet)
    )
    assert one_fail is False

    one_pass = (
        RegexCondition(re.compile("^doesn't match$"), Coord(0, 0)).check(sheet)
        & RegexCondition(re.compile("^doesn't match$"), Coord(0, 1)).check(sheet)
        & RegexCondition(re.compile("^First$"), Coord(1, 0)).check(sheet)
    )
    assert one_pass is False

    all_fail = (
        RegexCondition(re.compile("^doesn't match$"), Coord(0, 0)).check(sheet)
        & RegexCondition(re.compile("^doesn't match$"), Coord(0, 1)).check(sheet)
        & RegexCondition(re.compile("^doesn't match$"), Coord(1, 0)).check(sheet)
    )
    assert all_fail is False


def test_or(sheet):
    all_pass = (
        RegexCondition(re.compile("^Name$"), Coord(0, 0)).check(sheet)
        | RegexCondition(re.compile("^Values$"), Coord(0, 1)).check(sheet)
        | RegexCondition(re.compile("^First$"), Coord(1, 0)).check(sheet)
    )
    assert all_pass is True

    one_fail = (
        RegexCondition(re.compile("^Name$"), Coord(0, 0)).check(sheet)
        | RegexCondition(re.compile("^doesn't match$"), Coord(0, 1)).check(sheet)
        | RegexCondition(re.compile("^First$"), Coord(1, 0)).check(sheet)
    )
    assert one_fail is True

    one_pass = (
        RegexCondition(re.compile("^doesn't match$"), Coord(0, 0)).check(sheet)
        | RegexCondition(re.compile("^doesn't match$"), Coord(0, 1)).check(sheet)
        | RegexCondition(re.compile("^First$"), Coord(1, 0)).check(sheet)
    )
    assert one_pass is True

    all_fail = (
        RegexCondition(re.compile("^doesn't match$"), Coord(0, 0)).check(sheet)
        | RegexCondition(re.compile("^doesn't match$"), Coord(0, 1)).check(sheet)
        | RegexCondition(re.compile("^doesn't match$"), Coord(1, 0)).check(sheet)
    )
    assert all_fail is False


def test_neg(sheet):
    assert (~RegexCondition(re.compile("^Name$"), Coord(0, 0))).check(sheet) is False
    assert (~RegexCondition(re.compile("^3$"), Coord(3, 1))).check(sheet) is False
    assert (~RegexCondition(re.compile("no match"), Coord(0, 1))).check(sheet) is True


def test_stemmer(sheet):
    assert StemmerCondition("Names", Coord(0, 0)).check(sheet) is True, "Names should match Name"
    assert StemmerCondition("Value", Coord(0, 1)).check(sheet) is True, "Value should match Values"
