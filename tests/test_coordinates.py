import itertools

import pytest

from celus_nibbler.coordinates import Coord, CoordRange, Direction


def test_contains():
    assert Coord(0, 0) not in CoordRange(Coord(1, 1), Direction.UP)
    assert Coord(0, 0) not in CoordRange(Coord(1, 1), Direction.DOWN)
    assert Coord(0, 0) not in CoordRange(Coord(1, 1), Direction.LEFT)
    assert Coord(0, 0) not in CoordRange(Coord(1, 1), Direction.RIGHT)

    assert Coord(1, 1) in CoordRange(Coord(1, 1), Direction.UP)
    assert Coord(1, 1) in CoordRange(Coord(1, 1), Direction.DOWN)
    assert Coord(1, 1) in CoordRange(Coord(1, 1), Direction.LEFT)
    assert Coord(1, 1) in CoordRange(Coord(1, 1), Direction.RIGHT)

    assert Coord(0, 1) in CoordRange(Coord(1, 1), Direction.UP)
    assert Coord(0, 1) not in CoordRange(Coord(1, 1), Direction.DOWN)
    assert Coord(0, 1) not in CoordRange(Coord(1, 1), Direction.LEFT)
    assert Coord(0, 1) not in CoordRange(Coord(1, 1), Direction.RIGHT)

    assert Coord(1, 0) not in CoordRange(Coord(1, 1), Direction.UP)
    assert Coord(1, 0) not in CoordRange(Coord(1, 1), Direction.DOWN)
    assert Coord(1, 0) in CoordRange(Coord(1, 1), Direction.LEFT)
    assert Coord(1, 0) not in CoordRange(Coord(1, 1), Direction.RIGHT)

    assert Coord(1, 2) not in CoordRange(Coord(1, 1), Direction.UP)
    assert Coord(1, 2) not in CoordRange(Coord(1, 1), Direction.DOWN)
    assert Coord(1, 2) not in CoordRange(Coord(1, 1), Direction.LEFT)
    assert Coord(1, 2) in CoordRange(Coord(1, 1), Direction.RIGHT)

    assert Coord(2, 1) not in CoordRange(Coord(1, 1), Direction.UP)
    assert Coord(2, 1) in CoordRange(Coord(1, 1), Direction.DOWN)
    assert Coord(2, 1) not in CoordRange(Coord(1, 1), Direction.LEFT)
    assert Coord(2, 1) not in CoordRange(Coord(1, 1), Direction.RIGHT)


def test_iter():
    assert list(CoordRange(Coord(2, 2), Direction.UP)) == [
        Coord(2, 2),
        Coord(1, 2),
        Coord(0, 2),
    ]

    assert list(CoordRange(Coord(2, 2), Direction.LEFT)) == [
        Coord(2, 2),
        Coord(2, 1),
        Coord(2, 0),
    ]

    assert list(itertools.islice(CoordRange(Coord(2, 2), Direction.RIGHT), 0, 3)) == [
        Coord(2, 2),
        Coord(2, 3),
        Coord(2, 4),
    ]

    assert list(itertools.islice(CoordRange(Coord(2, 2), Direction.DOWN), 0, 3)) == [
        Coord(2, 2),
        Coord(3, 2),
        Coord(4, 2),
    ]


def test_getitem():
    assert CoordRange(Coord(2, 2), Direction.LEFT)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.LEFT)[1] == Coord(2, 1)
    assert CoordRange(Coord(2, 2), Direction.LEFT)[2] == Coord(2, 0)
    with pytest.raises(IndexError):
        CoordRange(Coord(2, 0), Direction.LEFT)[3]

    assert CoordRange(Coord(2, 2), Direction.UP)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.UP)[1] == Coord(1, 2)
    assert CoordRange(Coord(2, 2), Direction.UP)[2] == Coord(0, 2)
    with pytest.raises(IndexError):
        CoordRange(Coord(2, 0), Direction.LEFT)[3]

    assert CoordRange(Coord(2, 2), Direction.DOWN)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.DOWN)[1] == Coord(3, 2)
    assert CoordRange(Coord(2, 2), Direction.DOWN)[2] == Coord(4, 2)
    assert CoordRange(Coord(2, 2), Direction.DOWN)[3] == Coord(5, 2)

    assert CoordRange(Coord(2, 2), Direction.RIGHT)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.RIGHT)[1] == Coord(2, 3)
    assert CoordRange(Coord(2, 2), Direction.RIGHT)[2] == Coord(2, 4)
    assert CoordRange(Coord(2, 2), Direction.RIGHT)[3] == Coord(2, 5)
