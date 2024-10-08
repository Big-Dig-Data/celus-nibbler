import itertools
import json

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

    # max size
    assert Coord(0, 1) not in CoordRange(Coord(1, 1), Direction.UP, 1)
    assert Coord(2, 1) not in CoordRange(Coord(1, 1), Direction.DOWN, 1)
    assert Coord(1, 0) not in CoordRange(Coord(1, 1), Direction.LEFT, 1)
    assert Coord(1, 2) not in CoordRange(Coord(1, 1), Direction.RIGHT, 1)


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

    # with max size
    assert list(CoordRange(Coord(2, 2), Direction.UP, max_count=2)) == [
        Coord(2, 2),
        Coord(1, 2),
    ]

    assert list(CoordRange(Coord(2, 2), Direction.LEFT, max_count=2)) == [
        Coord(2, 2),
        Coord(2, 1),
    ]

    assert list(itertools.islice(CoordRange(Coord(2, 2), Direction.RIGHT, max_count=2), 0, 3)) == [
        Coord(2, 2),
        Coord(2, 3),
    ]

    assert list(itertools.islice(CoordRange(Coord(2, 2), Direction.DOWN, max_count=2), 0, 3)) == [
        Coord(2, 2),
        Coord(3, 2),
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

    # max sizes
    assert CoordRange(Coord(2, 2), Direction.LEFT, max_count=2)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.LEFT, max_count=2)[1] == Coord(2, 1)
    with pytest.raises(IndexError):
        CoordRange(Coord(2, 0), Direction.LEFT, max_count=2)[2]

    assert CoordRange(Coord(2, 2), Direction.UP, max_count=2)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.UP, max_count=2)[1] == Coord(1, 2)
    with pytest.raises(IndexError):
        CoordRange(Coord(2, 0), Direction.LEFT, max_count=2)[2]

    assert CoordRange(Coord(2, 2), Direction.DOWN, max_count=2)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.DOWN, max_count=2)[1] == Coord(3, 2)
    with pytest.raises(IndexError):
        CoordRange(Coord(2, 2), Direction.DOWN, max_count=2)[2]

    assert CoordRange(Coord(2, 2), Direction.RIGHT, max_count=2)[0] == Coord(2, 2)
    assert CoordRange(Coord(2, 2), Direction.RIGHT, max_count=2)[1] == Coord(2, 3)
    with pytest.raises(IndexError):
        CoordRange(Coord(2, 2), Direction.RIGHT, max_count=2)[2]


def test_serialization_and_deserialization():
    # Coord
    coord_json = Coord(1, 2, "start").json()
    assert coord_json == '{"row":1,"col":2,"row_relative_to":"start"}'
    assert Coord(**json.loads(coord_json)) == Coord(1, 2, "start")

    # Coord Range
    range_json = CoordRange(Coord(2, 3), Direction.DOWN).json()
    assert range_json == (
        '{"coord":{"row":2,"col":3,"row_relative_to":"area"},"direction":"down","max_count":null}'
    )
    assert CoordRange(**json.loads(range_json)) == CoordRange(Coord(2, 3), Direction.DOWN)

    range_json = CoordRange(Coord(2, 3), Direction.DOWN, 10).json()
    assert range_json == (
        '{"coord":{"row":2,"col":3,"row_relative_to":"area"},"direction":"down","max_count":10}'
    )
    assert CoordRange(**json.loads(range_json)) == CoordRange(Coord(2, 3), Direction.DOWN, 10)
