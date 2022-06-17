import json

import pytest
from pydantic import ValidationError

from celus_nibbler.conditions import AndCondition, RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.definitions import (
    DateSource,
    Definition,
    DimensionSource,
    DummyAreaDefinition,
    FixedAreaDefinition,
    MetricSource,
    TitleIdSource,
    TitleSource,
)


def test_fixed_area_definition():
    init_definition = FixedAreaDefinition(
        name="fixed",
        dates=DateSource(direction=Direction.DOWN, source=CoordRange(Coord(1, 5), Direction.LEFT)),
        titles=TitleSource(CoordRange(Coord(2, 0), Direction.DOWN)),
        title_ids=[
            TitleIdSource(name="ISBN", source=CoordRange(Coord(2, 1), Direction.DOWN)),
        ],
        dimensions=[
            DimensionSource(name="publisher", source=CoordRange(Coord(2, 3), Direction.DOWN)),
            DimensionSource(name="platform", source=CoordRange(Coord(2, 5), Direction.DOWN)),
        ],
        metrics=MetricSource(source=CoordRange(Coord(2, 4), Direction.DOWN)),
    )
    definition_dict = json.loads(init_definition.json())

    assert definition_dict == {
        "name": "fixed",
        "dates": {
            "direction": "down",
            "source": {"coord": {"row": 1, "col": 5}, "direction": "left"},
        },
        "dimensions": [
            {
                "name": "publisher",
                "required": True,
                "source": {"coord": {"row": 2, "col": 3}, "direction": "down"},
            },
            {
                "name": "platform",
                "required": True,
                "source": {"coord": {"row": 2, "col": 5}, "direction": "down"},
            },
        ],
        "metrics": {"source": {"coord": {"row": 2, "col": 4}, "direction": "down"}},
        "title_ids": [
            {
                "name": "ISBN",
                "source": {"coord": {"row": 2, "col": 1}, "direction": "down"},
            }
        ],
        "titles": {"source": {"coord": {"row": 2, "col": 0}, "direction": "down"}},
    }

    converted_definition = FixedAreaDefinition.parse(definition_dict)
    assert converted_definition.dict() == init_definition.dict()
    assert converted_definition == init_definition


def test_dummy_area_definition():
    init_definition = DummyAreaDefinition()
    definition_dict = json.loads(init_definition.json())
    assert definition_dict == {
        "name": "dummy",
    }
    converted_definition = DummyAreaDefinition.parse(definition_dict)
    assert converted_definition.dict() == init_definition.dict()
    assert converted_definition == init_definition


def test_errors():

    # Unknown definition
    with pytest.raises(ValidationError):
        DummyAreaDefinition.parse({"name": "unknown"})

    # Extra field
    with pytest.raises(ValidationError):
        DummyAreaDefinition.parse({"name": "dummy", "extra": 3})

    # Wrong field type
    with pytest.raises(ValidationError):
        FixedAreaDefinition.parse(
            {
                "name": "fixed",
                "dates": {
                    "direction": "wrong",  # wrong direction
                    "source": {"coord": {"row": 1, "col": 5}, "direction": "left"},
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "required": True,
                        "source": {"coord": {"row": 2, "col": 3}, "direction": "down"},
                    },
                    {
                        "name": "platform",
                        "required": True,
                        "source": {"coord": {"row": 1, "col": 5}, "direction": "down"},
                    },
                ],
                "metrics": {
                    "source": {"coord": {"row": 2, "col": 4}, "direction": "down"},
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {"coord": {"row": 2, "col": 1}, "direction": "down"},
                    }
                ],
                "titles": {"source": {"coord": {"row": 2, "col": 0}, "direction": "down"}},
            }
        )

    # Missing field
    with pytest.raises(ValidationError):
        FixedAreaDefinition.parse(
            {
                "name": "fixed",
                "dates": {
                    "direction": "down",
                    "source": {"coord": {"row": 1, "col": 6}, "direction": "left"},
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "required": True,
                        "source": {"coord": {"row": 2, "col": 3}, "direction": "down"},
                    },
                    {
                        "name": "platform",
                        "source": {"direction": "down", "coord": {"row": 1, "col": 5}},
                    },
                ],
                "metrics": {
                    "source": {"coord": {"row": 2, "col": 4}, "direction": "down"},
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {"coord": {"row": 2, "col": 1}, "direction": "down"},
                    }
                ],
            }
        )


def test_definition():
    init_definition = Definition(
        parser_name="Parser1",
        format_name="Format1",
        platforms=["Platform1", "Platform2"],
        dimensions=["Dim1", "Dim2"],
        heuristics=AndCondition(
            conds=[
                RegexCondition("AAA", Coord(1, 1)),
                RegexCondition("BBB", Coord(2, 2)),
            ]
        ),
        areas=[
            FixedAreaDefinition(
                name="fixed",
                dates=DateSource(
                    direction=Direction.DOWN, source=CoordRange(Coord(1, 5), Direction.RIGHT)
                ),
                titles=TitleSource(CoordRange(Coord(2, 0), Direction.DOWN)),
                title_ids=[
                    TitleIdSource(name="ISBN", source=CoordRange(Coord(2, 1), Direction.DOWN)),
                ],
                dimensions=[
                    DimensionSource(
                        name="publisher", source=CoordRange(Coord(2, 3), Direction.DOWN)
                    ),
                    DimensionSource(
                        name="platform", source=CoordRange(Coord(2, 5), Direction.DOWN)
                    ),
                ],
                metrics=MetricSource(source=CoordRange(Coord(2, 4), Direction.DOWN)),
            )
        ],
    )

    definition_dict = json.loads(init_definition.json())

    assert definition_dict == {
        "version": 1,
        "parser_name": "Parser1",
        "format_name": "Format1",
        "platforms": ["Platform1", "Platform2"],
        "dimensions_to_skip": {},
        "dimensions": ["Dim1", "Dim2"],
        "dimension_aliases": [],
        "heuristics": {
            "conds": [
                {"kind": "regex", "pattern": "AAA", "coord": {"col": 1, "row": 1}},
                {"kind": "regex", "pattern": "BBB", "coord": {"col": 2, "row": 2}},
            ],
            "kind": "and",
        },
        "areas": [
            {
                "dates": {
                    "direction": "down",
                    "source": {"direction": "right", "coord": {"col": 5, "row": 1}},
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "required": True,
                        "source": {"coord": {"col": 3, "row": 2}, "direction": "down"},
                    },
                    {
                        "name": "platform",
                        "required": True,
                        "source": {"coord": {"col": 5, "row": 2}, "direction": "down"},
                    },
                ],
                "metrics": {"source": {"coord": {"col": 4, "row": 2}, "direction": "down"}},
                "name": "fixed",
                "titles": {"source": {"coord": {"col": 0, "row": 2}, "direction": "down"}},
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {"coord": {"col": 1, "row": 2}, "direction": "down"},
                    },
                ],
            },
        ],
        "metric_aliases": [],
        "metrics_to_skip": [],
        "titles_to_skip": [],
    }
