import json

import pytest
from pydantic import ValidationError

from celus_nibbler.conditions import AndCondition, RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.definitions import (
    DateBasedAreaDefinition,
    DateBasedDefinition,
    MetricBasedAreaDefinition,
)
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    Role,
    TitleIdSource,
    TitleSource,
)


def test_date_based_area_definition():
    init_definition = DateBasedAreaDefinition(
        kind="non_counter.date_based",
        data_headers=DataHeaders(
            roles=[
                DateSource(source=CoordRange(Coord(1, 5), Direction.LEFT)),
            ],
            data_cells=CoordRange(Coord(2, 5), Direction.LEFT),
            data_direction=Direction.DOWN,
        ),
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
        "kind": "non_counter.date_based",
        "data_headers": {
            "roles": [
                {
                    "source": {"coord": {"row": 1, "col": 5}, "direction": "left"},
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "role": Role.DATE,
                }
            ],
            "data_direction": "down",
            "data_cells": {
                "coord": {"col": 5, "row": 2},
                "direction": "left",
            },
            "data_default": None,
        },
        "dimensions": [
            {
                "name": "publisher",
                "required": True,
                "source": {"coord": {"row": 2, "col": 3}, "direction": "down"},
                "regex": None,
                "default": None,
                "last_value_as_default": False,
                "role": Role.DIMENSION,
            },
            {
                "name": "platform",
                "required": True,
                "source": {"coord": {"row": 2, "col": 5}, "direction": "down"},
                "regex": None,
                "default": None,
                "last_value_as_default": False,
                "role": Role.DIMENSION,
            },
        ],
        "metrics": {
            "source": {"coord": {"row": 2, "col": 4}, "direction": "down"},
            "regex": None,
            "default": None,
            "last_value_as_default": False,
            "role": Role.METRIC,
        },
        "title_ids": [
            {
                "name": "ISBN",
                "source": {"coord": {"row": 2, "col": 1}, "direction": "down"},
                "regex": None,
                "default": None,
                "last_value_as_default": False,
                "role": Role.TITLE_ID,
            }
        ],
        "titles": {
            "source": {"coord": {"row": 2, "col": 0}, "direction": "down"},
            "regex": None,
            "default": None,
            "last_value_as_default": False,
            "role": Role.TITLE,
        },
        "organizations": None,
    }

    converted_definition = DateBasedAreaDefinition.parse(definition_dict)
    assert converted_definition.dict() == init_definition.dict()
    assert converted_definition == init_definition


def test_errors():

    # Unknown definition
    with pytest.raises(ValidationError):
        MetricBasedAreaDefinition.parse({"name": "unknown"})

    # Extra field
    with pytest.raises(ValidationError):
        MetricBasedAreaDefinition.parse({"name": "dummy", "extra": 3})

    # Wrong field type
    with pytest.raises(ValidationError):
        DateBasedAreaDefinition.parse(
            {
                "kind": "non_counter.date_based",
                "dates": {
                    "direction": "wrong",  # wrong direction
                    "source": {"coord": {"row": 1, "col": 5}, "direction": "left"},
                    "role": Role.DATE,
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "required": True,
                        "source": {"coord": {"row": 2, "col": 3}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.DIMENSION,
                    },
                    {
                        "name": "platform",
                        "required": True,
                        "source": {"coord": {"row": 1, "col": 5}, "direction": "down"},
                        "role": Role.DIMENSION,
                    },
                ],
                "metrics": {
                    "source": {"coord": {"row": 2, "col": 4}, "direction": "down"},
                    "role": Role.METRIC,
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {"coord": {"row": 2, "col": 1}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.TITLE_ID,
                    }
                ],
                "titles": {
                    "source": {"coord": {"row": 2, "col": 0}, "direction": "down"},
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "role": Role.TITLE,
                },
            }
        )

    # Missing field
    with pytest.raises(ValidationError):
        DateBasedAreaDefinition.parse(
            {
                "kind": "non_counter.date_based",
                "dates": {
                    "source": {"coord": {"row": 1, "col": 6}, "direction": "left"},
                    "role": Role.DATE,
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "required": True,
                        "source": {"coord": {"row": 2, "col": 3}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.DIMENSION,
                    },
                    {
                        "name": "platform",
                        "source": {"direction": "down", "coord": {"row": 1, "col": 5}},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.DIMENSION,
                    },
                ],
                "metrics": {
                    "source": {"coord": {"row": 2, "col": 4}, "direction": "down"},
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "role": Role.METRIC,
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {"coord": {"row": 2, "col": 1}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.TITLE_ID,
                    }
                ],
            }
        )


def test_date_based_definition():
    init_definition = DateBasedDefinition(
        parser_name="Parser1",
        data_format=DataFormatDefinition(name="Format1"),
        platforms=["Platform1", "Platform2"],
        dimensions=["Dim1", "Dim2"],
        heuristics=AndCondition(
            conds=[
                RegexCondition("AAA", Coord(1, 1)),
                RegexCondition("BBB", Coord(2, 2)),
            ]
        ),
        areas=[
            DateBasedAreaDefinition(
                kind="non_counter.date_based",
                data_headers=DataHeaders(
                    roles=[
                        DateSource(
                            source=CoordRange(Coord(1, 5), Direction.RIGHT),
                        ),
                    ],
                    data_cells=CoordRange(Coord(2, 5), Direction.RIGHT),
                    data_direction=Direction.DOWN,
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
        "kind": "non_counter.date_based",
        "version": 1,
        "parser_name": "Parser1",
        "data_format": {"name": "Format1", "id": None},
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
                "kind": "non_counter.date_based",
                "data_headers": {
                    "roles": [
                        {
                            "source": {"direction": "right", "coord": {"col": 5, "row": 1}},
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "role": Role.DATE,
                        }
                    ],
                    "data_direction": Direction.DOWN,
                    "data_cells": {
                        "coord": {"col": 5, "row": 2},
                        "direction": "right",
                    },
                    "data_default": None,
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "required": True,
                        "source": {"coord": {"col": 3, "row": 2}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.DIMENSION,
                    },
                    {
                        "name": "platform",
                        "required": True,
                        "source": {"coord": {"col": 5, "row": 2}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.DIMENSION,
                    },
                ],
                "metrics": {
                    "source": {"coord": {"col": 4, "row": 2}, "direction": "down"},
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "role": Role.METRIC,
                },
                "titles": {
                    "source": {"coord": {"col": 0, "row": 2}, "direction": "down"},
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "role": Role.TITLE,
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {"coord": {"col": 1, "row": 2}, "direction": "down"},
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "role": Role.TITLE_ID,
                    },
                ],
                "organizations": None,
            },
        ],
        "metric_aliases": [],
        "metrics_to_skip": [],
        "titles_to_skip": [],
    }
