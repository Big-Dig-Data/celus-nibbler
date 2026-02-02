import json

import pytest
from pydantic import ValidationError

from celus_nibbler.conditions import AndCondition, RegexCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataFormatDefinition, DataHeaders
from celus_nibbler.definitions import (
    BaseParserDefinition,
    GenericAreaDefinition,
    GenericDefinition,
    get_all_definitions,
)
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    Role,
    TitleIdSource,
    TitleSource,
)


def test_generic_area_definition():
    init_definition = GenericAreaDefinition(
        kind="non_counter.generic",
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
        "kind": "non_counter.generic",
        "data_headers": {
            "roles": [
                {
                    "source": {
                        "coord": {"row": 1, "col": 5, "row_relative_to": "area"},
                        "direction": "left",
                        "max_count": None,
                    },
                    "extract_params": {
                        "blank_values": [None, ""],
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "max_idx": None,
                        "skip_validation": False,
                        "special_extraction": "no",
                        "prefix": "",
                        "suffix": "",
                        "on_validation_error": "fail",
                        "skip_condition": None,
                        "value_overrides": [],
                    },
                    "fallback": None,
                    "preferred_date_format": "us",
                    "cleanup_during_header_processing": True,
                    "composed": None,
                    "force_aligned": False,
                    "date_pattern": None,
                    "role": Role.DATE,
                }
            ],
            "data_direction": "down",
            "data_cells": {
                "coord": {"col": 5, "row": 2, "row_relative_to": "area"},
                "direction": "left",
                "max_count": None,
            },
            "data_cells_options": {
                "use_header_year": True,
                "use_header_month": True,
            },
            "data_extract_params": {
                "blank_values": [None, ""],
                "regex": None,
                "default": None,
                "last_value_as_default": False,
                "max_idx": None,
                "skip_validation": False,
                "special_extraction": "no",
                "prefix": "",
                "suffix": "",
                "on_validation_error": "stop",
                "skip_condition": None,
                "value_overrides": [],
            },
            "rules": [
                {
                    "condition": None,
                    "role_extract_params_override": None,
                    "on_condition_failed": "stop",
                    "on_condition_passed": "proceed",
                    "on_error": "stop",
                    "role_idx": None,
                    "role_source_offset": 0,
                }
            ],
            "condition": None,
            "data_allow_negative": False,
        },
        "dimensions": [
            {
                "name": "publisher",
                "source": {
                    "coord": {"row": 2, "col": 3, "row_relative_to": "area"},
                    "direction": "down",
                    "max_count": None,
                },
                "extract_params": {
                    "blank_values": [None, ""],
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "max_idx": None,
                    "skip_validation": False,
                    "special_extraction": "no",
                    "prefix": "",
                    "suffix": "",
                    "on_validation_error": "fail",
                    "skip_condition": None,
                    "value_overrides": [],
                },
                "fallback": None,
                "cleanup_during_header_processing": True,
                "role": Role.DIMENSION,
            },
            {
                "name": "platform",
                "source": {
                    "coord": {"row": 2, "col": 5, "row_relative_to": "area"},
                    "direction": "down",
                    "max_count": None,
                },
                "extract_params": {
                    "blank_values": [None, ""],
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "max_idx": None,
                    "skip_validation": False,
                    "special_extraction": "no",
                    "prefix": "",
                    "suffix": "",
                    "on_validation_error": "fail",
                    "skip_condition": None,
                    "value_overrides": [],
                },
                "fallback": None,
                "cleanup_during_header_processing": True,
                "role": Role.DIMENSION,
            },
        ],
        "metrics": {
            "source": {
                "coord": {"row": 2, "col": 4, "row_relative_to": "area"},
                "direction": "down",
                "max_count": None,
            },
            "extract_params": {
                "blank_values": [None, ""],
                "regex": None,
                "default": None,
                "last_value_as_default": False,
                "max_idx": None,
                "skip_validation": False,
                "special_extraction": "no",
                "prefix": "",
                "suffix": "",
                "on_validation_error": "fail",
                "skip_condition": None,
                "value_overrides": [],
            },
            "fallback": None,
            "cleanup_during_header_processing": True,
            "role": Role.METRIC,
        },
        "titles": {
            "source": {
                "coord": {"row": 2, "col": 0, "row_relative_to": "area"},
                "direction": "down",
                "max_count": None,
            },
            "extract_params": {
                "blank_values": [None, ""],
                "regex": None,
                "default": None,
                "last_value_as_default": False,
                "max_idx": None,
                "skip_validation": False,
                "special_extraction": "no",
                "prefix": "",
                "suffix": "",
                "on_validation_error": "fail",
                "skip_condition": None,
                "value_overrides": [],
            },
            "fallback": None,
            "cleanup_during_header_processing": True,
            "role": Role.TITLE,
        },
        "title_ids": [
            {
                "name": "ISBN",
                "source": {
                    "coord": {"row": 2, "col": 1, "row_relative_to": "area"},
                    "direction": "down",
                    "max_count": None,
                },
                "extract_params": {
                    "blank_values": [None, ""],
                    "regex": None,
                    "default": None,
                    "last_value_as_default": False,
                    "max_idx": None,
                    "skip_validation": False,
                    "special_extraction": "no",
                    "prefix": "",
                    "suffix": "",
                    "on_validation_error": "fail",
                    "skip_condition": None,
                    "value_overrides": [],
                },
                "validator_opts": None,
                "fallback": None,
                "cleanup_during_header_processing": True,
                "role": Role.TITLE_ID,
            }
        ],
        "items": None,
        "dates": None,
        "item_ids": [],
        "item_authors": None,
        "item_publication_date": None,
        "organizations": None,
        "aggregate_same_records": False,
        "max_areas_generated": 1,
        "min_valid_areas": 1,
    }

    converted_definition = GenericAreaDefinition.parse(definition_dict)
    assert converted_definition.dict() == init_definition.dict()
    assert converted_definition == init_definition


def test_errors():
    # Unknown definition
    with pytest.raises(ValidationError):
        GenericAreaDefinition.parse({"name": "unknown"})

    # Extra field
    with pytest.raises(ValidationError):
        GenericAreaDefinition.parse({"name": "dummy", "extra": 3})

    # Wrong field type
    with pytest.raises(ValidationError):
        GenericAreaDefinition.parse(
            {
                "kind": "non_counter.generic",
                "dates": {
                    "direction": "wrong",  # wrong direction
                    "source": {
                        "coord": {"row": 1, "col": 5, "row_relative_to": "area"},
                        "direction": "left",
                        "max_count": None,
                    },
                    "preferred_date_format": "us",
                    "cleanup_during_header_processing": True,
                    "composed": None,
                    "force_aligned": False,
                    "date_pattern": None,
                    "role": Role.DATE,
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "source": {
                            "coord": {"row": 2, "col": 3, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "fallback": None,
                        "role": Role.DIMENSION,
                    },
                    {
                        "name": "platform",
                        "source": {
                            "coord": {"row": 1, "col": 5, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "role": Role.DIMENSION,
                    },
                ],
                "metrics": {
                    "source": {
                        "coord": {"row": 2, "col": 4, "row_relative_to": "area"},
                        "direction": "down",
                        "max_count": None,
                    },
                    "role": Role.METRIC,
                },
                "titles": {
                    "source": {
                        "coord": {"row": 2, "col": 0, "row_relative_to": "area"},
                        "direction": "down",
                        "max_count": None,
                    },
                    "extract_params": {
                        "blank_values": [None, ""],
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "max_idx": None,
                        "skip_validation": False,
                        "special_extraction": "no",
                        "prefix": "",
                        "suffix": "",
                        "on_validation_error": "fail",
                        "skip_condition": None,
                        "value_overrides": [],
                    },
                    "fallback": None,
                    "cleanup_during_header_processing": True,
                    "role": Role.TITLE,
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {
                            "coord": {"row": 2, "col": 1, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "validator_opts": None,
                        "fallback": None,
                        "role": Role.TITLE_ID,
                    }
                ],
                "items": None,
                "item_ids": [],
                "item_authors": None,
                "item_publication_date": None,
                "aggregate_same_records": False,
                "max_areas_generated": 1,
                "min_valid_areas": 1,
            }
        )

    # Missing field
    with pytest.raises(ValidationError):
        GenericAreaDefinition.parse(
            {
                "kind": "non_counter.generic",
                "dates": {
                    "source": {
                        "coord": {"row": 1, "col": 6, "row_relative_to": "area"},
                        "direction": "left",
                        "max_count": None,
                    },
                    "preferred_date_format": "us",
                    "cleanup_during_header_processing": True,
                    "composed": None,
                    "force_aligned": False,
                    "date_pattern": None,
                    "role": Role.DATE,
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "source": {
                            "coord": {"row": 2, "col": 3, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "fallback": None,
                        "cleanup_during_header_processing": True,
                        "role": Role.DIMENSION,
                    },
                    {
                        "name": "platform",
                        "source": {
                            "direction": "down",
                            "coord": {"row": 1, "col": 5, "row_relative_to": "area"},
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "fallback": None,
                        "cleanup_during_header_processing": True,
                        "role": Role.DIMENSION,
                    },
                ],
                "metrics": {
                    "source": {
                        "coord": {"row": 2, "col": 4, "row_relative_to": "area"},
                        "direction": "down",
                        "max_count": None,
                    },
                    "extract_params": {
                        "blank_values": [None, ""],
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "max_idx": None,
                        "skip_validation": False,
                        "special_extraction": "no",
                        "prefix": "",
                        "suffix": "",
                        "on_validation_error": "fail",
                        "skip_condition": None,
                        "value_overrides": [],
                    },
                    "fallback": None,
                    "cleanup_during_header_processing": True,
                    "role": Role.METRIC,
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {
                            "coord": {"row": 2, "col": 1, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "validator_opts": None,
                        "fallback": None,
                        "cleanup_during_header_processing": True,
                        "role": Role.TITLE_ID,
                    }
                ],
                "items": None,
                "item_ids": [],
                "item_authors": None,
                "item_publication_date": None,
                "aggregate_same_records": False,
                "max_areas_generated": 1,
                "min_valid_areas": 1,
            }
        )


def test_generic_definition():
    init_definition = GenericDefinition(
        parser_name="Parser1",
        data_format=DataFormatDefinition(name="Format1"),
        platforms=["Platform1", "Platform2"],
        heuristics=AndCondition(
            conds=[
                RegexCondition("AAA", Coord(1, 1)),
                RegexCondition("BBB", Coord(2, 2)),
            ]
        ),
        areas=[
            GenericAreaDefinition(
                kind="non_counter.generic",
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
        "kind": "non_counter.generic",
        "version": 1,
        "parser_name": "Parser1",
        "data_format": {"name": "Format1", "id": None},
        "platforms": ["Platform1", "Platform2"],
        "dimensions_to_skip": {},
        "dimensions_validators": {},
        "dimension_aliases": [],
        "heuristics": {
            "conds": [
                {
                    "kind": "regex",
                    "pattern": "AAA",
                    "coord": {"col": 1, "row": 1, "row_relative_to": "area"},
                },
                {
                    "kind": "regex",
                    "pattern": "BBB",
                    "coord": {"col": 2, "row": 2, "row_relative_to": "area"},
                },
            ],
            "kind": "and",
        },
        "areas": [
            {
                "kind": "non_counter.generic",
                "data_headers": {
                    "roles": [
                        {
                            "source": {
                                "direction": "right",
                                "coord": {"col": 5, "row": 1, "row_relative_to": "area"},
                                "max_count": None,
                            },
                            "extract_params": {
                                "blank_values": [None, ""],
                                "regex": None,
                                "default": None,
                                "last_value_as_default": False,
                                "max_idx": None,
                                "skip_validation": False,
                                "special_extraction": "no",
                                "prefix": "",
                                "suffix": "",
                                "on_validation_error": "fail",
                                "skip_condition": None,
                                "value_overrides": [],
                            },
                            "fallback": None,
                            "preferred_date_format": "us",
                            "cleanup_during_header_processing": True,
                            "composed": None,
                            "force_aligned": False,
                            "date_pattern": None,
                            "role": Role.DATE,
                        }
                    ],
                    "data_direction": Direction.DOWN,
                    "data_cells": {
                        "coord": {"col": 5, "row": 2, "row_relative_to": "area"},
                        "direction": "right",
                        "max_count": None,
                    },
                    "data_cells_options": {
                        "use_header_year": True,
                        "use_header_month": True,
                    },
                    "data_extract_params": {
                        "blank_values": [None, ""],
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "max_idx": None,
                        "skip_validation": False,
                        "special_extraction": "no",
                        "prefix": "",
                        "suffix": "",
                        "on_validation_error": "stop",
                        "skip_condition": None,
                        "value_overrides": [],
                    },
                    "rules": [
                        {
                            "condition": None,
                            "role_extract_params_override": None,
                            "on_condition_failed": "stop",
                            "on_condition_passed": "proceed",
                            "on_error": "stop",
                            "role_idx": None,
                            "role_source_offset": 0,
                        }
                    ],
                    "condition": None,
                    "data_allow_negative": False,
                },
                "dimensions": [
                    {
                        "name": "publisher",
                        "source": {
                            "coord": {"col": 3, "row": 2, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "fallback": None,
                        "cleanup_during_header_processing": True,
                        "role": Role.DIMENSION,
                    },
                    {
                        "name": "platform",
                        "source": {
                            "coord": {"col": 5, "row": 2, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "fallback": None,
                        "cleanup_during_header_processing": True,
                        "role": Role.DIMENSION,
                    },
                ],
                "metrics": {
                    "source": {
                        "coord": {"col": 4, "row": 2, "row_relative_to": "area"},
                        "direction": "down",
                        "max_count": None,
                    },
                    "extract_params": {
                        "blank_values": [None, ""],
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "max_idx": None,
                        "skip_validation": False,
                        "special_extraction": "no",
                        "prefix": "",
                        "suffix": "",
                        "on_validation_error": "fail",
                        "skip_condition": None,
                        "value_overrides": [],
                    },
                    "fallback": None,
                    "cleanup_during_header_processing": True,
                    "role": Role.METRIC,
                },
                "titles": {
                    "source": {
                        "coord": {"col": 0, "row": 2, "row_relative_to": "area"},
                        "direction": "down",
                        "max_count": None,
                    },
                    "extract_params": {
                        "blank_values": [None, ""],
                        "regex": None,
                        "default": None,
                        "last_value_as_default": False,
                        "max_idx": None,
                        "skip_validation": False,
                        "special_extraction": "no",
                        "prefix": "",
                        "suffix": "",
                        "on_validation_error": "fail",
                        "skip_condition": None,
                        "value_overrides": [],
                    },
                    "fallback": None,
                    "cleanup_during_header_processing": True,
                    "role": Role.TITLE,
                },
                "title_ids": [
                    {
                        "name": "ISBN",
                        "source": {
                            "coord": {"col": 1, "row": 2, "row_relative_to": "area"},
                            "direction": "down",
                            "max_count": None,
                        },
                        "extract_params": {
                            "blank_values": [None, ""],
                            "regex": None,
                            "default": None,
                            "last_value_as_default": False,
                            "max_idx": None,
                            "skip_validation": False,
                            "special_extraction": "no",
                            "prefix": "",
                            "suffix": "",
                            "on_validation_error": "fail",
                            "skip_condition": None,
                            "value_overrides": [],
                        },
                        "validator_opts": None,
                        "fallback": None,
                        "cleanup_during_header_processing": True,
                        "role": Role.TITLE_ID,
                    },
                ],
                "items": None,
                "item_ids": [],
                "item_authors": None,
                "item_publication_date": None,
                "dates": None,
                "organizations": None,
                "aggregate_same_records": False,
                "max_areas_generated": 1,
                "min_valid_areas": 1,
            },
        ],
        "metric_aliases": [],
        "metric_value_extraction_overrides": {},
        "metrics_to_skip": [],
        "available_metrics": None,
        "on_metric_check_failed": "skip",
        "titles_to_skip": [],
        "possible_row_offsets": [0],
        "uses_titles": None,
        "uses_items": None,
    }


def test_get_all_definitions():
    definitions = get_all_definitions()
    assert all(issubclass(e, BaseParserDefinition) for e in definitions)
    assert all(hasattr(e, "kind") for e in definitions), "kind in definitions"
