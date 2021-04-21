import pathlib

import pytest


@pytest.fixture
def base_path():
    return pathlib.Path(__file__).parent
