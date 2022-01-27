from io import StringIO

import pytest

from celus_nibbler.reader import SheetReader


@pytest.fixture
def sheet_csv():
    return StringIO("Name,Value\nFirst,1\nSecond,2\nThird,3\nFourth,4\n")


@pytest.fixture
def sheet(sheet_csv):
    return SheetReader(0, None, sheet_csv)
