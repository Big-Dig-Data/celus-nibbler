import pytest

from celus_nibbler.utils import normalize_row, normalize_spaces


@pytest.mark.parametrize(
    "text,expected",
    (
        ("a  a c", "a a c"),
        ("a   b    c", "a b c"),
        ("abc", "abc"),
        ("a b c", "a b c"),
        ("", ""),
        ("  lead", " lead"),
        ("trail   ", "trail "),
        ("tab\t\ttab", "tab tab"),
        ("mixed \t \n run", "mixed run"),
        ("multi\r\nline", "multi line"),
        ("nbsp  here", "nbsp here"),
        ("space nbsp", "space nbsp"),
        ("single\ttab", "single tab"),
        ("new\nline", "new line"),
        ("\tlead", " lead"),
        ("em  space", "em space"),
    ),
)
def test_normalize_spaces(text, expected):
    assert normalize_spaces(text) == expected


def test_normalize_spaces_no_copy():
    text = "no doubled spaces"
    assert normalize_spaces(text) is text


def test_normalize_row():
    assert normalize_row(["a  b", "c", "d   e  f"]) == ["a b", "c", "d e f"]
    assert normalize_row(["a\t\tb", "c\n\nd", "e"]) == ["a b", "c d", "e"]

    row = ["a b", "c", ""]
    assert normalize_row(row) is row

    empty = []
    assert normalize_row(empty) is empty


def test_normalize_row_only_affected_cells():
    untouched = "no doubled spaces"
    row = [untouched, "needs  normalization", "x"]
    result = normalize_row(row)

    assert result is row  # normalized in place, no copy
    assert result == ["no doubled spaces", "needs normalization", "x"]
    assert result[0] is untouched  # cells without whitespaces to normalize stay the same objects
