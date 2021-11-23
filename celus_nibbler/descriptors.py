from enum import Enum, auto


class MonthsDirection(Enum):
    """
    describes whether the table has orientation with months verticaly or horizontaly.
    """

    VERTICAL = auto()
    HORIZONTAL = auto()


class RelatedTo(Enum):
    """
    This class instructs the parser how to iterate over the variable (titles, metrics, etc.) and assign values he finds in it to each record.

    TABLE -  the one value in this variable goes to each record in a table
    ROW  -  iterate over this variable and assign each value in it to every record in a row
    COL  -  iterate over this variable and assign each value in it to every record in a column
    FIELD   -   iterate over this variable and assign each value in it to only one record at the time
    """

    FIELD = auto()
    COL = auto()
    ROW = auto()
    TABLE = auto()


class String(Enum):
    """
    This class explain occurrence of `content` in the string.

    STARTSWITH = the string starts with the `content`
    ENDSWITH = the string ends with the `content`
    CONTAINS = the string contains the `content`
    IS = the string is the `content`
    """

    STARTSWITH = auto()
    ENDSWITH = auto()
    CONTAINS = auto()
    IS = auto()


class Coord:
    def __init__(self, start_row, start_col, content_type=String.IS, content=None, relation=None):
        self.start_row = start_row
        self.start_col = start_col
        self.content_type = content_type
        self.content = content
        self.relation = relation

    def __str__(self):
        return f'Coords are start_row:{self.start_row} start_col:{self.start_col} content:{self.content} relation:{self.relation}'
