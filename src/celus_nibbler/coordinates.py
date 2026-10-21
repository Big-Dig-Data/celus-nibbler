import abc
import typing
from enum import Enum

from pydantic.dataclasses import dataclass

from .errors import TableException
from .reader import SheetReader
from .utils import JsonEncorder, PydanticConfig


class Direction(str, Enum):
    """Direction in which a CoordRange walks over the sheet."""

    LEFT = "left"
    RIGHT = "right"
    UP = "up"
    DOWN = "down"


class RelativeTo(str, Enum):
    """What `Coord.row` is counted from.

    - `area`: the area offset, i.e. the row where `DataHeaders.condition` matched
      (the parser offset when the area has no condition).
    - `parser`: the parser offset, i.e. the entry of `possible_row_offsets` for which
      the heuristics matched.
    - `start`: the top of the sheet (absolute row).
    """

    AREA = "area"
    PARSER = "parser"
    START = "start"


class Content(metaclass=abc.ABCMeta):
    @property
    def changes(self) -> bool:
        """Indicates whether the content changes with next iteration"""
        return False

    @abc.abstractmethod
    def content(
        self,
        sheet: SheetReader,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        pass


@dataclass(config=PydanticConfig)
class Value(JsonEncorder, Content):
    """Constant value, e.g. `{"value": "Searches"}`. Used for data which isn't present
    in the sheet, such as the only metric of a report."""

    value: typing.Any
    """The constant itself."""

    def content(
        self,
        sheet: SheetReader,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        return self.value

    def __next__(self) -> "Value":
        return Value(self.value)

    def __getitem__(self, item: int) -> "Value":
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        return next(self)


@dataclass(config=PydanticConfig)
class SheetAttr(JsonEncorder, Content):
    """Value taken from the sheet itself rather than from a cell, e.g.
    `{"sheet_attr": "name"}` reads the sheet (tab) name."""

    sheet_attr: str
    """`name` for the sheet name or `sheet_idx` for the zero-based sheet index."""

    def content(
        self,
        sheet: SheetReader,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        return getattr(sheet, self.sheet_attr)

    def __iter__(self):
        return self

    def __next__(self) -> "SheetAttr":
        return SheetAttr(self.sheet_attr)

    def __getitem__(self, item: int) -> "SheetAttr":
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        return next(self)


@dataclass(config=PydanticConfig)
class Coord(JsonEncorder, Content):
    """Single cell, e.g. `{"row": 0, "col": 5}`. Indexing is zero-based.

    Note that a single cell has no `coord` key, `{"coord": {...}, ...}` is a CoordRange.
    """

    row: int
    """Zero-based row, counted from the offset given by `row_relative_to`."""
    col: int
    """Zero-based column (0 is column A). Columns are always absolute."""
    row_relative_to: RelativeTo = RelativeTo.AREA
    """What `row` is counted from, the area offset by default."""

    def row_absolute(
        self,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        if self.row_relative_to == RelativeTo.AREA:
            if area_row_offset is None:
                raise RuntimeError("Cord with row_relative_to area need to have area offset set")
            return self.row + area_row_offset
        elif self.row_relative_to == RelativeTo.PARSER:
            if parser_row_offset is None:
                raise RuntimeError(
                    "Cord with row_relative_to parser need to have parser offset set"
                )
            return self.row + parser_row_offset
        elif self.row_relative_to == RelativeTo.START:
            return self.row

    def content(
        self,
        sheet: SheetReader,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        row_absolute = self.row_absolute(parser_row_offset, area_row_offset)
        try:
            return sheet[row_absolute][self.col]
        except IndexError as e:
            raise TableException(
                row=row_absolute,
                col=self.col,
                sheet=sheet.sheet_idx,
                reason="out-of-bounds",
                action=TableException.Action.STOP,
            ) from e

    def __iter__(self):
        return self

    def __next__(self) -> "Coord":
        return Coord(self.row, self.col, self.row_relative_to)

    def __getitem__(self, item: int) -> "Coord":
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        return next(self)

    def __add__(self, other) -> "Coord":
        return Coord(self.row + other.row, self.col + other.col, self.row_relative_to)


@dataclass(config=PydanticConfig)
class CoordRange(JsonEncorder, Content):
    """Cells starting at `coord` and following each other in `direction`, e.g.
    `{"coord": {"row": 2, "col": 1}, "direction": "right"}`."""

    coord: Coord
    """The first cell."""
    direction: Direction
    """Direction in which to walk from `coord`."""
    max_count: typing.Optional[int] = None
    """Maximum number of cells. When omitted, the range ends at the edge of the sheet."""

    @property
    def changes(self):
        return True

    def content(
        self,
        sheet: SheetReader,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        return self[self.distance].content(sheet, parser_row_offset, area_row_offset)

    def row_absolute(
        self,
        parser_row_offset: typing.Optional[int] = None,
        area_row_offset: typing.Optional[int] = None,
    ):
        return self[self.distance].row_absolute(parser_row_offset, area_row_offset)

    def __contains__(self, item: Coord) -> bool:
        if not isinstance(item, Coord):
            raise TypeError(f"is '{type(item)}' not Coord")

        if self.direction == Direction.LEFT:
            res = item.row == self.coord.row and item.col <= self.coord.col
            if self.max_count is not None:
                res = res and abs(item.col - self.coord.col) < self.max_count
            return res

        elif self.direction == Direction.RIGHT:
            res = item.row == self.coord.row and item.col >= self.coord.col
            if self.max_count is not None:
                res = res and abs(item.col - self.coord.col) < self.max_count
            return res

        elif self.direction == Direction.UP:
            res = item.col == self.coord.col and item.row <= self.coord.row
            if self.max_count is not None:
                res = res and abs(item.row - self.coord.row) < self.max_count
            return res

        elif self.direction == Direction.DOWN:
            res = item.col == self.coord.col and item.row >= self.coord.row
            if self.max_count is not None:
                res = res and abs(item.row - self.coord.row) < self.max_count
            return res

        raise NotImplementedError()

    def __iter__(self):
        self.distance = 0
        return self

    def __next__(self) -> "Coord":
        col = self.coord.col
        row = self.coord.row
        if self.direction == Direction.LEFT:
            if self.coord.col - self.distance < 0:
                raise StopIteration
            col -= self.distance
        elif self.direction == Direction.RIGHT:
            col += self.distance
        elif self.direction == Direction.UP:
            if self.coord.row - self.distance < 0:
                raise StopIteration
            row -= self.distance
        elif self.direction == Direction.DOWN:
            row += self.distance
        self.distance += 1

        if self.max_count is not None:
            if self.max_count < self.distance:
                raise StopIteration

        return Coord(row, col, self.coord.row_relative_to)

    def __getitem__(self, item: int) -> Coord:
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        self.distance = item

        if self.max_count is not None:
            if self.max_count < self.distance:
                raise IndexError(f"{item} is not in range of {self}")

        try:
            return next(self)
        except StopIteration:
            raise IndexError(f"{item} is not in range of {self}")

    def skip(self, count: int) -> "CoordRange":
        return CoordRange(self[count], self.direction)
