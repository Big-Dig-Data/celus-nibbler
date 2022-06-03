import typing
from enum import Enum

from pydantic.dataclasses import dataclass

from .errors import TableException
from .reader import SheetReader
from .utils import JsonEncorder, PydanticConfig


class Direction(str, Enum):

    LEFT = 'left'
    RIGHT = 'right'
    UP = 'up'
    DOWN = 'down'


@dataclass(config=PydanticConfig)
class Value(JsonEncorder):
    value: typing.Any

    def content(self, sheet: SheetReader):
        return self.value

    def __next__(self) -> 'Value':
        return Value(self.value)

    def __getitem__(self, item: int) -> 'Value':
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        return next(self)


@dataclass(config=PydanticConfig)
class SheetAttr(JsonEncorder):
    sheet_attr: str

    def content(self, sheet: SheetReader):
        return getattr(sheet, self.sheet_attr)

    def __iter__(self):
        return self

    def __next__(self) -> 'SheetAttr':
        return SheetAttr(self.sheet_attr)

    def __getitem__(self, item: int) -> 'SheetAttr':
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        return next(self)


@dataclass(config=PydanticConfig)
class Coord(JsonEncorder):
    row: int
    col: int

    def content(self, sheet: SheetReader):
        try:
            return sheet[self.row][self.col]
        except IndexError as e:
            raise TableException(
                row=self.row, col=self.col, sheet=sheet.sheet_idx, reason="out-of-bounds"
            ) from e

    def __iter__(self):
        return self

    def __next__(self) -> 'Coord':
        return Coord(self.row, self.col)

    def __getitem__(self, item: int) -> 'Coord':
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        return next(self)


@dataclass(config=PydanticConfig)
class CoordRange(JsonEncorder):
    coord: Coord
    direction: Direction

    def __contains__(self, item: Coord) -> bool:
        if not isinstance(item, Coord):
            raise TypeError(f"is '{type(item)}' not Coord")

        if self.direction == Direction.LEFT:
            return item.row == self.coord.row and item.col <= self.coord.col
        elif self.direction == Direction.RIGHT:
            return item.row == self.coord.row and item.col >= self.coord.col
        elif self.direction == Direction.UP:
            return item.col == self.coord.col and item.row <= self.coord.row
        elif self.direction == Direction.DOWN:
            return item.col == self.coord.col and item.row >= self.coord.row

        raise NotImplementedError()

    def __iter__(self):
        self.distance = 0
        return self

    def __next__(self) -> 'Coord':
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

        return Coord(row, col)

    def __getitem__(self, item: int) -> Coord:
        if isinstance(item, slice):
            raise NotImplementedError("Slicing is not supported use itertools and generators")
        if not isinstance(item, int):
            raise ValueError("Only int are allowed as keys")

        self.distance = item
        try:
            return next(self)
        except StopIteration:
            raise IndexError(f"{item} is not in range of {self}")
