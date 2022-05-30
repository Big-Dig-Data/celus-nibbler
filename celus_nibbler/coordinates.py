from enum import Enum
from json import dumps as dumps_json

from pydantic.dataclasses import dataclass
from pydantic.json import pydantic_encoder

from .errors import TableException
from .reader import SheetReader


class Direction(Enum):

    LEFT = 'left'
    RIGHT = 'right'
    UP = 'up'
    DOWN = 'down'


class JsonEncorder:
    def json(self):
        return dumps_json(self, default=pydantic_encoder)


@dataclass
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


class CoordIter:
    def __init__(self, coord: Coord, direction: Direction):
        self.coord = Coord(coord.row, coord.col)
        self.direction = direction
        self.distance = 0

    def __iter__(self):
        self.distance = 0
        return self

    def __next__(self):
        col = self.coord.col
        row = self.coord.row
        if self.direction == Direction.LEFT:
            if self.coord.col - self.distance <= 0:
                raise StopIteration
            col -= 1
        elif self.direction == Direction.RIGHT:
            col += 1
        elif self.direction == Direction.UP:
            if self.coord.row - self.distance <= 0:
                raise StopIteration
            row -= 1
        elif self.direction == Direction.DOWN:
            row += 1
        self.distance += 1
        return Coord(row, col)


@dataclass
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

    def __next__(self):
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
