from dataclasses import dataclass

from .errors import TableException
from .reader import SheetReader


@dataclass
class Coord:
    row: int
    col: int

    def content(self, sheet: SheetReader):
        try:
            return sheet[self.row][self.col]
        except IndexError as e:
            raise TableException(
                row=self.row, col=self.col, sheet=sheet.sheet_idx, reason="out-of-bounds"
            ) from e
