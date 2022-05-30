import itertools

from pydantic import ValidationError

from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import VerticalArea


class CounterHeaderArea(VerticalArea):
    HEADER_DATE_START = 1
    MAX_HEADER_ROW = 50
    DOI_NAMES = {
        "Book DOI",
        "DOI",
        "Journal DOI",
    }
    ISBN_NAMES = {"ISBN"}
    ISSN_NAMES = {
        "ISSN",
        "Print ISSN",
        "Print_ISSN",
    }
    EISSN_NAMES = {
        "Online ISSN",
        "Online_ISSN",
    }
    PROPRIETARY_NAMES = {
        "Proprietary",
        "Proprietary ID",
        "Proprietary_ID",
        "Proprietary Identifier",
    }
    DIMENSION_NAMES_MAP = [
        ("Publisher", {"Publisher"}),
        ("Platform", {"Platform", "platform"}),
    ]

    @property
    def header_row(self) -> CoordRange:
        """Find the line where counter header is"""
        # Right now it picks a first row with more than one column were the last column
        # is a date

        # Try to use cached
        if hasattr(self, '_header_row'):
            return self._header_row

        for idx in range(self.MAX_HEADER_ROW):
            crange = CoordRange(Coord(idx, self.HEADER_DATE_START), Direction.RIGHT)

            last = None
            for cell in crange:
                try:
                    content = cell.content(self.sheet)
                    if content and content.strip():
                        last = cell
                except TableException as e:
                    if e.reason in ["out-of-bounds"]:
                        break  # last cell reached
                    raise

            if last:
                try:
                    # Throws exception when it is not a date
                    self.parse_date(last)
                    self._header_row = CoordRange(Coord(idx, 0), Direction.RIGHT)
                    return self._header_row
                except ValidationError:
                    continue  # doesn't match the header

        raise TableException(
            sheet=self.sheet.sheet_idx,
            reason="no-counter-header-found",
        )

    @property
    def date_header_cells(self):
        # First date which is parsed in the header
        for cell in itertools.islice(self.header_row, 1, None):
            try:
                # Throws exception when it is not a date
                self.parse_date(cell)
                return CoordRange(cell, Direction.RIGHT)
            except ValidationError:
                continue  # doesn't match the header

    @property
    def title_cells(self):
        return CoordRange(Coord(self.header_row[0].row + 1, 0), Direction.DOWN)

    @property
    def title_ids_cells(self):
        ids_cells = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached

            if content.strip() in self.DOI_NAMES:
                ids_cells["DOI"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
            elif content.strip() in self.ISBN_NAMES:
                ids_cells["ISBN"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
            elif content.strip() in self.ISSN_NAMES:
                ids_cells["Online_ISSN"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
            elif content.strip() in self.EISSN_NAMES:
                ids_cells["Print_ISSN"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)
            elif content.strip() in self.PROPRIETARY_NAMES:
                ids_cells["Proprietary"] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)

        return ids_cells

    @property
    def dimensions_cells(self):
        dim_cells = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached
            for dimension, names in self.DIMENSION_NAMES_MAP:
                if content.strip() in names:
                    dim_cells[dimension] = CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN)

        return dim_cells