import itertools
import logging
import typing

from pydantic import ValidationError

from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.non_counter.date_based import BaseDateArea
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    ExtractParams,
    MetricSource,
    OrganizationSource,
    TitleIdSource,
    TitleSource,
)

logger = logging.getLogger(__name__)


class CounterHeaderArea(BaseDateArea):
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

    TITLE_COLUMN_NAMES: typing.List[str] = []
    ORGANIZATION_COLUMN_NAMES: typing.List[str] = []
    METRIC_COLUMN_NAMES: typing.List[str] = []

    ORGANIZATION_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    METRIC_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    DATE_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    DATA_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    TITLE_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    TITLE_IDS_EXTRACT_PARAMS: typing.Dict[str, ExtractParams] = {}
    DIMENSIONS_EXTRACT_PARAMS: typing.Dict[str, ExtractParams] = {}

    @property
    def dimensions(self) -> typing.List[str]:
        return [e[0] for e in self.DIMENSION_NAMES_MAP]

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
    def data_headers(self):
        # First date which is parsed in the header
        for cell in itertools.islice(self.header_row, 1, None):
            try:
                # Throws exception when it is not a date
                self.parse_date(cell)
                first_data_cell = CoordRange(cell, Direction.DOWN)[1]
                return DataHeaders(
                    roles=[
                        DateSource(
                            source=CoordRange(cell, Direction.RIGHT),
                            extract_params=self.DATE_EXTRACT_PARAMS,
                        ),
                    ],
                    data_cells=CoordRange(first_data_cell, Direction.RIGHT),
                    data_direction=Direction.DOWN,
                    data_extract_params=self.DATA_EXTRACT_PARAMS,
                )
            except ValidationError:
                continue  # doesn't match the header

    @property
    def title_source(self) -> typing.Optional[TitleSource]:
        if not self.TITLE_COLUMN_NAMES:
            # Name of title column not defined -> assumed the title is in first column
            return TitleSource(
                CoordRange(self.header_row[0], Direction.DOWN).skip(1),
                extract_params=self.TITLE_EXTRACT_PARAMS,
            )

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached

            if content.strip() in self.TITLE_COLUMN_NAMES:
                return TitleSource(
                    CoordRange(cell, Direction.DOWN).skip(1),
                    extract_params=self.TITLE_EXTRACT_PARAMS,
                )

    @property
    def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
        ids_sources = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached

            # Validate name
            content = content.strip()

            if content in self.DOI_NAMES:
                name = "DOI"
            elif content in self.ISBN_NAMES:
                name = "ISBN"
            elif content.strip() in self.ISSN_NAMES:
                name = "Print_ISSN"
            elif content.strip() in self.EISSN_NAMES:
                name = "Online_ISSN"
            elif content.strip() in self.PROPRIETARY_NAMES:
                name = "Proprietary"
            else:
                # empty or other field
                continue

            source = TitleIdSource(
                name,
                CoordRange(cell, Direction.DOWN).skip(1),
                extract_params=self.TITLE_IDS_EXTRACT_PARAMS.get(name, ExtractParams()),
            )
            ids_sources[name] = source

        return ids_sources

    @property
    def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
        dim_sources = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached

            content = content.strip()
            for dimension, names in self.DIMENSION_NAMES_MAP:
                if content in names:
                    dim_sources[dimension] = DimensionSource(
                        content,
                        CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN),
                        extract_params=self.DIMENSIONS_EXTRACT_PARAMS.get(
                            dimension, ExtractParams()
                        ),
                    )

        return dim_sources

    @property
    def organization_source(self) -> typing.Optional[OrganizationSource]:
        if not self.ORGANIZATION_COLUMN_NAMES:
            # Organization column not defined
            return None

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    break  # last cell reached

            if content.strip() in self.ORGANIZATION_COLUMN_NAMES:
                return OrganizationSource(
                    CoordRange(cell, Direction.DOWN).skip(1),
                    extract_params=self.ORGANIZATION_EXTRACT_PARAMS,
                )

        return None

    @property
    def metric_source(self) -> typing.Optional[MetricSource]:
        if not self.METRIC_COLUMN_NAMES:
            return None

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
                if content and content.strip().lower() in [
                    e.lower() for e in self.METRIC_COLUMN_NAMES
                ]:

                    return MetricSource(
                        CoordRange(cell, Direction.DOWN).skip(1),
                        extract_params=self.METRIC_EXTRACT_PARAMS,
                    )
            except TableException as e:
                if e.reason in ["out-of-bounds"]:
                    raise TableException(
                        value=self.METRIC_COLUMN_NAMES,
                        row=cell.row,
                        sheet=self.sheet.sheet_idx,
                        reason="missing-metric-in-header",
                    )

        return None
