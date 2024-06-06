import itertools
import logging
import re
import typing
from functools import lru_cache

from celus_nibbler.conditions import IsDateCondition
from celus_nibbler.coordinates import Coord, CoordRange, Direction
from celus_nibbler.data_headers import DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.non_counter.date_based import BaseDateArea
from celus_nibbler.sources import (
    AuthorsSource,
    DateSource,
    DimensionSource,
    ExtractParams,
    ItemIdSource,
    ItemSource,
    MetricSource,
    OrganizationSource,
    PublicationDateSource,
    TitleIdKind,
    TitleIdSource,
    TitleSource,
)

logger = logging.getLogger(__name__)


class CounterHeaderArea(BaseDateArea):
    HEADER_DATE_COL_START = 1
    MAX_HEADER_ROW = 50
    TITLE_DOI_NAMES = {
        "Book DOI",
        "DOI",
        "Journal DOI",
    }
    TITLE_ISBN_NAMES = {"ISBN"}
    TITLE_ISSN_NAMES = {
        "ISSN",
        "Print ISSN",
        "Print_ISSN",
        "Printed_ISSN",
        "Printed ISSN",
    }
    TITLE_URI_NAMES = {
        "URI",
    }
    TITLE_EISSN_NAMES = {
        "Online ISSN",
        "Online_ISSN",
    }
    TITLE_PROPRIETARY_NAMES = {
        "Proprietary",
        "Proprietary ID",
        "Proprietary_ID",
        "Proprietary Identifier",
    }
    ITEM_DOI_NAMES: typing.Set[str] = set()
    ITEM_ISBN_NAMES: typing.Set[str] = set()
    ITEM_ISSN_NAMES: typing.Set[str] = set()
    ITEM_URI_NAMES: typing.Set[str] = set()
    ITEM_EISSN_NAMES: typing.Set[str] = set()
    ITEM_PROPRIETARY_NAMES: typing.Set[str] = set()
    ITEM_PUBLICATION_DATE_NAMES: typing.Set[str] = set()
    ITEM_AUTHORS_NAMES: typing.Set[str] = set()
    DIMENSION_NAMES_MAP = [
        ("Publisher", {"Publisher"}),
        ("Platform", {"Platform"}),
    ]

    TITLE_COLUMN_NAMES: typing.List[str] = []
    ITEM_COLUMN_NAMES: typing.List[str] = []
    ORGANIZATION_COLUMN_NAMES: typing.List[str] = []
    METRIC_COLUMN_NAMES: typing.List[str] = []

    ORGANIZATION_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    METRIC_EXTRACT_PARAMS: ExtractParams = ExtractParams(
        # Totals are usually without metrics => skip it instead of failing
        on_validation_error=TableException.Action.SKIP,
    )
    DATE_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    DATA_EXTRACT_PARAMS: ExtractParams = ExtractParams(
        default=0,
    )
    TITLE_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    TITLE_IDS_EXTRACT_PARAMS: typing.Dict[str, ExtractParams] = {}
    ITEM_EXTRACT_PARAMS: ExtractParams = ExtractParams()
    ITEM_IDS_EXTRACT_PARAMS: typing.Dict[str, ExtractParams] = {}
    DIMENSIONS_EXTRACT_PARAMS: typing.Dict[str, ExtractParams] = {}

    @property
    @lru_cache
    def dimensions(self) -> typing.List[str]:
        return [e[0] for e in self.DIMENSION_NAMES_MAP]

    def date_check(self, coord):
        if re.match(r"^\d+$", coord.content(self.sheet)):
            return False
        return IsDateCondition(coord=coord).check(self.sheet)

    @property
    @lru_cache
    def header_row(self) -> CoordRange:
        """Find the line where counter header is"""
        # Right now it checks whether a single continuous date area is present

        for idx in range(self.MAX_HEADER_ROW):
            crange = CoordRange(Coord(idx, self.HEADER_DATE_COL_START), Direction.RIGHT)

            matching = None
            twice = False
            last_content = None
            for cell in crange:
                try:
                    last_content = cell.content(self.sheet).strip() or last_content
                    if self.date_check(cell):
                        if matching is False:
                            twice = True
                        matching = True
                    else:
                        if matching is not None:
                            matching = False
                except TableException as e:
                    if e.action == TableException.Action.STOP:
                        break  # last cell reached
                    raise

            if matching is not None and not twice:
                return CoordRange(Coord(idx, 0), Direction.RIGHT)

            if not matching and last_content == "Reporting_Period_Total":
                # When no months are found with the row which ends with
                # "Reporting_Period_Total" it means that
                # Exclude_Monthly_Details=True was used during report
                # creation -> we should raise a special exception in that case
                raise TableException(
                    row=idx,
                    sheet=self.sheet.sheet_idx,
                    reason="counter-header-without-monthly-details",
                )

        raise TableException(
            sheet=self.sheet.sheet_idx,
            reason="no-counter-header-found",
        )

    @property
    @lru_cache
    def data_headers(self):
        # First date which is parsed in the header
        for cell in itertools.islice(self.header_row, 1, None):
            if not self.date_check(coord=cell):
                continue

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

    @property
    @lru_cache
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
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            if content.strip() in self.TITLE_COLUMN_NAMES:
                return TitleSource(
                    CoordRange(cell, Direction.DOWN).skip(1),
                    extract_params=self.TITLE_EXTRACT_PARAMS,
                )

        # title column was not found, assuming that first column is the title
        return TitleSource(
            CoordRange(self.header_row[0], Direction.DOWN).skip(1),
            extract_params=self.TITLE_EXTRACT_PARAMS,
        )

    @property
    @lru_cache
    def title_ids_sources(self) -> typing.Dict[str, TitleIdSource]:
        ids_sources = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            # Validate name
            content = content.strip()

            if content in self.TITLE_DOI_NAMES:
                name = TitleIdKind.DOI
            elif content in self.TITLE_ISBN_NAMES:
                name = TitleIdKind.ISBN
            elif content.strip() in self.TITLE_ISSN_NAMES:
                name = TitleIdKind.Print_ISSN
            elif content.strip() in self.TITLE_EISSN_NAMES:
                name = TitleIdKind.Online_ISSN
            elif content.strip() in self.TITLE_PROPRIETARY_NAMES:
                name = TitleIdKind.Proprietary
            elif content.strip() in self.TITLE_URI_NAMES:
                name = TitleIdKind.URI
            else:
                # empty or other field
                continue

            source = TitleIdSource(
                name,
                CoordRange(cell, Direction.DOWN).skip(1),
                extract_params=self.TITLE_IDS_EXTRACT_PARAMS.get(name, ExtractParams()),
            )
            ids_sources[str(name)] = source

        return ids_sources

    @property
    @lru_cache
    def item_source(self) -> typing.Optional[ItemSource]:
        if not self.ITEM_COLUMN_NAMES:
            # Need to have item column defined
            return None

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            if content.strip() in self.ITEM_COLUMN_NAMES:
                return ItemSource(
                    CoordRange(cell, Direction.DOWN).skip(1),
                    extract_params=self.ITEM_EXTRACT_PARAMS,
                )

        # title column was not found, assuming that first column is the title
        return ItemSource(
            CoordRange(self.header_row[0], Direction.DOWN).skip(1),
            extract_params=self.ITEM_EXTRACT_PARAMS,
        )

    @property
    @lru_cache
    def item_ids_sources(self) -> typing.Dict[str, ItemIdSource]:
        if not self.ITEM_COLUMN_NAMES:
            # Need to have item column defined
            return {}

        ids_sources = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            # Validate name
            content = content.strip()

            if content in self.ITEM_DOI_NAMES:
                name = TitleIdKind.DOI
            elif content in self.ITEM_ISBN_NAMES:
                name = TitleIdKind.ISBN
            elif content.strip() in self.ITEM_ISSN_NAMES:
                name = TitleIdKind.Print_ISSN
            elif content.strip() in self.ITEM_EISSN_NAMES:
                name = TitleIdKind.Online_ISSN
            elif content.strip() in self.ITEM_PROPRIETARY_NAMES:
                name = TitleIdKind.Proprietary
            elif content.strip() in self.ITEM_URI_NAMES:
                name = TitleIdKind.URI
            else:
                # empty or other field
                continue

            source = ItemIdSource(
                name,
                CoordRange(cell, Direction.DOWN).skip(1),
                extract_params=self.ITEM_IDS_EXTRACT_PARAMS.get(name, ExtractParams()),
            )
            ids_sources[str(name)] = source

        return ids_sources

    @property
    @lru_cache
    def item_authors_source(self) -> typing.Optional[AuthorsSource]:
        if not self.ITEM_AUTHORS_NAMES:
            return None

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            if content.strip() in self.ITEM_AUTHORS_NAMES:
                return AuthorsSource(CoordRange(cell, Direction.DOWN).skip(1))

    @property
    @lru_cache
    def item_publication_date_source(self) -> typing.Optional[PublicationDateSource]:
        if not self.ITEM_PUBLICATION_DATE_NAMES:
            return None

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            if content.strip() in self.ITEM_PUBLICATION_DATE_NAMES:
                return PublicationDateSource(CoordRange(cell, Direction.DOWN).skip(1))

    @property
    @lru_cache
    def dimensions_sources(self) -> typing.Dict[str, DimensionSource]:
        dim_sources = {}
        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            content = content.strip()
            for dimension, names in self.DIMENSION_NAMES_MAP:
                if content.lower() in [e.lower() for e in names]:
                    dim_sources[dimension] = DimensionSource(
                        content,
                        CoordRange(Coord(cell.row + 1, cell.col), Direction.DOWN),
                        extract_params=self.DIMENSIONS_EXTRACT_PARAMS.get(
                            dimension, ExtractParams()
                        ),
                    )

        return dim_sources

    @property
    @lru_cache
    def organization_source(self) -> typing.Optional[OrganizationSource]:
        if not self.ORGANIZATION_COLUMN_NAMES:
            # Organization column not defined
            return None

        for cell in self.header_row:
            try:
                content = cell.content(self.sheet)
            except TableException as e:
                if e.action == TableException.Action.STOP:
                    break  # last cell reached

            if content.strip() in self.ORGANIZATION_COLUMN_NAMES:
                return OrganizationSource(
                    CoordRange(cell, Direction.DOWN).skip(1),
                    extract_params=self.ORGANIZATION_EXTRACT_PARAMS,
                )

        return None

    @property
    @lru_cache
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
                if e.action == TableException.Action.STOP:
                    raise TableException(
                        value=self.METRIC_COLUMN_NAMES,
                        row=cell.row,
                        sheet=self.sheet.sheet_idx,
                        reason="missing-metric-in-header",
                    )

        return None
