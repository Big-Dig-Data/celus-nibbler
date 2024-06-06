import copy
import datetime
import logging
import typing

from celus_nibbler.coordinates import Coord, CoordRange, Direction, Value
from celus_nibbler.data_headers import DataHeaders
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseHeaderArea, BaseTabularParser
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
    TitleIdSource,
    TitleSource,
)

from .base import BaseNonCounterParser

logger = logging.getLogger(__name__)


class BaseCelusFormatArea(BaseHeaderArea):
    title_column_names: typing.List[str] = []
    item_column_names: typing.List[str] = []
    organization_column_names: typing.List[str] = []
    metric_column_names: typing.List[str] = []
    default_metric: typing.Optional[str] = None
    title_ids_mapping: typing.Dict[str, str] = {}
    item_ids_mapping: typing.Dict[str, str] = {}
    item_publication_date_column_names: typing.List[str] = []
    item_authors_column_names: typing.List[str] = []
    dimension_mapping: typing.Dict[str, str] = {}
    value_extract_params: ExtractParams = ExtractParams()

    @property
    def data_headers(self) -> DataHeaders:
        # Should raise validation error when column is unidentified

        # Convert title_ids_sources and dimensions_sources to instance variables
        # otherwise class variable would be modified here causing
        # it to influence other parsers
        self.title_ids_sources = {}
        self.dimensions_sources = {}
        self.item_ids_sources = {}
        self.item_publication_date_source = None
        self.item_authors_source = None

        date_source = None
        for cell in CoordRange(Coord(0, 0), Direction.RIGHT):
            data_source = CoordRange(cell, Direction.DOWN).skip(1)
            try:
                # First try to extract date
                try:
                    test_date_source = DateSource(CoordRange(cell, Direction.RIGHT))
                    if test_date_source.extract(self.sheet, 0):
                        # Set date source based on the first found date
                        if not date_source:
                            date_source = test_date_source
                        continue
                except TableException as e:
                    if e.reason != "date":
                        raise

                content = cell.content(self.sheet)

                # Try title
                if content in self.title_column_names:
                    self.title_source = TitleSource(source=data_source)
                    continue

                # Try item
                if content in self.item_column_names:
                    self.item_source = ItemSource(source=data_source)
                    continue

                # Try metric
                if content in self.metric_column_names:
                    self.metric_source = MetricSource(source=data_source)
                    continue

                # Try organization
                if content in self.organization_column_names:
                    self.organization_source = OrganizationSource(source=data_source)
                    continue

                # Try item authors
                if content in self.item_authors_column_names:
                    self.item_authors_source = AuthorsSource(source=data_source)
                    continue

                # Try item publication date
                if content in self.item_publication_date_column_names:
                    self.item_publication_date_source = PublicationDateSource(source=data_source)
                    continue

                # Try dimensions
                if name := self.dimension_mapping.get(content):
                    self.dimensions_sources[name] = DimensionSource(name=name, source=data_source)
                    continue

                # Try title ids
                if name := self.title_ids_mapping.get(content):
                    self.title_ids_sources[name] = TitleIdSource(name=name, source=data_source)
                    continue

                # Try title ids
                if name := self.item_ids_mapping.get(content):
                    self.item_ids_sources[name] = ItemIdSource(name=name, source=data_source)
                    continue

                # Unknown column
                raise TableException(
                    value=content,
                    row=cell.row,
                    col=cell.col,
                    sheet=self.sheet.sheet_idx,
                    reason="unknown-column",
                )
            except TableException as e:
                # Stop processing when an exception occurs
                # (index out of bounds or unable to parse next field)
                if e.action == TableException.Action.STOP:
                    logger.debug("Header parsing terminated: %s", e)
                    break
                else:
                    raise

        if self.default_metric:
            if self.metric_source:
                # Need to clone here to copy&modify class variable
                # to instance variable
                cloned = copy.deepcopy(self.metric_source)
                cloned.extract_params.default = self.default_metric
                self.metric_source = cloned
            else:
                self.metric_source = MetricSource(Value(self.default_metric))

        # Raise exception when first row doesn't contain any date
        if not date_source:
            raise TableException(
                value=None,
                row=0,
                col=None,
                sheet=self.sheet.sheet_idx,
                reason="no-date-in-header",
            )

        # data cells are right under date_source
        data_cells: CoordRange = copy.deepcopy(date_source.source)
        data_cells.coord.row += 1

        return DataHeaders(
            roles=[date_source],
            data_cells=data_cells,
            data_direction=Direction.DOWN,
            data_extract_params=self.value_extract_params,
        )

    def dimensions(self) -> typing.List[str]:
        # populate dimensions_sources
        self.find_data_cells(self.sheet, self.current_row_offset, lambda x, y: y, lambda x, y: None)
        return list(self.dimensions_sources.keys())

    def get_months(self, row_offset: typing.Optional[int]) -> typing.List[datetime.date]:
        return self._get_months_from_header(row_offset)


class BaseCelusFormatParser(BaseNonCounterParser, BaseTabularParser):
    """Fist row of generic celus format is the header and the rest are data"""

    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None  # Celus format has no heuristcs
