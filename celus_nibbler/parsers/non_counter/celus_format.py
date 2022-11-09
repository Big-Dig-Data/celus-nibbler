import logging
import typing

from celus_nigiri import CounterRecord

from celus_nibbler.coordinates import Coord, CoordRange, Direction, Value
from celus_nibbler.data_headers import DataCells
from celus_nibbler.errors import TableException
from celus_nibbler.parsers.base import BaseTabularArea, BaseTabularParser
from celus_nibbler.sources import (
    DateSource,
    DimensionSource,
    MetricSource,
    OrganizationSource,
    TitleIdSource,
    TitleSource,
    ValueSource,
)
from celus_nibbler.utils import end_month, start_month

from .base import BaseNonCounterParser

logger = logging.getLogger(__name__)


class BaseCelusFormatArea(BaseTabularArea):
    title_column_names: typing.List[str] = []
    organization_column_names: typing.List[str] = []
    metric_column_names: typing.List[str] = []
    override_metric: typing.Optional[str] = None  # when metric column is found
    title_ids_mapping: typing.Dict[str, str] = {}
    dimension_mapping: typing.Dict[str, str] = {}

    def find_data_cells(self) -> typing.List[DataCells]:
        # Should raise validation error when column is unidentified
        res = []

        # Convert title_ids_sources and dimensions_sources to instance variables
        # otherwise class variable would be modified here causing
        # it to influence other parsers
        self.title_ids_sources = {}
        self.dimensions_sources = {}

        for cell in CoordRange(Coord(0, 0), Direction.RIGHT):
            data_source = CoordRange(cell, Direction.DOWN).skip(1)
            try:
                # First try to extract date
                try:
                    if date := DateSource(cell).extract(self.sheet, 0):
                        record = CounterRecord(
                            value=0,
                            start=start_month(date),
                            end=end_month(date),
                        )
                        res.append(DataCells(record, ValueSource(source=data_source)))
                        continue
                except TableException as e:
                    if e.reason != "date":
                        raise

                content = cell.content(self.sheet)

                # Try title
                if content in self.title_column_names:
                    self.title_source = TitleSource(source=data_source)
                    continue

                # Try metric
                if content in self.metric_column_names:
                    self.metric_source = MetricSource(source=data_source)
                    continue

                # Try organization
                if content in self.organization_column_names:
                    self.organization_source = OrganizationSource(source=data_source)
                    continue

                # Try dimensions
                if name := self.dimension_mapping.get(content):
                    self.dimensions_sources[name] = DimensionSource(name=name, source=data_source)
                    continue

                # Try title ids
                if name := self.title_ids_mapping.get(content):
                    self.title_ids_sources[name] = TitleIdSource(name=name, source=data_source)
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
                if e.reason == "out-of-bounds":
                    logger.debug("Header parsing terminated: %s", e)
                    break
                else:
                    raise

        if self.override_metric:
            self.metric_source = MetricSource(Value(self.override_metric))

        # Raise exception that no data cell was found
        return res

    def dimensions(self) -> typing.List[str]:
        # populate dimensions_sources
        self.find_data_cells()
        return list(self.dimensions_sources.keys())

    def get_months(self):
        return [e.header_data.start for e in self.find_data_cells()]


class BaseCelusFormatParser(BaseNonCounterParser, BaseTabularParser):
    """Fist row of generic celus format is the header and the rest are data"""

    platforms: typing.List[str] = []
    metrics_to_skip: typing.List[str] = []
    titles_to_skip: typing.List[str] = []
    dimensions_to_skip: typing.Dict[str, typing.List[str]] = {}

    heuristics = None  # Celus format has no heuristcs
