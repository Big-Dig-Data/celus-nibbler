import datetime
import logging
import typing
from abc import ABCMeta

from pydantic import ValidationError

from celus_nibbler.coordinates import CoordRange, Direction
from celus_nibbler.errors import TableException

from ..base import BaseArea, MonthDataCells

logger = logging.getLogger(__name__)


class BaseDateArea(BaseArea, metaclass=ABCMeta):
    def get_months(self) -> typing.List[datetime.date]:
        return [e.values[0] for e in self.find_data_cells()]


class VerticalDateArea(BaseDateArea):
    def find_data_cells(self) -> typing.List[MonthDataCells]:
        res = []
        for cell in self.header_cells:
            try:
                date = self.parse_date(cell)
                res.append(
                    MonthDataCells(
                        date.replace(day=1),
                        CoordRange(cell, Direction.DOWN).skip(1),
                    )
                )

            except TableException as e:
                if e.reason == "out-of-bounds":
                    # We reached the end of row
                    break
                raise
            except ValidationError:
                # Found a content which can't be parse e.g. "Total"
                # We can exit here
                break

        logger.debug(f"Found data cells: {res}")
        return res
