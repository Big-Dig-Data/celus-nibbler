import logging
from datetime import date
from typing import Generator, List, Type, Union

from celus_nigiri import CounterRecord
from celus_nigiri.counter5 import (
    Counter5DRReport,
    Counter5IRM1Report,
    Counter5IRReport,
    Counter5PRReport,
    Counter5TRReport,
)
from pydantic import ValidationError

from celus_nibbler import validators
from celus_nibbler.conditions import SheetExtraCondition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.errors import JsonException
from celus_nibbler.parsers.base import BaseArea, BaseJsonArea, BaseJsonParser
from celus_nibbler.reader import JsonCounter5SheetReader

from . import c5 as c5tabular

logger = logging.getLogger(__name__)


class NigiriArea(BaseJsonArea):
    REPORT_ID_TO_NIGIRI_REPORT = {
        "DR": Counter5DRReport,
        "PR": Counter5PRReport,
        "TR": Counter5TRReport,
        "IR": Counter5IRReport,
        "IR_M1": Counter5IRM1Report,
    }

    @property
    def nigiri_report_class(
        self,
    ) -> Union[
        Type[Counter5TRReport],
        Type[Counter5DRReport],
        Type[Counter5PRReport],
        Type[Counter5IRReport],
        Type[Counter5IRM1Report],
    ]:
        if not self.sheet.extra:
            # This should never happen
            raise RuntimeError("SheetReader doesn't support CounterJsons")

        if code := self.sheet.extra.get("Report_ID"):
            if report := self.REPORT_ID_TO_NIGIRI_REPORT.get(code):
                return report
            else:
                raise JsonException(f"Unsupported 'Report_ID': {code}")
        else:
            raise JsonException("Missing 'Report_ID' in 'Report_Header'")

    def get_months(self) -> List[date]:
        months_str = set()
        for item_dict in self.sheet:
            for permformance_item in item_dict.get("Performance", []):
                if period := permformance_item.get("Period"):
                    if begin_date := period.get("Begin_Date"):
                        months_str.add(begin_date)

        months = set()
        for month_str in months_str:
            try:
                months.add(validators.Date(value=month_str).value)
            except ValidationError:
                logger.warn("Wrong date in Report_Item: '%s'", month_str)

        return sorted(list(months))

    @property
    def dimensions(self) -> List[str]:
        return self.nigiri_report_class.dimensions


class BaseCounter5JsonParser(BaseJsonParser):
    areas = [NigiriArea]

    @property
    def name(self):
        return f"counter5.{self.data_format.name}"

    def _parse_area(self, area: BaseArea) -> Generator[CounterRecord, None, None]:
        if isinstance(area, NigiriArea):
            report = area.nigiri_report_class()
            if isinstance(self.sheet, JsonCounter5SheetReader):
                return report.read_report(self.sheet.extra, self.sheet)
            raise TypeError(f"Only JsonCounter5SheetReader is allowed to be used in {type(self)}")
        raise TypeError(f"Only NigiriArea is allowed to be used in {type(self)}")


class DR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="DR")
    platforms = c5tabular.DR.platforms
    data_format = DataFormatDefinition(name="DR")


class PR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="PR")
    platforms = c5tabular.PR.platforms
    data_format = DataFormatDefinition(name="PR")


class TR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="TR")
    platforms = c5tabular.TR.platforms
    data_format = DataFormatDefinition(name="TR")


class IR_M1(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="IR_M1")
    platforms = c5tabular.IR_M1.platforms
    data_format = DataFormatDefinition(name="IR_M1")
