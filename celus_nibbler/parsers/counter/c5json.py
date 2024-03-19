import logging
from datetime import date
from typing import Generator, List

from celus_nigiri import CounterRecord
from celus_nigiri.counter5 import (
    Counter5DRReport,
    Counter5IRM1Report,
    Counter5IRReport,
    Counter5PRReport,
    Counter5ReportBase,
    Counter5TRReport,
)
from pydantic import ValidationError

from celus_nibbler import validators
from celus_nibbler.conditions import SheetExtraCondition
from celus_nibbler.data_headers import DataFormatDefinition
from celus_nibbler.parsers.base import BaseArea, BaseJsonArea, BaseJsonParser
from celus_nibbler.reader import JsonCounter5SheetReader

from . import c5 as c5tabular

logger = logging.getLogger(__name__)


class NigiriBaseArea(BaseJsonArea):
    nigiri_report_class = Counter5ReportBase

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


class NigiriDRArea(NigiriBaseArea):
    nigiri_report_class = Counter5DRReport


class NigiriPRArea(NigiriBaseArea):
    nigiri_report_class = Counter5PRReport


class NigiriTRArea(NigiriBaseArea):
    nigiri_report_class = Counter5TRReport


class NigiriIR_M1Area(NigiriBaseArea):
    nigiri_report_class = Counter5IRM1Report


class NigiriIRArea(NigiriBaseArea):
    nigiri_report_class = Counter5IRReport


class BaseCounter5JsonParser(c5tabular.Counter5ParserAnalyzeMixin, BaseJsonParser):
    @property
    def name(self):
        return f"counter5.{self.data_format.name}"

    def _parse_area(self, area: BaseArea) -> Generator[CounterRecord, None, None]:
        if isinstance(area, NigiriBaseArea):
            report = area.nigiri_report_class()
            if isinstance(self.sheet, JsonCounter5SheetReader):
                return report.read_report(self.sheet.extra, self.sheet)
            raise TypeError(f"Only JsonCounter5SheetReader is allowed to be used in {type(self)}")
        raise TypeError(f"Only NigiriArea is allowed to be used in {type(self)}")

    def get_extras(self):
        return self.sheet.extra or {}


class DR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="DR")
    platforms = c5tabular.DR.platforms
    data_format = DataFormatDefinition(name="DR")

    areas = [NigiriDRArea]


class PR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="PR")
    platforms = c5tabular.PR.platforms
    data_format = DataFormatDefinition(name="PR")

    areas = [NigiriPRArea]


class TR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="TR")
    platforms = c5tabular.TR.platforms
    data_format = DataFormatDefinition(name="TR")

    areas = [NigiriTRArea]


class IR_M1(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="IR_M1")
    platforms = c5tabular.IR_M1.platforms
    data_format = DataFormatDefinition(name="IR_M1")

    areas = [NigiriIR_M1Area]


class IR(BaseCounter5JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="IR")
    platforms = c5tabular.IR.platforms
    data_format = DataFormatDefinition(name="IR")

    areas = [NigiriIRArea]
