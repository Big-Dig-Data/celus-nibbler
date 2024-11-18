import logging
from datetime import date
from typing import Generator, List, Set

from celus_nigiri import CounterRecord
from celus_nigiri.counter51 import (
    Counter51DRReport,
    Counter51IRReport,
    Counter51PRReport,
    Counter51ReportBase,
    Counter51TRReport,
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
    nigiri_report_class = Counter51ReportBase

    def _convert_months_str_to_months(self, months_str: Set[str]) -> Set[date]:
        months = set()
        for month_str in months_str:
            try:
                months.add(validators.Date(value=month_str).value)
            except ValidationError:
                logger.warn("Wrong date in Report_Item: '%s'", month_str)

        return months

    def get_months(self) -> List[date]:
        months_str: Set[str] = set()
        for item_dict in self.sheet:
            for ap in item_dict.get("Attribute_Performance", []):
                for dates in ap.get("Performance", {}).values():
                    months_str.update(dates.keys())

        return sorted(list(self._convert_months_str_to_months(months_str)))

    @property
    def dimensions(self) -> List[str]:
        return self.nigiri_report_class.dimensions


class NigiriDRArea(NigiriBaseArea):
    nigiri_report_class = Counter51DRReport


class NigiriPRArea(NigiriBaseArea):
    nigiri_report_class = Counter51PRReport


class NigiriTRArea(NigiriBaseArea):
    nigiri_report_class = Counter51TRReport


class NigiriIRArea(NigiriBaseArea):
    nigiri_report_class = Counter51IRReport

    def get_months(self) -> List[date]:
        months_str: Set[str] = set()
        for items_dict in self.sheet:
            for item in items_dict.get("Items", []):
                for ap in item.get("Attribute_Performance", []):
                    for dates in ap.get("Performance", {}).values():
                        months_str.update(dates.keys())

        return sorted(list(self._convert_months_str_to_months(months_str)))


class BaseCounter51JsonParser(c5tabular.Counter5ParserAnalyzeMixin, BaseJsonParser):
    @property
    def name(self):
        return f"counter51.{self.data_format.name}"

    def _parse_area(self, area: BaseArea) -> Generator[CounterRecord, None, None]:
        if isinstance(area, NigiriBaseArea):
            report = area.nigiri_report_class()
            if isinstance(self.sheet, JsonCounter5SheetReader):
                return report.read_report(self.sheet.extra, self.sheet)
            raise TypeError(f"Only JsonCounter5SheetReader is allowed to be used in {type(self)}")
        raise TypeError(f"Only NigiriArea is allowed to be used in {type(self)}")

    def get_extras(self):
        return self.sheet.extra or {}


class DR(BaseCounter51JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="DR") & SheetExtraCondition(
        field_name="Release", value="5.1"
    )
    platforms = c5tabular.DR.platforms
    data_format = DataFormatDefinition(name="DR51")

    areas = [NigiriDRArea]


class PR(BaseCounter51JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="PR") & SheetExtraCondition(
        field_name="Release", value="5.1"
    )
    platforms = c5tabular.PR.platforms
    data_format = DataFormatDefinition(name="PR51")

    areas = [NigiriPRArea]


class TR(BaseCounter51JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="TR") & SheetExtraCondition(
        field_name="Release", value="5.1"
    )
    platforms = c5tabular.TR.platforms
    data_format = DataFormatDefinition(name="TR51")

    areas = [NigiriTRArea]


class IR(BaseCounter51JsonParser):
    heuristics = SheetExtraCondition(field_name="Report_ID", value="IR") & SheetExtraCondition(
        field_name="Release", value="5.1"
    )
    platforms = c5tabular.IR.platforms
    data_format = DataFormatDefinition(name="IR51")

    areas = [NigiriIRArea]
