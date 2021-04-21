import csv
import pathlib
import re
import typing
from datetime import date
from time import strptime

from celus_nibbler import CounterRecord
from celus_nibbler.errors import WrongFormatError
from celus_nibbler.utils import end_month, start_month

from .base import BaseReport


class CsvDefaultReport(BaseReport):
    name = "csv-default"
    human_name = "CSV Default"

    platforms = []  # TODO check platforms
    metrics = []  # TODO check metrics

    def name_to_index(self, line: typing.Tuple[str, ...]) -> dict:
        result = {"title": None, "title_ids": None, "metric": None, "months": {}, "dimensions": {}}

        for (idx, item) in enumerate(line):
            lower = item.lower()
            if lower == "title":
                result["title"] = idx
                self.title_idx = idx
            elif lower == "metric":
                result["metric"] = idx
            elif re.match(
                r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-[12][09][0-9][0-9]$",
                lower,
                re.IGNORECASE,
            ):
                result["months"][item] = idx
            # TODO don't know how to process title ids
            # perhaps if there is some ISBN column ?
            else:
                result["dimensions"][item] = idx

        return result

    def process_line(
        self, line: typing.Tuple[str, ...], name2idx: dict
    ) -> typing.List[CounterRecord]:
        if not name2idx:
            raise WrongFormatError()

        title = line[name2idx["title"]] if name2idx["title"] is not None else None
        metric = line[name2idx["metric"]] if name2idx["metric"] is not None else None
        dimension_data = {}
        for key, value in name2idx["dimensions"].items():
            dimension_data[key] = line[name2idx["dimensions"][key]]

        # TODO process isbn, ...
        title_ids = {}

        res = []
        for date_str, idx in name2idx["months"].items():
            month_str, year_str = date_str.split("-")
            year = int(year_str)
            month: int = strptime(month_str, '%b').tm_mon
            value = int(line[idx])
            res.append(
                CounterRecord(
                    platform_name=self.platform_name,
                    title=title,
                    metric=metric,
                    start=start_month(date(year, month, 1)),
                    end=end_month(date(year, month, 1)),
                    dimension_data=dimension_data,
                    title_ids=title_ids,
                    value=value,
                )
            )
            # convert to date

        return res

    def output(self) -> typing.Generator[CounterRecord, None, None]:
        with self.path.open() as f:
            name2idx = None
            reader = csv.reader(f)
            for line in reader:
                if not name2idx:
                    name2idx = self.name_to_index(line)
                    continue
                for record in self.process_line(line, name2idx):
                    yield record

    def __init__(self, csv_path: str, platform_name: str):
        self.header = None
        self.path = pathlib.Path(csv_path)
        self.platform_name = platform_name
