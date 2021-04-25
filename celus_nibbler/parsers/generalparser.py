import datetime
import typing

from pydantic import ValidationError

from celus_nibbler import validators
from celus_nibbler.errors import TableException
from celus_nibbler.record import CounterRecord
from celus_nibbler.utils import end_month, start_month


class GeneralParser:

    platforms = None

    metric_list = None

    table_map = None

    def __init__(self, table: list, platform: str = None):
        self.header = None
        self.table = table
        self.platform = platform

    def heuristic_check(self) -> bool:
        """
        check if there is an expected content in the expected location of the table
        """
        for heuristic in self.table_map['heuristics']:
            row = heuristic['row']
            col = heuristic['col']
            content = heuristic['content']
            if self.table[row][col] != content:
                return False
        return True


class HorizontalDatesParser(GeneralParser):
    def metric_title_check(self) -> bool:
        """
        check if column with metrics has expected title
        """
        row = self.table_map['metric_title']['row']
        col = self.table_map['metric_title']['col']
        content = self.table_map['metric_title']['content']
        return self.table[row][col] == content

    def find_new_metrics(self) -> typing.List[str]:
        """
        check if expected matrics are present in the metrics column
        """
        first_row_with_metrics = self.table_map['metric_title']['row'] + 1
        col_with_metrics = self.table_map['metric_title']['col']
        new_metrics = []
        for row_with_metrics_idx, row_with_metrics in enumerate(
            self.table[first_row_with_metrics:]
        ):
            metric = row_with_metrics[col_with_metrics]
            if metric not in self.metric_list and metric not in new_metrics:
                try:
                    metric = validators.Metric(metric=metric).metric
                except ValidationError as e:
                    raise TableException(
                        metric,
                        first_row_with_metrics + row_with_metrics_idx,
                        col_with_metrics,
                        'metric',
                    ) from e
                new_metrics.append(metric)
        return new_metrics

    def parse_dates(self) -> typing.List[datetime.date]:
        row_with_dates = self.table_map['months']['start_at']['row']
        first_col_with_dates = self.table_map['months']['start_at']['col']
        cells_with_dates = self.table[row_with_dates][first_col_with_dates:]
        parsed_dates = []
        for string_idx, string in enumerate(cells_with_dates):
            try:
                date = validators.Date(date=string).date
            except ValidationError as e:
                raise TableException(
                    string,
                    row_with_dates,
                    first_col_with_dates + string_idx,
                    'date',
                ) from e

            parsed_dates.append(date)
        return parsed_dates

    def parse(self) -> typing.List[CounterRecord]:
        counter_report = []
        first_row_with_values = self.table_map['months']['start_at']['row'] + 1
        first_col_with_values = self.table_map['months']['start_at']['col']
        col_with_metrics = self.table_map['metric_title']['col']
        dates = self.parse_dates()
        for row_with_values_idx, row_with_values in enumerate(self.table[first_row_with_values:]):
            metric = row_with_values[col_with_metrics]
            try:
                metric = validators.Metric(metric=metric).metric
            except ValidationError as e:
                raise TableException(
                    metric,
                    first_row_with_values + row_with_values_idx,
                    col_with_metrics,
                    'metric',
                ) from e
            cells_with_values = row_with_values[first_col_with_values:]
            for col_with_values_idx, value in enumerate(cells_with_values):
                try:
                    value = validators.Value(value=value).value
                except ValidationError as e:
                    raise TableException(
                        value,
                        first_row_with_values + row_with_values_idx,
                        first_col_with_values + col_with_values_idx,
                        'value',
                    ) from e
                float_value = float(value)
                counter_report.append(
                    CounterRecord(
                        platform=self.platform,
                        title=None,
                        metric=metric,
                        start=start_month(dates[col_with_values_idx]),
                        end=end_month(dates[col_with_values_idx]),
                        dimension_data=None,
                        title_ids=None,
                        value=round(float_value),
                    )
                )
        return counter_report


class VerticalDatesParser(GeneralParser):
    # TODO this classes will have own parse(), parse_dates() and find_new_metrics() as HorizontalDatesParser class does.
    # all subtype parsers will inherite from one of these two classes (HorizontalDatesParser or VerticalDatesParser)

    def find_new_metrics(self) -> typing.List[str]:
        # raise NotImplementedErorr()
        pass
