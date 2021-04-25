from dateutil.parser import parse as parse_datetimes

from celus_nibbler.record import CounterRecord
from celus_nibbler.utils import end_month, start_month


class GeneralParser:

    platforms = [
        'platorm1',
        'platorm2',
    ]

    metric_list = [
        'metric1',
        'metric2',
    ]
    table_map = {
        'heuristics': [
            {'row': 0, 'col': 0, 'content': 'text1'},
            {'row': 0, 'col': 0, 'content': 'text2'},
        ],
        'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
        'months': {
            'direction': 'in cols',  # 'in lines'
            'start_at': {
                'row': 0,
                'col': 1,
                'month': 'january',  # could be any other month
            },
        },
    }

    def __init__(self, table, platform=None):
        self.header = None
        self.table = table
        self.platform = platform

    def heuristic_check(self):
        """
        check if there is an expected content in the expected location of the table
        """
        for heuristic in self.table_map['heuristics']:
            row = heuristic['row']
            col = heuristic['col']
            content = heuristic['content']
            if self.table[row][col] == content:
                continue
            else:
                return False
        return True

    def metric_title_check(self):
        """
        check if column with metrics has expected title
        """
        row = self.table_map['metric_title']['row']
        col = self.table_map['metric_title']['col']
        content = self.table_map['metric_title']['content']
        if self.table[row][col] == content:
            return True

    def find_new_metrics(self):
        """
        check if expected matrics are present in the metrics column
        """
        first_metric_line = self.table_map['metric_title']['row'] + 1
        metrics_col = self.table_map['metric_title']['col']
        new_metrics = []

        for line in self.table[first_metric_line:]:
            metric = line[metrics_col]
            if metric not in self.metric_list and metric not in new_metrics:
                new_metrics.append(metric)
        return new_metrics

    def parse_dates(self):
        dates_row = self.table_map['months']['start_at']['row']
        first_date_col = self.table_map['months']['start_at']['col']
        line_of_dates = self.table[dates_row][first_date_col:]
        dates = []
        for item in line_of_dates:
            dates.append(parse_datetimes(item).date())
        return dates

    def parse(self):
        counter_report = []
        values_first_row = self.table_map['months']['start_at']['row'] + 1
        values_first_col = self.table_map['months']['start_at']['col']
        metrics_col = self.table_map['metric_title']['col']
        dates = self.parse_dates()
        for line_with_values in self.table[values_first_row:]:
            metric = line_with_values[metrics_col]
            cells_with_values = line_with_values[values_first_col:]
            dates_idx = 0
            for value in cells_with_values:
                counter_report.append(
                    CounterRecord(
                        platform=self.platform,
                        title=None,
                        metric=metric,
                        start=start_month(dates[dates_idx]),
                        end=end_month(dates[dates_idx]),
                        dimension_data=None,
                        title_ids=None,
                        value=int(value),
                    )
                )
                dates_idx += 1
        return counter_report
