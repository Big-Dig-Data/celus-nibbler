import datetime
import logging
import typing

from pydantic import ValidationError

from celus_nibbler import validators
from celus_nibbler.errors import NibblerValidation, TableException
from celus_nibbler.record import CounterRecord
from celus_nibbler.settings import IGNORE_METRICS, IGNORE_MONTHS, IGNORE_TITLES
from celus_nibbler.templates import RelatedTo
from celus_nibbler.utils import assign_by_relatedto, end_month, start_month

from .generalparser import GeneralParser

# from celus_nibbler.warnings import ValueNotUsedWarning

logger = logging.getLogger(__name__)


class HorizontalDatesParser(GeneralParser):
    def parse_dates(self) -> typing.List[typing.Optional[datetime.date]]:
        parsed_dates = []

        def parse_date(value) -> datetime.date:
            if isinstance(value, datetime.date):
                return value
            else:
                return self.date_validation(date=value).date

        if self.months.relation == RelatedTo.COL:
            cells_with_dates: list = self.sheet.values[self.months.start_row][
                self.months.start_col :
            ]

            if self.separate_year is not None:
                if self.separate_year.relation == RelatedTo.TABLE:
                    cells_with_years: list = [
                        self.sheet.values[self.separate_year.start_row][
                            self.separate_year.start_col
                        ]
                        for e in range(len(cells_with_dates))
                    ]
                elif self.separate_year.relation == RelatedTo.COL:
                    cells_with_years: list = self.sheet.values[self.separate_year.start_row][
                        self.separate_year.start_col :
                    ]
            else:
                cells_with_years = None

            for cell_with_date_idx, cell_with_date in enumerate(cells_with_dates):
                if cell_with_date in IGNORE_MONTHS:
                    parsed_dates.append(None)
                else:
                    try:
                        date = parse_date(cell_with_date)
                    except NibblerValidation:
                        logger.warning(
                            f"parser could not parse '{cell_with_date}' as a date, therefore this value will be ignored. "
                            f'Location: col {self.months.start_col + cell_with_date_idx} in sheet {self.sheet_idx}.'
                        )
                        parsed_dates.append(None)
                        continue

                    if cells_with_years is not None:
                        try:
                            separate_year = parse_date(
                                str(cells_with_years[cell_with_date_idx])
                            ).year
                        except NibblerValidation:
                            logger.warning(
                                f"parser could not parse '{cells_with_years[cell_with_date_idx]}' as a year, "
                                f'therefore these values will be ignored. Location: row {self.separate_year.start_row} col '
                                f'{self.separate_year.start_col + cell_with_date_idx} in sheet {self.sheet_idx}'
                            )
                            parsed_dates.append(None)
                            continue
                        date = date.replace(year=separate_year)

                    logger.debug('date is: %s ', date)
                    parsed_dates.append(date)

        else:
            # TODO add parsing for dates which has RelatedTo.ROW and possibly RelatedTo.TABLE
            pass
        return parsed_dates

    def parse(self) -> typing.List[CounterRecord]:
        counter_report = []
        dates: list = self.parse_dates()

        # prepare how to parse Metrics
        metric_one_for_whole_sheet, col_with_metrics = assign_by_relatedto(self.metric, self.sheet)

        # prepare how to parse Titles
        title_one_for_whole_sheet, col_with_titles = assign_by_relatedto(self.title, self.sheet)

        # PARSING row by row
        for row_with_values_idx, row_with_values in enumerate(
            self.sheet.values[self.values.start_row :]
        ):

            # parsing of metrics
            if metric_one_for_whole_sheet is not None:
                metric = metric_one_for_whole_sheet
            elif col_with_metrics is not None:
                if row_with_values[col_with_metrics] is not None:
                    metric = row_with_values[col_with_metrics]
                else:
                    logger.info(
                        f'parsing ends here: row {row_with_values_idx + self.values.start_row} no metric was found in this row'
                    )
                    break
            else:
                logger.warning(
                    f'sheet {self.sheet_idx} wont be parsed, no instructions on how to parse metrics was provided'
                )
                break
            if metric.lower() in IGNORE_METRICS:
                logger.info(
                    'metric \'%s\' was ignored, because was found in "IGNORE_METRICS"', metric
                )
                continue
            try:
                metric = validators.Metric(metric=metric).metric
            except ValidationError as e:
                raise TableException(
                    metric,
                    self.values.start_row + row_with_values_idx,
                    col_with_metrics,
                    self.sheet_idx,
                    'metric',
                ) from e
            logger.debug('metric is: \'%s\' ', metric)

            # parsing of titles
            if title_one_for_whole_sheet is not None:
                title = title_one_for_whole_sheet
            elif col_with_titles is not None:
                if row_with_values[col_with_titles] is not None:
                    title = row_with_values[col_with_titles]
                else:
                    title = None
                    logger.info(
                        f'in row {row_with_values_idx + self.values.start_row} no title was found'
                    )
            else:
                title = None

            if title is not None:
                if title.lower() in IGNORE_TITLES:
                    title = None
                    logger.info(
                        'title \'%s\' was ignored, because was found in "IGNORE_TITLES"', title
                    )
                else:
                    try:
                        title = validators.Title(title=title).title
                    except ValidationError as e:
                        raise TableException(
                            title,
                            self.values.start_row + row_with_values_idx,
                            col_with_titles,
                            self.sheet_idx,
                            'title',
                        ) from e
                    logger.debug('title is: \'%s\' ', title)

            # PARSING cell by cell
            cells_with_values = row_with_values[self.values.start_col :]
            for col_with_values_idx, value in enumerate(cells_with_values):
                if col_with_values_idx >= len(dates) or dates[col_with_values_idx] is None:
                    logger.warning(
                        f'value: {value} was ignored. It does not have a corresponding date. The date value is either included in settings.IGNORE_MONTHS or the values position is outside the sheet. Position is sheet: {self.sheet_idx}, col: {self.values.start_col + col_with_values_idx}, row: {self.values.start_row + row_with_values_idx}'
                    )
                    continue
                else:
                    try:
                        value = validators.Value(value=value).value
                    except ValidationError as e:
                        raise TableException(
                            value,
                            self.values.start_row + row_with_values_idx,
                            self.values.start_col + col_with_values_idx,
                            self.sheet_idx,
                            'value',
                        ) from e
                    float_value = float(value)
                    logger.debug('float_value is: %s ', float_value)

                    counter_report.append(
                        CounterRecord(
                            platform=self.platform,
                            title=title,
                            metric=metric,
                            start=start_month(dates[col_with_values_idx]),
                            end=end_month(dates[col_with_values_idx]),
                            dimension_data=None,
                            title_ids=None,
                            value=round(float_value),
                        )
                    )
        return counter_report
