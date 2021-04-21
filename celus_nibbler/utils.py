from datetime import date, timedelta


def start_month(in_date: date) -> date:
    return in_date.replace(day=1)


def end_month(in_date: date):
    if date.month == 12:
        return date(year=in_date.year + 1, month=1, day=1)
    else:
        return date(year=in_date.year, month=in_date.month + 1, day=1) - timedelta(days=1)
