import typing
from dataclasses import dataclass
from datetime import date


@dataclass
class CounterRecord:
    value: int
    platform: typing.Optional[str] = None  # name of the issuer
    start: typing.Optional[date] = None  # mandatory, each record should have at least a start date
    end: typing.Optional[
        date
    ] = None  # otional really, if the report is for whole month then the start date (first day of the month) suffice
    title: typing.Optional[str] = None  # name of the publication
    title_ids: typing.Dict[
        str, str
    ] = None  # ISBN (books), ISSN (periodics), EISSN (same thing only for electronic version of publications)
    dimension_data: typing.Dict[str, str] = None  # contains more details
    metric: typing.Optional[str] = None

    # TODO validations
