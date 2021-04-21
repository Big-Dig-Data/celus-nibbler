import typing
from dataclasses import dataclass
from datetime import date


@dataclass
class CounterRecord:
    value: int
    platform_name: typing.Optional[str] = None
    start: typing.Optional[date] = None
    end: typing.Optional[date] = None
    title: typing.Optional[str] = None
    title_ids: typing.Dict[str, str] = None
    dimension_data: typing.Dict[str, str] = None
    metric: typing.Optional[str] = None


# TODO validations
