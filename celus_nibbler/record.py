import typing
from dataclasses import dataclass
from datetime import date


@dataclass
class CounterRecord:
    platform_name: typing.Optional[str] = None
    platform_name: typing.Optional[str] = None
    start: typing.Optional[date] = None
    end: typing.Optional[date] = None
    title: typing.Optional[str] = None
    title_ids: typing.Dict[str, str] = None
    dimension_data: None
    metric: typing.Optional[str] = None
    value: int


# TODO validations
