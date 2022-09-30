import json
import typing
from datetime import date, timedelta

from pydantic.json import pydantic_encoder


class PydanticConfig:
    extra = "forbid"


class JsonEncorder:
    def dict(self):
        return pydantic_encoder(self)

    def json(self):
        return json.dumps(self, default=pydantic_encoder)

    @classmethod
    def parse(cls, obj: typing.Dict[str, typing.Any]):
        # triggers validations
        if hasattr(cls, '__pydantic_model__'):
            # Run for data_classes
            pydantic_obj = cls.__pydantic_model__.parse_obj(obj)

            # convert to original object (dataclass)
            return cls(**pydantic_obj.dict())
        else:
            # For pydantic models
            return cls.parse_obj(obj)


def start_month(in_date: date) -> date:
    return in_date.replace(day=1)


def end_month(in_date: date) -> date:
    if in_date.month == 12:
        return date(year=in_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        return date(year=in_date.year, month=in_date.month + 1, day=1) - timedelta(days=1)


def colnum_to_colletters(colnum: int) -> str:
    colletters = ""
    while colnum > 0:
        colnum, remainder = divmod(colnum - 1, 26)
        colletters = chr(65 + remainder) + colletters
    return colletters
