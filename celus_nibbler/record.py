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
    title_ids: typing.Optional[
        typing.Dict[str, str]
    ] = None  # ISBN (books), ISSN (periodics), EISSN (same thing only for electronic version of publications)
    dimension_data: typing.Optional[typing.Dict[str, str]] = None  # contains more details
    metric: typing.Optional[str] = None

    # TODO validations

    def serialize(self) -> typing.Tuple[str, ...]:
        def serialize_dict(mapping: typing.Optional[dict]) -> str:
            if not mapping:
                return ""
            return "|".join(f"{k}:{v}" for k, v in mapping.items())

        def format_date(dato_obj: typing.Optional[date]):
            if not dato_obj:
                return ""
            return dato_obj.strftime("%Y-%m-%d")

        return (
            format_date(self.start),
            format_date(self.end),
            self.platform or "",
            self.title or "",
            self.metric or "",
            serialize_dict(self.dimension_data),
            serialize_dict(self.title_ids),
            str(self.value),
        )
