import abc
import copy
import shelve
import tempfile
import typing

from celus_nigiri import CounterRecord


class BaseAggregator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def aggregate(
        self, records: typing.Generator[CounterRecord, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        pass


class NoAggregator(BaseAggregator):
    """Doesn't aggregate anything just passes the data"""

    def aggregate(
        self, records: typing.Generator[CounterRecord, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        for record in records:
            yield record


class SameAggregator(BaseAggregator):
    """Aggregates the same records - sums values"""

    def aggregate(
        self, records: typing.Generator[CounterRecord, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        with tempfile.NamedTemporaryFile() as tmpfile:
            with shelve.open(tmpfile.name, 'n') as db:
                # Read all records
                for record in records:
                    # Derive key
                    new_record = copy.deepcopy(record)
                    new_record.value = -1
                    key = "|".join(new_record.as_csv())

                    # Aggregate
                    if existing_record := db.get(key):
                        existing_record.value += record.value
                        db[key] = existing_record
                    else:
                        db[key] = record

                for record in db.values():
                    yield record
