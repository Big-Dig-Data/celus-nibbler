import abc
import copy
import tempfile
import typing
from collections import deque
from datetime import date

from celus_nigiri import CounterRecord
from diskcache import Cache

from celus_nibbler.errors import SameRecordsInOutput


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
        yield from records


class SameAggregator(BaseAggregator):
    """Aggregates the same records - sums values"""

    def aggregate(
        self, records: typing.Generator[CounterRecord, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        with tempfile.TemporaryDirectory() as tmpdir:
            with Cache(tmpdir) as db, db.transact():
                # Read all records
                for record in records:
                    # Derive key
                    new_record = copy.deepcopy(record)
                    new_record.value = -1
                    key = new_record.as_csv()

                    # Aggregate
                    if existing_record := db.get(key):
                        existing_record.value += record.value
                        db[key] = existing_record
                    else:
                        db[key] = record

                for key in db.iterkeys():
                    yield db[key]


class CheckConflictingRecordsAggregator(BaseAggregator):
    """
    Checks whether last n records doesn't contain same conflicting records e.g.

    Title1,Dim1,Metric1,2021-01-01,2021-01-31,999
    Title2,Dim1,Metric1,2021-01-01,2021-01-31,888
    Title1,Dim1,Metric1,2021-01-01,2021-01-31,999

    is wrong, because it contains same Title, Dimensions and metric for 2020-01 twice

    """

    def __init__(self, size: int = 100):
        self.hash_buffer: typing.Deque[int] = deque(maxlen=size)

    @staticmethod
    def make_record_hash(record: CounterRecord):
        return hash(tuple(e for e in record.as_csv()[:-1]))  # skip value (last in csv)

    def aggregate(
        self, records: typing.Generator[CounterRecord, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        for record in records:
            hsh = self.make_record_hash(record)
            if hsh in self.hash_buffer:
                raise SameRecordsInOutput(record)
            self.hash_buffer.append(hsh)
            yield record


class CounterOrdering(BaseAggregator):
    """
    Extracts same records with different dates together e.g.

    Title1,Dim1,Metric1,2021-01-01,2021-01-31,999
    Title2,Dim1,Metric1,2021-01-01,2021-01-31,888
    Title1,Dim1,Metric1,2021-02-01,2021-02-28,777
    Title2,Dim1,Metric1,2021-02-01,2021-02-28,666

    ->

    Title1,Dim1,Metric1,2021-01-01,2021-01-31,999
    Title1,Dim1,Metric1,2021-02-01,2021-02-28,777
    Title2,Dim1,Metric1,2021-01-01,2021-01-31,888
    Title2,Dim1,Metric1,2021-02-01,2021-02-28,666
    """

    def aggregate(
        self, records: typing.Generator[CounterRecord, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        with tempfile.TemporaryDirectory() as tmpdir:
            with Cache(tmpdir) as db, db.transact():
                # Read all records
                for record in records:
                    # Derive key
                    new_record = copy.deepcopy(record)
                    new_record.value = -1
                    # Date aggregation -> fix date in key
                    new_record.start = date(2020, 1, 1)
                    new_record.end = date(2020, 1, 31)
                    key = new_record.as_csv()

                    # Aggregate
                    record_dict: typing.Dict[date, CounterRecord] = db.get(key, {})
                    if rec := record_dict.get(record.start):
                        rec.value += record.value
                    else:
                        record_dict[record.start] = record
                    db[key] = record_dict

                for db_key in db.iterkeys():
                    record_dict = db[db_key]
                    yield from [record_dict[key] for key in sorted(record_dict.keys())]
