import typing
from abc import ABCMeta, abstractmethod

from celus_nibbler import CounterRecord


class BaseReport(metaclass=ABCMeta):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def human_name(self) -> str:
        pass

    @property
    @abstractmethod
    def platforms(self) -> typing.List[str]:
        pass

    @property
    @abstractmethod
    def metrics(self) -> typing.List[str]:
        pass

    @abstractmethod
    def output(self) -> typing.Generator[CounterRecord, None, None]:
        pass
