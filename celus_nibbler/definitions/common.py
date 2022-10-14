import typing
from abc import ABCMeta, abstractmethod

from ..parsers.base import BaseArea


class AreaGeneratorMixin(metaclass=ABCMeta):
    @abstractmethod
    def make_area(self) -> typing.Type[BaseArea]:
        pass
