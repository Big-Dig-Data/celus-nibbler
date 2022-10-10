import abc

from pydantic.dataclasses import dataclass

from ..utils import PydanticConfig


@dataclass(config=PydanticConfig)
class BaseParserDefinition(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_parser(self):
        pass
