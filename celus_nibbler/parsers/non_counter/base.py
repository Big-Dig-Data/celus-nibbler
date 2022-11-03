from celus_nibbler.parsers.base import BaseTabularParser


class BaseNonCounterParser(BaseTabularParser):
    @property
    def name(self):
        return f"non_counter.{self.data_format.name}"
