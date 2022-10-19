from celus_nibbler.parsers.base import BaseParser


class BaseNonCounterParser(BaseParser):
    @property
    def name(self):
        return f"non_counter.{self.data_format.name}"
