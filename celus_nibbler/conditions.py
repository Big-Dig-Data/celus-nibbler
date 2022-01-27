import re
from abc import ABCMeta, abstractmethod

from .coordinates import Coord
from .reader import SheetReader


class BaseCondition(metaclass=ABCMeta):
    @abstractmethod
    def check(self, sheet: SheetReader):
        pass

    def __invert__(self):
        return NegCondition(self)

    def __or__(self, other):
        return OrCondidtion(self, other)

    def __and__(self, other):
        return AndCondition(self, other)


class NegCondition(BaseCondition):
    def __init__(self, cond: BaseCondition):
        self.cond = cond

    def check(self, sheet: SheetReader):
        return not self.cond.check(sheet)


class AndCondition(BaseCondition):
    def __init__(self, *conds: BaseCondition):
        self.conds = conds

    def check(self, sheet: SheetReader):
        return all(e.check(sheet) for e in self.conds)


class OrCondidtion(BaseCondition):
    def __init__(self, *conds: BaseCondition):
        self.conds = conds

    def any(self, sheet: SheetReader):
        return all(e.check(sheet) for e in self.conds)


class RegexCondition(BaseCondition):
    def __init__(self, pattern: re.Pattern, coord: Coord):
        self.pattern = pattern
        self.coord = coord

    def check(self, sheet: SheetReader):
        return bool(self.pattern.match(self.coord.content(sheet)))
