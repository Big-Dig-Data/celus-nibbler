import typing
from enum import Enum

from celus_nigiri import CounterRecord

from celus_nibbler.utils import colnum_to_colletters

# TODO more verbose exception (e.g. write why does it failed)


class NibblerError(Exception):
    def dict(self) -> dict:
        return {"name": f"{self.__class__.__name__}"}


class RecordError(NibblerError):
    def __init__(self, idx: int, record: CounterRecord):
        self.record = record
        self.idx = idx

    def dict(self) -> dict:
        return {
            "idx": self.idx,
            "name": f"{self.__class__.__name__}",
            "start": self.record.start,
            "end": self.record.end,
            "metric": self.record.metric,
            "title": self.record.title,
            "organization": self.record.organization,
            "dimensions": self.record.dimension_data,
            "title_ids": self.record.title_ids,
        }

    def __eq__(self, other):
        if not isinstance(self, type(other)):
            return False
        return self.dict() == other.dict()


class WrongFileFormatError(NibblerError):
    def __init__(self, file, file_suffix):
        super().__init__()
        self.file = file
        self.file_suffix = file_suffix

    def __str__(self):
        return f'file: "{self.file}" with extension "{self.file_suffix}" is not supported.'

    def __repr__(self):
        return f"{self.__class__.__name__}(file={self.file},file_suffix={self.file_suffix})"

    def __eq__(self, other):
        if not isinstance(self, type(other)):
            return False
        return all(
            getattr(self, attr, None) == getattr(other, attr, None)
            for attr in ["file", "file_suffix"]
        )


class XlsError(NibblerError):
    def __init__(self, xls_exception):
        self.xls_exception = xls_exception

    def __str__(self):
        return f'XLS Exception: "{self.xls_exception}"'


class JsonException(NibblerError):
    """
    Error while parsing JSON format
    """

    def __init__(self, reason: str):
        self.reason = reason
        pass

    def __str__(self):
        return f"json format error: {self.reason}"

    def __repr__(self):
        return f"{self.__class__.__name__}(reason={self.reason})"


class TableException(NibblerError):
    """
    General exception informing about position in which the exception occured
    while parsing tabular format
    """

    class Action(str, Enum):
        FAIL = "fail"
        SKIP = "skip"
        STOP = "stop"
        PASS = "pass"

    ATTRS = ["value", "row", "col", "sheet", "reason"]

    def __init__(
        self,
        value=None,
        row: typing.Optional[int] = None,
        col: typing.Optional[int] = None,
        sheet: typing.Optional[int] = None,
        reason: str = "unspecified",
        action: Action = Action.FAIL,
    ):
        super().__init__()
        self.value = value
        self.row = row
        self.col = col
        self.sheet = sheet
        self.reason = reason
        self.action = action

    def __str__(self):
        laymancount_sheet = self.sheet + 1 if self.sheet is not None else "unspecified"
        laymancount_row = self.row + 1 if self.row is not None else "unspecified"
        laymancount_col = self.col + 1 if self.col is not None else "unspecified"
        colletters_explanation = (
            f' (col "{colnum_to_colletters(laymancount_col)}" if using software for table sheets)'
            if self.col is not None
            else ""
        )
        location = (
            f"sheet {laymancount_sheet}, row {laymancount_row}, col {laymancount_col}"
            f"{colletters_explanation}"
        )
        return f"""\
Problem with parsing your format has occured.
Value causing this exception: {self.value}
Location of this value: {location}.
Reason: {self.reason}.
"""

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(reason={self.reason},sheet={self.sheet},"
            f"row={self.row},col={self.col},value={self.value})"
        )

    def __eq__(self, other):
        if not isinstance(self, type(other)):
            return False
        return all(getattr(self, attr, None) == getattr(other, attr, None) for attr in self.ATTRS)

    def dict(self) -> dict:
        return {
            "name": f"{self.__class__.__name__}",
            "reason": self.reason,
            "sheet_idx": self.sheet,
            "row": self.row,
            "col": self.col,
            "value": self.value,
        }


class NoParserFound(NibblerError):
    def __init__(self, sheet_idx, *args):
        self.sheet_idx = sheet_idx

    def __repr__(self):
        return f"{self.__class__.__name__}(sheet_idx={self.sheet_idx})"

    def dict(self) -> dict:
        return {
            "name": f"{self.__class__.__name__}",
            "sheet_idx": self.sheet_idx,
        }


class NoParserForPlatformFound(NoParserFound):
    pass


class NoParserMatchesHeuristics(NoParserFound):
    def __init__(self, sheet_idx: int, parsers_info: dict, *args):
        self.sheet_idx = sheet_idx
        self.parsers_info = parsers_info

    def dict(self) -> dict:
        return {
            "name": f"{self.__class__.__name__}",
            "sheet_idx": self.sheet_idx,
            "parsers_info": self.parsers_info,
        }


class NoParserForFileTypeFound(NoParserFound):
    pass


class MultipleParsersFound(NibblerError):
    def __init__(self, sheet_idx, *args):
        self.sheet_idx = sheet_idx
        self.parsers = args

    def dict(self) -> dict:
        return {
            "name": f"{self.__class__.__name__}",
            "sheet_idx": self.sheet_idx,
            "parsers": list(self.parsers),
        }


class SameRecordsInOutput(RecordError):
    def __init__(self, clashing_idx: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clashing_idx = clashing_idx

    def dict(self) -> dict:
        dct = super().dict()
        dct["clashing_idx"] = self.clashing_idx
        return dct


class NegativeValueInOutput(RecordError):
    pass
