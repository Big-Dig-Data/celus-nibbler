from celus_nibbler.utils import colnum_to_colletters

# TODO more verbose exception (e.g. write why does it failed)


class NibblerError(Exception):
    def dict(self) -> dict:
        return {"name": f"{self.__class__.__name__}"}


class RecordError(NibblerError):
    pass


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


class TableException(NibblerError):
    """
    General exception informing about position in which the exception occured
    """

    def __init__(
        self,
        value=None,
        row: int = None,
        col: int = None,
        sheet: int = None,
        reason: str = "unspecified",
    ):
        super().__init__()
        self.value = value
        self.row = row
        self.col = col
        self.sheet = sheet
        self.reason = reason

    def __str__(self):
        self.laymancount_sheet = self.sheet + 1 if self.sheet is not None else "unspecified"
        self.laymancount_row = self.row + 1 if self.row is not None else "unspecified"
        self.laymancount_col = self.col + 1 if self.col is not None else "unspecified"
        self.colletters_explanation = (
            f" (col \"{colnum_to_colletters(self.laymancount_col)}\" if using software for table sheets)"
            if self.col is not None
            else ""
        )
        return f'Problem with parsing your format has occured.\nValue causing this exception: {self.value}\nLocation of this value: sheet {self.laymancount_sheet}, row {self.laymancount_row}, col {self.laymancount_col}{self.colletters_explanation}.\nReason: {self.reason}.'

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(reason={self.reason},sheet={self.sheet},"
            f"row={self.row},col={self.col},value={self.value})"
        )

    def __eq__(self, other):
        if not isinstance(self, type(other)):
            return False
        return all(
            getattr(self, attr, None) == getattr(other, attr, None)
            for attr in ["value", "row", "col", "sheet", "reason"]
        )

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
