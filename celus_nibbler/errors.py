from celus_nibbler.utils import colnum_to_colletters

# TODO more verbose exception (e.g. write why does it failed)


class NibblerError(Exception):
    pass


class RecordError(NibblerError):
    pass


class WrongFormatError(NibblerError):
    pass


class NibblerValidation(NibblerError):
    def __init__(self, reason="unknown"):
        self.reason = reason
        super().__init__(reason)

    def __str__(self):
        return self.reason


class TableException(NibblerError):
    """
    General exception informing about position in which the exception occured
    """

    def __init__(
        self,
        value=None,
        row: int = None,
        col: int = None,
        reason: str = "unspecified",
    ):
        super().__init__()
        self.value = value
        self.row = row
        self.col = col
        self.reason = reason

    def __str__(self):
        self.laymancount_row = self.row + 1 if self.row is not None else "unspecified"
        self.laymancount_col = self.col + 1 if self.col is not None else "unspecified"
        self.colletters_explanation = (
            f" (col \"{colnum_to_colletters(self.laymancount_col)}\" if using software for table sheets)"
            if self.col is not None
            else ""
        )
        return f'Problem with parsing your format has occured.\nValue causing this exception: {self.value}\nLocation of this value: row {self.laymancount_row}, col {self.laymancount_col}{self.colletters_explanation}.\nReason: {self.reason}.'

    def __repr__(self):
        return str(self)
