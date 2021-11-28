from celus_nibbler.utils import colnum_to_colletters


class NibblerWarning:
    pass


class ValueNotUsedWarning(NibblerWarning):
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
        self.sheet = self.sheet if self.sheet is not None else "unspecified"
        self.row = self.row if self.row is not None else "unspecified"
        self.col = self.col if self.col is not None else "unspecified"
        self.colletters_explanation = (
            f" (col \"{colnum_to_colletters(self.col + 1)}\" if using software for table sheets)"
            if self.col is not None
            else ""
        )
        return f'Value \'{self.value}\' was ignored.\nPosition: sheet {self.sheet}, col {self.col}{self.colletters_explanation}, row {self.row}) is outside the table.\nReason: {self.reason}.'

    def __repr__(self):
        return str(self)
