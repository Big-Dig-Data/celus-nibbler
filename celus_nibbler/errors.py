# TODO more verbose exception (e.g. write why does it failed)


class RecordError(Exception):
    pass


class WrongFormatError(Exception):
    pass


class TableException(Exception):
    """
    General exception informing about position in which the exception occured
    """

    def __init__(self, row=None, col=None, message="Problem with parsing your format has occured."):
        self.row = row
        self.col = col
        self.message = message
        super().__init__(self.message)  # necessary?

    def __str__(self):
        if self.row or self.col:
            return (
                f'{self.message} Exception found on row: {self.row}, col: {self.col} in the form.'
            )
        else:
            return f'{self.message} Exeption position in the form undefined.'

    def __repr__(self):
        return self.__str__()


class FindParserException(TableException):
    """
    Exceptions raised during search for the right parser
    """

    def __init__(self, problem):
        self.problem = problem
        super().__init__()

    def __str__(self):
        return f'{super().__str__()}\nThe problem is: {self.problem}.'

    def __repr__(self):
        return self.__str__()
