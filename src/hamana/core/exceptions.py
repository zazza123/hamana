class HamanaException(Exception):
    """
        General exception of Hamana library
    """
    def __init__(self, description):
        self.description = description

    def __str__(self):
        return self.description

class ColumnParserPandasNumberError(HamanaException):
    """
        Exception raised when there is an error parsing a number column
    """
    def __init__(self, description):
        super().__init__(description)

class ColumnParserPandasDatetimeError(HamanaException):
    """
        Exception raised when there is an error parsing a datetime column
    """
    def __init__(self, description):
        super().__init__(description)

class ColumnIdentifierError(HamanaException):
    """
        Exception raised when there is an error identifying a column.
    """
    def __init__(self, description):
        super().__init__(description)

class ColumnIdentifierEmptySeriesError(HamanaException):
    """
        Exception raised when a series is empty.
    """
    def __init__(self, description):
        super().__init__(description)