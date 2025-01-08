from ...core.exceptions import HamanaException

class CSVColumnNumberMismatchError(HamanaException):
    """
        Error when the number of columns in the CSV file does not match the number of columns in the schema.
    """
    pass

class CSVDecodeRowError(HamanaException):
    """
        Error when decoding a row from the CSV file.
    """
    pass