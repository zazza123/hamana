from ...core.exceptions import HamanaException

class DatabaseConnetionError(HamanaException):
    pass

class QueryResultNotAvailable(HamanaException):
    pass

class QueryColumnsNotAvailable(HamanaException):
    pass

class ColumnDataTypeConversionError(HamanaException):
    pass