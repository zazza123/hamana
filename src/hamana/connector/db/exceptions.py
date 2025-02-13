from ...core.exceptions import HamanaException

class DatabaseConnetionError(HamanaException):
    pass

class QueryInitializationError(HamanaException):
    pass

class QueryResultNotAvailable(HamanaException):
    pass

class QueryColumnsNotAvailable(HamanaException):
    pass

class ColumnDataTypeConversionError(HamanaException):
    pass

class TableAlreadyExists(HamanaException):
    pass

# HamanaConnector exceptions
class HamanaConnectorAlreadyInitialised(HamanaException):
    """
        Exception to raise when HamanaConnector is already initialised
    """
    def __init__(self):
        super().__init__("HamanaConnector is already initialised.")

class HamanaConnectorNotInitialised(HamanaException):
    """
        Exception to raise when HamanaConnector is not initialised.
    """
    def __init__(self):
        super().__init__("No instance of HamanaConnector is initialised.")