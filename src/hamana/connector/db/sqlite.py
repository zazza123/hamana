import logging
from sqlite3 import Connection
from typing import Any

from ...core.column import Column
from .base import BaseConnector
from .exceptions import ColumnDataTypeConversionError

# set logger
logger = logging.getLogger(__name__)

class SQLiteConnector(BaseConnector):
    """
        Class representing a connector to a SQLite database.
    """

    def __init__(self, path: str, **kwargs: dict[str, Any]) -> None:
        self.path = path
        self.kwargs = kwargs
        self.connection: Connection

    def _connect(self) -> Connection:
        return Connection(self.path)

    def get_column_from_dtype(self, dtype: Any, column_name: str, order: int) -> Column:
        # SQLite connector does not provide datatypes
        raise ColumnDataTypeConversionError(f"Data type {dtype} does not have a corresponding mapping.")