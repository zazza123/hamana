import logging
from sqlite3 import Connection
from typing import Any

from .base import BaseConnector

# set logger
logger = logging.getLogger(__name__)

class SQLiteConnector(BaseConnector):
    """
        Class to represent a connector to an SQLite database.
    """

    def __init__(self, path: str, **kwargs: dict[str, Any]) -> None:
        self.path = path
        self.kwargs = kwargs
        self.connection: Connection

    def _connect(self) -> Connection:
        return Connection(self.path)