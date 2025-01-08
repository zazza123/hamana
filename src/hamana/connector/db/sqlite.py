import logging
from sqlite3 import Connection
from typing import Any

from .base import BaseConnector

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