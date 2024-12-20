import logging
from typing import Any, Type
from types import TracebackType
from abc import ABCMeta
from pathlib import Path
from sqlite3 import Connection, connect

from ..connector.db.sqlite import SQLiteConnector
from .exceptions import HamanaDatabaseAlreadyInitialised, HamanaDatabaseNotInitialised

# set logger
logger = logging.getLogger(__name__)

# set singleton db class
class HamanaSingleton(type):
    """
        Singleton metaclass for HamanaDatabase to ensure only 
        one instance during the code execution.
    """
    _instances = {}

    def __call__(cls, path: str | Path = ":memory:", *args: tuple, **kwargs: dict[str, Any]):
        if cls not in cls._instances:
            instance = super().__call__(path, *args, **kwargs)
            cls._instances[cls] = instance
            logger.info("HamanaDatabase is initialized.")
        else:
            logger.error("HamanaDatabase is already initialized.")
            raise HamanaDatabaseAlreadyInitialised()
        return cls._instances[cls]

class HamanaDatabaseMeta(HamanaSingleton, ABCMeta):
    pass

class HamanaDatabase(SQLiteConnector, metaclass = HamanaDatabaseMeta):
    """
        This class is responsible for handling the database connection of the library.  
        For each execution, only one instance of this class is allowed to manage a single 
        database connection.

        Parameters:
            path: path like string that define the SQLite database to load/create.  
                By default the database is created in memory.

        **Example**
        ```python
        from hamana.core import HamanaDtaabase

        # init internal database
        hamana_db = HamanaDatabase()

        # doing operations..
        query.to_sqlite(table_name = "t_oracle_output")

        # close connection
        hamana_db.close()
        ```
    """

    _connection: Connection| None = None

    def __init__(self, path: str | Path = ":memory:") -> None:
        path_str = str(path)
        super().__init__(path_str)
        self._connection = connect(database = path_str)

    def _connect(self) -> Connection:
        if self._connection is None:
            logger.error("Database connection is not initialized.")
            raise HamanaDatabaseNotInitialised()
        return self._connection

    def __exit__(self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, exc_traceback: TracebackType | None) -> None:
        logger.debug("connection not closed for HamanaDatabase, use close() method to close the connection.")
        return

    @classmethod
    def get_instance(cls) -> "HamanaDatabase":
        """
            Get the instance of the internal database connector.

            Raises:
                HamanaDatabaseNotInitialised: If the database is not initialized.
        """
        logger.debug("start")
        _instance = cls._instances.get(cls)

        if _instance is None:
            logger.error("HamanaDatabase is not initialized.")
            raise HamanaDatabaseNotInitialised()

        logger.debug("end")
        return _instance

    def get_connection(self) -> Connection:
        """
            Get the database connection.

            Raises:
                HamanaDatabaseNotInitialised: If the database is not initialized.
        """
        logger.debug("start")
        if self._connection is None:
            logger.error("Database connection is not initialized.")
            raise HamanaDatabaseNotInitialised()
        logger.debug("end")
        return self._connection

    def close(self) -> None:
        """
            Close the database connection.  
            Use this method at the end of the process to ensure that the database 
            connection is properly closed. If the connection is already closed, 
            this method will log a warning message.
        """
        logger.debug("start")
        if self._connection is None:
            logger.warning("Database connection is already closed.")
        else:
            self._connection.close()
            self._connection = None

            # remove instance from singleton
            HamanaDatabase._instances.pop(HamanaDatabase)
        logger.debug("end")
        return