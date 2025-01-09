import logging
from typing import Any, Type, overload
from types import TracebackType
from abc import ABCMeta
from pathlib import Path
from sqlite3 import Connection, connect as sqlite_connect

from .query import Query
from .sqlite import SQLiteConnector
from .exceptions import HamanaConnectorAlreadyInitialised, HamanaConnectorNotInitialised

# set logger
logger = logging.getLogger(__name__)

# set singleton db class
class HamanaSingleton(type):
    """
        Singleton metaclass for `HamanaConnector` to ensure only 
        one instance during the code execution.
    """
    _instances = {}

    def __call__(cls, path: str | Path = ":memory:", *args: tuple, **kwargs: dict[str, Any]):
        if cls not in cls._instances:
            instance = super().__call__(path, *args, **kwargs)
            cls._instances[cls] = instance
            logger.info("HamanaConnector is initialized.")
        else:
            logger.error("HamanaConnector is already initialized.")
            raise HamanaConnectorAlreadyInitialised()
        return cls._instances[cls]

class HamanaConnectorMeta(HamanaSingleton, ABCMeta):
    pass

class HamanaConnector(SQLiteConnector, metaclass = HamanaConnectorMeta):
    """
        This class is responsible for handling the database connection of the library.  
        For each execution, only one instance of this class is allowed to manage a single 
        database connection.

        Parameters:
            path: path like string that define the SQLite database to load/create.  
                By default the database is created in memory.

        **Example**
        ```python
        from hamana.connector.db.hamana import HamanaConnector

        # init internal database
        hamana_db = HamanaConnector()

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
        self._connection = sqlite_connect(database = path_str)

    def _connect(self) -> Connection:
        if self._connection is None:
            logger.error("Database connection is not initialized.")
            raise HamanaConnectorNotInitialised()
        return self._connection

    def __exit__(self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, exc_traceback: TracebackType | None) -> None:
        logger.debug("connection not closed for HamanaConnector, use close() method to close the connection.")
        return

    @classmethod
    def get_instance(cls) -> "HamanaConnector":
        """
            Get the instance of the internal database connector.

            Raises:
                HamanaConnectorNotInitialised: If the database is not initialized.
        """
        logger.debug("start")
        _instance = cls._instances.get(cls)

        if _instance is None:
            logger.error("HamanaConnector is not initialized.")
            raise HamanaConnectorNotInitialised()

        logger.debug("end")
        return _instance

    def get_connection(self) -> Connection:
        """
            Get the database connection.

            Raises:
                HamanaConnectorNotInitialised: If the database is not initialized.
        """
        logger.debug("start")
        if self._connection is None:
            logger.error("Database connection is not initialized.")
            raise HamanaConnectorNotInitialised()
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
            HamanaConnector._instances.pop(HamanaConnector)
        logger.debug("end")
        return

def connect(path: str | Path = ":memory:") -> None:
    """
        Connect to the database using the path provided.  
        This function is a helper function to connect to the database 
        without creating an instance of the `HamanaConnector` class.

        Parameters:
            path: path like string that define the SQLite database to load/create.  
                By default the database is created in memory.
    """
    logger.debug("start")

    # create connection
    HamanaConnector(path)
    logger.info(f"Connected to the database ({path}).")

    logger.debug("end")
    return

def disconnect() -> None:
    """
        Disconnect from the database.  
        This function is a helper function to disconnect from the database 
        without using an instance of the `HamanaConnector` class.
    """
    logger.debug("start")

    # close connection
    HamanaConnector.get_instance().close()
    logger.info("Disconnected from the database.")

    logger.debug("end")
    return

@overload
def execute(query: str) -> Query: ...

@overload
def execute(query: Query) -> None: ...

def execute(query: Query | str) -> None | Query:
    """
        Execute the query on the internal database.  
        This function is a helper function to execute a query on the `hamana` SQLite database.

        Parameters:
            query: query to execute on database. The query could be 
                a string or a `Query` object. If the query is a string, 
                then the function automatically creates a `Query` object.

        Returns:
            The result depends on the input provided. 
                If query is a string, then  the function automatically 
                creates a `Query` object, executes the extraction and 
                returns the `Query` object with the result. 
                If query is a `Query` object, then the function performs 
                the extraction and return None because the result is stored 
                in the object itself.
    """
    logger.debug("start")

    # execute query
    result = HamanaConnector.get_instance().execute(query)
    logger.info(f"Query executed successfully: {query}")

    logger.debug("end")
    return result