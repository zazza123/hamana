from __future__ import annotations
from typing import Any, Generator, Protocol, TYPE_CHECKING
from abc import ABCMeta, abstractmethod

if TYPE_CHECKING:
    from .query import Query

"""PEP 249 Standard Database API Specification v2.0"""
class ConnectionProtocol(Protocol):
    """
        The following abtract class defines a general database connection.  
        A connection is used in order to connect to a database and perform operations on it.
    """

    def close(self) -> None:
        """Function used to close the database connection."""
        ...

    def commit(self) -> None:
        """Function used to commit the transaction."""
        ...

    def rollback(self) -> None:
        """Function used to rollback the transaction."""
        ...

    def cursor(self) -> Any:
        """Function used to create a cursor."""
        ...

class DatabaseConnectorABC(metaclass = ABCMeta):
    """
        The following abtract class defines a general database connector.  
        A connector is used in order to connect to a database and perform operations on it.
    """

    connection: ConnectionProtocol
    """Database connection."""

    @abstractmethod
    def _connect(self) -> ConnectionProtocol:
        """
            Function used to connect to the database.

            Return:
                connection to the database.
        """
        raise NotImplementedError

    @abstractmethod
    def __enter__(self) -> "DatabaseConnectorABC":
        """Open a connection."""
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Close a connection."""
        raise NotImplementedError

    @abstractmethod
    def ping(self) -> None:
        """Function used to check the database connection."""
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: Query | str) -> None | Query:
        """
            Function used to extract data from the database.

            Parameters:
                query: query to execute on database.

            Return:
                dataframe with the resulting rows.
        """
        raise NotImplementedError

    @abstractmethod
    def batch_execute(self, query: Query, batch_size: int) -> Generator[Any, None, None]:
        """
            Function used to execute a query on the database and return the results in batches. 
            This approach is used to avoid memory issues when dealing with large datasets.

            Parameters:
                query: query to execute on database.
                batch_size: size of the batch to return.
        """
        raise NotImplementedError

    @abstractmethod
    def to_sqlite(self, query: Query, table_name: str, batch_size: int = 1000) -> None:
        """
            Function used to extract data from the database and insert it 
            into the internal SQLite database.

            Parameters:
                query: query to execute on database.
                table_name: name of the table to insert the data.
                batch_size: size of the batch to return.
        """
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
                hasattr(subclass, '_connect') and callable(subclass._connect)
            and hasattr(subclass, '__enter__') and callable(subclass.__enter__)
            and hasattr(subclass, '__exit__') and callable(subclass.__exit__)
            and hasattr(subclass, 'ping') and callable(subclass.ping)
            and hasattr(subclass, 'execute') and callable(subclass.execute)
            and hasattr(subclass, 'batch_execute') and callable(subclass.batch_execute)
            and hasattr(subclass, 'to_sqlite') and callable(subclass.to_sqlite)
            or NotImplemented
        )