from typing import Any, Generator
from abc import ABCMeta, abstractmethod

from ...query import Query

"""PEP 249 Standard Database API Specification v2.0"""
class ConnectionABC(metaclass = ABCMeta):
    """
        The following abtract class defines a general database connection.  
        A connection is used in order to connect to a database and perform operations on it.
    """

    @abstractmethod
    def close(self) -> None:
        """Function used to close the database connection."""
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        """Function used to commit the transaction."""
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        """Function used to rollback the transaction."""
        raise NotImplementedError

    @abstractmethod
    def cursor(self) -> Any:
        """Function used to create a cursor."""
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
                hasattr(subclass, 'close') and callable(subclass.close)
            and hasattr(subclass, 'commit') and callable(subclass.commit)
            and hasattr(subclass, 'rollback') and callable(subclass.rollback)
            and hasattr(subclass, 'cursor') and callable(subclass.cursor)
            or NotImplemented
        )

class DatabaseConnectorABC(metaclass = ABCMeta):
    """
        The following abtract class defines a general database connector.  
        A connector is used in order to connect to a database and perform operations on it.
    """

    connection: ConnectionABC
    """Database connection."""

    @abstractmethod
    def _connect(self) -> ConnectionABC:
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

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
                hasattr(subclass, '_connect') and callable(subclass._connect)
            and hasattr(subclass, '__enter__') and callable(subclass.__enter__)
            and hasattr(subclass, '__exit__') and callable(subclass.__exit__)
            and hasattr(subclass, 'ping') and callable(subclass.ping)
            and hasattr(subclass, 'execute') and callable(subclass.execute)
            and hasattr(subclass, 'batch_execute') and callable(subclass.batch_execute)
            or NotImplemented
        )