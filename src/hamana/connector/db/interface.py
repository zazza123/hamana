from typing import Any, Generator
from abc import ABCMeta, abstractmethod

from ...query import Query

class DatabaseConnectorABC(metaclass = ABCMeta):
    """
        The following abtract class defines a general database connector.  
        A connector is used in order to connect to a database and perform operations on it.
    """

    @abstractmethod
    def ping(self) -> None:
        """Function used to check the database connection."""
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: Query) -> Any:
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
                hasattr(subclass, 'ping') and callable(subclass.ping)
            and hasattr(subclass, 'execute') and callable(subclass.execute)
            and hasattr(subclass, 'batch_execute') and callable(subclass.batch_execute)
            or NotImplemented
        )