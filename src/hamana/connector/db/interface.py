from typing import Any
from abc import ABCMeta, abstractmethod

from .query import Query

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
            Function used to execute data from the database.

            Parameters:
                - query: query to execute on database.

            Return:
                dataframe with the resulting rows.
        """
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
                hasattr(subclass, 'ping') and callable(subclass.ping)
            and hasattr(subclass, 'execute') and callable(subclass.execute)
            or NotImplemented
        )