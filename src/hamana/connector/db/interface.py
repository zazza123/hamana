from __future__ import annotations
from typing import Any, Generator, Protocol, TYPE_CHECKING
from abc import ABCMeta, abstractmethod

from ...core.column import Column
from .schema import SQLiteDataImportMode

if TYPE_CHECKING:
    from .query import Query

"""PEP 249 Standard Database API Specification v2.0"""
class Cursor(Protocol):
    """
        The following abstract class defines a general database cursor.
    """

    @property
    def description(self) -> tuple[tuple[str, Any, Any, Any, Any, Any, Any], ...]:
        """
            This read-only attribute is a sequence of 7-item sequences. 
            Each of these sequences contains information describing one result column.

            According to the PEP 249 Standard Database API Specification v2.0, the 7-item
            sequence contains the following elements:

            - `name`: the name of the column returned.
            - `type_code`: the type of the column.
            - `display_size`: the actual length of the column.
            - `internal_size`: the size in bytes of the column.
            - `precision`: the precision of the column.
            - `scale`: the scale of the column.
            - `null_ok`: whether the column can contain null values.

            Observer, even if the PEP standard considers manadatory the 
            `name` and `type_code` fields, in practise some database 
            provides only the `name` field. All the other fields could
            be `None`.
        """
        ...

    def close(self) -> None:
        """Function used to close the cursor."""
        ...

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute a database operation."""
        ...

    def fetchmany(self, size: int) -> Any:
        """Fetch the next set of rows of a query result."""
        ...

    def fetchall(self) -> Any:
        """Fetch all the rows of a query result."""
        ...

class ConnectionProtocol(Protocol):
    """
        The following abstract class defines a general database connection.  
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

    def cursor(self) -> Cursor:
        """Function used to create a cursor."""
        ...

class DatabaseConnectorABC(metaclass = ABCMeta):
    """
        The following abstract class defines a general database connector.  
        A connector is used in order to connect to a database and perform operations on it.

        All `hamana` connectors must inherit from this class.
    """

    connection: ConnectionProtocol
    """
        Database connection.

        This attribute is used to store the connection to the database 
        and **must** satisfy the PEP 249 Standard Database API Specification v2.0.
    """

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
    def parse_cursor_description(self, cursor: Cursor) -> dict[str, Column | None]:
        """
            The function is used to take a cursor object, 
            parse its description, and extract a corresponding list 
            of `Column` objects.

            Parameters:
                cursor: cursor to parse.

            Returns:
                dictionary containing the columns extracted from the cursor.
        """
        raise NotImplementedError

    @abstractmethod
    def get_column_from_dtype(self, dtype: Any, column_name: str, order: int) -> Column:
        """
            This function is designed to create a mapping between the 
            datatypes provided by the different database connectors and 
            standard `hamana` datatypes.

            Parameters:
                dtype: data type of the column.
                column_name: name of the column.
                order: order of the column.

            Returns:
                `Column` object representing the column.

            Raises:
                ColumnDataTypeConversionError: if the datatype 
                    does not have a corresponding mapping.
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: Query | str) -> None | Query:
        """
            Function used to extract data from the database by 
            executin a SELECT query.

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
        raise NotImplementedError

    @abstractmethod
    def batch_execute(self, query: Query, batch_size: int) -> Generator[Any, None, None]:
        """
            Function used to execute a query on the database and return the results in batches. 
            This approach is used to avoid memory issues when dealing with large datasets.

            Parameters:
                query: query to execute on database.
                batch_size: size of the batch to return.

            Returns:
                Generator used to return the results in batches.
        """
        raise NotImplementedError

    @abstractmethod
    def to_sqlite(self, query: Query, table_name: str, raw_insert: bool = False, batch_size: int = 10_000, mode: SQLiteDataImportMode = SQLiteDataImportMode.REPLACE) -> None:
        """
            This function is used to extract data from the database and insert it 
            into the `hamana` internal database (`HamanaConnector`).

            The `hamana` db is a SQLite database, for this reason 
            `bool`, `datetime` and `timestamp` data types are not supported.
            If some of the columns are defined with these data types, 
            then the method could perform an automatic conversion to 
            a SQLite data type.

            The conversions are:

            - `bool` columns are mapped to `INTEGER` data type, with the values 
                `True` and `False` converted to `1` and `0`.
            - `datetime` columns are mapped to `REAL` data type, with the values 
                converted to a float number using the following format: `YYYYMMDD.HHmmss`.
                Observe that the integer part represents the date in the format `YYYYMMDD`,
                while the decimal part represents the time component in the format `HHmmss`.

            By default, the method performs the automatic datatype 
            conversion. However, use the parameter `raw_insert` to 
            **avoid** this conversion and improve the INSERT efficiency. 

            Parameters:
                query: query to execute on database.
                table_name: name of the table to insert the data.
                    By assumption, the table's name is converted to uppercase.
                raw_insert: bool value to disable/activate the datatype 
                    conversion during the INSERT process. By default, it is 
                    set to `False`.
                batch_size: size of the batch used during the inserting process.
                mode: mode of importing the data into the database.
        """
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
                hasattr(subclass, '_connect') and callable(subclass._connect)
            and hasattr(subclass, '__enter__') and callable(subclass.__enter__)
            and hasattr(subclass, '__exit__') and callable(subclass.__exit__)
            and hasattr(subclass, 'ping') and callable(subclass.ping)
            and hasattr(subclass, 'parse_cursor_description') and callable(subclass.parse_cursor_description)
            and hasattr(subclass, 'get_column_from_dtype') and callable(subclass.get_column_from_dtype)
            and hasattr(subclass, 'execute') and callable(subclass.execute)
            and hasattr(subclass, 'batch_execute') and callable(subclass.batch_execute)
            and hasattr(subclass, 'to_sqlite') and callable(subclass.to_sqlite)
            or NotImplemented
        )