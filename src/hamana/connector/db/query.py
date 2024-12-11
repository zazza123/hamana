import logging
from enum import Flag, auto

import pandas as pd
from pydantic import BaseModel

from .schema import SQLiteDataImportMode
from .exceptions import QueryResultNotAvailable, QueryColumnsNotAvailable

# set logging
logger = logging.getLogger(__name__)

# param value custom type
ParamValue = int | float | str | bool

class ColumnDataType(Flag):
    """
        Enumeration to represent the data types of the columns.
    """

    INTEGER = auto()
    """Integer data type."""

    NUMBER = auto()
    """Number data type."""

    TEXT = auto()
    """String data type."""

    BOOLEAN = auto()
    """Boolean data type."""

    DATETIME = auto()
    """Date data type."""

    TIMESTAMP = auto()
    """Timestamp data type."""

    @classmethod
    def from_pandas(cls, dtype: str) -> "ColumnDataType":
        """
            Function to map a pandas data type to a ColumnDataType.

            Parameters:
                dtype: pandas data type.

            Returns:
                ColumnDataType mapped from the pandas data type.
        """
        if "int" in dtype:
            return ColumnDataType.INTEGER
        elif "float" in dtype:
            return ColumnDataType.NUMBER
        elif dtype == "object":
            return ColumnDataType.TEXT
        elif dtype == "bool":
            return ColumnDataType.BOOLEAN
        elif dtype == "datetime64":
            return ColumnDataType.DATETIME
        elif dtype == "datetime64[ns, UTC]":
            return ColumnDataType.TIMESTAMP
        else:
            return ColumnDataType.TEXT

    @classmethod
    def to_sqlite(cls, dtype: "ColumnDataType") -> str:
        """
            Function to map a ColumnDataType to a SQLite data type.

            Parameters:
                dtype: ColumnDataType to be mapped.

            Returns:
                SQLite data type mapped from the ColumnDataType.
        """
        if dtype == ColumnDataType.INTEGER:
            return "INTEGER"
        elif dtype == ColumnDataType.NUMBER:
            return "REAL"
        elif dtype == ColumnDataType.TEXT:
            return "TEXT"
        elif dtype == ColumnDataType.BOOLEAN:
            return "INTEGER"
        elif dtype == ColumnDataType.DATETIME:
            return "TEXT"
        elif dtype == ColumnDataType.TIMESTAMP:
            return "INTEGER"
        else:
            return ""

class QueryColumn(BaseModel):
    """
        Class to represent a column returned by a query. 
        A column is represented by its order, source and name.
    """

    order: int
    """Order of the column in the query result."""

    name: str
    """Name of the column provided by the database result."""

    dtype: ColumnDataType = ColumnDataType.TEXT
    """
        Data type of the column.  
        The data type is used to map the column to the application data.
    """

class QueryParam(BaseModel):
    """
        Class to represent a parameter used in a query.  
        A paramter is represented by a name and its value.

        Usually, parameters are used to define general query conditions 
        and are replaced by the actual values when the query is executed.
    """

    name: str
    """Name of the parameter."""

    value: ParamValue
    """Value of the parameter."""

class Query:
    """
        Class to represent a query to be executed in the database.
    """

    def __init__(
        self,
        query: str,
        columns: list[QueryColumn] | None = None,
        params: list[QueryParam] | dict[str, ParamValue] | None = None
    ) -> None:
        self.query = query
        self.columns = columns
        self.params = params

    query: str
    """Query to be executed in the database."""

    params: list[QueryParam] | dict[str, ParamValue] | None = None
    """
        List of parameters used in the query.  
        The parameters are replaced by their values when the query is executed.
    """

    columns: list[QueryColumn] | None = None
    """
        Definition of the columns returned by the query. 
        The columns are used to map the query result to the application data.
        If not provided, the query result columns matches the database output.
    """

    result: pd.DataFrame | None = None
    """
        Result of the query execution. 
        The result is a DataFrame with the columns defined in the query.
    """

    def get_params(self) -> dict[str, ParamValue] | None:
        """
            Returns the query parameters as a dictionary.
            Returns None if there are no parameters.
        """
        if isinstance(self.params, list):
            _params = {param.name : param.value for param in self.params}
        else:
            _params = self.params
        return _params

    def to_sqlite(self, table_name: str, mode: SQLiteDataImportMode = SQLiteDataImportMode.REPLACE) -> None:
        """
            This function is used to insert the query result into a 
            table hosted on the `hamana` internal database (HamanaDatabase).

            The `hamana` db is a SQLite database, for this reason 
            `bool`, `datetime` and `timestamp` data types are supported.
            If some of the columns are defined with these data types, 
            then the method performs an automatic conversion to a SQLite data type.

            In particular, the conversions are:
            - `bool` columns are mapped to `INTEGER` data type, with the values 
            `True` and `False` converted to `1` and `0`.
            - `datetime` columns are mapped to `TEXT` data type, with the values 
            converted to a string in the format `YYYY-MM-DD`.
            - `timestamp` columns are mapped to `NUMERIC` data type, with the values
            converted to a float representing the Unix timestamp.

            Parameters:
                table_name: name of the table to create into the database.
                    By assumption, the table's name is converted to uppercase.
                mode: mode of importing the data into the database.

            Raises:
                QueryResultNotAvailable: if no result is available.
        """
        logger.debug("start")

        # check result 
        if self.result is None:
            logger.error("no result to save")
            raise QueryResultNotAvailable("no result to save")
        df_insert = self.result.copy()

        # set dtype
        columns_dtypes: dict | None = None
        if self.columns is not None:
            columns_dtypes = {}
            for column in self.columns:
                columns_dtypes[column.name] = ColumnDataType.to_sqlite(column.dtype)

                # convert columns
                match column.dtype:
                    case ColumnDataType.BOOLEAN:
                        df_insert[column.name] = df_insert[column.name].astype(int)
                    case ColumnDataType.DATETIME:
                        df_insert[column.name] = df_insert[column.name].dt.strftime("%Y-%m-%d")
                    case ColumnDataType.TIMESTAMP:
                        # convert to Unix timestamp, suggested by pandas documentation
                        df_insert[column.name] = (df_insert[column.name] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

        # import internal database
        from ...core.db import HamanaDatabase

        # get instance
        db = HamanaDatabase.get_instance()

        # insert data
        table_name_upper = table_name.upper()
        try:
            with db:
                logger.debug(f"inserting data into table {table_name_upper}")
                logger.debug(f"mode: {mode.value}")
                df_insert.to_sql(
                    name = table_name_upper,
                    con = db.connection,
                    if_exists = mode.value,
                    dtype = columns_dtypes,
                    index = False
                )
                logger.info(f"data inserted into table {table_name_upper}")
        except Exception as e:
            logger.error(f"error inserting data into table {table_name_upper}")
            logger.exception(e)
            raise e

        logger.debug("end")
        return

    def get_insert_query(self, table_name: str) -> str:
        """
            This function returns a query to insert the query result into a table.

            Parameters:
                table_name: name of the table to insert the data.
                    By assumption, the table's name is converted to uppercase.

            Returns:
                query to insert the data into the table.
        """
        logger.debug("start")

        # check columns availablity
        if self.columns is None:
            logger.error("no columns available")
            raise QueryColumnsNotAvailable("no columns available")

        # get columns
        columns = ", ".join([column.name for column in self.columns])
        logger.debug("columns string created")

        # get values
        values = ", ".join(["?" for _ in self.columns])
        logger.debug("values string created")

        # build query
        table_name_upper = table_name.upper()
        query = f"INSERT INTO {table_name_upper} ({columns}) VALUES ({values})"
        logger.info(f"query to insert data into table {table_name_upper} created")
        logger.info(f"query: {query}")

        logger.debug("end")
        return query

    def get_create_query(self, table_name: str) -> str:
        """
            This function returns a query to create a table based on the query result.

            Parameters:
                table_name: name of the table to be created.
                    By assumption, the table's name is converted to uppercase.

            Returns:
                query to create the table.
        """
        logger.debug("start")

        # check columns availablity
        if self.columns is None:
            logger.error("no columns available")
            raise QueryColumnsNotAvailable("no columns available")

        # build query
        table_name_upper = table_name.upper()
        query = "CREATE TABLE " + table_name_upper + " (\n" + \
                "".rjust(4) + ", ".rjust(4).join([f"{column.name} {ColumnDataType.to_sqlite(column.dtype)}\n" for column in self.columns]) + \
                ")"
        logger.info(f"query to create table {table_name_upper} created")
        logger.info(f"query: {query}")

        logger.debug("end")
        return query

    def __str__(self) -> str:
        return self.query