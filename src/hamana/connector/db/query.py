import logging
from dataclasses import dataclass

import pandas as pd
from pathlib import Path
from typing import TypeVar, Generic

from ...core.column import Column, DataType
from .schema import SQLiteDataImportMode
from .exceptions import QueryInitializationError, QueryResultNotAvailable, QueryColumnsNotAvailable, ColumnDataTypeConversionError

# set logging
logger = logging.getLogger(__name__)

# param value custom type
ParamValue = int | float | str | bool
TColumn = TypeVar("TColumn", bound = Column, covariant = True)

@dataclass
class QueryParam:
    """
        Class to represent a parameter used in a query.  
        A parameter is represented by a name and its value.

        Usually, parameters are used to define general query conditions 
        and are replaced by the actual values when the query is executed.
    """

    name: str
    """Name of the parameter."""

    value: ParamValue
    """Value of the parameter."""

class Query(Generic[TColumn]):
    """
        Class to represent a query object.
    """

    def __init__(
        self,
        query: str | Path,
        columns: list[TColumn] | None = None,
        params: list[QueryParam] | dict[str, ParamValue] | None = None
    ) -> None:

        # setup query
        if isinstance(query, Path):
            logger.info(f"loading query from file: {query}")

            if not query.exists():
                raise QueryInitializationError(f"file {query} not found")

            self.query = query.read_text()
        elif isinstance(query, str):
            if Path(query).exists():
                logger.info(f"loading query from file: {query}")
                with open(query, "r") as f:
                    self.query = f.read()
            else:
                self.query = query

        self.columns = columns
        self.params = params

    query: str
    """
        Query to be executed in the database. It is possible to provide directly the 
        SQL query as a string or to load it from a file by providing the file path.
    """

    params: list[QueryParam] | dict[str, ParamValue] | None = None
    """
        List of parameters used in the query. 
        The parameters are replaced by their values when the query is executed.
    """

    columns: list[TColumn] | None = None
    """
        Definition of the columns returned by the query. 
        The columns are used to map the query result to the application data.
        If not provided, then the columns are inferred from the result.
    """

    flag_executed: bool = False
    """Flag to indicate if the query has been executed."""

    @property
    def result(self) -> pd.DataFrame:
        """
            Result of the query execution. 
            The result is a `pandas.DataFrame` with columns
            equals the ones defined in the `columns` attribute, 
            or inferred from the extraction.

            Raises:
                QueryResultNotAvailable: if no result is available; e.g., the query has not been executed.
        """
        if not self.flag_executed:
            logger.error("query not executed")
            raise QueryResultNotAvailable("query not executed")
        return self._result

    @result.setter
    def result(self, value: pd.DataFrame) -> None:
        self._result = value
        self.flag_executed = True

    def get_params(self) -> dict[str, ParamValue] | None:
        """
            Returns the query parameters as a dictionary.
            Returns `None` if there are no parameters.
        """
        if isinstance(self.params, list):
            _params = {param.name : param.value for param in self.params}
        else:
            _params = self.params
        return _params

    def to_sqlite(self, table_name: str, mode: SQLiteDataImportMode = SQLiteDataImportMode.REPLACE) -> None:
        """
            This function is used to insert the query result into a 
            table hosted on the `hamana` internal database (`HamanaConnector`).

            The `hamana` db is a SQLite database, for this reason 
            `bool`, `datetime` and `timestamp` data types are **not** supported.
            If some of the columns are defined with these data types, 
            then the method performs an automatic conversion to a SQLite data type.

            In particular, the conversions are:

            - `bool` columns are mapped to `INTEGER` data type, with the values 
            `True` and `False` converted to `1` and `0`.
            - `date` and `datetime` columns are mapped to `INTEGER` datatype, with the values 
                converted to an int number using the following format: `YYYYMMDDHHmmss`
                for `dateitme`, `YYYYMMDD` for `date`.

            Parameters:
                table_name: name of the table to create into the database.
                    By assumption, the table's name is converted to uppercase.
                mode: mode of importing the data into the database.
        """
        logger.debug("start")
        df_insert = self.result.copy()

        # set dtype
        columns_dtypes: dict | None = None
        if self.columns is not None:
            columns_dtypes = {}
            for column in self.columns:
                columns_dtypes[column.name] = DataType.to_sqlite(column.dtype)

                # convert columns
                match column.dtype:
                    case DataType.BOOLEAN:
                        df_insert[column.name] = df_insert[column.name].astype(int)
                    case DataType.DATE:
                        df_insert[column.name] = df_insert[column.name].dt.strftime("%Y%m%d").astype(int)
                    case DataType.DATETIME:
                        df_insert[column.name] = df_insert[column.name].dt.strftime("%Y%m%d%H%M%S").astype(int)

        # import internal database
        from .hamana import HamanaConnector

        # get instance
        db = HamanaConnector.get_instance()

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
                "".rjust(4) + ", ".rjust(4).join([f"{column.name} {DataType.to_sqlite(column.dtype)}\n" for column in self.columns]) + \
                ")"
        logger.info(f"query to create table {table_name_upper} created")
        logger.info(f"query: {query}")

        logger.debug("end")
        return query

    def get_column_names(self) -> list[str]:
        """
            This function returns the column names of the query.

            Returns:
                list of column names.
        """
        logger.debug("start")

        # check columns availablity
        if self.columns is None:
            logger.error("no columns available")
            raise QueryColumnsNotAvailable("no columns available")
        columns = [column.name for column in self.columns]

        logger.debug("end")
        return columns

    def adjust_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
            This function is used to adjust a `pandas.DataFrame` (usually 
            the result of a query) based on the columns provided.

            The function re-orders the columns of the `DataFrame` 
            and checks the data types; if they do not match, then 
            the function will try to convert the requested one.

            Parameters:
                df: DataFrame to adjust

            Raises:
                QueryColumnsNotAvailable: if the columns do not match between the query and the result.
                ColumnDataTypeConversionError: if there is an error during the data type conversion.
        """
        logger.debug("start")

        # check columns availablity
        if self.columns is None:
            logger.error("no columns available")
            raise QueryColumnsNotAvailable("no columns available")
        columns = self.columns

        # get columns
        columns_query = []
        columns_df = df.columns.to_list()

        logger.info("get query columns ordered")
        for col in sorted(columns, key = lambda col: col.order if col.order is not None else 0):
            columns_query.append(col.name)

        # check columns_query is a subset of columns_df
        if not set(columns_query).issubset(columns_df):
            logger.error("columns do not match between query and resuls")
            raise QueryColumnsNotAvailable(f"columns do not match {set(columns_query).difference(columns_df)}")

        # re-order
        if columns_query != columns_df:
            logger.info("re-ordering columns")
            logger.info(f"order > {columns_query}")
            df = df.copy()[columns_query]
        else:
            logger.info("columns already in the correct order")

        # check data types
        logger.info("check data types")
        dtypes_df = df.dtypes
        for column in columns:

            dtype_query = column.dtype
            dtype_df = DataType.from_pandas(dtypes_df[column.name].name)
            logger.debug(f"column: {column.name}")
            logger.debug(f"datatype (query): {dtype_query}")
            logger.debug(f"datatype (df): {dtype_df}")

            if dtype_query != dtype_df:
                try:
                    logger.info(f"different datatype for '{column.name}' column -> (query) {dtype_query} != (df) {dtype_df}")

                    if column.parser is None:
                        logger.warning(f"no parser available for {column.name} (order: {column.order})")
                        logger.warning("skip column")
                        continue

                    df[column.name] = column.parser.pandas(df[column.name])
                except Exception as e:
                    logger.error("ERROR: on datatype change")
                    logger.error(e)
                    raise ColumnDataTypeConversionError(f"ERROR: on datatype change for {column.name} (order: {column.order})")

        logger.debug("end")
        return df

    def __str__(self) -> str:
        return self.query