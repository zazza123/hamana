import logging
from enum import Flag, auto
from collections.abc import Callable

import pandas as pd
from pydantic import BaseModel

from .schema import SQLiteDataImportMode
from .exceptions import QueryResultNotAvailable, QueryColumnsNotAvailable, ColumnDataTypeConversionError

# set logging
logger = logging.getLogger(__name__)

# param value custom type
ParamValue = int | float | str | bool
QueryColumnParserType = Callable[[pd.Series], pd.Series]

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
    """Datetime data type."""

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
        elif "datetime" in dtype:
            return ColumnDataType.DATETIME
        else:
            logger.warning(f"unknown data type: {dtype}")
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
            return "REAL"
        else:
            return ""

class QueryColumnParser:
    """
        Class representing a parser used to convert a column to a specific data type.
        A parser is used during a query execution; each column can have a specific parser.

        It is composed by a function for every `ColumnDataType` type. 
        Observe that each function must have a single parameter, which is a pandas Series, 
        and must return a pandas Series. If no function is provided for a specific data type, 
        then a default function is used.

        Parameters:
            to_int: function to convert a column of `ColumnDataType.INTEGER` type.
                Default function converts the column to `int64` using the `astype` method.
            to_number: function to convert a column of `ColumnDataType.NUMBER` type.
                Default function converts the column to `float64` using the `astype` method.
            to_text: function to convert a column of `ColumnDataType.TEXT` type.
                Default function converts the column to `object` using the `astype` method.
            to_boolean: function to convert a column of `ColumnDataType.BOOLEAN` type.
                Default function converts the column to `bool` using the `astype` method.
            to_datetime: function to convert a column of `ColumnDataType.DATETIME` type.
                Default function converts the column to `datetime` using the `pd.to_datetime` method.
    """

    def __init__(
        self, 
        to_int: QueryColumnParserType | None = None,
        to_number: QueryColumnParserType | None = None,
        to_text: QueryColumnParserType | None = None,
        to_boolean: QueryColumnParserType | None = None,
        to_datetime: QueryColumnParserType | None = None
    ) -> None:

        self.boolean_mapper = {
            "True": True, "False": False,
            "true": True, "false": False,
            "1": True, "0": False,
            "Y": True, "N": False,
            1: True, 0: False
        }
        """Mapper used to convert boolean values."""

        # set default parsers
        self.to_int = to_int if to_int else lambda series: series.astype("int64")
        self.to_number = to_number if to_number else lambda series: series.astype("float64")
        self.to_text = to_text if to_text else lambda series: series.astype("object")
        self.to_boolean = to_boolean if to_boolean else lambda series: series.map(self.boolean_mapper)
        self.to_datetime = to_datetime if to_datetime else lambda series: pd.to_datetime(series)

    def parse(self, series: pd.Series, dtype: ColumnDataType) -> pd.Series:
        """
            Function to parse a column to a specific data type.

            Parameters:
                series: pandas Series to be parsed.
                dtype: data type to parse the column.

            Returns:
                pandas Series parsed to the specific data type.
        """
        match dtype:
            case ColumnDataType.INTEGER:
                series = self.to_int(series)
            case ColumnDataType.NUMBER:
                series = self.to_number(series)
            case ColumnDataType.TEXT:
                series = self.to_text(series)
            case ColumnDataType.BOOLEAN:
                series = self.to_boolean(series)
            case ColumnDataType.DATETIME:
                series = self.to_datetime(series)
        return series

class QueryColumn:
    """
        Class to represent a column returned by a query. 
        A column is represented by its order, source and name.
    """

    def __init__(
        self,
        order: int,
        name: str,
        dtype: ColumnDataType = ColumnDataType.TEXT,
        parser: QueryColumnParser = QueryColumnParser()
    ) -> None:
        self.order = order
        self.name = name
        self.dtype = dtype
        self.parser = parser

    order: int
    """Order of the column in the query result."""

    name: str
    """Name of the column provided by the database result."""

    dtype: ColumnDataType
    """
        Data type of the column.  
        The data type is used to map the column to the application data.
    """

    parser: QueryColumnParser
    """
        Parser used to convert the column to a specific data type.
        The parser is used during the query execution.
    """

    def __eq__(self, value: object) -> bool:
        if isinstance(value, QueryColumn):
            return (self.order, self.name, self.dtype) == (value.order, value.name, value.dtype)
        return NotImplemented

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
            - `datetime` columns are mapped to `REAL` data type, with the values 
            converted to a float number using the following format: `YYYYMMDD.HHmmss`.
                Observes that the integer part represents the date in the format YYYYMMDD,
                while the decimal part represents the time component in the format HHmmss.

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
                        df_insert[column.name] = df_insert[column.name].dt.strftime("%Y%m%d.%H%M%S").astype(float)

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
            This function is used to adjust a DataFrame (usually 
            the result of a query) based on the columns provided.

            The function re-orders the columns of the DataFrame 
            and check the data types; if they do not match, then 
            the function will try to convert the requested one.

            Parameters:
                df: DataFrame to adjust
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
        for col in sorted(columns, key = lambda col : col.order):
            columns_query.append(col.name)

        # check columns_query is a subset of columns_df
        if not set(columns_query).issubset(columns_df):
            logger.error("columns do not match between query and resuls")
            raise QueryColumnsNotAvailable(f"columns do not match {set(columns_query).difference(columns_df)}")

        # re-order
        if columns_query != columns_df:
            logger.info("re-ordering columns")
            logger.info(f"order > {columns_query}")
            df = df[columns_query]
        else:
            logger.info("columns already in the correct order")

        # check data types
        logger.info("check data types")
        dtypes_df = df.dtypes
        for column in columns:

            dtype_query = column.dtype
            dtype_df = ColumnDataType.from_pandas(dtypes_df[column.name].name)
            logger.debug(f"column: {column.name}")
            logger.debug(f"datatype (query): {dtype_query}")
            logger.debug(f"datatype (df): {dtype_df}")

            if dtype_query != dtype_df:
                try:
                    logger.info(f"different datatype for '{column.name}' column -> (query) {dtype_query} != (df) {dtype_df}")
                    df[column.name] = column.parser.parse(df[column.name], dtype_query)
                except Exception as e:
                    logger.error("ERROR: on datatype change")
                    logger.error(e)
                    raise ColumnDataTypeConversionError(f"ERROR: on datatype change for {column.name}")

        logger.debug("end")
        return df

    def __str__(self) -> str:
        return self.query