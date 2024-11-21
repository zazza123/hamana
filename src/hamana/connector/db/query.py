import logging

from pandas import DataFrame
from pydantic import BaseModel

from .exceptions import QueryResultNotAvailable, QueryColumnsNotAvailable

# set logging
logger = logging.getLogger(__name__)

# param value custom type
ParamValue = int | float | str | bool

class QueryColumn(BaseModel):
    """
        Class to represent a column returned by a query. 
        A column is represented by its order, source and name.
    """

    order: int
    """Order of the column in the query result."""

    source: str
    """Source name of the column provided by the database result."""

    name: str | None = None
    """
        Name of the column to be used in the application. 
        Observe that the source name is used if the name is not provided.
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

    result: DataFrame | None = None
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

    def to_sqlite(self, table_name: str) -> None:
        """
            This function is used to insert the query result into a 
            table hosted on the hamana internal database (HamanaDatabase). 

            Parameters:
                table_name: name of the table to create into the database.

            Raises:
                QueryResultNotAvailable: if no result is available.
        """
        logger.debug("start")

        # check result 
        if self.result is None:
            logger.error("no result to save")
            raise QueryResultNotAvailable("no result to save")

        # import internal database
        from ...core.db import HamanaDatabase

        # get instance
        db = HamanaDatabase.get_instance()
        with db as conn:
            logger.debug(f"inserting data into table {table_name}")
            self.result.to_sql(
                name = table_name,
                con = conn.connection,
                if_exists = "replace",
                index = False
            )
            logger.info(f"data inserted into table {table_name}")

        logger.debug("end")
        return

    def get_insert_query(self, table_name: str) -> str:
        """
            This function returns a query to insert the query result into a table.

            Parameters:
                table_name: name of the table to insert the data.

            Returns:
                query to insert the data into the table.
        """
        logger.debug("start")

        # check columns availablity
        if self.columns is None:
            logger.error("no columns available")
            raise QueryColumnsNotAvailable("no columns available")

        # get columns
        columns = ", ".join([column.source for column in self.columns])
        logger.debug("columns string created")

        # get values
        values = ", ".join(["?" for _ in self.columns])
        logger.debug("values string created")

        # build query
        query = f"INSERT INTO {table_name.upper()} ({columns}) VALUES ({values})"
        logger.info(f"query to insert data into table {table_name} created")
        logger.info(f"query: {query}")

        logger.debug("end")
        return query

    def get_create_query(self, table_name: str) -> str:
        """
            This function returns a query to create a table based on the query result.

            Parameters:
                table_name: name of the table to be created.

            Returns:
                query to create the table.
        """
        logger.debug("start")

        # check columns availablity
        if self.columns is None:
            logger.error("no columns available")
            raise QueryColumnsNotAvailable("no columns available")

        # get columns
        columns = ", ".rjust(4).join([f"{column.source}\n" for column in self.columns])
        logger.debug("columns string created")

        # build query
        query = "CREATE TABLE " + table_name.upper() + " (\n" + \
                "".rjust(4) + ", ".rjust(4).join([f"{column.source}\n" for column in self.columns]) + \
                ")"
        logger.info(f"query to create table {table_name} created")
        logger.info(f"query: {query}")

        logger.debug("end")
        return query

    def __str__(self) -> str:
        return self.query