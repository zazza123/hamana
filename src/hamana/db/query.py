from pydantic import BaseModel

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
        query: str, columns: list[QueryColumn] | None = None,
        params: list[QueryParam] | None = None
    ) -> None:
        self.query = query
        self.columns = columns
        self.params = params

    query: str
    """Query to be executed in the database."""

    params: list[QueryParam] | None
    """
        List of parameters used in the query.  
        The parameters are replaced by their values when the query is executed.
    """

    columns: list[QueryColumn] | None
    """
        Definition of the columns returned by the query. 
        The columns are used to map the query result to the application data.
        If not provided, the query result columns matches the database output.
    """

    def get_params(self) -> dict[str, ParamValue] | None:
        """
            Returns the query parameters as a dictionary.
            Returns None if there are no parameters.
        """
        return {param.name : param.value for param in self.params} if self.params else None