from enum import Enum
from pydantic import BaseModel

class SQLiteDataImportMode(str, Enum):
    """
        Enum to represent the mode of importing data to an 
        SQLite database via `to_sqlite` method.
    """

    REPLACE = "replace"
    """Replace the data in the table."""

    APPEND = "append"
    """Append the data to the table."""

    FAIL = "fail"
    """Fail if the table already exists."""

class BooleanMapper(BaseModel):
    """
        Class used to define a list of values to me mapped 
        to a boolean value.
    """

    true: list[str | int | float]
    """List of values to be mapped to `True`."""

    false: list[str | int | float]
    """List of values to be mapped to `False`."""

    def to_pandas_map(self) -> dict:
        """
            Method to convert the class to a dictionary that can 
            be used to map values in a pandas `DataFrame` via 
            `map`method.

            Returns:
                dict: The output dictionary has a structure of `{value: boolean}`.
        """

        return {**{value: True for value in self.true}, **{value: False for value in self.false}}

class DatabaseConnectorConfig(BaseModel):
    """
        Class to represent the **base** configuration of a database.
    """

    host: str | None = None
    """Host of the database."""

    port: int = 0
    """Port of the database."""

    user: str | None = None
    """User to connect to the database."""

    password: str | None = None
    """Password of the user to connect to the database."""