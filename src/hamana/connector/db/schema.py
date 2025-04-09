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