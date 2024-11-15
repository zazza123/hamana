from pydantic import BaseModel

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