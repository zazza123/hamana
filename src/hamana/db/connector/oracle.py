from __future__ import annotations
import logging
from typing import Any, Generator, overload

from pydantic import computed_field
from oracledb import Connection, ConnectParams
from oracledb.exceptions import OperationalError

from hamana.db.query import Query

from ..connector.config import DatabaseConnectorConfig
from ..connector.interface import DatabaseConnectorABC
from ..connector.exceptions import DatabaseConnetionError

# set logger
logger = logging.getLogger(__name__)

class OracleConnectorConfig(DatabaseConnectorConfig):
    """
        Class to represent the configuration of an Oracle database.
    """
    
    port: int = 1521
    """Port of the Oracle database. Default is 1521."""

    data_source_name: str | None = None
    """DSN connection string to connect on the database."""

    @computed_field
    @property
    def connect_params(self) -> ConnectParams:
        return ConnectParams(host = self.host, port = self.port, service_name = self.service, user = self.user, password = self.password) # type: ignore
    
    def get_data_source_name(self) -> str:
        return self.data_source_name if self.data_source_name else self.connect_params.get_connect_string()
    

class OracleConnector(DatabaseConnectorABC):
    """
        Class to represent a connector to an Oracle database.
    """

    def __init__(self, config: OracleConnectorConfig, **kwargs: dict[str, Any]) -> None:
        self.config = config
        self.kwargs = kwargs
        self.connector: Connection

    def __enter__(self) -> "OracleConnector":
        """
            Open a connection.
        """
        logger.debug("start")
        self.connector = Connection(dsn= self.config.get_data_source_name(), params = self.config.connect_params)
        logger.info("connection opened")
        logger.debug("end")
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        """
            Close a connection.
        """
        logger.debug("start")

        if exc_type is not None:
            logger.warning(exc_type)

        self.connector.close()

        logger.info("connection closed")
        logger.debug("end")
        return

    def ping(self) -> None:
        logger.debug("start")

        try:
            with self as db:
                db.connector.ping()
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError("unable to establish connection with database.")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        return

    @overload
    def execute(self, query: Query, batch_size: None) -> list[tuple]:
        ...

    @overload
    def execute(self, query: Query, batch_size: int) -> Generator[list[tuple], None, None]:
        ...

    def execute(self, query: Query, batch_size: int | None = None) -> list[tuple] | Generator[list[tuple], None, None]:
        logger.debug("start")

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data (using: {self.config.user}) ...")
                with conn.connector.cursor() as cursor:
                    logger.info(query.query)

                    # execute query
                    cursor.execute(query.query, parameters = query.get_params()) # type: ignore

                    if batch_size is None:
                        # fetch all
                        results = cursor.fetchall()
                        logger.info(f"data extracted ({cursor.rowcount} rows)")
                    else:
                        # fetch in batches
                        results = cursor.fetchmany(batch_size)
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError(f"unable to establish connection with database")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        yield results