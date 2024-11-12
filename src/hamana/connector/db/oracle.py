from __future__ import annotations
import logging
from types import TracebackType
from typing import Any, Generator, Type

from pandas import DataFrame
from oracledb import Connection, ConnectParams
from oracledb.exceptions import OperationalError

from ...query import Query, QueryColumn

from .config import DatabaseConnectorConfig
from .interface import DatabaseConnectorABC
from .exceptions import DatabaseConnetionError

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

    def __exit__(self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, exc_traceback: TracebackType | None) -> None:
        """
            Close a connection.
        """
        logger.debug("start")

        # log exception
        if exc_type is not None:
            logger.error(f"exception occurred: {exc_type}")
            logger.error(f"exception value: {exc_value}")
            logger.error(f"exception traceback: {exc_traceback}")

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

    def execute(self, query: Query) -> DataFrame:
        logger.debug("start")

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data (using: {self.config.user}) ...")
                with conn.connector.cursor() as cursor:

                    # execute query
                    cursor.execute(query.query, parameters = query.get_params()) # type: ignore
                    logger.info(f"query: {query.query}")
                    logger.info(f"parameters: {query.get_params()}")

                    # fetch results
                    result = cursor.fetchall()
                    logger.info(f"data extracted ({cursor.rowcount} rows)")
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError(f"unable to establish connection with database")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("convert to Dataframe")
        columns = [QueryColumn(order = i, source = desc[0]) for i, desc in enumerate(cursor.description)]
        df_result = DataFrame(result, columns = [column.source for column in columns])

        # adjust columns
        if query.columns:
            logger.info("adjust columns")

            rename = {}
            for col in sorted(query.columns, key = lambda col : col.order):
                rename[col.source] = col.name if col.name else col.source
            order = list(rename.values())

            # re-name
            logger.info(f"rename > {rename}")
            df_result = df_result.rename(columns = rename)

            # re-order
            logger.info(f"order > {order}")
            df_result = df_result[order]
        else:
            logger.info("query column updated")
            query.columns = columns

        logger.debug("end")
        return df_result

    def batch_execute(self, query: Query, batch_size: int) -> Generator[list[tuple], None, None]:
        logger.debug("start")

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data (using: {self.config.user}) ...")
                with conn.connector.cursor() as cursor:

                    # execute query
                    cursor.execute(query.query, parameters = query.get_params()) # type: ignore
                    logger.info(f"query: {query.query}")
                    logger.info(f"parameters: {query.get_params()}")

                    # fetch in batches
                    while True:
                        results = cursor.fetchmany(batch_size)
                        if not results:
                            break
                        yield results
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError(f"unable to establish connection with database")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        return