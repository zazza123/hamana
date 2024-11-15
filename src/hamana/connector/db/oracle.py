from __future__ import annotations
import logging
from typing import Any, Generator, overload

from pandas import DataFrame
from oracledb import Connection, ConnectParams
from oracledb.exceptions import OperationalError

from .base import BaseConnector
from .config import DatabaseConnectorConfig
from .exceptions import DatabaseConnetionError
from ...query import Query, QueryColumn

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


class OracleConnector(BaseConnector):
    """
        Class to represent a connector to an Oracle database.
    """

    def __init__(self, config: OracleConnectorConfig, **kwargs: dict[str, Any]) -> None:
        self.config = config
        self.kwargs = kwargs
        self.connection: Connection

    @classmethod
    def create_config(
        cls,
        user: str,
        password: str,
        host: str | None = None,
        service: str | None = None,
        port: int = 1521,
        data_source_name: str | None = None
    ) -> OracleConnectorConfig:
        """
            Use this function to create a new Oracle connector configuration.

            Parameters:
                user: User to connect to the database.
                password: Password to uso to connect to the database.
                host: Host of the database.
                service: Service name of the database.
                port: Port of the database.
                data_source_name: DSN connection string to connect on the database. 
                    Observe that if DSN is provided, then host, service and port are ignored.
        """
        logger.debug("start")

        if data_source_name:
            logger.debug("data source name provided")
            config = OracleConnectorConfig(user = user, password = password, data_source_name = data_source_name)
        else:
            logger.debug(f"host: {host}, service: {service}, port: {port}")
            config = OracleConnectorConfig(user = user, password = password, host = host, service = service, port = port)

        logger.debug("end")
        return config

    @classmethod
    def new(
        cls,
        user: str,
        password: str,
        host: str | None = None,
        service: str | None = None,
        port: int = 1521,
        data_source_name: str | None = None
    ) -> "OracleConnector":
        """
            Use this function to create a new Oracle connector.

            Parameters:
                user: User to connect to the database.
                password: Password to uso to connect to the database.
                host: Host of the database.
                service: Service name of the database.
                port: Port of the database.
                data_source_name: DSN connection string to connect on the database. 
                    Observe that if DSN is provided, then host, service and port are ignored.
        """
        logger.debug("start")
        config = cls.create_config(
            user = user,
            password = password,
            host = host,
            service = service,
            port = port,
            data_source_name = data_source_name
        )
        logger.debug("end")
        return cls(config)

    def _connect(self) -> Connection:
        return Connection(dsn= self.config.get_data_source_name(), params = self.config.connect_params)

    def ping(self) -> None:
        logger.debug("start")

        try:
            super().ping()
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError("unable to establish connection with database.")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        return

    @overload
    def execute(self, query: str) -> Query: ...

    @overload
    def execute(self, query: Query) -> None: ...

    def execute(self, query: Query | str) -> None | Query:
        logger.debug("start")

        flag_query_str = isinstance(query, str)
        if flag_query_str:
            logger.info("query string provided")
            query = Query(query)

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data (using: {self.config.user}) ...")
                with conn.connection.cursor() as cursor:

                    # execute query
                    cursor.execute(query.query, parameters = query.get_params()) # type: ignore
                    logger.info(f"query: {query.query}")
                    logger.info(f"parameters: {query.get_params()}")

                    # set columns
                    columns = [QueryColumn(order = i, source = desc[0]) for i, desc in enumerate(cursor.description)]

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

        # set query result
        query.result = df_result

        logger.debug("end")
        return query if flag_query_str else None

    def batch_execute(self, query: Query, batch_size: int) -> Generator[list[tuple], None, None]:
        logger.debug("start")

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data (using: {self.config.user}) ...")
                with conn.connection.cursor() as cursor:

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