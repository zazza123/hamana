from __future__ import annotations
import logging
from typing import Any, Generator, overload

from oracledb import Connection, ConnectParams
from oracledb.exceptions import OperationalError

from .query import Query
from .base import BaseConnector
from .schema import DatabaseConnectorConfig
from .exceptions import DatabaseConnetionError, ColumnDataTypeConversionError
from ...core.column import Column, NumberColumn, StringColumn, DatetimeColumn, DateColumn

# set logger
logger = logging.getLogger(__name__)

class OracleConnectorConfig(DatabaseConnectorConfig):
    """
        Class to represent the configuration of an Oracle database.
    """

    port: int = 1521
    """Port of the database. Default is 1521."""

    service: str | None = None
    """Service name of the database."""

    data_source_name: str | None = None
    """DSN connection string to connect on the database."""

    @property
    def connect_params(self) -> ConnectParams:
        """Get the connection parameters to connect on the database."""
        return ConnectParams(host = self.host, port = self.port, service_name = self.service, user = self.user, password = self.password) # type: ignore

    def get_data_source_name(self) -> str:
        """Get the DSN connection string to connect on the database."""
        return self.data_source_name if self.data_source_name else self.connect_params.get_connect_string()


class OracleConnector(BaseConnector):
    """
        Class representing a connector to an Oracle database.
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

            Returns:
                Oracle connector configuration.
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

            Returns:
                Oracle connector.
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
        return Connection(dsn = self.config.get_data_source_name(), params = self.config.connect_params)

    def ping(self) -> None:
        logger.debug("start")

        try:
            super().ping()
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError("unable to establish connection with database.")
        except Exception as e:
            raise e

        logger.debug("end")
        return

    def get_column_from_dtype(self, dtype: Any, column_name: str, order: int) -> Column:
        logger.debug("start")

        column = None
        dtype_str = dtype.name
        match dtype_str:
            case "DB_TYPE_BINARY_DOUBLE " | "DB_TYPE_BINARY_FLOAT" | "DB_TYPE_BINARY_INTEGER" | "DB_TYPE_NUMBER":
                column = NumberColumn(name = column_name, order = order)
            case "DB_TYPE_CHAR" | "DB_TYPE_LONG" | "DB_TYPE_NCHAR" | "DB_TYPE_NVARCHAR" | "DB_TYPE_VARCHAR" | "DB_TYPE_ROWID" | "DB_TYPE_UROWID":
                column = StringColumn(name = column_name, order = order)
            case "DB_TYPE_DATE":
                column = DateColumn(name = column_name, order = order, format = "%Y-%m-%d 00:00:00")
            case "DB_TYPE_TIMESTAMP" | "DB_TYPE_TIMESTAMP_LTZ" | "DB_TYPE_TIMESTAMP_TZ":
                column = DatetimeColumn(name = column_name, order = order)
            case _:
                raise ColumnDataTypeConversionError(f"Data type {dtype} does not have a corresponding mapping.")

        logger.debug("end")
        return column

    @overload
    def execute(self, query: str) -> Query: ...

    @overload
    def execute(self, query: Query) -> None: ...

    def execute(self, query: Query | str) -> None | Query:
        try:
            return super().execute(query)
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError("unable to establish connection with database.")
        except Exception as e:
            raise e

    def batch_execute(self, query: Query, batch_size: int) -> Generator[list[tuple], None, None]:
        try:
            return super().batch_execute(query, batch_size)
        except OperationalError as e:
            logger.exception(e)
            raise DatabaseConnetionError("unable to establish connection with database.")
        except Exception as e:
            raise e