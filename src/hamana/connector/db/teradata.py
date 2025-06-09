import logging
import decimal
import datetime
from typing import Any

from teradatasql import TeradataConnection as Connection

from .base import BaseConnector
from .exceptions import ColumnDataTypeConversionError
from ...core.column import Column, NumberColumn, IntegerColumn, StringColumn, DatetimeColumn, DateColumn

# set logger
logger = logging.getLogger(__name__)

class TeradataConnector(BaseConnector):
    """
        Class representing a connector to a Teradata database.
    """

    def __init__(
        self,
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        database: str | None = None,
        port: int = 1025,
        logmech: str = "LDAP",
        **kwargs: Any
    ) -> None:
        """
            Initialize the Teradata connector with the given parameters.  
            Teradata provides a whole range of options to configure the connection 
            with the database, see `teradatasql` Documentation (https://github.com/Teradata/python-driver).  
            For this reasons, the most common parameters can be passed directly to the constructor, 
            while all others can be specified directly as keyword arguments.

            Parameters:
                user: username to connect to the database.
                password: password to connect to the database.
                host: hostname or IP address of the Teradata database.
                database: name of the database to connect to.
                port: port of the Teradata database. Default is 1025.
                logmech: logon mechanism to use for the connection. Default is "LDAP".
                **kwargs: additional keyword arguments to pass to the Teradata connection.
        """
        logger.debug("start")

        # commons
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.logmech = logmech

        # additional parameters
        self.kwargs = kwargs

        # connection
        self.connection: Connection

        logger.debug("end")
        return

    def _get_connection_params(self) -> dict[str, Any]:
        """
            Get the connection parameters to connect to the Teradata database.
            Returns a dictionary with the connection parameters.
        """
        logger.debug("start")

        config_dict = {}
        # set common parameters
        for param in ["user", "password", "host", "database", "port", "logmech"]:
            value = getattr(self, param)
            if value is not None:
                _param = "dbs_port" if param == "port" else param
                config_dict[_param] = value

        # add additional parameters
        config_dict.update(self.kwargs)

        logger.debug("end")
        return config_dict

    def _connect(self) -> Connection: # type: ignore [teradatasql supports PEP-249]
        return Connection(**self._get_connection_params())

    def get_column_from_dtype(self, dtype: Any, column_name: str, order: int) -> Column:
        logger.debug("start")

        column = None
        if dtype is int or dtype is bytes:
            column = IntegerColumn(name = column_name, order = order)
        elif dtype is float or dtype is decimal.Decimal:
            column = NumberColumn(name = column_name, order = order)
        elif dtype is str:
            column = StringColumn(name = column_name, order = order)
        elif dtype is datetime.date:
            column = DateColumn(name = column_name, order = order)
        elif dtype is datetime.datetime or dtype is datetime.time:
            column = DatetimeColumn(name = column_name, order = order)
        else:
            raise ColumnDataTypeConversionError(f"Data type {dtype} does not have a corresponding mapping.")

        logger.debug("end")
        return column