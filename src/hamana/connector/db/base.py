import logging
from types import TracebackType
from typing import Type

from .interface import DatabaseConnectorABC, ConnectionProtocol

# set logger
logger = logging.getLogger(__name__)

class BaseConnector(DatabaseConnectorABC):
    """
        Class to represent a connector to a database.
    """

    def __init__(self) -> None:
        self.connection: ConnectionProtocol

    def __enter__(self) -> "BaseConnector":
        logger.debug("start")
        self.connection = self._connect()
        logger.info("connection opened")
        logger.debug("end")
        return self

    def __exit__(self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, exc_traceback: TracebackType | None) -> None:
        logger.debug("start")

        # log exception
        if exc_type is not None:
            logger.error(f"exception occurred: {exc_type}")
            logger.error(f"exception value: {exc_value}")
            logger.error(f"exception traceback: {exc_traceback}")

        self.connection.close()

        logger.info("connection closed")
        logger.debug("end")
        return

    def ping(self) -> None:
        logger.debug("start")
        with self as _:
            logger.debug("end")
            return None