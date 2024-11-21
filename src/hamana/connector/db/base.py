import logging
from types import TracebackType
from typing import Type

from .query import Query
from .interface import DatabaseConnectorABC

# set logger
logger = logging.getLogger(__name__)

class BaseConnector(DatabaseConnectorABC):
    """
        Class to represent a connector to a database.
    """

    def __enter__(self):
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

    def to_sqlite(self, query: Query, table_name: str, batch_size: int = 1000) -> None:
        logger.debug("start")

        insert_query: str | None = None

        # import internal database
        from ...core.db import HamanaDatabase
        logger.debug("imported internal database")

        # get instance
        hamana_db = HamanaDatabase.get_instance()
        hamana_connection = hamana_db.get_connection()
        logger.debug("internal database instance obtained")

        # execute extraction
        logger.info(f"extracting data, batch size: {batch_size}")
        hamana_cursor = hamana_connection.cursor()
        for row_batch in self.batch_execute(query, batch_size):

            if insert_query is None:
                logger.info("generating insert query")
                insert_query = query.get_insert_query(table_name)

                logger.info(f"creating table {table_name}")
                hamana_cursor.execute(query.get_create_query(table_name))
                hamana_connection.commit()
                logger.debug("table created")

            hamana_cursor.executemany(insert_query, row_batch)
            hamana_connection.commit()

        logger.info("data inserted into table")
        hamana_cursor.close()

        logger.debug("end")
        return