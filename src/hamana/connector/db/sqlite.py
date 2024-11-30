import logging
from sqlite3 import Connection
from typing import Any, Generator

from .base import BaseConnector
from .query import Query, QueryColumn

# set logger
logger = logging.getLogger(__name__)

class SQLiteConnector(BaseConnector):
    """
        Class to represent a connector to an SQLite database.
    """

    def __init__(self, path: str, **kwargs: dict[str, Any]) -> None:
        self.path = path
        self.kwargs = kwargs
        self.connection: Connection

    def _connect(self) -> Connection:
        return Connection(self.path)

    def batch_execute(self, query: Query, batch_size: int) -> Generator[list[tuple], None, None]:
        logger.debug("start")

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data (from: {self.path}) ...")
                
                logger.debug("open cursor")
                cursor = conn.connection.cursor()

                # execute query
                params = query.get_params()
                if params is not None:
                    cursor.execute(query.query, params)
                else:
                    cursor.execute(query.query)
                logger.info(f"query: {query.query}")
                logger.info(f"parameters: {query.get_params()}")

                # set columns
                if query.columns is None:
                    logger.info("set query columns")
                    query.columns = [QueryColumn(order = i, name = desc[0]) for i, desc in enumerate(cursor.description)]

                # fetch in batches
                while True:
                    results = cursor.fetchmany(batch_size)
                    if not results:
                        break
                    yield results

                cursor.close()
                logger.debug("cursor closed")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        return