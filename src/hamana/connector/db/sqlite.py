import logging
from sqlite3 import Connection
from typing import Any, Generator, overload

from pandas import DataFrame

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

    def ping(self) -> None:
        logger.debug("start")

        try:
            super().ping()
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
                logger.info(f"extracting data (from: {self.path}) ...")

                logger.debug("open cursor")
                cursor = conn.connection.cursor()
                logger.debug("cursor opened")

                # execute query
                params = query.get_params()
                if params is not None:
                    cursor.execute(query.query, params)
                else:
                    cursor.execute(query.query)
                logger.info(f"query: {query.query}")
                logger.info(f"parameters: {query.get_params()}")

                # set columns
                columns = [QueryColumn(order = i, source = desc[0]) for i, desc in enumerate(cursor.description)]

                # fetch results
                result = cursor.fetchall()
                logger.info(f"data extracted ({len(result)} rows)")

                cursor.close()
                logger.debug("cursor closed")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("convert to Dataframe")
        df_result = DataFrame(result, columns = [column.source for column in columns])

        # adjust columns
        if query.columns:
            self._adjust_query_result_df(df_result, query.columns)
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
                    query.columns = [QueryColumn(order = i, source = desc[0]) for i, desc in enumerate(cursor.description)]

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